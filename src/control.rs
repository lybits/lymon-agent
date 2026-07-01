// Agent control channel (agent-as-gateway, Sprint 18 — Rust client).
//
// The agent dials the cloud gateway's `/agent-control` WebSocket and holds a
// long-lived stream open (agent-initiated: the agent is behind the customer's
// NAT, the cloud can't reach in). Over it:
//   agent → cloud:  hello, heartbeat, query_response, write_ack
//   cloud → agent:  hello_ack, provision, query_request, write_request
//
// On a query_request the cloud asks the agent to run a datasource op against a
// PRIVATE source on the customer network; the agent executes it locally and
// returns just the result — credentials never leave the box. The set of
// datasources (config + secrets) is pushed by the cloud in `provision` frames
// and held in memory here.
//
// PR1 establishes the channel, auth, heartbeat, reconnect, and provision
// storage. Local adapters that actually run queries land in PR2 — until then a
// query_request is answered with `agent_unsupported`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::collector::{Collector, Conn, Ingest};
use crate::enroll::Credentials;
use crate::modbus::ModbusClient;

/// Shared connector store (connector_id → connector), owned by the collector
/// and consulted here so a query_request can target an agent-host connector.
type ConnStore = Arc<Mutex<HashMap<String, Conn>>>;

/// A datasource the agent fronts: its adapter type + non-secret config +
/// resolved secrets, as pushed by the cloud. Kept in memory only.
#[derive(Clone)]
#[allow(dead_code)] // fields consumed by adapters in PR2
struct ProvisionedDs {
    ds_type: String,
    config: Value,
    secrets: Value,
}

type Store = Arc<Mutex<HashMap<String, ProvisionedDs>>>;

/// ADR 50 P2 — open streaming subscriptions, keyed by the cloud's request_id.
/// Each holds one task per subscribed point (the plugin's `subscribe` op is
/// single-selection); aborting them drops the PluginStreams (kill_on_drop).
type Subs = Arc<Mutex<HashMap<String, Vec<tokio::task::JoinHandle<()>>>>>;

/// Capacity of the queue in front of the single WS writer task. Small on
/// purpose: it only needs to absorb short writer hiccups; sustained slowness
/// must push back on producers, not buffer 100k-row responses in memory.
const WRITER_QUEUE: usize = 32;

/// Run the control channel forever: connect, serve, and reconnect with capped
/// exponential backoff. Never returns under normal operation. The `collector`
/// (Phase 2) owns the provisioned connectors + ingests; provision frames drive
/// its reconfigure, and its connector store backs query_request lookups.
pub async fn run(creds: Credentials, capabilities: Vec<String>, collector: Arc<Collector>) {
    let (url, tenant_id) = match (creds.control_endpoint.clone(), creds.tenant_id.clone()) {
        (Some(u), Some(t)) => (u, t),
        _ => {
            warn!("control channel disabled: credentials carry no control endpoint / tenant id");
            return;
        }
    };

    let mut backoff = 1u64;
    loop {
        match serve(
            &url,
            &creds.agent_id,
            &tenant_id,
            &creds.token,
            &capabilities,
            &collector,
        )
        .await
        {
            Ok(()) => info!("control channel closed by peer; reconnecting"),
            Err(e) => error!(error = %e, "control channel error; reconnecting"),
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff.saturating_mul(2)).min(30);
    }
}

async fn serve(
    url: &str,
    agent_id: &str,
    tenant_id: &str,
    token: &str,
    capabilities: &[String],
    collector: &Arc<Collector>,
) -> Result<()> {
    info!(url, "connecting agent control channel");
    let (ws, _resp) = connect_async(url).await.context("ws connect")?;
    let (mut write, mut read) = ws.split();

    // A single writer task owns the sink; the read loop + heartbeat push frames
    // through this channel so there's exactly one writer (no split-sink races).
    //
    // The channel is BOUNDED: query responses can carry up to MAX_ROWS
    // (100k) rows each, so an unbounded queue in front of a slow/stalled
    // socket would grow without limit. Every producer on this channel —
    // hello, heartbeat, pong, and the spawned query/respond_err handlers — is
    // low-frequency and runs in an async context, so `send().await` gives
    // natural backpressure: a query handler simply parks until the writer
    // drains, holding at most its own one response. A stalled heartbeat is
    // fine — if the socket is that backed up the connection is already dying
    // and the reconnect loop will recycle it.
    //
    // NOTE: the plugin-SDK push/subscribe path (the high-frequency producer)
    // does NOT flow through this control channel — those samples are written
    // to the durable SQLite buffer (collector::run_stream -> enqueue_with_origin)
    // and uploaded by the ingest streamer. So there is no hot-path producer
    // here that `send().await` could stall; every sender on `tx` can safely
    // block on backpressure. (The buffer's own overflow is bounded by its
    // high-water-mark / drop-oldest cap, not by this queue.)
    let (tx, mut rx) = mpsc::channel::<Message>(WRITER_QUEUE);

    // 1) hello (authenticates + binds the stream to this tenant/agent).
    let hello = serde_json::json!({
        "kind": "hello",
        "agent_id": agent_id,
        "tenant_id": tenant_id,
        "token": token,
        "capabilities": capabilities,
        "agent_version": env!("CARGO_PKG_VERSION"),
    });
    let _ = tx.send(Message::Text(hello.to_string())).await;

    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if write.send(m).await.is_err() {
                break;
            }
        }
    });

    // Heartbeat so the cloud's idle-timeout doesn't drop us.
    let hb_tx = tx.clone();
    let hb_id = agent_id.to_string();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let m = serde_json::json!({ "kind": "heartbeat", "agent_id": hb_id });
            if hb_tx.send(Message::Text(m.to_string())).await.is_err() {
                break;
            }
        }
    });

    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    // ADR 50 P2 — live streaming subscriptions opened by the cloud over this stream.
    let subscriptions: Subs = Arc::new(Mutex::new(HashMap::new()));

    let result = read_loop(&mut read, &tx, &store, &subscriptions, collector).await;

    // Tear down every streaming subscription when the control stream drops — the
    // cloud re-subscribes on reconnect. Aborting kills the plugin processes.
    for (_id, handles) in subscriptions.lock().await.drain() {
        for h in handles {
            h.abort();
        }
    }
    heartbeat.abort();
    writer.abort();
    result
}

async fn read_loop<S>(
    read: &mut S,
    tx: &mpsc::Sender<Message>,
    store: &Store,
    subscriptions: &Subs,
    collector: &Arc<Collector>,
) -> Result<()>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let connectors = collector.connectors();
    let plugins = collector.plugins();
    while let Some(frame) = read.next().await {
        let msg = frame.context("ws read")?;
        match msg {
            Message::Text(text) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                match v.get("kind").and_then(Value::as_str) {
                    Some("hello_ack") => {
                        if v.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                            info!("control channel authenticated");
                        } else {
                            let detail = v
                                .get("detail")
                                .and_then(Value::as_str)
                                .unwrap_or("rejected");
                            anyhow::bail!("hello rejected: {detail}");
                        }
                    }
                    Some("provision") => {
                        let partial = v.get("partial").and_then(Value::as_bool).unwrap_or(false);
                        // ADR 49 W2.3 — track the control-signing public key. Every
                        // provision carries it when signing is enabled; absence means
                        // disabled, so we mirror the frame's state exactly.
                        {
                            use base64::Engine as _;
                            let pk = v
                                .get("control_public_key")
                                .and_then(Value::as_str)
                                .and_then(|s| {
                                    base64::engine::general_purpose::STANDARD.decode(s).ok()
                                });
                            *collector.control_pubkey().lock().await = pk;
                        }
                        // A frame touches a dimension only if its key is present
                        // (collector pushes omit `datasources`, and vice-versa).
                        if let Some(list) = v.get("datasources").and_then(Value::as_array) {
                            let mut s = store.lock().await;
                            if !partial {
                                s.clear();
                            }
                            for d in list {
                                if let Some(id) = d.get("datasource_id").and_then(Value::as_str) {
                                    s.insert(
                                        id.to_string(),
                                        ProvisionedDs {
                                            ds_type: d
                                                .get("type")
                                                .and_then(Value::as_str)
                                                .unwrap_or("")
                                                .to_string(),
                                            config: d.get("config").cloned().unwrap_or(Value::Null),
                                            secrets: d
                                                .get("secrets")
                                                .cloned()
                                                .unwrap_or(Value::Null),
                                        },
                                    );
                                }
                            }
                            info!(count = s.len(), partial, "provisioned datasources updated");
                        }
                        // Phase-2 collector set: reconfigure when either key is
                        // present (a connector-only / ingest-only push counts).
                        if v.get("connectors").is_some() || v.get("ingests").is_some() {
                            collector
                                .reconfigure(parse_connectors(&v), parse_ingests(&v))
                                .await;
                        }
                    }
                    Some("query_request") => {
                        let tx = tx.clone();
                        let store = store.clone();
                        let connectors = connectors.clone();
                        let plugins = plugins.clone();
                        tokio::spawn(async move {
                            handle_query(v, store, connectors, plugins, tx).await
                        });
                    }
                    Some("write_request") => {
                        // ADR 49 W2.1 — a supervisory device write. Run it off the
                        // read loop and reply with a write_ack (device-confirmed).
                        let tx = tx.clone();
                        let connectors = connectors.clone();
                        let plugins = plugins.clone();
                        let pubkey = collector.control_pubkey();
                        tokio::spawn(async move {
                            handle_write(v, connectors, plugins, pubkey, tx).await
                        });
                    }
                    Some("subscribe_request") => {
                        // ADR 50 P2 — open a streaming subscription on a connector's
                        // points (push instead of poll). One plugin stream per point.
                        let tx = tx.clone();
                        let connectors = connectors.clone();
                        let plugins = plugins.clone();
                        let subscriptions = subscriptions.clone();
                        tokio::spawn(async move {
                            handle_subscribe(v, connectors, plugins, tx, subscriptions).await
                        });
                    }
                    Some("unsubscribe_request") => {
                        // ADR 50 P2 — stop a subscription: abort its tasks (drops the
                        // plugin streams, killing the processes).
                        if let Some(request_id) = v.get("request_id").and_then(Value::as_str) {
                            if let Some(handles) = subscriptions.lock().await.remove(request_id) {
                                for h in handles {
                                    h.abort();
                                }
                            }
                        }
                    }
                    Some("update") => {
                        // Cloud-triggered self-update (phase 2). We DON'T touch
                        // our own binary (we can't — DynamicUser, and we're
                        // running): we drop a trigger file and let the
                        // privileged OS updater (systemd path-unit on Linux, a
                        // watcher task on Windows) download the bundle, swap the
                        // binary + plugins, and restart us.
                        let version = v.get("version").and_then(Value::as_str).unwrap_or("");
                        let repo = v.get("repo").and_then(Value::as_str);
                        match request_update(version, repo) {
                            Ok(path) => info!(
                                version,
                                repo = repo.unwrap_or("(default)"),
                                path = %path.display(),
                                "self-update requested by cloud; wrote update trigger"
                            ),
                            Err(e) => warn!(error = %e, "ignoring invalid self-update request"),
                        }
                    }
                    Some("get_logs") => {
                        // Cloud asked for the tail of our log ring buffer.
                        let request_id = v
                            .get("request_id")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let n = v.get("lines").and_then(Value::as_u64).unwrap_or(500) as usize;
                        let lines = crate::logbuf::tail(n);
                        let resp = serde_json::json!({
                            "kind": "logs_response",
                            "request_id": request_id,
                            "lines": lines,
                        });
                        let _ = tx.send(Message::Text(resp.to_string())).await;
                    }
                    _ => {}
                }
            }
            Message::Ping(p) => {
                // Bounded send: if the writer queue is full the read loop
                // pauses here — backpressure on a stalled socket, by design.
                let _ = tx.send(Message::Pong(p)).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// Validate + persist a self-update request as a trigger file the privileged
/// updater consumes. We deliberately do NOT act on it here: the updater (root)
/// owns the bundle swap + restart. The file lives next to the buffer (same dir
/// as credentials.json), resolved from LYMON_BUFFER_PATH.
///
/// `version`/`repo` come off the wire, so they're strictly validated — they end
/// up in a download URL the updater builds. version: optional leading `v` then
/// [0-9A-Za-z._-]. repo: `owner/name` with the same safe alphabet.
fn request_update(version: &str, repo: Option<&str>) -> Result<std::path::PathBuf> {
    let safe_tag = |s: &str| {
        let body = s.strip_prefix('v').unwrap_or(s);
        !body.is_empty()
            && body.len() <= 64
            && body
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    if !safe_tag(version) {
        anyhow::bail!("invalid version {version:?}");
    }
    if let Some(r) = repo {
        let ok = r.len() <= 140
            && r.matches('/').count() == 1
            && r.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'));
        if !ok {
            anyhow::bail!("invalid repo {r:?}");
        }
    }

    let buffer_path = std::env::var("LYMON_BUFFER_PATH")
        .unwrap_or_else(|_| "/var/lib/lymon-agent/buffer.db".to_string());
    let dir = std::path::Path::new(&buffer_path)
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let path = dir.join("update-request.json");
    let payload = match repo {
        Some(r) => serde_json::json!({ "version": version, "repo": r }),
        None => serde_json::json!({ "version": version }),
    };
    std::fs::write(&path, serde_json::to_vec(&payload)?).context("writing update-request.json")?;
    Ok(path)
}

/// Per-op row cap the agent enforces locally (matches the cloud default).
const MAX_ROWS: usize = 100_000;

async fn handle_query(
    req: Value,
    store: Store,
    connectors: ConnStore,
    plugins: Arc<crate::plugins::PluginHost>,
    tx: mpsc::Sender<Message>,
) {
    let request_id = req
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let ds_id = req
        .get("datasource_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let op = req
        .get("op")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let args = req.get("args").cloned().unwrap_or(Value::Null);
    let timeout_ms = req
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(30_000);
    // ADR 42 P1 — the originating HTTP request's correlation id (distinct from
    // request_id, which matches THIS query to its response). Logged as a field
    // so the agent's lines for this work join the cloud request that triggered
    // it. Empty when the cloud routed outside a request (e.g. a hub tick).
    let correlation_id = req
        .get("correlation_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    info!(
        request_id = %request_id,
        correlation_id = %correlation_id,
        ds_id = %ds_id,
        op = %op,
        "handling agent query"
    );
    // ADR 42 P2 — adopt the gateway's W3C traceparent (sent over the control
    // channel, which isn't HTTP) so the agent's OTLP spans for this work become
    // children of the originating request's trace — one end-to-end timeline.
    let span = tracing::info_span!("agent.query", correlation_id = %correlation_id, ds_id = %ds_id, op = %op);
    if let Some(tp) = req
        .get("traceparent")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        let mut carrier = std::collections::HashMap::new();
        carrier.insert("traceparent".to_string(), tp.to_string());
        span.set_parent(opentelemetry::global::get_text_map_propagator(|p| {
            p.extract(&carrier)
        }));
    }

    // Resolve the origin: a legacy provisioned datasource OR a Phase-2 agent-host
    // connector (same id space from the cloud's view). Clone the (type, config,
    // secrets) out so we don't hold a lock across the awaiting adapter call.
    let resolved: Option<(String, Value, Value)> = {
        if let Some(d) = store.lock().await.get(&ds_id) {
            Some((d.ds_type.clone(), d.config.clone(), d.secrets.clone()))
        } else {
            connectors
                .lock()
                .await
                .get(&ds_id)
                .map(|c| (c.ds_type.clone(), c.config.clone(), c.secrets.clone()))
        }
    };
    let Some((ds_type, config, secrets)) = resolved else {
        respond_err(
            &tx,
            &request_id,
            "agent_unknown_datasource",
            &format!("agent has no config for {ds_id}"),
        )
        .await;
        return;
    };

    // Dispatch to the local adapter for this origin's type. Built-ins first;
    // any other type falls through to a connector plugin (execd) if one serves
    // it — so Browse/Test work for plugin connectors (e.g. opcua), no recompile.
    let outcome = async {
        if op == "read" {
            // ADR 41 F3 — the live route: a batched multi-point read. `args.points`
            // is an array of {selection, naming}; the plugin reads them all in one
            // round trip and we return {samples} for the cloud to map back.
            let points = args.get("points").cloned().unwrap_or(Value::Null);
            match plugins.for_type(&ds_type) {
                Some(plugin) => plugin
                    .read_points(&ds_type, &config, &secrets, &points)
                    .await
                    .map(|samples| serde_json::json!({ "samples": samples })),
                None => Err(anyhow::anyhow!("agent has no plugin for {ds_type} read")),
            }
        } else {
            match ds_type.as_str() {
                "pss" => crate::pss::run(&op, &args, &config, &secrets, timeout_ms, MAX_ROWS).await,
                other => match plugins.for_type(other) {
                    Some(plugin) => plugin.query(&op, other, &config, &secrets, &args).await,
                    None => Err(anyhow::anyhow!("agent has no adapter for {other}.{op} yet")),
                },
            }
        }
    }
    .instrument(span)
    .await;

    match outcome {
        Ok(result) => {
            let resp = serde_json::json!({
                "kind": "query_response",
                "request_id": request_id,
                "ok": true,
                "result": result,
            });
            let _ = tx.send(Message::Text(resp.to_string())).await;
        }
        Err(e) => {
            // Log locally too — the error was only relayed to the cloud, so the
            // agent's own logs showed the plugin spawn with no visible cause.
            warn!(ds_id = %ds_id, op = %op, error = %e, "agent query failed");
            respond_err(&tx, &request_id, "agent_query_failed", &e.to_string()).await;
        }
    }
}

/// ADR 50 P2 — open a streaming subscription for a connector's points. Acks
/// support, then spawns one task per point that opens a plugin `subscribe`
/// stream and forwards every pushed batch to the cloud as `point_data`. The
/// plugin echoes each point's `variable_id`, so the cloud maps samples back.
async fn handle_subscribe(
    req: Value,
    connectors: ConnStore,
    plugins: Arc<crate::plugins::PluginHost>,
    tx: mpsc::Sender<Message>,
    subscriptions: Subs,
) {
    let request_id = req
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let connector_id = req
        .get("connector_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let ds_type = req
        .get("ds_type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let points = req
        .get("points")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    // Need the connector config + a plugin that can stream this type. Otherwise
    // refuse so the cloud falls back to polling.
    let conn = connectors.lock().await.get(&connector_id).cloned();
    let supported = conn.is_some() && plugins.for_type(&ds_type).is_some() && !points.is_empty();
    let _ = tx.send(subscribe_ack(&request_id, supported)).await;
    let Some(conn) = conn.filter(|_| supported) else {
        return;
    };

    let mut handles = Vec::with_capacity(points.len());
    for (i, pt) in points.iter().enumerate() {
        let selection = pt.get("selection").cloned().unwrap_or(Value::Null);
        let variable_id = pt
            .get("naming")
            .and_then(|n| n.get("variable_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| i.to_string());
        let ing = Ingest {
            ingest_id: format!("sub:{request_id}:{i}"),
            connector_id: connector_id.clone(),
            selection,
            interval_s: 0,
            naming: serde_json::json!({ "variable_id": variable_id }),
            transform: Value::Null,
        };
        let tx = tx.clone();
        let plugins = plugins.clone();
        let conn = conn.clone();
        let rid = request_id.clone();
        handles.push(tokio::spawn(async move {
            stream_point(plugins, conn, ing, rid, tx).await
        }));
    }
    subscriptions.lock().await.insert(request_id, handles);
}

/// One subscribed point: (re)open the plugin stream and forward each pushed
/// batch as a `point_data` frame, reopening with a short backoff if it closes.
/// Exits when the control channel is gone (tx send fails) — which, with
/// kill_on_drop, kills the plugin process.
async fn stream_point(
    plugins: Arc<crate::plugins::PluginHost>,
    conn: Conn,
    ing: Ingest,
    request_id: String,
    tx: mpsc::Sender<Message>,
) {
    let Some(plugin) = plugins.for_type(&conn.ds_type) else {
        return;
    };
    loop {
        match plugin.open_stream(&conn, &ing).await {
            Ok(mut stream) => loop {
                match stream.next().await {
                    Ok(Some(samples)) => {
                        let arr: Vec<Value> = samples
                            .iter()
                            .map(|s| {
                                serde_json::json!({
                                    "variable_id": s.point_id,
                                    "value": s.value,
                                    "ts_ms": s.ts_ms,
                                    "quality": s.quality,
                                })
                            })
                            .collect();
                        let frame = serde_json::json!({
                            "kind": "point_data",
                            "request_id": request_id,
                            "samples": arr,
                        });
                        if tx.send(Message::Text(frame.to_string())).await.is_err() {
                            return; // control channel gone → stop (drops the stream)
                        }
                    }
                    Ok(None) => break, // stream closed → reopen
                    Err(e) => {
                        warn!(ingest = %ing.ingest_id, error = %e, "subscription stream error; reopening");
                        break;
                    }
                }
            },
            Err(e) => {
                warn!(ingest = %ing.ingest_id, error = %e, "opening subscription stream failed; retrying")
            }
        }
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// ADR 50 P2 — a subscribe_ack frame (supported true/false).
fn subscribe_ack(request_id: &str, supported: bool) -> Message {
    Message::Text(
        serde_json::json!({ "kind": "subscribe_ack", "request_id": request_id, "supported": supported })
            .to_string(),
    )
}

/// Parse the `connectors` array of a provision frame into (id, Conn) pairs.
fn parse_connectors(v: &Value) -> Vec<(String, Conn)> {
    v.get("connectors")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    let id = c.get("connector_id").and_then(Value::as_str)?.to_string();
                    // ADR 49 W2.1 — build the write allow-list keys ("{fn}:{address}").
                    let writable = c
                        .get("writable_targets")
                        .and_then(Value::as_array)
                        .map(|arr| arr.iter().map(target_key).collect())
                        .unwrap_or_default();
                    Some((
                        id,
                        Conn {
                            ds_type: c
                                .get("type")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            config: c.get("config").cloned().unwrap_or(Value::Null),
                            secrets: c.get("secrets").cloned().unwrap_or(Value::Null),
                            writable,
                        },
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parse the `ingests` array of a provision frame into Ingest jobs.
fn parse_ingests(v: &Value) -> Vec<Ingest> {
    v.get("ingests")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|i| {
                    let ingest_id = i.get("ingest_id").and_then(Value::as_str)?.to_string();
                    let connector_id = i.get("connector_id").and_then(Value::as_str)?.to_string();
                    Some(Ingest {
                        ingest_id,
                        connector_id,
                        selection: i.get("selection").cloned().unwrap_or(Value::Null),
                        interval_s: i.get("interval_s").and_then(Value::as_u64).unwrap_or(60),
                        naming: i.get("naming").cloned().unwrap_or(Value::Null),
                        transform: i.get("transform").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn respond_err(tx: &mpsc::Sender<Message>, request_id: &str, code: &str, detail: &str) {
    let resp = serde_json::json!({
        "kind": "query_response",
        "request_id": request_id,
        "ok": false,
        "error": { "code": code, "detail": detail },
    });
    let _ = tx.send(Message::Text(resp.to_string())).await;
}

// ---------------------------------------------------------------------------
// ADR 49 W2.1 — control write-back (Modbus). On a write_request the cloud has
// already validated/authorized the value (W1 layers); the agent applies the
// inverse scaling, performs the native write, optionally reads it back, and
// replies with a device-confirmed write_ack. Supervisory only — never a loop.
// ---------------------------------------------------------------------------

async fn handle_write(
    req: Value,
    connectors: ConnStore,
    plugins: Arc<crate::plugins::PluginHost>,
    control_pubkey: Arc<Mutex<Option<Vec<u8>>>>,
    tx: mpsc::Sender<Message>,
) {
    let command_id = req
        .get("command_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let connector_id = req
        .get("connector_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let value = req.get("value").and_then(Value::as_f64).unwrap_or(f64::NAN);
    let target = req.get("target").cloned().unwrap_or(Value::Null);
    let want_readback = req
        .get("readback")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let timeout_ms = req
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(5000);

    let span =
        tracing::info_span!("agent.write", connector = %connector_id, command_id = %command_id);
    let outcome = async {
        verify_signature(
            &req,
            &control_pubkey,
            &command_id,
            &connector_id,
            value,
            &target,
        )
        .await?;
        do_write(
            &connectors,
            &plugins,
            &connector_id,
            value,
            &target,
            want_readback,
            timeout_ms,
        )
        .await
    }
    .instrument(span)
    .await;

    let resp = match outcome {
        Ok(rb) => {
            let mut j = serde_json::json!({
                "kind": "write_ack", "command_id": command_id, "status": "acked",
            });
            if let Some(v) = rb {
                j["readback"] = serde_json::json!(v);
            }
            info!(command_id = %command_id, "device write acked");
            j
        }
        Err(e) => {
            warn!(command_id = %command_id, error = %e, "device write failed");
            serde_json::json!({
                "kind": "write_ack", "command_id": command_id, "status": "failed", "detail": e.to_string(),
            })
        }
    };
    let _ = tx.send(Message::Text(resp.to_string())).await;
}

/// The allow-list key for a write target, generalized across protocols: Modbus
/// "{fn}:{address}", OPC-UA "node:{node_id}", else the canonical JSON. Both the
/// provisioned list (parse_connectors) and the check use this, so they agree.
fn target_key(target: &Value) -> String {
    if let (Some(f), Some(a)) = (
        target.get("fn").and_then(Value::as_str),
        target.get("address").and_then(Value::as_u64),
    ) {
        format!("{f}:{a}")
    } else if let Some(n) = target.get("node_id").and_then(Value::as_str) {
        format!("node:{n}")
    } else {
        target.to_string()
    }
}

async fn do_write(
    connectors: &ConnStore,
    plugins: &Arc<crate::plugins::PluginHost>,
    connector_id: &str,
    value: f64,
    target: &Value,
    want_readback: bool,
    timeout_ms: u64,
) -> Result<Option<f64>> {
    // Resolve the connector (clone the fields; never hold the lock across I/O).
    let resolved = {
        connectors.lock().await.get(connector_id).map(|c| {
            (
                c.ds_type.clone(),
                c.config.clone(),
                c.secrets.clone(),
                c.writable.clone(),
            )
        })
    };
    let Some((ds_type, config, secrets, writable)) = resolved else {
        anyhow::bail!("agent has no connector {connector_id}");
    };
    if !value.is_finite() {
        anyhow::bail!("non-finite value");
    }

    // ADR 49 W2.1 — defence in depth: only targets the cloud provisioned as
    // writable. A compromised gateway can't write arbitrary registers/nodes.
    let key = target_key(target);
    if !writable.contains(&key) {
        anyhow::bail!("target {key} not provisioned writable on {connector_id}");
    }

    if ds_type == "modbus" {
        write_modbus(&config, target, value, want_readback, timeout_ms).await
    } else if let Some(plugin) = plugins.for_type(&ds_type) {
        // ADR 49 W2.2 — supervisory write via a connector plugin (OPC-UA, …).
        plugin
            .write(&ds_type, &config, &secrets, target, value, want_readback)
            .await
    } else {
        anyhow::bail!("connector {connector_id} type {ds_type} has no write handler");
    }
}

/// ADR 49 W2.1 — the Modbus device write (extracted so do_write can branch by
/// protocol). Applies the inverse scaling, encodes per datatype, writes, and
/// optionally reads back.
async fn write_modbus(
    config: &Value,
    target: &Value,
    value: f64,
    want_readback: bool,
    timeout_ms: u64,
) -> Result<Option<f64>> {
    let host = config
        .get("host")
        .and_then(Value::as_str)
        .context("modbus connector config.host missing")?;
    let port = config.get("port").and_then(Value::as_u64).unwrap_or(502) as u16;

    let fnclass = target
        .get("fn")
        .and_then(Value::as_str)
        .unwrap_or("holding");
    let address = target
        .get("address")
        .and_then(Value::as_u64)
        .context("target.address missing")? as u16;
    let datatype = target
        .get("datatype")
        .and_then(Value::as_str)
        .unwrap_or("uint16");
    let word_order = target
        .get("word_order")
        .and_then(Value::as_str)
        .unwrap_or("big");
    let scale = target.get("scale").and_then(Value::as_f64).unwrap_or(1.0);
    let offset = target.get("offset").and_then(Value::as_f64).unwrap_or(0.0);
    // Engineering → raw (the inverse of the read path's raw*scale + offset).
    let raw = (value - offset) / if scale == 0.0 { 1.0 } else { scale };

    let mut client = ModbusClient::new(
        host.to_string(),
        port,
        Duration::from_millis(timeout_ms.max(1000)),
    );

    if fnclass == "coil" {
        client.write_coil(address, value != 0.0).await?;
        Ok(None) // coil read-back omitted in v1
    } else {
        let words = encode_words(datatype, raw, word_order)?;
        client.write_holding(address, &words).await?;
        if want_readback {
            let regs = client.read(address, words.len() as u16, false).await?;
            Ok(Some(decode_words(
                datatype, &regs, word_order, scale, offset,
            )))
        } else {
            Ok(None)
        }
    }
}

/// ADR 49 W2.3 — verify the command signature when signing is enabled. No-op
/// when no public key is provisioned. Verifies the gateway's exact signed bytes
/// (`sig_payload`) and then checks they describe THIS command, so neither a
/// forged signature nor a tampered request value is accepted.
async fn verify_signature(
    req: &Value,
    control_pubkey: &Arc<Mutex<Option<Vec<u8>>>>,
    command_id: &str,
    connector_id: &str,
    value: f64,
    target: &Value,
) -> Result<()> {
    let pubkey = control_pubkey.lock().await.clone();
    let Some(pubkey) = pubkey else {
        return Ok(()); // signing not required
    };

    use base64::Engine as _;
    let sig_b64 = req
        .get("signature")
        .and_then(Value::as_str)
        .context("command signing required but signature missing")?;
    let payload = req
        .get("sig_payload")
        .and_then(Value::as_str)
        .context("command signing required but sig_payload missing")?;
    let sig = base64::engine::general_purpose::STANDARD
        .decode(sig_b64)
        .context("signature not base64")?;

    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &pubkey)
        .verify(payload.as_bytes(), &sig)
        .map_err(|_| anyhow::anyhow!("invalid command signature"))?;

    // The signed bytes must describe the command we're about to run. The target
    // key is protocol-generalized (Modbus "{fn}:{addr}" or OPC-UA "node:{id}").
    let parts: Vec<&str> = payload.split('|').collect();
    let signed_value: f64 = parts
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(f64::NAN);
    if parts.len() != 4
        || parts[0] != command_id
        || parts[1] != connector_id
        || parts[2] != target_key(target)
        || (signed_value - value).abs() > 1e-9
    {
        anyhow::bail!("signed payload does not match the command");
    }
    Ok(())
}

fn split_u32(v: u32, word_order: &str) -> Vec<u16> {
    let hi = (v >> 16) as u16;
    let lo = (v & 0xffff) as u16;
    if word_order == "little" {
        vec![lo, hi]
    } else {
        vec![hi, lo]
    }
}

fn join_u32(regs: &[u16], word_order: &str) -> u32 {
    let a = regs.first().copied().unwrap_or(0);
    let b = regs.get(1).copied().unwrap_or(0);
    let (hi, lo) = if word_order == "little" {
        (b, a)
    } else {
        (a, b)
    };
    (u32::from(hi) << 16) | u32::from(lo)
}

/// Encode an engineering raw value into the register word(s) for a datatype.
fn encode_words(datatype: &str, raw: f64, word_order: &str) -> Result<Vec<u16>> {
    let words = match datatype {
        "bool" => vec![if raw != 0.0 { 1 } else { 0 }],
        "uint16" => vec![(raw.round() as i64).clamp(0, u16::MAX as i64) as u16],
        "int16" => {
            vec![((raw.round() as i64).clamp(i16::MIN as i64, i16::MAX as i64) as i16) as u16]
        }
        "uint32" => split_u32(
            (raw.round() as i64).clamp(0, u32::MAX as i64) as u32,
            word_order,
        ),
        "int32" => split_u32(
            ((raw.round() as i64).clamp(i32::MIN as i64, i32::MAX as i64) as i32) as u32,
            word_order,
        ),
        "float32" => split_u32((raw as f32).to_bits(), word_order),
        other => anyhow::bail!("unsupported datatype {other}"),
    };
    Ok(words)
}

/// Decode register word(s) back to an engineering value (raw*scale + offset).
fn decode_words(datatype: &str, regs: &[u16], word_order: &str, scale: f64, offset: f64) -> f64 {
    let raw = match datatype {
        "bool" => {
            if regs.first().copied().unwrap_or(0) != 0 {
                1.0
            } else {
                0.0
            }
        }
        "uint16" => f64::from(regs.first().copied().unwrap_or(0)),
        "int16" => f64::from(regs.first().copied().unwrap_or(0) as i16),
        "uint32" => f64::from(join_u32(regs, word_order)),
        "int32" => f64::from(join_u32(regs, word_order) as i32),
        "float32" => f64::from(f32::from_bits(join_u32(regs, word_order))),
        _ => f64::from(regs.first().copied().unwrap_or(0)),
    };
    raw * scale + offset
}

#[cfg(test)]
mod write_tests {
    // ADR 49 W2.1 — encode/decode round-trips for the supervisory write path.
    use super::{decode_words, encode_words, join_u32, split_u32};

    fn roundtrip(datatype: &str, value: f64, scale: f64, offset: f64, word_order: &str) -> f64 {
        let raw = (value - offset) / if scale == 0.0 { 1.0 } else { scale };
        let words = encode_words(datatype, raw, word_order).expect("encode");
        decode_words(datatype, &words, word_order, scale, offset)
    }

    #[test]
    fn uint16_identity() {
        assert_eq!(roundtrip("uint16", 100.0, 1.0, 0.0, "big"), 100.0);
    }

    #[test]
    fn uint16_with_scale_offset() {
        // engineering 50.0 with scale 0.1 → raw 500 → back to 50.0
        let v = roundtrip("uint16", 50.0, 0.1, 0.0, "big");
        assert!((v - 50.0).abs() < 1e-6, "got {v}");
    }

    #[test]
    fn int16_negative() {
        assert_eq!(roundtrip("int16", -5.0, 1.0, 0.0, "big"), -5.0);
    }

    #[test]
    fn float32_roundtrip_both_word_orders() {
        for wo in ["big", "little"] {
            let v = roundtrip("float32", 3.25, 1.0, 0.0, wo);
            assert!((v - 3.25).abs() < 1e-4, "wo={wo} got {v}");
        }
    }

    #[test]
    fn uint32_word_order() {
        // 70000 needs two registers; split/join must invert under both orders.
        for wo in ["big", "little"] {
            let words = split_u32(70000, wo);
            assert_eq!(join_u32(&words, wo), 70000);
        }
        // big-endian word order puts the high word first.
        assert_eq!(split_u32(0x0001_0002, "big"), vec![0x0001, 0x0002]);
        assert_eq!(split_u32(0x0001_0002, "little"), vec![0x0002, 0x0001]);
    }

    #[test]
    fn bool_coil() {
        assert_eq!(roundtrip("bool", 1.0, 1.0, 0.0, "big"), 1.0);
        assert_eq!(roundtrip("bool", 0.0, 1.0, 0.0, "big"), 0.0);
    }

    #[test]
    fn unsupported_datatype_errors() {
        assert!(encode_words("float64", 1.0, "big").is_err());
    }

    #[test]
    fn target_key_per_protocol() {
        use super::target_key;
        assert_eq!(
            target_key(&serde_json::json!({ "fn": "holding", "address": 40001 })),
            "holding:40001"
        );
        assert_eq!(
            target_key(&serde_json::json!({ "fn": "coil", "address": 5 })),
            "coil:5"
        );
        assert_eq!(
            target_key(&serde_json::json!({ "node_id": "ns=2;s=Sp" })),
            "node:ns=2;s=Sp"
        );
    }

    // ADR 49 W2.3 — signature verification round-trip + tamper rejection.
    #[tokio::test]
    async fn verifies_and_rejects_signatures() {
        use base64::Engine as _;
        use ring::rand::SystemRandom;
        use ring::signature::{Ed25519KeyPair, KeyPair};
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let pubkey = kp.public_key().as_ref().to_vec();

        let payload = "cmd_1|conn_1|holding:40001|42.5";
        let sig =
            base64::engine::general_purpose::STANDARD.encode(kp.sign(payload.as_bytes()).as_ref());
        let target = serde_json::json!({ "fn": "holding", "address": 40001, "datatype": "uint16" });
        let req = serde_json::json!({ "signature": sig, "sig_payload": payload });
        let store = Arc::new(Mutex::new(Some(pubkey)));

        // Valid signature for the matching command → ok.
        assert!(
            super::verify_signature(&req, &store, "cmd_1", "conn_1", 42.5, &target)
                .await
                .is_ok()
        );
        // A tampered value (request says 99, signature covers 42.5) → rejected.
        assert!(
            super::verify_signature(&req, &store, "cmd_1", "conn_1", 99.0, &target)
                .await
                .is_err()
        );
        // Missing signature while a key is set → rejected.
        let unsigned = serde_json::json!({});
        assert!(
            super::verify_signature(&unsigned, &store, "cmd_1", "conn_1", 42.5, &target)
                .await
                .is_err()
        );
        // No key provisioned → signing not required, passes.
        let no_key = Arc::new(Mutex::new(None));
        assert!(
            super::verify_signature(&unsigned, &no_key, "cmd_1", "conn_1", 42.5, &target)
                .await
                .is_ok()
        );
    }

    // ADR 49 W2.1 — the allow-list gate rejects a write to a target the cloud
    // never provisioned, BEFORE any device connection (defence in depth).
    #[tokio::test]
    async fn rejects_unprovisioned_target() {
        use crate::collector::Conn;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let mut map: HashMap<String, Conn> = HashMap::new();
        map.insert(
            "conn_1".to_string(),
            Conn {
                ds_type: "modbus".into(),
                config: serde_json::json!({ "host": "127.0.0.1", "port": 15999 }),
                writable: std::collections::HashSet::new(), // empty → reject every write
                ..Default::default()
            },
        );
        let store = Arc::new(Mutex::new(map));
        let plugins = crate::plugins::PluginHost::discover("/nonexistent-plugins", &[]);
        let target = serde_json::json!({ "fn": "holding", "address": 40001, "datatype": "uint16" });
        let r = super::do_write(&store, &plugins, "conn_1", 42.0, &target, false, 500).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("not provisioned"));
    }
}
