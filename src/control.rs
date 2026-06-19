// Agent control channel (agent-as-gateway, Sprint 18 — Rust client).
//
// The agent dials the cloud gateway's `/agent-control` WebSocket and holds a
// long-lived stream open (agent-initiated: the agent is behind the customer's
// NAT, the cloud can't reach in). Over it:
//   agent → cloud:  hello, heartbeat, query_response
//   cloud → agent:  hello_ack, provision, query_request
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
use tracing::{error, info, warn};

use crate::collector::{Collector, Conn, Ingest};
use crate::enroll::Credentials;

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

    let result = read_loop(&mut read, &tx, &store, collector).await;

    heartbeat.abort();
    writer.abort();
    result
}

async fn read_loop<S>(
    read: &mut S,
    tx: &mpsc::Sender<Message>,
    store: &Store,
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
    let outcome = if op == "read" {
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
    };

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
        Err(e) => respond_err(&tx, &request_id, "agent_query_failed", &e.to_string()).await,
    }
}

/// Parse the `connectors` array of a provision frame into (id, Conn) pairs.
fn parse_connectors(v: &Value) -> Vec<(String, Conn)> {
    v.get("connectors")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    let id = c.get("connector_id").and_then(Value::as_str)?.to_string();
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
