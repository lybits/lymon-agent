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

use crate::enroll::Credentials;

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

/// Run the control channel forever: connect, serve, and reconnect with capped
/// exponential backoff. Never returns under normal operation.
pub async fn run(creds: Credentials, capabilities: Vec<String>) {
    let (url, tenant_id) = match (creds.control_endpoint.clone(), creds.tenant_id.clone()) {
        (Some(u), Some(t)) => (u, t),
        _ => {
            warn!("control channel disabled: credentials carry no control endpoint / tenant id");
            return;
        }
    };

    let mut backoff = 1u64;
    loop {
        match serve(&url, &creds.agent_id, &tenant_id, &creds.token, &capabilities).await {
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
) -> Result<()> {
    info!(url, "connecting agent control channel");
    let (ws, _resp) = connect_async(url).await.context("ws connect")?;
    let (mut write, mut read) = ws.split();

    // A single writer task owns the sink; the read loop + heartbeat push frames
    // through this channel so there's exactly one writer (no split-sink races).
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // 1) hello (authenticates + binds the stream to this tenant/agent).
    let hello = serde_json::json!({
        "kind": "hello",
        "agent_id": agent_id,
        "tenant_id": tenant_id,
        "token": token,
        "capabilities": capabilities,
        "agent_version": env!("CARGO_PKG_VERSION"),
    });
    let _ = tx.send(Message::Text(hello.to_string()));

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
            if hb_tx.send(Message::Text(m.to_string())).is_err() {
                break;
            }
        }
    });

    let store: Store = Arc::new(Mutex::new(HashMap::new()));

    let result = read_loop(&mut read, &tx, &store).await;

    heartbeat.abort();
    writer.abort();
    result
}

async fn read_loop<S>(read: &mut S, tx: &mpsc::UnboundedSender<Message>, store: &Store) -> Result<()>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
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
                            let detail = v.get("detail").and_then(Value::as_str).unwrap_or("rejected");
                            anyhow::bail!("hello rejected: {detail}");
                        }
                    }
                    Some("provision") => {
                        let partial = v.get("partial").and_then(Value::as_bool).unwrap_or(false);
                        let list = v
                            .get("datasources")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        let mut s = store.lock().await;
                        if !partial {
                            s.clear();
                        }
                        for d in list {
                            if let Some(id) = d.get("datasource_id").and_then(Value::as_str) {
                                s.insert(
                                    id.to_string(),
                                    ProvisionedDs {
                                        ds_type: d.get("type").and_then(Value::as_str).unwrap_or("").to_string(),
                                        config: d.get("config").cloned().unwrap_or(Value::Null),
                                        secrets: d.get("secrets").cloned().unwrap_or(Value::Null),
                                    },
                                );
                            }
                        }
                        info!(count = s.len(), partial, "provisioned datasources updated");
                    }
                    Some("query_request") => {
                        let tx = tx.clone();
                        let store = store.clone();
                        tokio::spawn(async move { handle_query(v, store, tx).await });
                    }
                    _ => {}
                }
            }
            Message::Ping(p) => {
                let _ = tx.send(Message::Pong(p));
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// Per-op row cap the agent enforces locally (matches the cloud default).
const MAX_ROWS: usize = 100_000;

async fn handle_query(req: Value, store: Store, tx: mpsc::UnboundedSender<Message>) {
    let request_id = req.get("request_id").and_then(Value::as_str).unwrap_or("").to_string();
    let ds_id = req.get("datasource_id").and_then(Value::as_str).unwrap_or("").to_string();
    let op = req.get("op").and_then(Value::as_str).unwrap_or("").to_string();
    let args = req.get("args").cloned().unwrap_or(Value::Null);
    let timeout_ms = req.get("timeout_ms").and_then(Value::as_u64).unwrap_or(30_000);

    // Clone the provisioned config out of the store so we don't hold the lock
    // across the (awaiting) adapter call.
    let ds = { store.lock().await.get(&ds_id).cloned() };
    let Some(ds) = ds else {
        respond_err(&tx, &request_id, "agent_unknown_datasource", &format!("agent has no config for datasource {ds_id}"));
        return;
    };

    // Dispatch to the local adapter for this datasource type. PR2 ships PSS;
    // postgresql/http_rest/… follow.
    let outcome = match ds.ds_type.as_str() {
        "pss" => crate::pss::run(&op, &args, &ds.config, &ds.secrets, timeout_ms, MAX_ROWS).await,
        other => Err(anyhow::anyhow!("agent has no adapter for {other}.{op} yet")),
    };

    match outcome {
        Ok(result) => {
            let resp = serde_json::json!({
                "kind": "query_response",
                "request_id": request_id,
                "ok": true,
                "result": result,
            });
            let _ = tx.send(Message::Text(resp.to_string()));
        }
        Err(e) => respond_err(&tx, &request_id, "agent_query_failed", &e.to_string()),
    }
}

fn respond_err(tx: &mpsc::UnboundedSender<Message>, request_id: &str, code: &str, detail: &str) {
    let resp = serde_json::json!({
        "kind": "query_response",
        "request_id": request_id,
        "ok": false,
        "error": { "code": code, "detail": detail },
    });
    let _ = tx.send(Message::Text(resp.to_string()));
}
