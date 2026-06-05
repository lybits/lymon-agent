// Phase 2 collector (doc 31): the agent's native data-logger.
//
// The cloud provisions agent-host *connectors* (origin: type + config +
// secrets) and *ingests* (collect jobs: selection + interval + naming) over the
// control channel. This module owns one poll task per ingest: every interval it
// reads the connector's source locally and pushes the sample(s) into the
// durable buffer, tagged with the connector id as origin — so the streamer
// attributes them to that connector in the warehouse.
//
// reconfigure() is full-replace: on every provision it stops all tasks and
// respawns the desired set, so add / edit / delete / pause all converge (hot
// reconfig, no restart). The connector store is shared with the control channel
// so an ad-hoc query_request can resolve an agent-host connector too.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::buffer::BufferDb;
use crate::generated::lymon::ingest::v1::Sample;
use crate::modbus::ModbusClient;

/// Per-op row cap (matches the cloud default).
const MAX_ROWS: usize = 100_000;

/// An agent-host connector: adapter type + non-secret config + resolved secrets.
#[derive(Clone)]
pub struct Conn {
    pub ds_type: String,
    pub config: Value,
    pub secrets: Value,
}

/// A provisioned ingest (collect job) over a connector.
#[derive(Clone)]
pub struct Ingest {
    pub ingest_id: String,
    pub connector_id: String,
    pub selection: Value,
    pub interval_s: u64,
    pub naming: Value,
    /// Acquisition transform (scale/offset) applied before enqueue.
    pub transform: Value,
}

pub struct Collector {
    buffer: Arc<BufferDb>,
    /// connector_id → connector. Shared with the control channel (ad-hoc query).
    connectors: Arc<Mutex<HashMap<String, Conn>>>,
    /// ingest_id → running poll task.
    tasks: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl Collector {
    pub fn new(buffer: Arc<BufferDb>) -> Arc<Self> {
        Arc::new(Self {
            buffer,
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Mutex::new(HashMap::new()),
        })
    }

    /// The connector store, shared with the control channel so a query_request
    /// targeting an agent-host connector resolves to its config + secrets.
    pub fn connectors(&self) -> Arc<Mutex<HashMap<String, Conn>>> {
        self.connectors.clone()
    }

    /// Replace the provisioned connectors + ingests and (re)start poll tasks.
    /// Full-replace: every running task is aborted and the desired set
    /// respawned, so removals/edits/pauses take effect without a restart.
    pub async fn reconfigure(&self, connectors: Vec<(String, Conn)>, ingests: Vec<Ingest>) {
        {
            let mut map = self.connectors.lock().await;
            map.clear();
            for (id, c) in &connectors {
                map.insert(id.clone(), c.clone());
            }
        }
        let mut tasks = self.tasks.lock().await;
        for (_, h) in tasks.drain() {
            h.abort();
        }
        let conns = self.connectors.lock().await;
        for ing in ingests {
            let Some(conn) = conns.get(&ing.connector_id).cloned() else {
                warn!(ingest = %ing.ingest_id, connector = %ing.connector_id,
                    "ingest references unknown connector; skipped");
                continue;
            };
            let buffer = self.buffer.clone();
            let id = ing.ingest_id.clone();
            let handle = tokio::spawn(async move { run_ingest(buffer, conn, ing).await });
            tasks.insert(id, handle);
        }
        info!(
            connectors = conns.len(),
            ingests = tasks.len(),
            "collector reconfigured"
        );
    }
}

/// One ingest's poll loop: read → enqueue (origin = connector_id) → sleep.
async fn run_ingest(buffer: Arc<BufferDb>, conn: Conn, ing: Ingest) {
    let interval = Duration::from_secs(ing.interval_s.max(1));
    // A persistent Modbus connection per task (None for other adapters).
    let mut modbus: Option<ModbusClient> = None;
    info!(ingest = %ing.ingest_id, connector = %ing.connector_id, ty = %conn.ds_type,
        interval_s = ing.interval_s, "collector ingest started");
    loop {
        match collect_once(&conn, &ing, &mut modbus).await {
            Ok(samples) if !samples.is_empty() => {
                if let Err(e) = buffer
                    .enqueue_with_origin(Some(ing.connector_id.clone()), samples)
                    .await
                {
                    error!(ingest = %ing.ingest_id, error = %e, "collector enqueue failed");
                }
            }
            Ok(_) => {}
            Err(e) => warn!(ingest = %ing.ingest_id, error = %e, "collect cycle failed"),
        }
        tokio::time::sleep(interval).await;
    }
}

/// Read one sample set for an ingest via the connector's local adapter.
async fn collect_once(
    conn: &Conn,
    ing: &Ingest,
    modbus: &mut Option<ModbusClient>,
) -> Result<Vec<Sample>> {
    let ts_ms = now_ms();
    let var_id = ing
        .naming
        .get("variable_id")
        .and_then(Value::as_str)
        .unwrap_or(&ing.ingest_id)
        .to_string();

    match conn.ds_type.as_str() {
        "modbus" => {
            let host = conn
                .config
                .get("host")
                .and_then(Value::as_str)
                .context("modbus connector config.host missing")?;
            let port = conn
                .config
                .get("port")
                .and_then(Value::as_u64)
                .unwrap_or(502) as u16;
            let start =
                ing.selection
                    .get("register")
                    .and_then(Value::as_u64)
                    .context("modbus ingest selection.register missing")? as u16;
            let count = ing
                .selection
                .get("count")
                .and_then(Value::as_u64)
                .unwrap_or(1) as u16;
            let input = ing.selection.get("type").and_then(Value::as_str) == Some("input");
            if modbus.is_none() {
                *modbus = Some(ModbusClient::new(host.to_string(), port, count));
            }
            let regs = modbus.as_mut().unwrap().read(start, count, input).await?;
            let n = regs.len();
            Ok(regs
                .iter()
                .enumerate()
                .map(|(i, &raw)| Sample {
                    variable_id: if n == 1 {
                        var_id.clone()
                    } else {
                        format!("{var_id}/{i}")
                    },
                    ts_ms,
                    value: apply_scale(raw as f64, &ing.transform),
                    quality: 0,
                    attrs: Default::default(),
                })
                .collect())
        }
        "pss" => {
            let result = crate::pss::run(
                "query",
                &ing.selection,
                &conn.config,
                &conn.secrets,
                30_000,
                MAX_ROWS,
            )
            .await?;
            let value = scalar_value(&result)
                .with_context(|| format!("pss query for {var_id} returned no scalar value"))?;
            Ok(vec![Sample {
                variable_id: var_id,
                ts_ms,
                value: apply_scale(value, &ing.transform),
                quality: 0,
                attrs: Default::default(),
            }])
        }
        other => anyhow::bail!("agent cannot collect connector type '{other}'"),
    }
}

/// Apply an ingest's linear acquisition transform: value = raw*scale + offset.
/// Expression-based transforms are left to the cloud (the agent keeps raw);
/// scale defaults to 1, offset to 0.
fn apply_scale(raw: f64, transform: &Value) -> f64 {
    if transform
        .get("expression")
        .and_then(Value::as_str)
        .is_some()
    {
        return raw;
    }
    let scale = transform
        .get("scale")
        .and_then(Value::as_f64)
        .unwrap_or(1.0);
    let offset = transform
        .get("offset")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    raw * scale + offset
}

/// Extract a scalar f64 from an adapter result ({kind:"scalar", value}).
fn scalar_value(result: &Value) -> Option<f64> {
    let v = result.get("value")?;
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
