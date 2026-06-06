//! SDK for writing Lymon agent connector plugins (execd protocol v1, doc 32).
//!
//! A plugin is an executable the agent spawns and talks to over stdio: it reads
//! one JSON request line per poll and writes one JSON response line. Implement
//! [`Collector`] and call [`run`] — the SDK handles the framing.
//!
//! ```no_run
//! use lymon_collector_sdk::{run, Collector, ReadRequest, Sample};
//!
//! struct MyConnector;
//! impl Collector for MyConnector {
//!     fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String> {
//!         // req.config / req.secrets / req.selection / req.naming describe what
//!         // to read; open your device (cache the session across polls) here.
//!         Ok(vec![Sample::new(req.variable_id().unwrap_or("value"), 42.0)])
//!     }
//! }
//!
//! fn main() { run(MyConnector); }
//! ```
//!
//! Ship the binary with a `plugin.json` manifest under the agent's plugins dir:
//! `{ "name": "my-connector", "types": ["my_type"], "exec": "./my-connector" }`.

use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The protocol version this SDK implements.
pub const PROTOCOL: u32 = 1;

/// One request from the agent (a collect poll, or an ad-hoc query/test).
#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    #[serde(default)]
    pub v: u32,
    /// "read" (collect) or "query" (Browse/Test). Others are answered with ok.
    #[serde(default)]
    pub op: String,
    #[serde(default)]
    pub connector_id: String,
    /// The connector `type` (matches a `types` entry in your manifest).
    #[serde(rename = "type", default)]
    pub ds_type: String,
    /// Non-secret connection config (host/port/rack/slot/…).
    #[serde(default)]
    pub config: Value,
    /// Resolved secrets (in-memory only; never log them).
    #[serde(default)]
    pub secrets: Value,
    /// What to read (registers / nodes / db+byte / …).
    #[serde(default)]
    pub selection: Value,
    /// How the result maps to a variable (`{variable_id, unit, …}`).
    #[serde(default)]
    pub naming: Value,
}

impl ReadRequest {
    /// The output variable id from `naming.variable_id`, if present.
    pub fn variable_id(&self) -> Option<&str> {
        self.naming.get("variable_id").and_then(Value::as_str)
    }
    /// Convenience: a string field from `config`.
    pub fn config_str(&self, key: &str) -> Option<&str> {
        self.config.get(key).and_then(Value::as_str)
    }
    /// Convenience: a u64 field from `selection`.
    pub fn selection_u64(&self, key: &str) -> Option<u64> {
        self.selection.get(key).and_then(Value::as_u64)
    }
}

/// One reading to return. `ts_ms` defaults to the agent's clock; `quality` 0 = good.
#[derive(Debug, Serialize)]
pub struct Sample {
    pub variable_id: String,
    pub value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<i64>,
    pub quality: u32,
}

impl Sample {
    pub fn new(variable_id: impl Into<String>, value: f64) -> Self {
        Self {
            variable_id: variable_id.into(),
            value,
            ts_ms: None,
            quality: 0,
        }
    }
}

/// Implement this for your connector. `read` is called once per poll.
pub trait Collector {
    fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String>;
}

/// Run the plugin: read request lines on stdin, dispatch to `collector`, write
/// response lines on stdout. Blocks until stdin closes (the agent exits).
pub fn run<C: Collector>(mut collector: C) {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<ReadRequest>(&line) {
            Ok(req) if req.op == "read" => match collector.read(&req) {
                Ok(samples) => json!({ "ok": true, "samples": samples }),
                Err(e) => json!({ "ok": false, "error": e }),
            },
            // hello / query / unknown — acknowledge so the agent stays in sync.
            Ok(_) => json!({ "ok": true }),
            Err(e) => json!({ "ok": false, "error": format!("bad request: {e}") }),
        };
        if writeln!(out, "{resp}").is_err() {
            break;
        }
        if out.flush().is_err() {
            break;
        }
    }
}
