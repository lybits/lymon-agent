//! SDK for writing Lymon agent connector plugins (execd protocol v1, doc 32).
//!
//! A plugin is an executable the agent spawns and talks to over stdio: it reads
//! one JSON request line per request and writes one JSON response line.
//! Implement [`Collector`] and call [`run`] — the SDK handles the framing.
//!
//! The agent sends three kinds of request (`op`):
//! - `read` — a collect poll. Returns samples. Implement [`Collector::read`].
//! - `query` / `test` — read one value to validate a selection (powers the
//!   portal's "Test selection"). Defaults to [`Collector::read`] + the first
//!   sample shaped as a scalar; override [`Collector::query`] for richer results.
//! - `discover` — browse the source into a node tree (powers the portal source
//!   explorer). Opt-in: implement [`Collector::discover`].
//!
//! For `query`/`test`/`discover` the agent puts the request parameters in
//! `args`; the SDK copies `args` into `selection` when `selection` is absent, so
//! a plugin that reads `selection` works for all of them without extra code.
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

/// One request from the agent (a collect poll, or an ad-hoc query/test/discover).
#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    #[serde(default)]
    pub v: u32,
    /// "read" (collect), "query"/"test" (Browse a single value), or "discover".
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
    /// What to read (registers / nodes / db+byte / …). For query/test/discover
    /// the agent sends the params in `args`; the SDK mirrors them here.
    #[serde(default)]
    pub selection: Value,
    /// Ad-hoc op parameters (query/test/discover). Mirrored into `selection`.
    #[serde(default)]
    pub args: Value,
    /// How the result maps to a variable (`{variable_id, unit, …}`).
    #[serde(default)]
    pub naming: Value,
    /// Batched points to read in ONE op (ADR 41 F2). Each carries its own
    /// `selection` + `naming`. When empty, the SDK treats the request as a
    /// single point built from the top-level `selection`/`naming`, so
    /// single-point callers (and the args-mirror path) keep working. Plugins
    /// should iterate [`points`](ReadRequest::points) instead of reading
    /// `selection` directly, and resolve the whole batch in one device call
    /// (e.g. OPC-UA: one Read over every NodeId).
    #[serde(default)]
    pub points: Vec<Point>,
}

/// One point to read in a batched [`Collector::read`] / [`Collector::subscribe`]
/// (ADR 41 F2): its own `selection` (what to read) and `naming` (how it maps to
/// a variable).
#[derive(Debug, Clone, Deserialize)]
pub struct Point {
    #[serde(default)]
    pub selection: Value,
    #[serde(default)]
    pub naming: Value,
}

impl Point {
    /// The output variable id from `naming.variable_id`, if present.
    pub fn variable_id(&self) -> Option<&str> {
        self.naming.get("variable_id").and_then(Value::as_str)
    }
    /// Convenience: a string field from `selection`.
    pub fn selection_str(&self, key: &str) -> Option<&str> {
        self.selection.get(key).and_then(Value::as_str)
    }
    /// Convenience: a u64 field from `selection`.
    pub fn selection_u64(&self, key: &str) -> Option<u64> {
        self.selection.get(key).and_then(Value::as_u64)
    }
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

    /// The points this request reads: the explicit `points` array if non-empty,
    /// otherwise a single point synthesized from the top-level
    /// `selection`/`naming`. Plugins iterate this so they transparently support
    /// both single-point and batched reads.
    pub fn points(&self) -> Vec<Point> {
        if self.points.is_empty() {
            vec![Point {
                selection: self.selection.clone(),
                naming: self.naming.clone(),
            }]
        } else {
            self.points.clone()
        }
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

/// One node in a [`Discovery`] tree (a device, folder, or readable variable).
/// `id` is what a selection would reference (e.g. an OPC-UA NodeId text form);
/// `node_type` is a free label the portal shows ("folder" / "variable" / …);
/// leaf nodes have an empty `children`.
#[derive(Debug, Serialize)]
pub struct Node {
    pub id: String,
    pub label: String,
    pub node_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<Node>,
    #[serde(skip_serializing_if = "Value::is_null")]
    pub meta: Value,
}

impl Node {
    /// A leaf (readable) node.
    pub fn leaf(
        id: impl Into<String>,
        label: impl Into<String>,
        node_type: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            node_type: node_type.into(),
            children: Vec::new(),
            meta: Value::Null,
        }
    }
    /// A branch node with children.
    pub fn branch(
        id: impl Into<String>,
        label: impl Into<String>,
        node_type: impl Into<String>,
        children: Vec<Node>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            node_type: node_type.into(),
            children,
            meta: Value::Null,
        }
    }
}

/// The result of a [`Collector::discover`] browse: a labelled node tree the
/// portal renders as a source explorer. `schema_kind` is a free tag identifying
/// the shape (e.g. "opcua_nodes").
#[derive(Debug, Serialize)]
pub struct Discovery {
    pub schema_kind: String,
    pub nodes: Vec<Node>,
}

/// Implement this for your connector.
///
/// Only [`read`](Collector::read) is required; [`query`](Collector::query)
/// defaults to it (first sample as a scalar) and [`discover`](Collector::discover)
/// defaults to "unsupported".
pub trait Collector {
    /// Read sample(s) for a collect poll. Called once per interval.
    fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String>;

    /// Read a single value to validate a selection ("Test"). Defaults to
    /// [`read`](Collector::read) shaped as a scalar/vector; override for richer
    /// results. Returns an adapter result `Value` (`{kind, value|values}`).
    fn query(&mut self, req: &ReadRequest) -> Result<Value, String> {
        let samples = self.read(req)?;
        Ok(match samples.as_slice() {
            [one] => json!({ "kind": "scalar", "value": one.value }),
            many => {
                json!({ "kind": "vector", "values": many.iter().map(|s| s.value).collect::<Vec<_>>() })
            }
        })
    }

    /// Browse the source into a node tree (source explorer). Opt-in.
    fn discover(&mut self, _req: &ReadRequest) -> Result<Discovery, String> {
        Err("discover not supported by this plugin".into())
    }

    /// Stream samples as the source pushes them (e.g. OPC-UA subscriptions /
    /// monitored items), instead of being polled. Opt-in. Call `emit` with each
    /// batch as it arrives; this method BLOCKS, running the subscription until
    /// the agent kills the process (a reconfigure / shutdown). The agent runs a
    /// dedicated plugin process per subscription, so the loop owns the process.
    fn subscribe(
        &mut self,
        _req: &ReadRequest,
        _emit: &mut dyn FnMut(&[Sample]),
    ) -> Result<(), String> {
        Err("subscribe (push) not supported by this plugin".into())
    }
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
        let req = match serde_json::from_str::<ReadRequest>(&line) {
            Ok(mut req) => {
                // For query/test/discover/subscribe the agent sends params in
                // `args`; mirror into `selection` so a `read`-based plugin works.
                if req.selection.is_null() && !req.args.is_null() {
                    req.selection = req.args.clone();
                }
                req
            }
            Err(e) => {
                let _ = writeln!(
                    out,
                    "{}",
                    json!({ "ok": false, "error": format!("bad request: {e}") })
                );
                let _ = out.flush();
                continue;
            }
        };
        // Subscribe is a streaming op: it emits sample lines as values arrive
        // and blocks until the source/process ends, so it owns the write side.
        if req.op == "subscribe" {
            stream(&mut collector, &req, &mut out);
            continue;
        }
        let resp = dispatch(&mut collector, &req);
        if writeln!(out, "{resp}").is_err() {
            break;
        }
        if out.flush().is_err() {
            break;
        }
    }
}

/// Run a subscription: emit `{ok,samples}` lines as the collector pushes them,
/// then a final status line when it ends. Blocks for the subscription's life.
fn stream<C: Collector, W: Write>(collector: &mut C, req: &ReadRequest, out: &mut W) {
    let result = {
        let mut emit = |samples: &[Sample]| {
            let line = json!({ "ok": true, "samples": samples });
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        };
        collector.subscribe(req, &mut emit)
    };
    let resp = match result {
        Ok(()) => json!({ "ok": true }),
        Err(e) => json!({ "ok": false, "error": e }),
    };
    let _ = writeln!(out, "{resp}");
    let _ = out.flush();
}

fn dispatch<C: Collector>(collector: &mut C, req: &ReadRequest) -> Value {
    match req.op.as_str() {
        "read" => match collector.read(req) {
            Ok(samples) => json!({ "ok": true, "samples": samples }),
            Err(e) => json!({ "ok": false, "error": e }),
        },
        "discover" => match collector.discover(req) {
            Ok(d) => json!({ "ok": true, "result": {
                "kind": "tree", "schema_kind": d.schema_kind, "nodes": d.nodes,
            } }),
            Err(e) => json!({ "ok": false, "error": e }),
        },
        // query / test / history → a single value to validate a selection.
        "query" | "test" | "history" => match collector.query(req) {
            Ok(result) => json!({ "ok": true, "result": result }),
            Err(e) => json!({ "ok": false, "error": e }),
        },
        // hello / unknown — acknowledge so the agent stays in sync.
        _ => json!({ "ok": true }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_from(json_str: &str) -> ReadRequest {
        serde_json::from_str(json_str).expect("parse ReadRequest")
    }

    #[test]
    fn single_point_request_synthesizes_one_point() {
        // No `points` → one point built from the top-level selection/naming.
        let req = req_from(
            r#"{"op":"read","selection":{"node_id":"ns=2;s=T"},"naming":{"variable_id":"v1"}}"#,
        );
        let pts = req.points();
        assert_eq!(pts.len(), 1);
        assert_eq!(pts[0].selection_str("node_id"), Some("ns=2;s=T"));
        assert_eq!(pts[0].variable_id(), Some("v1"));
    }

    #[test]
    fn batched_points_are_returned_verbatim() {
        let req = req_from(
            r#"{"op":"read","points":[
                {"selection":{"node_id":"ns=2;s=A"},"naming":{"variable_id":"a"}},
                {"selection":{"node_id":"ns=2;s=B"},"naming":{"variable_id":"b"}}
            ]}"#,
        );
        let pts = req.points();
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0].variable_id(), Some("a"));
        assert_eq!(pts[1].selection_str("node_id"), Some("ns=2;s=B"));
    }

    #[test]
    fn explicit_points_take_precedence_over_top_level_selection() {
        // When both are present, the batch wins (the top-level fields are the
        // single-point fallback only).
        let req = req_from(
            r#"{"op":"read","selection":{"node_id":"ignored"},
                "points":[{"selection":{"node_id":"used"},"naming":{"variable_id":"v"}}]}"#,
        );
        let pts = req.points();
        assert_eq!(pts.len(), 1);
        assert_eq!(pts[0].selection_str("node_id"), Some("used"));
    }

    #[test]
    fn point_helpers_read_selection_fields() {
        let req = req_from(r#"{"op":"read","points":[{"selection":{"db":3,"byte":10}}]}"#);
        let p = &req.points()[0];
        assert_eq!(p.selection_u64("db"), Some(3));
        assert_eq!(p.selection_u64("byte"), Some(10));
        assert_eq!(p.variable_id(), None);
    }

    #[test]
    fn empty_request_yields_one_empty_point() {
        // A bare request still resolves to a single (empty) point so a plugin
        // can surface a clear "selection required" error rather than panicking.
        let req = req_from(r#"{"op":"read"}"#);
        assert_eq!(req.points().len(), 1);
        assert!(req.points()[0].selection.is_null());
    }
}
