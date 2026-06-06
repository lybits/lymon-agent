// Reference connector plugin (execd protocol v1) — copy as a starting point for
// your own connector (S7, OPC-UA, a vendor protocol). The protocol is
// language-agnostic (see docs/internal/32); this happens to be Rust.
//
// Drop a built binary + a plugin.json next to it under the agent's plugins dir:
//   plugins/echo/plugin.json  {"name":"echo","types":["echo"],"exec":"./echo"}
//   plugins/echo/echo         (this binary)
//
// Contract: read one JSON request line on stdin per poll, write one response
// line on stdout. config/secrets/selection/naming arrive in the request; a real
// plugin opens its device connection (keep it across polls) and reads values.

use std::io::{BufRead, Write};

fn main() {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(req) = line else { break };
        if req.trim().is_empty() {
            continue;
        }
        // A real plugin parses `req` (op/config/secrets/selection) and reads its
        // device here. This reference just returns a constant sample.
        let resp = r#"{"ok":true,"samples":[{"variable_id":"example.value","value":42.0,"quality":0}]}"#;
        if writeln!(out, "{resp}").is_err() {
            break;
        }
        let _ = out.flush();
    }
}
