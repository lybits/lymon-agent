// Example connector plugin: Siemens S7 (S7-300/400/1200/1500 over ISO-on-TCP).
//
// Shows how to build a connector on the SDK. The S7 wire read is a SKELETON
// (clearly marked) so it compiles + runs against the agent with no PLC and no
// native deps; a contributor drops in a real S7 client (e.g. rust-snap7) at the
// marked spot. Connector config/selection it expects:
//   config:    { "host": "10.0.0.5", "rack": 0, "slot": 1 }
//   selection: { "db": 1, "byte": 0, "type": "real" }   // real|int|dint|word|bool
//
// Deploy: build, then under the agent's plugins dir put
//   plugins/lymon-plugin-s7/plugin.json   (see plugin.json in this crate)
//   plugins/lymon-plugin-s7/lymon-plugin-s7   (this binary)
// and create a connector type="s7" host=agent in the portal.

use lymon_collector_sdk::{run, Collector, ReadRequest, Sample};

struct S7Connector;

impl Collector for S7Connector {
    fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String> {
        // --- parse the connector config + ingest selection -----------------
        let host = req.config_str("host").ok_or("config.host is required")?;
        let rack = req.config.get("rack").and_then(|v| v.as_u64()).unwrap_or(0);
        let slot = req.config.get("slot").and_then(|v| v.as_u64()).unwrap_or(1);
        let db = req.selection_u64("db").ok_or("selection.db is required")?;
        let byte = req.selection_u64("byte").unwrap_or(0);
        let dtype = req
            .selection
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("real");
        let var_id = req.variable_id().unwrap_or("s7.value").to_string();

        // --- read the value from the PLC -----------------------------------
        // TODO(real S7): open an ISO-on-TCP connection to `host:102` for
        // (rack, slot), read DB`db` at offset `byte` and decode as `dtype`
        // (real=f32 BE, int=i16, dint=i32, word=u16, bool=bit). Keep the
        // connection in `self` across polls instead of reconnecting each read.
        // Example with rust-snap7:
        //   let mut cli = snap7::S7Client::connect(host, rack, slot)?;
        //   let buf = cli.db_read(db, byte, len)?; decode(buf, dtype)
        let value = match dtype {
            "bool" => f64::from((byte % 2) as u32), // placeholder
            _ => db as f64 + byte as f64 / 1000.0,  // deterministic demo value
        };
        eprintln!("[s7] {host} r{rack}/s{slot} DB{db}.{byte} ({dtype}) → {value} (skeleton)");

        Ok(vec![Sample::new(var_id, value)])
    }
}

fn main() {
    run(S7Connector);
}
