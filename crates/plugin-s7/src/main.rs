// Lymon connector plugin: Siemens S7 (S7-300/400/1200/1500 over ISO-on-TCP),
// built on lymon-collector-sdk and a vendored rust7 client (src/s7/client.rs).
//
// Connector config / ingest selection it expects:
//   config:    { "host": "10.0.0.5", "family": "s7-1500" }   // or explicit "rack"/"slot"
//   secrets:   {}                                            // classic S7comm has no auth
//   selection: { "area": "db", "db": 1, "byte": 0, "bit": 0, "type": "real" }
//     area = db | m/merker | i/input | q/output   (db number required for area=db)
//     type = bool | sint | usint(byte) | int | uint(word) | dint | udint(dword) | real | lreal
//
// Reads are poll-only (S7comm has no push/subscribe); discover lists the CPU's DBs.
//
// Deploy: build, then under the agent's plugins dir put
//   plugins/lymon-plugin-s7/plugin.json        (see plugin.json in this crate)
//   plugins/lymon-plugin-s7/lymon-plugin-s7    (this binary)
// and create a connector type="s7" host=agent in the portal.

mod s7;

use lymon_collector_sdk::{run, Collector, Discovery, Node, ReadRequest, Sample};
use serde_json::Value;

use s7::client::{S7Client, S7_AREA_DB, S7_AREA_MK, S7_AREA_PA, S7_AREA_PE, S7_WL_BYTE};
use s7::decode::DType;

/// A live S7 session for one (host, rack, slot), kept across polls.
struct Conn {
    key: (String, u16, u16),
    client: S7Client,
}

struct S7Connector {
    conn: Option<Conn>,
}

impl S7Connector {
    fn new() -> Self {
        Self { conn: None }
    }

    /// Get a connected client for (host, rack, slot), (re)connecting on miss / key change.
    fn client_for(&mut self, host: &str, rack: u16, slot: u16) -> Result<&mut S7Client, String> {
        let key = (host.to_string(), rack, slot);
        let need = match &self.conn {
            Some(c) => c.key != key,
            None => true,
        };
        if need {
            // Drop any stale/other session before connecting (S7Client::drop disconnects).
            self.conn = None;
            let mut client = S7Client::new();
            // Bounded timeouts so a poll can't hang silently (the OPC-UA lesson, agent #11):
            // connect 5s, read 2s, write 2s.
            let _ = client.set_timeout(5000, 2000, 2000);
            client
                .connect_rack_slot(host, rack, slot)
                .map_err(|e| format!("s7 connect {host} rack{rack}/slot{slot}: {e}"))?;
            self.conn = Some(Conn { key, client });
        }
        Ok(&mut self.conn.as_mut().unwrap().client)
    }

    /// Drop the cached session so the next poll reconnects cleanly.
    fn reset(&mut self) {
        self.conn = None;
    }
}

impl Collector for S7Connector {
    fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String> {
        let host = req
            .config_str("host")
            .ok_or("config.host is required")?
            .to_string();
        let (rack, slot) = resolve_rack_slot(&req.config)?;

        // ADR 41 F2 — read every demanded point of this connector in one op,
        // over the shared session. S7comm has no multi-var read in this client,
        // so this is a sequential read per point on the one connection (still
        // one plugin round-trip instead of N). Parse all selections up front:
        // a parse error is a config mistake → fail the batch (like a bad node).
        let points = req.points();
        let mut targets: Vec<(String, Selection)> = Vec::with_capacity(points.len());
        for p in &points {
            let var_id = p.variable_id().unwrap_or("s7.value").to_string();
            let sel = parse_selection(&p.selection)?;
            targets.push((var_id, sel));
        }

        // A wire error on a point → a bad-quality sample (don't sink the rest)
        // and flag the session for reset so the next poll reconnects.
        let mut had_wire_err = false;
        let mut samples = Vec::with_capacity(targets.len());
        {
            let client = self.client_for(&host, rack, slot)?;
            for (var_id, sel) in &targets {
                match read_value(client, sel) {
                    Ok(v) => samples.push(Sample::new(var_id, v)),
                    Err(e) => {
                        eprintln!("[s7] {var_id} → bad ({})", map_s7_err(&e, sel));
                        samples.push(Sample {
                            variable_id: var_id.clone(),
                            value: 0.0,
                            ts_ms: None,
                            quality: 1,
                        });
                        had_wire_err = true;
                    }
                }
            }
        }
        if had_wire_err {
            self.reset();
        }
        Ok(samples)
    }

    /// Browse the CPU's data blocks (numbers + sizes) for the portal source explorer.
    /// Coarse by design — S7comm exposes DB numbers, not field layouts.
    fn discover(&mut self, req: &ReadRequest) -> Result<Discovery, String> {
        let host = req
            .config_str("host")
            .ok_or("config.host is required")?
            .to_string();
        let (rack, slot) = resolve_rack_slot(&req.config)?;
        let result = {
            let client = self.client_for(&host, rack, slot)?;
            s7::blocks::list_data_blocks(client)
        };
        let dbs = match result {
            Ok(d) => d,
            Err(e) => {
                self.reset();
                return Err(format!("s7 discover: {e}"));
            }
        };
        let nodes = dbs
            .into_iter()
            .map(|b| {
                let label = match b.size {
                    Some(sz) => format!("DB{} ({} bytes)", b.number, sz),
                    None => format!("DB{}", b.number),
                };
                let mut node = Node::leaf(format!("DB{}", b.number), label, "db");
                node.meta = serde_json::json!({ "db": b.number, "size_bytes": b.size });
                node
            })
            .collect();
        Ok(Discovery {
            schema_kind: "s7_blocks".into(),
            nodes,
        })
    }
}

/// Read one scalar described by `sel` from a connected client, coerced to f64.
fn read_value(client: &mut S7Client, sel: &Selection) -> Result<f64, String> {
    if sel.dtype == DType::Bool {
        let b = client
            .read_bit(sel.area, sel.db, sel.byte, sel.bit)
            .map_err(|e| e.to_string())?;
        return Ok(if b { 1.0 } else { 0.0 });
    }
    let mut buf = vec![0u8; sel.dtype.byte_len()];
    client
        .read_area(sel.area, sel.db, sel.byte, S7_WL_BYTE, &mut buf)
        .map_err(|e| e.to_string())?;
    sel.dtype.decode(&buf)
}

/// Resolve (rack, slot) from connector config: explicit rack+slot win; else `family`; else 0/1.
fn resolve_rack_slot(config: &Value) -> Result<(u16, u16), String> {
    let rack = config.get("rack").and_then(Value::as_u64);
    let slot = config.get("slot").and_then(Value::as_u64);
    if let (Some(r), Some(s)) = (rack, slot) {
        return Ok((r as u16, s as u16));
    }
    if let Some(fam) = config.get("family").and_then(Value::as_str) {
        let (fr, fs) = match fam.to_ascii_lowercase().as_str() {
            "s7-1500" | "s71500" | "s7-1200" | "s71200" => (0u16, 1u16),
            "s7-300" | "s7300" => (0, 2),
            "s7-400" | "s7400" => {
                return Err(
                    "family s7-400 has no fixed rack/slot — set rack and slot explicitly".into(),
                )
            }
            other => {
                return Err(format!(
                    "unknown family {other:?} (use s7-1500/s7-1200/s7-300, or set rack+slot)"
                ))
            }
        };
        // A lone explicit rack or slot still overrides the family default.
        return Ok((
            rack.map(|v| v as u16).unwrap_or(fr),
            slot.map(|v| v as u16).unwrap_or(fs),
        ));
    }
    // Default: S7-1200/1500 integrated PN interface.
    Ok((
        rack.map(|v| v as u16).unwrap_or(0),
        slot.map(|v| v as u16).unwrap_or(1),
    ))
}

/// A parsed ingest selection: where + what to read.
struct Selection {
    area: u8,
    db: u16,
    byte: u16,
    bit: u8,
    dtype: DType,
}

fn parse_area(s: &str) -> Result<u8, String> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "db" | "datablock" => S7_AREA_DB,
        "m" | "mk" | "merker" | "flag" => S7_AREA_MK,
        "i" | "e" | "input" | "pe" => S7_AREA_PE,
        "q" | "a" | "output" | "pa" => S7_AREA_PA,
        other => return Err(format!("unknown area {other:?} (expected db/m/i/q)")),
    })
}

fn parse_selection(sel: &Value) -> Result<Selection, String> {
    let area = match sel.get("area").and_then(Value::as_str) {
        Some(a) => parse_area(a)?,
        None => S7_AREA_DB,
    };
    let dtype = DType::parse(sel.get("type").and_then(Value::as_str).unwrap_or("real"))?;
    if area == S7_AREA_DB && sel.get("db").is_none() {
        return Err("selection.db is required for a DB read".into());
    }
    let db = sel.get("db").and_then(Value::as_u64).unwrap_or(0) as u16;
    let byte = sel.get("byte").and_then(Value::as_u64).unwrap_or(0) as u16;
    let bit = sel.get("bit").and_then(Value::as_u64).unwrap_or(0) as u8;
    if dtype == DType::Bool && bit > 7 {
        return Err(format!("selection.bit must be 0..7 for bool, got {bit}"));
    }
    Ok(Selection {
        area,
        db,
        byte,
        bit,
        dtype,
    })
}

/// Turn a rust7 S7Error string into an actionable message for the common cases.
fn map_s7_err(e: &str, sel: &Selection) -> String {
    let el = e.to_ascii_lowercase();
    if el.contains("invalid address") {
        format!(
            "s7 read failed: {e} (DB{} out of range, or the DB is optimized; set standard access)",
            sel.db
        )
    } else if el.contains("not found") {
        format!(
            "s7 read failed: {e} (DB{} does not exist in the CPU)",
            sel.db
        )
    } else {
        format!("s7 read failed: {e}")
    }
}

fn main() {
    run(S7Connector::new());
}

#[cfg(test)]
mod parse_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn family_resolves_rack_slot() {
        assert_eq!(
            resolve_rack_slot(&json!({"family":"s7-1500"})).unwrap(),
            (0, 1)
        );
        assert_eq!(
            resolve_rack_slot(&json!({"family":"s7-300"})).unwrap(),
            (0, 2)
        );
        assert_eq!(
            resolve_rack_slot(&json!({"rack":0,"slot":2})).unwrap(),
            (0, 2)
        );
        assert_eq!(resolve_rack_slot(&json!({})).unwrap(), (0, 1));
        // a lone explicit slot overrides the family default
        assert_eq!(
            resolve_rack_slot(&json!({"family":"s7-1500","slot":0})).unwrap(),
            (0, 0)
        );
        assert!(resolve_rack_slot(&json!({"family":"s7-400"})).is_err());
        assert!(resolve_rack_slot(&json!({"family":"nope"})).is_err());
    }

    #[test]
    fn selection_parses_and_validates() {
        let s = parse_selection(&json!({"area":"db","db":1,"byte":4,"type":"int"})).unwrap();
        assert_eq!(
            (s.area, s.db, s.byte, s.dtype),
            (S7_AREA_DB, 1, 4, DType::Int)
        );
        // missing db for a DB read
        assert!(parse_selection(&json!({"area":"db","type":"real"})).is_err());
        // bit out of range for bool
        assert!(parse_selection(&json!({"type":"bool","db":1,"bit":9})).is_err());
        // a non-DB area needs no db
        assert_eq!(
            parse_selection(&json!({"area":"m","byte":2,"type":"word"}))
                .unwrap()
                .area,
            S7_AREA_MK
        );
    }
}
