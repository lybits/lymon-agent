// PSS (Circutor Power Studio) gateway adapter — Rust port.
//
// Runs locally on the agent when the cloud routes a connection_mode='agent'
// PSS datasource op over the control channel. The PSS HTTP contract + result
// shapes match the cloud adapter (services/mcp-gateway/.../adapters/pss.ts)
// exactly, so a query answered here is indistinguishable from a cloud-direct
// one:
//   query   → {kind:"scalar", value} | {kind:"vector", values:[…]}
//   history → {kind:"timeseries", points:[{ts, value}]}
//   discover→ {kind:"tree", schema_kind:"pss_devices", nodes:[…]}
//
// XML is element-based (ids/values are child elements, not attributes). Dates
// are DDMMYYYYHHmmss in UTC. Auth: anonymous / basic / bearer.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

// --- PSS XML response shapes (only the fields we read) ---------------------

#[derive(Debug, Deserialize)]
struct Values {
    #[serde(default)]
    variable: Vec<VarRow>,
}

#[derive(Debug, Deserialize)]
struct VarRow {
    id: String,
    value: Option<String>,
    #[serde(rename = "textValue")]
    text_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RecordGroup {
    #[serde(default)]
    record: Vec<Record>,
}

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "dateTime")]
    date_time: Option<String>,
    #[serde(default)]
    field: Vec<VarRow>,
}

#[derive(Debug, Deserialize)]
struct DevicesIds {
    #[serde(default)]
    id: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DeviceInfo {
    #[serde(default)]
    device: Vec<Device>,
}

#[derive(Debug, Deserialize)]
struct Device {
    // `id` element is present in the XML but the device id we use comes from
    // the devices.xml list, so we don't bind it here.
    description: Option<String>,
    #[serde(default)]
    var: Vec<String>,
}

// --- entry point -----------------------------------------------------------

/// Run a PSS op and return the AdapterResult JSON the cloud expects.
pub async fn run(
    op: &str,
    args: &Value,
    config: &Value,
    secrets: &Value,
    timeout_ms: u64,
    max_rows: usize,
) -> Result<Value> {
    let base_url = config
        .get("base_url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("pss requires a base_url"))?
        .trim_end_matches('/')
        .to_string();
    let client = Client::builder()
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .build()
        .context("building http client")?;

    match op {
        "query" => query(&client, &base_url, config, secrets, args).await,
        "history" => history(&client, &base_url, config, secrets, args, max_rows).await,
        "discover" => discover(&client, &base_url, config, secrets).await,
        other => bail!("pss does not support op '{other}'"),
    }
}

async fn query(
    client: &Client,
    base_url: &str,
    config: &Value,
    secrets: &Value,
    args: &Value,
) -> Result<Value> {
    let vars = require_vars(args)?;
    let path = format!(
        "/services/user/values.xml?var={}",
        vars.iter().map(|v| urlencode(v)).collect::<Vec<_>>().join("?var=")
    );
    let xml = get(client, base_url, &path, config, secrets).await?;
    let parsed: Values = quick_xml::de::from_str(&xml).context("parsing PSS values.xml")?;

    if vars.len() == 1 {
        let hit = parsed.variable.iter().find(|r| r.id == vars[0]);
        let value = hit.map(normalize_value).unwrap_or(Value::Null);
        Ok(json!({ "kind": "scalar", "value": value }))
    } else {
        let values: Vec<Value> = vars
            .iter()
            .map(|v| {
                parsed
                    .variable
                    .iter()
                    .find(|r| &r.id == v)
                    .and_then(|r| r.value.as_deref())
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|n| n.is_finite())
                    .map(|n| json!(n))
                    .unwrap_or(Value::Null)
            })
            .collect();
        Ok(json!({ "kind": "vector", "values": values }))
    }
}

async fn history(
    client: &Client,
    base_url: &str,
    config: &Value,
    secrets: &Value,
    args: &Value,
    max_rows: usize,
) -> Result<Value> {
    let vars = require_vars(args)?;
    let variable = &vars[0];
    let period = args.get("period").and_then(Value::as_str).unwrap_or("ALL");
    let from = args.get("from").and_then(Value::as_str).ok_or_else(|| anyhow!("history requires `from`"))?;
    let to = args.get("to").and_then(Value::as_str).ok_or_else(|| anyhow!("history requires `to`"))?;
    let begin = format_pss_date(parse_iso(from)?);
    let end = format_pss_date(parse_iso(to)?);
    let path = format!(
        "/services/user/records.xml?begin={begin}?end={end}?var={}?period={period}",
        urlencode(variable)
    );
    let xml = get(client, base_url, &path, config, secrets).await?;
    let parsed: RecordGroup = quick_xml::de::from_str(&xml).context("parsing PSS records.xml")?;

    let mut points: Vec<Value> = Vec::new();
    for r in parsed.record.iter() {
        let ts = match r.date_time.as_deref().and_then(parse_pss_date) {
            Some(ts) => ts,
            None => continue,
        };
        let value = r
            .field
            .iter()
            .find(|f| &f.id == variable)
            .and_then(|f| match normalize_value(f) {
                Value::Number(n) => n.as_f64(),
                Value::String(s) => s.parse::<f64>().ok(),
                _ => None,
            })
            .filter(|n| n.is_finite());
        if let Some(v) = value {
            points.push(json!({ "ts": ts.to_rfc3339(), "value": v }));
            if points.len() >= max_rows {
                break;
            }
        }
    }
    Ok(json!({ "kind": "timeseries", "points": points }))
}

async fn discover(client: &Client, base_url: &str, config: &Value, secrets: &Value) -> Result<Value> {
    let list_xml = get(client, base_url, "/services/user/devices.xml?info=ALL", config, secrets).await?;
    let ids: DevicesIds = quick_xml::de::from_str(&list_xml).context("parsing PSS devices.xml")?;

    let mut nodes: Vec<Value> = Vec::new();
    for id in ids.id.iter() {
        let mut label = id.clone();
        let mut variables: Vec<String> = Vec::new();
        let info_path = format!("/services/user/deviceInfo.xml?id={}", urlencode(id));
        if let Ok(info_xml) = get(client, base_url, &info_path, config, secrets).await {
            if let Ok(info) = quick_xml::de::from_str::<DeviceInfo>(&info_xml) {
                if let Some(dev) = info.device.into_iter().next() {
                    if let Some(desc) = dev.description {
                        label = desc;
                    }
                    variables = dev.var;
                }
            }
        }
        let children: Vec<Value> = variables
            .into_iter()
            .map(|v| json!({ "id": v, "label": v, "node_type": "variable", "meta": { "device": id } }))
            .collect();
        nodes.push(json!({ "id": id, "label": label, "node_type": "device", "children": children }));
    }
    Ok(json!({ "kind": "tree", "schema_kind": "pss_devices", "nodes": nodes }))
}

// --- helpers ---------------------------------------------------------------

fn require_vars(args: &Value) -> Result<Vec<String>> {
    if let Some(v) = args.get("variable").and_then(Value::as_str) {
        if !v.is_empty() {
            return Ok(vec![v.to_string()]);
        }
    }
    if let Some(arr) = args.get("variables").and_then(Value::as_array) {
        let vars: Vec<String> = arr.iter().filter_map(|x| x.as_str().map(str::to_string)).collect();
        if !vars.is_empty() {
            return Ok(vars);
        }
    }
    bail!("pss query requires `variable` (e.g. \"DEV1/Power\") or a `variables` array")
}

fn normalize_value(row: &VarRow) -> Value {
    if let Some(t) = &row.text_value {
        return Value::String(t.clone());
    }
    match &row.value {
        None => Value::Null,
        Some(v) => match v.parse::<f64>() {
            Ok(n) if n.is_finite() => json!(n),
            _ => Value::String(v.clone()),
        },
    }
}

async fn get(
    client: &Client,
    base_url: &str,
    path: &str,
    config: &Value,
    secrets: &Value,
) -> Result<String> {
    let url = format!("{base_url}{path}");
    let mut req = client.get(&url);
    match config.get("auth").and_then(|a| a.get("type")).and_then(Value::as_str) {
        Some("basic") => {
            let user = config.get("auth").and_then(|a| a.get("username")).and_then(Value::as_str).unwrap_or("");
            let pass = secrets.get("password").and_then(Value::as_str).unwrap_or("");
            req = req.basic_auth(user, Some(pass));
        }
        Some("bearer") => {
            let token = secrets.get("token").and_then(Value::as_str).unwrap_or("");
            req = req.bearer_auth(token);
        }
        _ => {}
    }
    let resp = req.send().await.context("PSS request failed")?;
    let status = resp.status();
    if !status.is_success() {
        bail!("HTTP {status} from PSS");
    }
    resp.text().await.context("reading PSS response body")
}

/// PSS uses literal '?' as a parameter separator, so we percent-encode the
/// variable name itself but keep the surrounding '?var=' literal. Encode the
/// characters that would otherwise break the URL; PSS variable names are
/// dotted/identifier-ish (e.g. "R$CAL_IMO.ES_FV_KW").
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn format_pss_date(dt: DateTime<Utc>) -> String {
    format!(
        "{:02}{:02}{:04}{:02}{:02}{:02}",
        dt.day(),
        dt.month(),
        dt.year(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

fn parse_iso(s: &str) -> Result<DateTime<Utc>> {
    let dt = DateTime::parse_from_rfc3339(s).with_context(|| format!("invalid ISO timestamp: {s}"))?;
    Ok(dt.with_timezone(&Utc))
}

fn parse_pss_date(input: &str) -> Option<DateTime<Utc>> {
    let s: Vec<char> = input.chars().take(14).collect();
    if s.len() < 14 {
        return None;
    }
    let slice = |a: usize, b: usize| -> Option<i64> { s[a..b].iter().collect::<String>().parse().ok() };
    let day = slice(0, 2)? as u32;
    let month = slice(2, 4)? as u32;
    let year = slice(4, 8)? as i32;
    let hour = slice(8, 10)? as u32;
    let min = slice(10, 12)? as u32;
    let sec = slice(12, 14)? as u32;
    Utc.with_ymd_and_hms(year, month, day, hour, min, sec).single()
}
