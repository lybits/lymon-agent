// Connector plugin host (execd model — doc 31/32). Third parties ship a
// connector as an executable that speaks a tiny JSON-lines protocol over stdio;
// the agent discovers it from a manifest, spawns it long-lived, and routes
// collect/query for that connector `type` to it. No recompile of the agent.
//
// Protocol v1 (one request line → one response line):
//   agent → plugin: {"v":1,"op":"hello"}
//                   {"v":1,"op":"read","connector_id","type","config","secrets","selection","naming"}
//   plugin → agent: {"ok":true,"types":[…]}                      (to hello)
//                   {"ok":true,"samples":[{"variable_id","value","ts_ms?","quality?"}]}
//                   {"ok":false,"error":"…"}

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{info, warn};

/// Host-level backstop: max time to wait for a plugin to answer one request.
/// A plugin that reads the request but never replies (a crash that leaves stdout
/// open, a deadlock) would otherwise hold this connector's shared process lock
/// forever, blocking every later collect/query. Set WELL ABOVE any plugin's own
/// internal timeouts (e.g. the OPC-UA connect budget) so it only fires for a
/// genuinely stuck process — then we kill it and the next call respawns.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(60);

use crate::collector::{Conn, Ingest};
use crate::generated::lymon::ingest::v1::Sample;

const PROTOCOL: u32 = 1;

#[derive(Deserialize)]
struct Manifest {
    name: String,
    #[serde(default)]
    types: Vec<String>,
    exec: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    protocol: u32,
}

#[derive(Deserialize)]
struct PluginSample {
    variable_id: String,
    value: f64,
    #[serde(default)]
    ts_ms: Option<i64>,
    #[serde(default)]
    quality: Option<u32>,
}

#[derive(Deserialize)]
struct ReadResp {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    samples: Vec<PluginSample>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct QueryResp {
    #[serde(default)]
    ok: bool,
    /// Adapter result Value (`{kind:"scalar"|"vector"|"tree", …}`).
    #[serde(default)]
    result: Value,
    #[serde(default)]
    error: Option<String>,
}

struct Proc {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

/// One discovered plugin: its launch spec + the (lazily spawned, reused) process.
pub struct Plugin {
    name: String,
    dir: PathBuf,
    // Absolute path to the plugin binary. MUST be absolute: on Windows
    // CreateProcess resolves a relative program path against the PARENT's cwd
    // (the agent), not the `current_dir` we set for the child — so a manifest
    // `exec: ./lymon-plugin-opcua.exe` would fail to launch. We resolve it
    // against the plugin's own dir at discovery time.
    exec: PathBuf,
    args: Vec<String>,
    proc: Mutex<Option<Proc>>,
}

impl Plugin {
    /// Read one sample set for an ingest through the plugin process. Spawns the
    /// process on first use and keeps it resident; on any I/O error it kills the
    /// process so the next call respawns.
    pub async fn read(&self, conn: &Conn, ing: &Ingest) -> Result<Vec<Sample>> {
        let req = json!({
            "v": PROTOCOL, "op": "read",
            "connector_id": ing.connector_id, "type": conn.ds_type,
            "config": conn.config, "secrets": conn.secrets,
            "selection": ing.selection, "naming": ing.naming,
        });
        let line = self.exchange(&req).await?;
        let resp: ReadResp =
            serde_json::from_str(&line).context("plugin response not valid JSON")?;
        if !resp.ok {
            return Err(anyhow!(resp.error.unwrap_or_else(|| "plugin error".into())));
        }
        let now = now_ms();
        Ok(resp
            .samples
            .into_iter()
            .map(|s| Sample {
                point_id: s.variable_id,
                ts_ms: s.ts_ms.unwrap_or(now),
                value: s.value,
                quality: s.quality.unwrap_or(0),
                attrs: Default::default(),
            })
            .collect())
    }

    /// Run an ad-hoc op (query / test / discover) through the plugin and return
    /// the adapter result Value for the cloud's query_response. Same long-lived
    /// process + respawn-on-error semantics as [`read`](Self::read).
    pub async fn query(
        &self,
        op: &str,
        ds_type: &str,
        config: &Value,
        secrets: &Value,
        args: &Value,
    ) -> Result<Value> {
        let req = json!({
            "v": PROTOCOL, "op": op,
            "type": ds_type, "config": config, "secrets": secrets, "args": args,
        });
        let line = self.exchange(&req).await?;
        let resp: QueryResp =
            serde_json::from_str(&line).context("plugin response not valid JSON")?;
        if !resp.ok {
            return Err(anyhow!(resp.error.unwrap_or_else(|| "plugin error".into())));
        }
        Ok(resp.result)
    }

    /// ADR 49 W2.2 — supervisory device write via a connector plugin (OPC-UA,
    /// …). Sends one `write` op with the protocol-specific `target` + value;
    /// returns the read-back engineering value when the plugin verifies it.
    pub async fn write(
        &self,
        ds_type: &str,
        config: &Value,
        secrets: &Value,
        target: &Value,
        value: f64,
        readback: bool,
    ) -> Result<Option<f64>> {
        let req = json!({
            "v": PROTOCOL, "op": "write",
            "type": ds_type, "config": config, "secrets": secrets,
            "target": target, "write_value": value, "readback": readback,
        });
        let line = self.exchange(&req).await?;
        #[derive(serde::Deserialize)]
        struct WriteResp {
            ok: bool,
            #[serde(default)]
            readback: Option<f64>,
            #[serde(default)]
            error: Option<String>,
        }
        let resp: WriteResp =
            serde_json::from_str(&line).context("plugin write response not valid JSON")?;
        if !resp.ok {
            return Err(anyhow!(resp
                .error
                .unwrap_or_else(|| "plugin write error".into())));
        }
        Ok(resp.readback)
    }

    /// ADR 41 F3 — batched multi-point live read for the live route. Sends ONE
    /// `read` with an array of points (`[{selection, naming}]`) and returns the
    /// `samples` array verbatim (each sample's variable_id ↔ the point, plus
    /// value/ts_ms/quality) for the cloud's query_response. One round trip
    /// serves every point of the connector. Same long-lived process +
    /// respawn-on-error semantics as [`read`](Self::read).
    pub async fn read_points(
        &self,
        ds_type: &str,
        config: &Value,
        secrets: &Value,
        points: &Value,
    ) -> Result<Value> {
        let req = json!({
            "v": PROTOCOL, "op": "read",
            "type": ds_type, "config": config, "secrets": secrets,
            "points": points,
        });
        let line = self.exchange(&req).await?;
        let resp: Value = serde_json::from_str(&line).context("plugin response not valid JSON")?;
        if resp.get("ok").and_then(Value::as_bool) != Some(true) {
            let err = resp
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("plugin error");
            return Err(anyhow!(err.to_string()));
        }
        Ok(resp.get("samples").cloned().unwrap_or_else(|| json!([])))
    }

    /// Open a subscription (push): spawn a DEDICATED process (not the shared
    /// request/response one), send one `subscribe` request, and return a stream
    /// the caller drains. The process streams `{samples}` lines until killed;
    /// dropping the returned [`PluginStream`] kills it (kill_on_drop).
    pub async fn open_stream(&self, conn: &Conn, ing: &Ingest) -> Result<PluginStream> {
        info!(plugin = %self.name, ingest = %ing.ingest_id, "opening plugin subscription");
        let mut child = Command::new(&self.exec)
            .args(&self.args)
            .current_dir(&self.dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning plugin {} (subscribe)", self.name))?;
        let mut stdin = child.stdin.take().context("plugin stdin")?;
        let stdout = BufReader::new(child.stdout.take().context("plugin stdout")?).lines();
        forward_stderr(&self.name, child.stderr.take());
        let req = json!({
            "v": PROTOCOL, "op": "subscribe",
            "connector_id": ing.connector_id, "type": conn.ds_type,
            "config": conn.config, "secrets": conn.secrets,
            "selection": ing.selection, "naming": ing.naming,
        });
        stdin
            .write_all(format!("{req}\n").as_bytes())
            .await
            .context("sending subscribe request")?;
        let _ = stdin.flush().await;
        // The subscribe op never reads more input; closing stdin is harmless
        // (the process blocks streaming) and avoids holding a dangling handle.
        drop(stdin);
        Ok(PluginStream { child, stdout })
    }

    /// Send one request line, read one response line. Manages spawn + respawn.
    async fn exchange(&self, req: &Value) -> Result<String> {
        let mut guard = self.proc.lock().await;
        if guard.is_none() {
            *guard = Some(self.spawn().await?);
        }
        let proc = guard.as_mut().unwrap();
        let payload = format!("{}\n", req);
        let io: Result<String> = async {
            proc.stdin.write_all(payload.as_bytes()).await?;
            proc.stdin.flush().await?;
            match timeout(EXCHANGE_TIMEOUT, proc.stdout.next_line()).await {
                Ok(read) => match read? {
                    Some(l) => Ok(l),
                    None => Err(anyhow!("plugin closed its output")),
                },
                Err(_) => Err(anyhow!(
                    "plugin did not respond within {}s",
                    EXCHANGE_TIMEOUT.as_secs()
                )),
            }
        }
        .await;
        if let Err(e) = &io {
            // Surface why, then drop the broken process so the next call respawns.
            warn!(plugin = %self.name, error = %e, "plugin exchange failed; respawning");
            if let Some(mut p) = guard.take() {
                let _ = p.child.start_kill();
            }
        }
        io
    }

    async fn spawn(&self) -> Result<Proc> {
        info!(plugin = %self.name, "spawning connector plugin");
        let mut child = Command::new(&self.exec)
            .args(&self.args)
            .current_dir(&self.dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning plugin {}", self.name))?;
        let stdin = child.stdin.take().context("plugin stdin")?;
        let stdout = BufReader::new(child.stdout.take().context("plugin stdout")?).lines();
        forward_stderr(&self.name, child.stderr.take());
        // No runtime handshake: the manifest declares types + protocol, so the
        // process is a pure request→response loop (one response line per request
        // line). Avoids desync from an unread hello reply.
        Ok(Proc {
            child,
            stdin,
            stdout,
        })
    }
}

/// A live plugin subscription: a dedicated process pushing sample lines. Held
/// by the collector's stream task; dropping it kills the process (kill_on_drop
/// was set at spawn), so a reconfigure that aborts the task tears it down.
pub struct PluginStream {
    // Held to keep the process alive; reaped on drop. Not read directly.
    #[allow(dead_code)]
    child: Child,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl PluginStream {
    /// Await the next pushed batch. `Ok(None)` = the stream closed (process
    /// exited); empty/ack lines are skipped. Ts/quality default like `read`.
    pub async fn next(&mut self) -> Result<Option<Vec<Sample>>> {
        loop {
            let line = match self.stdout.next_line().await? {
                Some(l) if !l.trim().is_empty() => l,
                Some(_) => continue,
                None => return Ok(None),
            };
            let resp: ReadResp =
                serde_json::from_str(&line).context("plugin stream line not valid JSON")?;
            if !resp.ok {
                return Err(anyhow!(resp
                    .error
                    .unwrap_or_else(|| "plugin stream error".into())));
            }
            if resp.samples.is_empty() {
                continue; // ack / keepalive
            }
            let now = now_ms();
            return Ok(Some(
                resp.samples
                    .into_iter()
                    .map(|s| Sample {
                        point_id: s.variable_id,
                        ts_ms: s.ts_ms.unwrap_or(now),
                        value: s.value,
                        quality: s.quality.unwrap_or(0),
                        attrs: Default::default(),
                    })
                    .collect(),
            ));
        }
    }
}

/// Discovers + indexes connector plugins by the types they serve.
pub struct PluginHost {
    by_type: HashMap<String, Arc<Plugin>>,
}

impl PluginHost {
    /// Scan `dir` for `<name>/plugin.json` manifests. `allow` (if non-empty)
    /// restricts which plugin names may load. Built-in types are NOT registered
    /// here, so they always take precedence over a same-named plugin.
    pub fn discover(dir: &str, allow: &[String]) -> Arc<Self> {
        let mut by_type: HashMap<String, Arc<Plugin>> = HashMap::new();
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => {
                return Arc::new(Self { by_type });
            }
        };
        for entry in entries.flatten() {
            let pdir = entry.path();
            if !pdir.is_dir() {
                continue;
            }
            let manifest_path = pdir.join("plugin.json");
            let raw = match std::fs::read(&manifest_path) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let m: Manifest = match serde_json::from_slice(&raw) {
                Ok(m) => m,
                Err(e) => {
                    warn!(path = %manifest_path.display(), error = %e, "bad plugin manifest; skipped");
                    continue;
                }
            };
            if !allow.is_empty() && !allow.iter().any(|a| a == &m.name) {
                warn!(plugin = %m.name, "plugin not in allowlist; skipped");
                continue;
            }
            if m.protocol != 0 && m.protocol != PROTOCOL {
                warn!(plugin = %m.name, protocol = m.protocol, "unsupported plugin protocol; skipped");
                continue;
            }
            // Resolve the manifest `exec` (see the Plugin.exec doc — required
            // for Windows). A RELATIVE PATH (carries a separator, e.g.
            // `./lymon-plugin-opcua.exe`) is resolved against the plugin's own
            // dir. A bare command NAME (e.g. `sh`, `python`) is left untouched
            // so the OS resolves it on PATH.
            let exec_abs = {
                let e = std::path::Path::new(&m.exec);
                if e.is_absolute() || !(m.exec.contains('/') || m.exec.contains('\\')) {
                    e.to_path_buf()
                } else {
                    pdir.join(e)
                }
            };
            let plugin = Arc::new(Plugin {
                name: m.name.clone(),
                dir: pdir,
                exec: exec_abs,
                args: m.args,
                proc: Mutex::new(None),
            });
            for t in m.types {
                if by_type.contains_key(&t) {
                    warn!(ty = %t, plugin = %m.name, "type already served by another plugin; skipped");
                    continue;
                }
                by_type.insert(t, plugin.clone());
            }
            info!(plugin = %m.name, "connector plugin registered");
        }
        if !by_type.is_empty() {
            info!(types = by_type.len(), "connector plugins available");
        }
        Arc::new(Self { by_type })
    }

    pub fn for_type(&self, ds_type: &str) -> Option<Arc<Plugin>> {
        self.by_type.get(ds_type).cloned()
    }

    /// The connector types served by discovered plugins — advertised in the
    /// agent's control-channel capabilities so the cloud routes their queries
    /// (Browse/Test) here.
    pub fn types(&self) -> Vec<String> {
        self.by_type.keys().cloned().collect()
    }
}

/// Drain a plugin's piped stderr line-by-line into the agent's tracing output
/// (so it shows on the agent log AND lands in the log ring buffer for remote
/// retrieval). MUST run for every piped child — an unread stderr pipe would
/// fill and block the plugin's writes. Spawned detached; ends when stderr
/// closes (process exit).
fn forward_stderr(name: &str, stderr: Option<ChildStderr>) {
    let Some(stderr) = stderr else { return };
    let name = name.to_string();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            // target under lymon_agent so the default EnvFilter (info) keeps it.
            info!(target: "lymon_agent::plugin", plugin = %name, "{line}");
        }
    });
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::{Conn, Ingest};
    use tempfile::tempdir;

    // A fake plugin: a shell loop that emits one sample line per request line.
    #[tokio::test]
    async fn execd_plugin_read_roundtrip() {
        let dir = tempdir().unwrap();
        let pdir = dir.path().join("echo");
        std::fs::create_dir_all(&pdir).unwrap();
        std::fs::write(
            pdir.join("plugin.json"),
            r#"{"name":"echo","types":["echo"],"exec":"sh","args":["-c","while IFS= read -r l; do printf '{\"ok\":true,\"samples\":[{\"variable_id\":\"plug.v\",\"value\":42}]}\n'; done"]}"#,
        )
        .unwrap();

        let host = PluginHost::discover(dir.path().to_str().unwrap(), &[]);
        let plugin = host
            .for_type("echo")
            .expect("plugin registered for type echo");
        let conn = Conn {
            ds_type: "echo".into(),
            config: json!({}),
            secrets: json!({}),
            ..Default::default()
        };
        let ing = Ingest {
            ingest_id: "ing_1".into(),
            connector_id: "con_1".into(),
            selection: json!({}),
            interval_s: 60,
            naming: json!({ "variable_id": "plug.v" }),
            transform: Value::Null,
        };
        let samples = plugin.read(&conn, &ing).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].point_id, "plug.v");
        assert_eq!(samples[0].value, 42.0);
    }

    #[tokio::test]
    async fn unknown_type_has_no_plugin() {
        let dir = tempdir().unwrap();
        let host = PluginHost::discover(dir.path().to_str().unwrap(), &[]);
        assert!(host.for_type("nope").is_none());
    }
}
