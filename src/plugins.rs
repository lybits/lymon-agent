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
use std::time::SystemTime;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tracing::{info, warn};

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

struct Proc {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

/// One discovered plugin: its launch spec + the (lazily spawned, reused) process.
pub struct Plugin {
    name: String,
    dir: PathBuf,
    exec: String,
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
                variable_id: s.variable_id,
                ts_ms: s.ts_ms.unwrap_or(now),
                value: s.value,
                quality: s.quality.unwrap_or(0),
                attrs: Default::default(),
            })
            .collect())
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
            match proc.stdout.next_line().await? {
                Some(l) => Ok(l),
                None => Err(anyhow!("plugin closed its output")),
            }
        }
        .await;
        if io.is_err() {
            // Drop the broken process so the next call respawns it.
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
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning plugin {}", self.name))?;
        let stdin = child.stdin.take().context("plugin stdin")?;
        let stdout = BufReader::new(child.stdout.take().context("plugin stdout")?).lines();
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
            let plugin = Arc::new(Plugin {
                name: m.name.clone(),
                dir: pdir,
                exec: m.exec,
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
        assert_eq!(samples[0].variable_id, "plug.v");
        assert_eq!(samples[0].value, 42.0);
    }

    #[tokio::test]
    async fn unknown_type_has_no_plugin() {
        let dir = tempdir().unwrap();
        let host = PluginHost::discover(dir.path().to_str().unwrap(), &[]);
        assert!(host.for_type("nope").is_none());
    }
}
