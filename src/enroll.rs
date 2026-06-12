// Credential resolution + one-time enrollment (Azure-Arc-style onboarding).
//
// Precedence on start:
//   1. Stored credentials (credentials.json next to the buffer) — already
//      enrolled; reuse them.
//   2. Direct env credentials (LYMON_API_KEY + LYMON_AGENT_ID +
//      LYMON_INGEST_ENDPOINT) — legacy / advanced.
//   3. Enrollment exchange (LYMON_ENROLL_CODE + LYMON_ENROLL_URL): POST the
//      one-time code, receive {agent_id, token, ingest_endpoint}, persist it
//      so subsequent starts skip the exchange. Re-enrolling (fresh code)
//      rotates the credential.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::Config;

/// Concrete credentials the agent runs with. tenant_id + control_endpoint are
/// optional for backward compat with credentials.json written before the
/// control channel existed; when present the agent opens the gateway control
/// channel (agent-as-gateway).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub agent_id: String,
    pub token: String,
    pub ingest_endpoint: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub control_endpoint: Option<String>,
}

#[derive(Serialize)]
struct EnrollRequest<'a> {
    code: &'a str,
}

#[derive(Deserialize)]
struct EnrollResponse {
    agent_id: String,
    token: String,
    ingest_endpoint: String,
    #[serde(default)]
    tenant_id: Option<String>,
}

/// Derive the control-channel WebSocket URL from the enrollment URL. The
/// channel lives under /api so it rides the same proxy/ingress as the API:
///   https://host/api/agent/enroll      → wss://host/api/agent-control
///   http://host:3013/api/agent/enroll  → ws://host:3013/api/agent-control
fn derive_control_endpoint(enroll_url: &str) -> Option<String> {
    let base = enroll_url
        .strip_suffix("/api/agent/enroll")
        .unwrap_or(enroll_url)
        .trim_end_matches('/');
    if let Some(rest) = base.strip_prefix("https://") {
        Some(format!("wss://{rest}/api/agent-control"))
    } else {
        base.strip_prefix("http://")
            .map(|rest| format!("ws://{rest}/api/agent-control"))
    }
}

/// credentials.json lives next to the buffer db (a writable, persistent dir).
fn credentials_path(cfg: &Config) -> PathBuf {
    let buf = PathBuf::from(&cfg.buffer_path);
    let dir = buf
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    dir.join("credentials.json")
}

pub async fn resolve(cfg: &Config) -> Result<Credentials> {
    let path = credentials_path(cfg);

    // 1) Stored credentials from a previous enrollment.
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(creds) = serde_json::from_slice::<Credentials>(&bytes) {
            info!(agent_id = %creds.agent_id, path = %path.display(), "using stored agent credentials");
            return Ok(creds);
        }
    }

    // 2) Direct env credentials (legacy / advanced).
    if let (Some(agent_id), Some(token), Some(ingest_endpoint)) = (
        cfg.agent_id.clone(),
        cfg.api_key.clone(),
        cfg.ingest_endpoint.clone(),
    ) {
        info!(agent_id = %agent_id, "using credentials from environment");
        // Legacy/advanced path has no tenant/control endpoint → gateway control
        // channel stays disabled (ingest still works).
        return Ok(Credentials {
            agent_id,
            token,
            ingest_endpoint,
            tenant_id: None,
            control_endpoint: None,
        });
    }

    // 3) One-time enrollment exchange.
    let (code, url) = match (cfg.enroll_code.clone(), cfg.enroll_url.clone()) {
        (Some(c), Some(u)) => (c, u),
        _ => bail!(
            "no credentials available: set LYMON_ENROLL_CODE + LYMON_ENROLL_URL \
             (recommended) or LYMON_API_KEY + LYMON_AGENT_ID + LYMON_INGEST_ENDPOINT"
        ),
    };

    info!(url = %url, "enrolling agent with one-time code");
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&EnrollRequest { code: &code })
        .send()
        .await
        .context("enrollment request failed")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("enrollment rejected: HTTP {status} {body}");
    }
    let er: EnrollResponse = resp.json().await.context("parsing enrollment response")?;
    let creds = Credentials {
        agent_id: er.agent_id,
        token: er.token,
        ingest_endpoint: er.ingest_endpoint,
        tenant_id: er.tenant_id,
        control_endpoint: derive_control_endpoint(&url),
    };

    // Persist so the next start skips the exchange (the code is single-use).
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    write_credentials_file(&path, &serde_json::to_vec_pretty(&creds)?)
        .with_context(|| format!("saving credentials to {}", path.display()))?;
    info!(agent_id = %creds.agent_id, path = %path.display(), "enrolled — credentials saved");

    Ok(creds)
}

/// Write credentials.json owner-read/write only (0600): it holds the agent's
/// bearer token, so the default umask (typically 0644) would expose it to
/// every local user. On unix the file is created with mode 0600 and, in case
/// it already existed with laxer permissions from an older agent version,
/// permissions are re-applied on every rewrite. On non-unix targets this
/// degrades to a plain write (no POSIX modes there).
fn write_credentials_file(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600) // only effective when the file is created
            .open(path)?;
        // The mode above is ignored for pre-existing files: tighten those too.
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn credentials_file_created_with_0600() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        write_credentials_file(&path, b"{\"token\":\"secret\"}").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"{\"token\":\"secret\"}".to_vec()
        );
    }

    #[test]
    fn rewrite_tightens_lax_permissions_on_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        std::fs::write(&path, b"old-and-longer-content-here").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        write_credentials_file(&path, b"new").unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        // truncate(true): no stale tail from the longer previous content.
        assert_eq!(std::fs::read(&path).unwrap(), b"new".to_vec());
    }
}
