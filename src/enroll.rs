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

/// Derive the control-channel WebSocket URL from the enrollment URL:
///   https://host/api/agent/enroll → wss://host/agent-control
///   http://host:3013/api/agent/enroll → ws://host:3013/agent-control
fn derive_control_endpoint(enroll_url: &str) -> Option<String> {
    let base = enroll_url
        .strip_suffix("/api/agent/enroll")
        .unwrap_or(enroll_url)
        .trim_end_matches('/');
    if let Some(rest) = base.strip_prefix("https://") {
        Some(format!("wss://{rest}/agent-control"))
    } else {
        base.strip_prefix("http://")
            .map(|rest| format!("ws://{rest}/agent-control"))
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
    std::fs::write(&path, serde_json::to_vec_pretty(&creds)?)
        .with_context(|| format!("saving credentials to {}", path.display()))?;
    info!(agent_id = %creds.agent_id, path = %path.display(), "enrolled — credentials saved");

    Ok(creds)
}
