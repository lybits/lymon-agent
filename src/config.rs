// Configuration loaded from environment variables.
// File-based config can be added in Fase 1.

use anyhow::{Context, Result};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    /// Which local source this agent reports (still configured locally).
    pub datasource_id: String,
    pub modbus_host: String,
    pub modbus_port: u16,
    pub poll_interval_ms: u64,
    pub register_count: u16,
    // Location of the SQLite WAL buffer; agent credentials are stored
    // alongside it (credentials.json) after enrollment.
    pub buffer_path: String,
    // High-water mark for the buffer (max rows in pending_samples). When
    // exceeded, the oldest pending samples are dropped so an extended cloud
    // outage cannot fill the device disk (helm PV is 1Gi). See
    // buffer::DEFAULT_MAX_ROWS for the sizing rationale (~68 bytes/row).
    pub buffer_max_rows: u64,
    // OpenTelemetry OTLP HTTP exporter endpoint (e.g. http://jaeger:4318)
    pub otlp_endpoint: Option<String>,

    // --- Credentials: either provided directly (legacy) or obtained via a
    //     one-time enrollment code exchanged at first start. All optional
    //     here; resolved into concrete credentials by `enroll::resolve`. ---
    /// Direct token (legacy / advanced).
    pub api_key: Option<String>,
    /// Direct agent id (legacy / advanced).
    pub agent_id: Option<String>,
    /// Direct ingest endpoint (legacy / advanced).
    pub ingest_endpoint: Option<String>,
    /// One-time enrollment code (Azure-Arc-style onboarding).
    pub enroll_code: Option<String>,
    /// Enrollment exchange URL, e.g. https://host/api/agent/enroll
    pub enroll_url: Option<String>,
}

impl Config {
    pub fn load(_file_path: Option<&str>) -> Result<Self> {
        Ok(Config {
            datasource_id: env_required("LYMON_DATASOURCE_ID")?,
            modbus_host: env_required("LYMON_MODBUS_HOST")?,
            modbus_port: env_required("LYMON_MODBUS_PORT")?.parse()?,
            poll_interval_ms: env_optional("LYMON_POLL_INTERVAL_MS", "100").parse()?,
            register_count: env_optional("LYMON_REGISTER_COUNT", "100").parse()?,
            buffer_path: env_optional("LYMON_BUFFER_PATH", "/var/lib/lymon-agent/buffer.db"),
            buffer_max_rows: env_optional(
                "LYMON_BUFFER_MAX_ROWS",
                &crate::buffer::DEFAULT_MAX_ROWS.to_string(),
            )
            .parse()?,
            otlp_endpoint: std::env::var("LYMON_OTLP_ENDPOINT").ok(),
            api_key: std::env::var("LYMON_API_KEY").ok(),
            agent_id: std::env::var("LYMON_AGENT_ID").ok(),
            ingest_endpoint: std::env::var("LYMON_INGEST_ENDPOINT").ok(),
            enroll_code: std::env::var("LYMON_ENROLL_CODE").ok(),
            enroll_url: std::env::var("LYMON_ENROLL_URL").ok(),
        })
    }

    #[allow(dead_code)]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("environment variable {key} not set"))
}

fn env_optional(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
