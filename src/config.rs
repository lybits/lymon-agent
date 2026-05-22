// Configuration loaded from environment variables.
// File-based config can be added in Fase 1.

use anyhow::{Context, Result};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub agent_id: String,
    pub datasource_id: String,
    pub api_key: String,
    pub ingest_endpoint: String,
    pub modbus_host: String,
    pub modbus_port: u16,
    pub poll_interval_ms: u64,
    pub register_count: u16,
    // Día 3: location of the SQLite WAL buffer
    #[allow(dead_code)]
    pub buffer_path: String,
    // OpenTelemetry OTLP HTTP exporter endpoint (e.g. http://jaeger:4318)
    pub otlp_endpoint: Option<String>,
}

impl Config {
    pub fn load(_file_path: Option<&str>) -> Result<Self> {
        // Día 1: env-only loading. File loading deferred to Fase 1.
        Ok(Config {
            agent_id: env_required("LYMON_AGENT_ID")?,
            datasource_id: env_required("LYMON_DATASOURCE_ID")?,
            api_key: env_required("LYMON_API_KEY")?,
            ingest_endpoint: env_required("LYMON_INGEST_ENDPOINT")?,
            modbus_host: env_required("LYMON_MODBUS_HOST")?,
            modbus_port: env_required("LYMON_MODBUS_PORT")?.parse()?,
            poll_interval_ms: env_optional("LYMON_POLL_INTERVAL_MS", "100").parse()?,
            register_count: env_optional("LYMON_REGISTER_COUNT", "100").parse()?,
            buffer_path: env_optional("LYMON_BUFFER_PATH", "/var/lib/lymon-agent/buffer.db"),
            otlp_endpoint: std::env::var("LYMON_OTLP_ENDPOINT").ok(),
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
