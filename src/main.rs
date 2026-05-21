// Copyright 2026 Lybits
// Licensed under the Apache License, Version 2.0

use anyhow::Result;
use clap::Parser;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod config;
mod ingest_client;
mod modbus;

// Generated protobuf types live here.
pub mod generated {
    pub mod lymon {
        pub mod ingest {
            pub mod v1 {
                tonic::include_proto!("lymon.ingest.v1");
            }
        }
    }
}

use crate::generated::lymon::ingest::v1::Sample;
use crate::ingest_client::IngestClient;
use crate::modbus::ModbusClient;

#[derive(Parser, Debug)]
#[command(version, about = "Lymon Edge Agent")]
struct Cli {
    /// Optional path to a config file. Env vars take precedence.
    #[arg(long, env = "LYMON_CONFIG")]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let cfg = config::Config::load(cli.config.as_deref())?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        agent_id = %cfg.agent_id,
        datasource_id = %cfg.datasource_id,
        ingest_endpoint = %cfg.ingest_endpoint,
        modbus = format!("{}:{}", cfg.modbus_host, cfg.modbus_port),
        poll_ms = cfg.poll_interval_ms,
        registers = cfg.register_count,
        "lymon-agent starting (Día 2 — naive end-to-end, no buffer yet)"
    );

    // Channel between Modbus reader task and gRPC sender task.
    // Bounded so a slow ingest exerts backpressure on the reader instead of
    // unbounded growth. Día 3 replaces this with a durable SQLite buffer.
    let (tx, rx) = mpsc::channel::<Vec<Sample>>(256);

    // Spawn the Modbus reader.
    let modbus_handle = {
        let mut modbus =
            ModbusClient::new(cfg.modbus_host.clone(), cfg.modbus_port, cfg.register_count);
        let interval = Duration::from_millis(cfg.poll_interval_ms);

        tokio::spawn(async move {
            loop {
                match modbus.poll().await {
                    Ok(samples) => {
                        if tx.send(samples).await.is_err() {
                            error!("ingest channel closed — stopping Modbus reader");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Modbus poll failed; retrying in 1s");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
                tokio::time::sleep(interval).await;
            }
        })
    };

    // Run the ingest client in the foreground until the channel closes.
    let ingest = IngestClient::new(
        cfg.ingest_endpoint.clone(),
        cfg.api_key.clone(),
        cfg.agent_id.clone(),
        cfg.datasource_id.clone(),
    );

    let result = ingest.run(rx).await;

    modbus_handle.abort();
    result
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lymon_agent=info,tokio_modbus=warn"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .json()
        .init();
}
