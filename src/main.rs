// Copyright 2026 Lybits
// Licensed under the Apache License, Version 2.0

use anyhow::{Context, Result};
use clap::Parser;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

mod buffer;
mod config;
mod ingest_client;
mod modbus;

// Generated protobuf types.
pub mod generated {
    pub mod lymon {
        pub mod ingest {
            pub mod v1 {
                tonic::include_proto!("lymon.ingest.v1");
            }
        }
    }
}

use crate::buffer::BufferDb;
use crate::ingest_client::BufferStreamer;
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
    let cli = Cli::parse();
    let cfg = config::Config::load(cli.config.as_deref())?;

    init_tracing(cfg.otlp_endpoint.as_deref())?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        agent_id = %cfg.agent_id,
        datasource_id = %cfg.datasource_id,
        ingest_endpoint = %cfg.ingest_endpoint,
        modbus = format!("{}:{}", cfg.modbus_host, cfg.modbus_port),
        poll_ms = cfg.poll_interval_ms,
        registers = cfg.register_count,
        buffer_path = %cfg.buffer_path,
        "lymon-agent starting (Día 3 — durable SQLite WAL buffer)"
    );

    // Open the durable buffer. Crashes here are fatal — better to fail loud
    // than to start without a buffer and risk silent data loss.
    let buffer = Arc::new(
        BufferDb::open(&cfg.buffer_path)
            .map_err(|e| {
                error!(error = %e, "failed to open buffer database");
                e
            })?,
    );

    let (pending, in_flight) = buffer.counts().await?;
    info!(pending, in_flight, "buffer opened");

    // Spawn the Modbus reader. It pushes samples into the durable buffer.
    let modbus_handle = {
        let buffer = buffer.clone();
        let mut modbus = ModbusClient::new(
            cfg.modbus_host.clone(),
            cfg.modbus_port,
            cfg.register_count,
        );
        let interval = Duration::from_millis(cfg.poll_interval_ms);

        tokio::spawn(async move {
            loop {
                match modbus.poll().await {
                    Ok(samples) => {
                        if let Err(e) = buffer.enqueue(samples).await {
                            error!(error = %e, "failed to enqueue samples to buffer");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Modbus poll failed; retry in 1s");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
                tokio::time::sleep(interval).await;
            }
        })
    };

    // Run the streamer in the foreground. It reconnects with backoff forever.
    let streamer = BufferStreamer::new(
        buffer.clone(),
        cfg.ingest_endpoint.clone(),
        cfg.api_key.clone(),
        cfg.agent_id.clone(),
        cfg.datasource_id.clone(),
    );

    let result = streamer.run().await;

    modbus_handle.abort();
    result
}

fn init_tracing(otlp_endpoint: Option<&str>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lymon_agent=info,tokio_modbus=warn"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .json();

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    match otlp_endpoint {
        Some(endpoint) => {
            let exporter = SpanExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/v1/traces"))
                .with_timeout(Duration::from_secs(3))
                .build()
                .context("creating OTLP exporter")?;

            let resource = Resource::builder()
                .with_attribute(KeyValue::new("service.name", "lymon-agent"))
                .with_attribute(KeyValue::new(
                    "service.version",
                    env!("CARGO_PKG_VERSION"),
                ))
                .build();

            let provider = TracerProvider::builder()
                .with_batch_exporter(exporter, runtime::Tokio)
                .with_resource(resource)
                .build();

            let tracer = provider.tracer("lymon-agent");
            opentelemetry::global::set_tracer_provider(provider);

            registry.with(OpenTelemetryLayer::new(tracer)).init();
            info!(endpoint, "OpenTelemetry tracing enabled");
        }
        None => {
            registry.init();
        }
    }

    Ok(())
}
