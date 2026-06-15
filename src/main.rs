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
mod collector;
mod config;
mod control;
mod enroll;
mod ingest_client;
mod modbus;
mod plugins;
mod pss;

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

    // Install a process-wide rustls crypto provider (ring) before any TLS. The
    // control channel's wss handshake (tokio-tungstenite) uses the process
    // default; without this, rustls 0.23 panics on first connect. Idempotent —
    // ignore the error if another component already installed one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing(cfg.otlp_endpoint.as_deref())?;

    // Resolve credentials: stored → direct env → one-time enrollment.
    let creds = enroll::resolve(&cfg).await.map_err(|e| {
        error!(error = %e, "failed to resolve agent credentials");
        e
    })?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        agent_id = %creds.agent_id,
        datasource_id = %cfg.datasource_id,
        ingest_endpoint = %creds.ingest_endpoint,
        buffer_path = %cfg.buffer_path,
        buffer_max_rows = cfg.buffer_max_rows,
        "lymon-agent starting (Día 3 — durable SQLite WAL buffer)"
    );

    // Open the durable buffer. Crashes here are fatal — better to fail loud
    // than to start without a buffer and risk silent data loss.
    let buffer = Arc::new(
        BufferDb::open(&cfg.buffer_path, cfg.buffer_max_rows).map_err(|e| {
            error!(error = %e, "failed to open buffer database");
            e
        })?,
    );

    let (pending, in_flight) = buffer.counts().await?;
    info!(pending, in_flight, "buffer opened");

    // NOTE: the standalone Modbus poller (Spike A "Día 3") was removed — Modbus
    // is now collected only when an operator provisions a Modbus connector/ingest,
    // via the Phase-2 collector below (no more unconditional CHANGE_ME:502 polling).

    // Agent-as-gateway control channel. Opens only when the agent enrolled
    // with a tenant + control endpoint (modern enrollment); legacy/env creds
    // skip it. Best-effort + self-reconnecting, so a failure here never stops
    // ingestion. PR1: connects + heartbeats + stores provisioned datasources;
    // query execution (local adapters) lands in PR2.
    // Phase 2 collector: owns the buffer poll loops for provisioned ingests and
    // backs the control channel's connector store. Provision frames drive it.
    // Third-party connector plugins (execd) are discovered from the plugins dir.
    let plugins_dir = cfg.plugins_dir.clone().unwrap_or_else(|| {
        std::path::Path::new(&cfg.buffer_path)
            .parent()
            .map(|p| p.join("plugins").to_string_lossy().into_owned())
            .unwrap_or_else(|| "plugins".to_string())
    });
    let plugins = crate::plugins::PluginHost::discover(&plugins_dir, &cfg.plugins_allow);
    // Capture plugin-served types before the host moves into the collector, so
    // we can advertise them as control-channel capabilities (Browse/Test route
    // a query_request for e.g. `opcua` to its plugin).
    let plugin_types = plugins.types();
    let collector = crate::collector::Collector::new(buffer.clone(), plugins);

    let control_handle = if creds.tenant_id.is_some() && creds.control_endpoint.is_some() {
        let creds = creds.clone();
        let collector = collector.clone();
        Some(tokio::spawn(async move {
            // Advertise the adapters this agent can run locally (gateway queries
            // + collection): built-ins + every type served by a loaded plugin.
            let mut capabilities = vec!["pss".to_string(), "modbus".to_string()];
            capabilities.extend(plugin_types);
            control::run(creds, capabilities, collector).await;
        }))
    } else {
        info!("control channel not configured (no tenant/control endpoint in credentials); gateway queries disabled");
        None
    };

    // Run the streamer in the foreground. It reconnects with backoff forever.
    let streamer = BufferStreamer::new(
        buffer.clone(),
        creds.ingest_endpoint.clone(),
        creds.token.clone(),
        creds.agent_id.clone(),
        cfg.datasource_id.clone(),
    );

    let result = streamer.run().await;

    if let Some(h) = control_handle {
        h.abort();
    }
    result
}

fn init_tracing(otlp_endpoint: Option<&str>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lymon_agent=info,tokio_modbus=warn"));

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true).json();

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    match otlp_endpoint {
        Some(endpoint) => {
            let exporter = SpanExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/v1/traces"))
                .with_timeout(Duration::from_secs(3))
                .build()
                .context("creating OTLP exporter")?;

            let resource = Resource::new(vec![
                KeyValue::new("service.name", "lymon-agent"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ]);

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
