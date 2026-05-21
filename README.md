# Lymon Agent

The Lymon Agent is a small native Rust binary that runs on-premise in industrial environments. It connects to local data sources (PLCs, MQTT brokers, REST APIs, OPC-UA servers), buffers telemetry locally with durable storage, and forwards it to the Lymon platform — whether that is a cloud SaaS instance or an on-premise Lymon Edge.

## Status

🚧 **Spike A in progress.** Not yet production-ready. The first milestone is validating the agent→cloud pipeline with Modbus TCP as the source protocol.

## Design goals

- Single static binary, < 15 MB stripped
- No GC pauses on 24/7 industrial hosts (Rust + Tokio)
- Native Windows Service / systemd unit integration
- Durable local buffer (SQLite WAL) — agent survives cloud outages
- Exactly-once delivery via idempotency keys
- Pluggable connectors (Modbus, OPC-UA, MQTT, REST, BACnet, ...)
- Configurable via environment variables and config file

## Build

Prerequisites: Rust 1.83+, protoc, optionally Docker.

```bash
# Native build
cargo build --release

# Docker build (multi-stage, produces a distroless image)
docker build -t lymon-agent:dev .
```

## Run (standalone)

```bash
export LYMON_INGEST_ENDPOINT=http://localhost:50051
export LYMON_API_KEY=spike-secret-dev-only
export LYMON_AGENT_ID=spike-agent-01
export LYMON_DATASOURCE_ID=diagslave-modbus
export LYMON_MODBUS_HOST=127.0.0.1
export LYMON_MODBUS_PORT=5020
export LYMON_POLL_INTERVAL_MS=100
export LYMON_REGISTER_COUNT=100

./target/release/lymon-agent
```

For the full development workflow (with diagslave, Timescale, Jaeger, and the Ingest Gateway), see the [lymon-ingest-spike](https://dev.azure.com/lybits/Lymon/_git/lymon-ingest-spike) repo.

## License

Apache License 2.0 — see [LICENSE](./LICENSE).

## Contributing

Lymon Agent is open-source and welcomes contributions from the industrial automation community. Contribution guidelines and CLA will be published as the project reaches alpha.

## Roadmap

- **v0.1 (Spike A):** Modbus TCP connector, gRPC ingest, SQLite WAL buffer, exactly-once
- **v0.2 (Fase 1):** OPC-UA, MQTT, REST connectors; structured config file; auto-update channels
- **v0.3 (Fase 1):** Local UI (`localhost:7878`), Windows Service / systemd integration
- **v0.4 (Fase 2):** Bidirectional cloud channel (remote config push, command execution); edge analytics
- **v1.0:** Stable agent ↔ cloud contract; CLA in place
