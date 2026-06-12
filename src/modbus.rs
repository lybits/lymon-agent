// Modbus TCP polling client.
//
// Wraps `tokio-modbus` with reconnection on failure and converts holding
// register reads into Sample protobuf messages.
//
// Every network operation (DNS, connect, register read) is wrapped in
// `tokio::time::timeout`: a half-open connection (PLC powered off without
// sending RST, NAT mapping expired — common on plant networks) would
// otherwise block the poll future forever, silently freezing capture with
// no error logged.

use anyhow::{Context, Result};
use std::time::{Duration, SystemTime};
use tokio::net::lookup_host;
use tokio::time::timeout;
use tokio_modbus::client::tcp;
use tokio_modbus::prelude::*;
use tracing::{debug, info, warn};

use crate::generated::lymon::ingest::v1::Sample;

pub struct ModbusClient {
    host: String,
    port: u16,
    register_count: u16,
    /// Per-operation I/O deadline (connect and each read).
    io_timeout: Duration,
    ctx: Option<tokio_modbus::client::Context>,
}

impl ModbusClient {
    pub fn new(host: String, port: u16, register_count: u16, poll_interval: Duration) -> Self {
        // Proportional to the poll cadence so slow links with long intervals
        // get more slack, but bounded: never under 5s (TCP handshakes over
        // WAN routinely take seconds) and never over 30s (a stuck poll should
        // not stall capture for minutes).
        let io_timeout = (poll_interval * 3).clamp(Duration::from_secs(5), Duration::from_secs(30));
        Self {
            host,
            port,
            register_count,
            io_timeout,
            ctx: None,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        let addr_str = format!("{}:{}", self.host, self.port);

        // Resolve hostname (Docker DNS, mDNS, etc.) — SocketAddr only parses
        // IP:port, not host:port. Bounded too: a broken resolver can hang.
        let addr = timeout(self.io_timeout, lookup_host(&addr_str))
            .await
            .with_context(|| format!("DNS lookup timed out for {addr_str}"))?
            .with_context(|| format!("DNS lookup failed for {addr_str}"))?
            .next()
            .with_context(|| format!("no addresses resolved for {addr_str}"))?;

        info!(host = %addr_str, resolved = %addr, "connecting to Modbus TCP");
        // Without the timeout a SYN to a silently-dropped destination waits
        // for the kernel's TCP timeout (minutes on most OSes).
        let ctx = timeout(self.io_timeout, tcp::connect(addr))
            .await
            .with_context(|| {
                format!(
                    "Modbus connect to {addr} timed out after {:?}",
                    self.io_timeout
                )
            })?
            .with_context(|| format!("failed to connect to Modbus at {addr}"))?;
        self.ctx = Some(ctx);
        Ok(())
    }

    /// Read all configured holding registers and convert to Samples.
    /// On any error, drops the connection so the next poll reconnects.
    #[tracing::instrument(skip(self), fields(register_count = self.register_count))]
    pub async fn poll(&mut self) -> Result<Vec<Sample>> {
        if self.ctx.is_none() {
            self.connect().await?;
        }

        let ctx = self.ctx.as_mut().expect("connected");
        // Bound the read: a half-open socket never errors and never answers,
        // so an unbounded await here freezes capture indefinitely.
        let read_result = match timeout(
            self.io_timeout,
            ctx.read_holding_registers(0, self.register_count),
        )
        .await
        {
            Ok(r) => r,
            Err(_elapsed) => {
                warn!(
                    timeout = ?self.io_timeout,
                    "Modbus read timed out (half-open connection?), will reconnect"
                );
                // Drop the context: the connection is unusable and a late
                // response would desync request/response pairing anyway.
                self.ctx = None;
                anyhow::bail!("Modbus read timed out after {:?}", self.io_timeout);
            }
        };

        let regs = match read_result {
            Ok(Ok(regs)) => regs,
            Ok(Err(exc)) => {
                warn!(exception = ?exc, "Modbus exception, will reconnect");
                self.ctx = None;
                anyhow::bail!("Modbus exception: {exc:?}");
            }
            Err(e) => {
                warn!(error = %e, "Modbus transport error, will reconnect");
                self.ctx = None;
                return Err(e.into());
            }
        };

        let ts_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis() as i64;

        let samples: Vec<Sample> = regs
            .iter()
            .enumerate()
            .map(|(idx, &raw)| Sample {
                variable_id: format!("holding_register/{idx}"),
                ts_ms,
                value: raw as f64,
                quality: 0,
                attrs: Default::default(),
            })
            .collect();

        debug!(count = samples.len(), "polled Modbus registers");
        Ok(samples)
    }
}
