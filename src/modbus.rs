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
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::time::timeout;
use tokio_modbus::client::tcp;
use tokio_modbus::prelude::*;
use tracing::{info, warn};

pub struct ModbusClient {
    host: String,
    port: u16,
    /// Per-operation I/O deadline (connect and each read).
    io_timeout: Duration,
    ctx: Option<tokio_modbus::client::Context>,
}

impl ModbusClient {
    pub fn new(host: String, port: u16, poll_interval: Duration) -> Self {
        // Proportional to the poll cadence so slow links with long intervals
        // get more slack, but bounded: never under 5s (TCP handshakes over
        // WAN routinely take seconds) and never over 30s (a stuck poll should
        // not stall capture for minutes).
        let io_timeout = (poll_interval * 3).clamp(Duration::from_secs(5), Duration::from_secs(30));
        Self {
            host,
            port,
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

    /// Read `count` registers starting at `start` (holding, or input when
    /// `input=true`), reconnecting on failure. Used by the Phase-2 collector to
    /// service a provisioned Modbus ingest's selection. Returns raw register
    /// words; the caller scales/names them.
    pub async fn read(&mut self, start: u16, count: u16, input: bool) -> Result<Vec<u16>> {
        if self.ctx.is_none() {
            self.connect().await?;
        }
        let ctx = self.ctx.as_mut().expect("connected");
        let result = if input {
            ctx.read_input_registers(start, count).await
        } else {
            ctx.read_holding_registers(start, count).await
        };
        match result {
            Ok(Ok(regs)) => Ok(regs),
            Ok(Err(exc)) => {
                warn!(exception = ?exc, "Modbus exception, will reconnect");
                self.ctx = None;
                anyhow::bail!("Modbus exception: {exc:?}");
            }
            Err(e) => {
                warn!(error = %e, "Modbus transport error, will reconnect");
                self.ctx = None;
                Err(e.into())
            }
        }
    }

    /// ADR 49 W2.1 — write holding register(s) starting at `addr`, reconnecting
    /// on failure. One word → write-single-register (FC 06); many → write-
    /// multiple-registers (FC 16). Same half-open-PLC timeout protection as read.
    pub async fn write_holding(&mut self, addr: u16, values: &[u16]) -> Result<()> {
        if self.ctx.is_none() {
            self.connect().await?;
        }
        let ctx = self.ctx.as_mut().expect("connected");
        let result = if values.len() == 1 {
            ctx.write_single_register(addr, values[0]).await
        } else {
            ctx.write_multiple_registers(addr, values).await
        };
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(exc)) => {
                warn!(exception = ?exc, "Modbus write exception, will reconnect");
                self.ctx = None;
                anyhow::bail!("Modbus exception: {exc:?}");
            }
            Err(e) => {
                warn!(error = %e, "Modbus write transport error, will reconnect");
                self.ctx = None;
                Err(e.into())
            }
        }
    }

    /// ADR 49 W2.1 — write a single coil (FC 05), reconnecting on failure.
    pub async fn write_coil(&mut self, addr: u16, on: bool) -> Result<()> {
        if self.ctx.is_none() {
            self.connect().await?;
        }
        let ctx = self.ctx.as_mut().expect("connected");
        let result = ctx.write_single_coil(addr, on).await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(exc)) => {
                warn!(exception = ?exc, "Modbus write exception, will reconnect");
                self.ctx = None;
                anyhow::bail!("Modbus exception: {exc:?}");
            }
            Err(e) => {
                warn!(error = %e, "Modbus write transport error, will reconnect");
                self.ctx = None;
                Err(e.into())
            }
        }
    }
}
