// gRPC streaming client backed by a durable SQLite WAL buffer.
//
// Día 3 architecture:
//
//   Modbus reader → buffer.enqueue()  (durable, survives crash)
//                       ↓
//          BufferStreamer.run():
//            - On startup: recover in-flight batches
//            - Loop: claim batch from buffer → send via gRPC → wait ACK → mark buffer
//            - On stream/connection error: exponential backoff, reconnect
//
// Exactly-once delivery: server-side idempotency on batch_id ensures that
// resending an in-flight batch after agent restart does not duplicate.

use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tonic::Request;
use tracing::{debug, info, warn};

use crate::buffer::{BufferDb, ClaimedBatch};
use crate::generated::lymon::ingest::v1::{
    ingest_service_client::IngestServiceClient, AckStatus, BatchAck, SampleBatch,
};

pub struct BufferStreamer {
    buffer: Arc<BufferDb>,
    endpoint: String,
    api_key: String,
    agent_id: String,
    datasource_id: String,
    max_batch_size: usize,
}

impl BufferStreamer {
    pub fn new(
        buffer: Arc<BufferDb>,
        endpoint: String,
        api_key: String,
        agent_id: String,
        datasource_id: String,
    ) -> Self {
        Self {
            buffer,
            endpoint,
            api_key,
            agent_id,
            datasource_id,
            max_batch_size: 1000,
        }
    }

    /// Main loop. Reconnects with exponential backoff on stream/transport
    /// errors. Only returns Ok(()) if the program is being intentionally
    /// shut down (currently: never).
    pub async fn run(self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        loop {
            match self.run_one_attempt().await {
                Ok(()) => {
                    info!("ingest stream ended cleanly");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "stream error, will retry"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    async fn run_one_attempt(&self) -> Result<()> {
        // 1. Connect.
        //
        // IMPORTANT: do NOT set .timeout() on the Endpoint. It applies to
        // the entire RPC lifetime, which for a long-lived bidirectional
        // stream means the stream gets cancelled after N seconds even when
        // healthy. Only set connect_timeout (TCP/HTTP2 handshake).
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .context("invalid ingest endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10));
        let channel = endpoint.connect().await.context("ingest connect failed")?;

        let api_key = self.api_key.clone();
        let mut client =
            IngestServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
                req.metadata_mut().insert(
                    "x-api-key",
                    MetadataValue::try_from(&api_key).expect("api key not ascii"),
                );
                Ok(req)
            });

        // 2. Set up the outgoing batch stream.
        let (out_tx, out_rx) = mpsc::channel::<SampleBatch>(8);
        let outbound = ReceiverStream::new(out_rx);

        info!(endpoint = %self.endpoint, "opening gRPC stream to ingest");
        let response = client.stream_samples(Request::new(outbound)).await?;
        let mut inbound = response.into_inner();

        // 3. Recover and resend any batches that were in-flight when the agent
        //    last terminated. Server idempotency protects against duplicates.
        let in_flight = self.buffer.recover_in_flight().await?;
        if !in_flight.is_empty() {
            info!(
                count = in_flight.len(),
                "recovering in-flight batches from previous run"
            );
        }
        for batch in in_flight {
            self.send_and_ack(&out_tx, &mut inbound, batch).await?;
        }

        // 4. Main loop: claim a batch from the buffer, send, wait for ACK, process.
        info!("entering main send loop");
        let mut batches_sent: u64 = 0;
        loop {
            match self.buffer.claim_batch(self.max_batch_size).await? {
                Some(batch) => {
                    let batch_id = batch.batch_id.clone();
                    let n = batch.samples.len();
                    info!(batch_id = %batch_id, sample_count = n, "claimed; sending");
                    self.send_and_ack(&out_tx, &mut inbound, batch).await?;
                    batches_sent += 1;
                    if batches_sent % 10 == 0 {
                        info!(batches_sent, "progress");
                    }
                }
                None => {
                    // Buffer empty — sleep briefly to avoid a busy loop.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    async fn send_and_ack(
        &self,
        out_tx: &mpsc::Sender<SampleBatch>,
        inbound: &mut tonic::Streaming<BatchAck>,
        claimed: ClaimedBatch,
    ) -> Result<()> {
        let batch_id = claimed.batch_id.clone();
        let sample_count = claimed.samples.len();

        let proto_batch = SampleBatch {
            batch_id: batch_id.clone(),
            agent_id: self.agent_id.clone(),
            datasource_id: self.datasource_id.clone(),
            sent_at_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            samples: claimed.samples,
        };

        out_tx
            .send(proto_batch)
            .await
            .context("failed to push batch onto stream")?;

        // gRPC bidirectional streams preserve order within each direction,
        // so the next ACK corresponds to this batch.
        let ack = match inbound.message().await? {
            Some(a) => a,
            None => anyhow::bail!("server closed ACK stream unexpectedly"),
        };

        if ack.batch_id != batch_id {
            warn!(
                expected = %batch_id,
                received = %ack.batch_id,
                "ACK batch_id mismatch — gRPC ordering violated?"
            );
        }

        let status = AckStatus::try_from(ack.status).unwrap_or(AckStatus::Unspecified);
        match status {
            AckStatus::Ok | AckStatus::Duplicate => {
                debug!(
                    batch_id = %batch_id,
                    sample_count,
                    status = ?status,
                    "batch ACKed, clearing buffer"
                );
                self.buffer.ack_ok(batch_id).await?;
            }
            AckStatus::Rejected => {
                warn!(
                    batch_id = %batch_id,
                    detail = %ack.detail,
                    "batch REJECTED — dropping samples"
                );
                // Permanently drop rather than retry forever.
                self.buffer.ack_ok(batch_id).await?;
            }
            AckStatus::Retry => {
                warn!(
                    batch_id = %batch_id,
                    detail = %ack.detail,
                    "batch RETRY — returning to pending"
                );
                self.buffer.ack_retry(batch_id).await?;
            }
            AckStatus::Unspecified => {
                warn!(
                    batch_id = %batch_id,
                    "ACK UNSPECIFIED — treating as retry"
                );
                self.buffer.ack_retry(batch_id).await?;
            }
        }

        Ok(())
    }
}
