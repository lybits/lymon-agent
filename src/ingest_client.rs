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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{ClientTlsConfig, Endpoint};
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
    /// errors. Backoff is reset by `run_one_attempt` whenever a batch is
    /// successfully ACKed, so a long outage followed by a quick recovery
    /// doesn't penalize subsequent reconnects.
    pub async fn run(self) -> Result<()> {
        let backoff_secs = Arc::new(AtomicU64::new(1));
        let max_backoff: u64 = 60;

        loop {
            match self.run_one_attempt(backoff_secs.clone()).await {
                Ok(()) => {
                    info!("ingest stream ended cleanly");
                    backoff_secs.store(1, Ordering::Relaxed);
                }
                Err(e) => {
                    let current = backoff_secs.load(Ordering::Relaxed);
                    warn!(
                        error = %e,
                        backoff_secs = current,
                        "stream error, will retry"
                    );
                    tokio::time::sleep(Duration::from_secs(current)).await;
                    let next = (current * 2).min(max_backoff);
                    backoff_secs.store(next, Ordering::Relaxed);
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    async fn run_one_attempt(&self, backoff_secs: Arc<AtomicU64>) -> Result<()> {
        // 1. Connect.
        //
        // IMPORTANT: do NOT set .timeout() on the Endpoint. It applies to
        // the entire RPC lifetime, which for a long-lived bidirectional
        // stream means the stream gets cancelled after N seconds even when
        // healthy. Only set connect_timeout (TCP/HTTP2 handshake).
        //
        // HTTP/2 keepalive is tight (5s ping interval, 3s response timeout,
        // active even when idle) so we detect a dead upstream within ~8s
        // rather than after tens of seconds of silent buffering.
        let mut endpoint = Endpoint::from_shared(self.endpoint.clone())
            .context("invalid ingest endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .keep_alive_timeout(Duration::from_secs(3))
            .keep_alive_while_idle(true);
        // A cloud ingest is reached over https (TLS via a public CA); a local /
        // edge ingest stays plaintext h2c. tonic needs explicit TLS for https.
        if self.endpoint.starts_with("https") {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .context("configuring TLS for ingest")?;
        }
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
                    self.send_and_ack(&out_tx, &mut inbound, batch).await?;
                    // Reset backoff: we've had a successful round-trip, so
                    // the next transport error should start from 1s again.
                    backoff_secs.store(1, Ordering::Relaxed);
                    batches_sent += 1;
                    if batches_sent % 100 == 0 {
                        debug!(
                            batches_sent,
                            samples_dropped = self.buffer.dropped_total(),
                            "progress"
                        );
                    }
                }
                None => {
                    // Buffer empty — sleep briefly to avoid a busy loop.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    #[tracing::instrument(skip(self, out_tx, inbound, claimed), fields(
        batch_id = %claimed.batch_id,
        sample_count = claimed.samples.len()
    ))]
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

        // Resolve the ACK by batch_id, never by arrival order. This streamer
        // is strictly lock-step (exactly one batch in flight per
        // send_and_ack call), so id-based resolution reduces to: keep
        // reading ACKs until one names the in-flight batch, discarding
        // unknown ones with a warn. Applying a mismatched ACK's status to
        // the in-flight batch could delete undelivered samples from the
        // buffer (breaking exactly-once) if the server ever pipelines,
        // duplicates, or reorders ACKs. A full in-flight map only becomes
        // necessary if the streamer itself starts pipelining batches.
        let (status, detail) = loop {
            let ack = match inbound.message().await? {
                Some(a) => a,
                None => anyhow::bail!("server closed ACK stream unexpectedly"),
            };
            match resolve_ack(&ack, &batch_id) {
                Some(status) => break (status, ack.detail),
                None => warn!(
                    expected = %batch_id,
                    received = %ack.batch_id,
                    "discarding ACK for unknown batch (not in flight)"
                ),
            }
        };
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
                    detail = %detail,
                    "batch REJECTED — dropping samples"
                );
                // Permanently drop rather than retry forever.
                self.buffer.ack_ok(batch_id).await?;
            }
            AckStatus::Retry => {
                warn!(
                    batch_id = %batch_id,
                    detail = %detail,
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

/// Match an incoming ACK against the batch currently in flight. Returns the
/// decoded status when the ACK names that batch, or `None` when it references
/// a different (unknown) batch and must be discarded — a status must never be
/// applied to a batch the ACK does not name.
fn resolve_ack(ack: &BatchAck, in_flight_batch_id: &str) -> Option<AckStatus> {
    if ack.batch_id != in_flight_batch_id {
        return None;
    }
    Some(AckStatus::try_from(ack.status).unwrap_or(AckStatus::Unspecified))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(batch_id: &str, status: i32) -> BatchAck {
        BatchAck {
            batch_id: batch_id.to_string(),
            status,
            detail: String::new(),
        }
    }

    #[test]
    fn resolve_ack_matches_in_flight_batch() {
        let a = ack("01ARZ3NDEKTSV4RRFFQ69G5FAV", AckStatus::Ok as i32);
        assert_eq!(
            resolve_ack(&a, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            Some(AckStatus::Ok)
        );
    }

    #[test]
    fn resolve_ack_discards_unknown_batch() {
        // An ACK for a batch we don't have in flight must NOT resolve — its
        // status (here RETRY) must never be applied to the in-flight batch.
        let a = ack("other-batch", AckStatus::Retry as i32);
        assert_eq!(resolve_ack(&a, "in-flight-batch"), None);
    }

    #[test]
    fn resolve_ack_decodes_every_status_for_matching_batch() {
        for status in [
            AckStatus::Ok,
            AckStatus::Duplicate,
            AckStatus::Rejected,
            AckStatus::Retry,
        ] {
            let a = ack("b1", status as i32);
            assert_eq!(resolve_ack(&a, "b1"), Some(status));
        }
    }

    #[test]
    fn resolve_ack_maps_unknown_status_to_unspecified() {
        // Unknown enum value from a newer server → Unspecified (treated as
        // retry by the caller), not a panic or a silent drop.
        let a = ack("b1", 999);
        assert_eq!(resolve_ack(&a, "b1"), Some(AckStatus::Unspecified));
    }
}
