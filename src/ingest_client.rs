// gRPC streaming client for the Lymon Ingest Gateway.
//
// Día 2 implementation: naive direct send (no buffer, no persistence).
// Día 3 will add a SQLite WAL buffer between Modbus reader and this client.

use anyhow::{Context, Result};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tonic::Request;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::generated::lymon::ingest::v1::{
    ingest_service_client::IngestServiceClient, AckStatus, Sample, SampleBatch,
};

pub struct IngestClient {
    endpoint: String,
    api_key: String,
    agent_id: String,
    datasource_id: String,
}

impl IngestClient {
    pub fn new(
        endpoint: String,
        api_key: String,
        agent_id: String,
        datasource_id: String,
    ) -> Self {
        Self {
            endpoint,
            api_key,
            agent_id,
            datasource_id,
        }
    }

    /// Main loop. Consumes batches of samples from `rx` and pushes them to the
    /// ingest server via a single bidirectional gRPC stream. Reconnects
    /// indefinitely with backoff on transport failure.
    pub async fn run(&self, mut rx: mpsc::Receiver<Vec<Sample>>) -> Result<()> {
        loop {
            match self.run_one_stream(&mut rx).await {
                Ok(()) => {
                    info!("ingest stream closed (channel exhausted); exiting");
                    return Ok(());
                }
                Err(e) => {
                    warn!(error = %e, "ingest stream error; reconnecting in 2s");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn run_one_stream(&self, rx: &mut mpsc::Receiver<Vec<Sample>>) -> Result<()> {
        // Build channel with API key interceptor
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .context("invalid ingest endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));

        let channel = endpoint.connect().await.context("ingest connect failed")?;

        let api_key = self.api_key.clone();
        let mut client = IngestServiceClient::with_interceptor(
            channel,
            move |mut req: Request<()>| {
                req.metadata_mut().insert(
                    "x-api-key",
                    MetadataValue::try_from(&api_key).expect("api key not ascii"),
                );
                Ok(req)
            },
        );

        // Build a stream that converts incoming Sample vectors into SampleBatch
        // protobuf messages, generating a new ULID per batch.
        let (batch_tx, batch_rx) = mpsc::channel::<SampleBatch>(32);

        let agent_id = self.agent_id.clone();
        let datasource_id = self.datasource_id.clone();

        // Producer task: pulls samples, wraps them as SampleBatch, pushes to batch_tx
        let producer = tokio::spawn(async move {
            while let Some(samples) = rx.recv().await {
                let batch = SampleBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent_id.clone(),
                    datasource_id: datasource_id.clone(),
                    sent_at_ms: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0),
                    samples,
                };
                if batch_tx.send(batch).await.is_err() {
                    break;
                }
            }
        });

        // Open the stream
        let outbound = ReceiverStream::new(batch_rx);
        info!(endpoint = %self.endpoint, "opening gRPC stream to ingest");
        let response = client
            .stream_samples(Request::new(outbound))
            .await
            .context("StreamSamples open failed")?;

        // Consume ACKs
        let mut inbound = response.into_inner();
        loop {
            match inbound.message().await {
                Ok(Some(ack)) => {
                    let status = AckStatus::try_from(ack.status).unwrap_or(AckStatus::Unspecified);
                    match status {
                        AckStatus::Ok => debug!(batch_id = %ack.batch_id, "ACK OK"),
                        AckStatus::Duplicate => {
                            debug!(batch_id = %ack.batch_id, "ACK DUPLICATE (idempotent)");
                        }
                        AckStatus::Rejected => {
                            warn!(batch_id = %ack.batch_id, detail = %ack.detail, "ACK REJECTED");
                        }
                        AckStatus::Retry => {
                            warn!(batch_id = %ack.batch_id, detail = %ack.detail, "ACK RETRY");
                        }
                        AckStatus::Unspecified => {
                            warn!(batch_id = %ack.batch_id, "ACK UNSPECIFIED — protocol error");
                        }
                    }
                }
                Ok(None) => {
                    info!("server closed ACK stream cleanly");
                    producer.abort();
                    return Ok(());
                }
                Err(e) => {
                    producer.abort();
                    return Err(anyhow::Error::new(e).context("error reading ACK"));
                }
            }
        }
    }
}
