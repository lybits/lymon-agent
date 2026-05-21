// gRPC streaming client for the Lymon Ingest Gateway.
//
// Día 2: naive single-attempt streaming. No retry, no buffer.
// If the connection fails, the agent exits and the container
// orchestrator (compose `restart: unless-stopped`) restarts it.
//
// Día 3 will introduce a SQLite WAL buffer so reconnect can resume
// from a cursor without sample loss.

use anyhow::{Context, Result};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
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

    /// Run the ingest stream once. Consumes `self` and `sample_rx`.
    /// Returns when the channel is exhausted or the stream errors.
    pub async fn run(self, sample_rx: mpsc::Receiver<Vec<Sample>>) -> Result<()> {
        // Transform Vec<Sample> → SampleBatch inline (no spawn — keeps the
        // stream `'static + Send` as tonic requires while not aliasing rx).
        let agent_id = self.agent_id.clone();
        let datasource_id = self.datasource_id.clone();
        let outbound = ReceiverStream::new(sample_rx).map(move |samples| SampleBatch {
            batch_id: Ulid::new().to_string(),
            agent_id: agent_id.clone(),
            datasource_id: datasource_id.clone(),
            sent_at_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            samples,
        });

        // Build endpoint + connect.
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .context("invalid ingest endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));

        let channel = endpoint.connect().await.context("ingest connect failed")?;

        // Interceptor that injects the API key on every request.
        let api_key = self.api_key.clone();
        let mut client =
            IngestServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
                req.metadata_mut().insert(
                    "x-api-key",
                    MetadataValue::try_from(&api_key).expect("api key not ascii"),
                );
                Ok(req)
            });

        info!(endpoint = %self.endpoint, "opening gRPC stream to ingest");
        let response = client
            .stream_samples(Request::new(outbound))
            .await
            .context("StreamSamples open failed")?;

        // Drain ACKs until the server closes the stream.
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
                    return Ok(());
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e).context("error reading ACK"));
                }
            }
        }
    }
}
