// Durable SQLite WAL buffer for samples awaiting upload.
//
// Design:
//
//   pending_samples (one row per sample)
//     id              autoincrement
//     variable_id     text
//     ts_ms           int
//     value           real
//     quality         int
//     attrs_json      text nullable
//     batch_id        text nullable  — NULL until claimed; set when in-flight
//
//   in_flight_batches (one row per batch claimed but not yet acked)
//     batch_id        primary key
//     sent_at_ms      int
//
// State machine for a sample row:
//   1. enqueue              → batch_id = NULL                 (PENDING)
//   2. claim_batch (sender) → batch_id = <ulid>, in_flight+1  (IN_FLIGHT)
//   3a. ack_ok              → row DELETED                     (DELIVERED)
//   3b. ack_retry           → batch_id = NULL, in_flight−1    (PENDING again)
//
// On agent restart, any rows with batch_id != NULL are recovered as
// in-flight batches and retried; idempotency on the server side
// (ingested_batches table) ensures no duplicates.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::debug;
use ulid::Ulid;

use crate::generated::lymon::ingest::v1::Sample;

#[derive(Debug)]
pub struct ClaimedBatch {
    pub batch_id: String,
    pub samples: Vec<Sample>,
}

pub struct BufferDb {
    conn: Arc<Mutex<Connection>>,
}

impl BufferDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating buffer dir {parent:?}"))?;
        }

        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .with_context(|| format!("opening buffer at {path:?}"))?;

        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -8192;

            CREATE TABLE IF NOT EXISTS pending_samples (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                variable_id  TEXT NOT NULL,
                ts_ms        INTEGER NOT NULL,
                value        REAL NOT NULL,
                quality      INTEGER NOT NULL DEFAULT 0,
                attrs_json   TEXT,
                batch_id     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pending_batch
                ON pending_samples (batch_id);

            CREATE INDEX IF NOT EXISTS idx_pending_unclaimed
                ON pending_samples (id) WHERE batch_id IS NULL;

            CREATE TABLE IF NOT EXISTS in_flight_batches (
                batch_id    TEXT PRIMARY KEY,
                sent_at_ms  INTEGER NOT NULL
            );
            "#,
        )
        .context("applying buffer schema")?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Append samples to the buffer.
    #[tracing::instrument(skip(self, samples), fields(sample_count = samples.len()))]
    pub async fn enqueue(&self, samples: Vec<Sample>) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = conn.lock().expect("buffer lock poisoned");
            let tx = conn.transaction()?;
            {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO pending_samples \
                     (variable_id, ts_ms, value, quality, attrs_json) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for s in &samples {
                    let attrs_json = if s.attrs.is_empty() {
                        None
                    } else {
                        Some(serde_json::to_string(&s.attrs)?)
                    };
                    stmt.execute(params![
                        s.variable_id,
                        s.ts_ms,
                        s.value,
                        s.quality,
                        attrs_json,
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    /// Atomically claim up to `max_size` unbatched samples and tag them with
    /// a new batch_id. Returns None if the buffer is empty.
    #[tracing::instrument(skip(self))]
    pub async fn claim_batch(&self, max_size: usize) -> Result<Option<ClaimedBatch>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<ClaimedBatch>> {
            let mut conn = conn.lock().expect("buffer lock poisoned");
            let tx = conn.transaction()?;

            let ids: Vec<i64> = {
                let mut stmt = tx.prepare_cached(
                    "SELECT id FROM pending_samples \
                     WHERE batch_id IS NULL \
                     ORDER BY id LIMIT ?1",
                )?;
                let rows = stmt.query_map([max_size as i64], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<Vec<i64>>>()?
            };

            if ids.is_empty() {
                return Ok(None);
            }

            let batch_id = Ulid::new().to_string();
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            {
                let mut stmt =
                    tx.prepare_cached("UPDATE pending_samples SET batch_id = ?1 WHERE id = ?2")?;
                for id in &ids {
                    stmt.execute(params![batch_id, id])?;
                }
            }

            tx.execute(
                "INSERT INTO in_flight_batches (batch_id, sent_at_ms) VALUES (?1, ?2)",
                params![batch_id, now_ms],
            )?;

            let samples = load_samples_for_batch(&tx, &batch_id)?;

            tx.commit()?;

            debug!(
                batch_id = %batch_id,
                count = samples.len(),
                "claimed batch from buffer"
            );

            Ok(Some(ClaimedBatch { batch_id, samples }))
        })
        .await?
    }

    /// Acknowledge successful delivery (ACK_OK or ACK_DUPLICATE or ACK_REJECTED).
    /// Removes the samples from the buffer permanently.
    pub async fn ack_ok(&self, batch_id: String) -> Result<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = conn.lock().expect("buffer lock poisoned");
            let tx = conn.transaction()?;
            tx.execute(
                "DELETE FROM pending_samples WHERE batch_id = ?1",
                params![batch_id],
            )?;
            tx.execute(
                "DELETE FROM in_flight_batches WHERE batch_id = ?1",
                params![batch_id],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    /// Acknowledge transient failure (ACK_RETRY). The samples go back to PENDING
    /// state and will be picked up by the next claim_batch call.
    pub async fn ack_retry(&self, batch_id: String) -> Result<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = conn.lock().expect("buffer lock poisoned");
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE pending_samples SET batch_id = NULL WHERE batch_id = ?1",
                params![batch_id],
            )?;
            tx.execute(
                "DELETE FROM in_flight_batches WHERE batch_id = ?1",
                params![batch_id],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    /// On agent startup: load any batches that were in-flight when the agent
    /// last terminated. These will be re-sent (idempotency on the server side
    /// ensures no duplicates).
    pub async fn recover_in_flight(&self) -> Result<Vec<ClaimedBatch>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<ClaimedBatch>> {
            let conn = conn.lock().expect("buffer lock poisoned");

            let batch_ids: Vec<String> = {
                let mut stmt =
                    conn.prepare("SELECT batch_id FROM in_flight_batches ORDER BY sent_at_ms")?;
                let rows = stmt.query_map([], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<Vec<String>>>()?
            };

            let mut batches = Vec::with_capacity(batch_ids.len());
            for batch_id in batch_ids {
                let samples = load_samples_for_batch(&conn, &batch_id)?;
                batches.push(ClaimedBatch { batch_id, samples });
            }

            Ok(batches)
        })
        .await?
    }

    /// Counts for observability. Returns (pending, in_flight).
    pub async fn counts(&self) -> Result<(i64, i64)> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<(i64, i64)> {
            let conn = conn.lock().expect("buffer lock poisoned");
            let pending: i64 = conn.query_row(
                "SELECT count(*) FROM pending_samples WHERE batch_id IS NULL",
                [],
                |row| row.get(0),
            )?;
            let in_flight: i64 = conn.query_row(
                "SELECT count(*) FROM pending_samples WHERE batch_id IS NOT NULL",
                [],
                |row| row.get(0),
            )?;
            Ok((pending, in_flight))
        })
        .await?
    }
}

fn load_samples_for_batch(
    conn: &impl std::ops::Deref<Target = Connection>,
    batch_id: &str,
) -> Result<Vec<Sample>> {
    let mut stmt = conn.prepare_cached(
        "SELECT variable_id, ts_ms, value, quality, attrs_json \
         FROM pending_samples WHERE batch_id = ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map([batch_id], |row| {
        let attrs_json: Option<String> = row.get(4)?;
        let attrs: std::collections::HashMap<String, String> = match attrs_json {
            Some(s) => serde_json::from_str(&s).unwrap_or_default(),
            None => std::collections::HashMap::new(),
        };
        Ok(Sample {
            variable_id: row.get(0)?,
            ts_ms: row.get(1)?,
            value: row.get(2)?,
            quality: row.get::<_, i64>(3)? as u32,
            attrs,
        })
    })?;
    let samples = rows.collect::<rusqlite::Result<Vec<Sample>>>()?;
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample(var: &str, ts: i64, val: f64) -> Sample {
        Sample {
            variable_id: var.to_string(),
            ts_ms: ts,
            value: val,
            quality: 0,
            attrs: Default::default(),
        }
    }

    #[tokio::test]
    async fn enqueue_claim_ack_roundtrip() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db")).unwrap();

        buffer
            .enqueue(vec![sample("v1", 100, 1.0), sample("v2", 200, 2.0)])
            .await
            .unwrap();

        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!((pending, in_flight), (2, 0));

        let claimed = buffer.claim_batch(10).await.unwrap().expect("has batch");
        assert_eq!(claimed.samples.len(), 2);
        assert_eq!(claimed.samples[0].variable_id, "v1");

        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!((pending, in_flight), (0, 2));

        buffer.ack_ok(claimed.batch_id).await.unwrap();

        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!((pending, in_flight), (0, 0));
    }

    #[tokio::test]
    async fn ack_retry_returns_samples_to_pending() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db")).unwrap();

        buffer.enqueue(vec![sample("v1", 100, 1.0)]).await.unwrap();
        let claimed = buffer.claim_batch(10).await.unwrap().unwrap();
        buffer.ack_retry(claimed.batch_id).await.unwrap();

        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!((pending, in_flight), (1, 0));

        // Same sample should be claimable again with a fresh batch_id
        let new_claimed = buffer.claim_batch(10).await.unwrap().unwrap();
        assert_eq!(new_claimed.samples.len(), 1);
    }

    #[tokio::test]
    async fn recover_in_flight_after_simulated_crash() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("buf.db");

        let buffer = BufferDb::open(&db_path).unwrap();
        buffer
            .enqueue(vec![sample("v1", 100, 1.0), sample("v2", 200, 2.0)])
            .await
            .unwrap();
        let claimed = buffer.claim_batch(10).await.unwrap().unwrap();
        let original_batch_id = claimed.batch_id.clone();
        // Simulate crash: drop the buffer without ack
        drop(buffer);

        // Re-open
        let buffer = BufferDb::open(&db_path).unwrap();
        let recovered = buffer.recover_in_flight().await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch_id, original_batch_id);
        assert_eq!(recovered[0].samples.len(), 2);

        // After recovery, the (pending, in_flight) shows in_flight=2
        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!((pending, in_flight), (0, 2));
    }

    #[tokio::test]
    async fn claim_returns_none_when_empty() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db")).unwrap();
        assert!(buffer.claim_batch(10).await.unwrap().is_none());
    }
}
