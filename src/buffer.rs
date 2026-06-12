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
//
// Capacity: the table is capped at `max_rows` (high-water mark). When an
// enqueue would push past it, the OLDEST rows still in PENDING state are
// dropped in chunks (drop-oldest) — losing the oldest history is better than
// filling the device disk, which would make every enqueue fail and can
// corrupt the WAL. In-flight rows are never dropped: the streamer already
// claimed them and will DELETE/UNCLAIM them itself; touching them would
// desync the in_flight_batches bookkeeping.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, warn};
use ulid::Ulid;

use crate::generated::lymon::ingest::v1::Sample;

#[derive(Debug)]
pub struct ClaimedBatch {
    pub batch_id: String,
    pub samples: Vec<Sample>,
    /// Origin connector id for this batch, or None for the agent's default
    /// datasource (legacy). All samples in a batch share one origin.
    pub origin: Option<String>,
}

/// Default high-water mark for `pending_samples`.
///
/// Measured on the real schema (table + both indexes, Modbus-style rows with
/// no attrs): ~68 bytes/row on disk. 5M rows ≈ 340 MB of data file, which
/// leaves comfortable headroom on the 1Gi helm PV for the WAL (can transiently
/// approach the data size before checkpoint), free-page fragmentation and
/// credentials.json — well under the ~512 MB budget. At the default capture
/// rate (100 registers @ 100ms = 1000 samples/s) that is ~83 minutes of
/// offline buffering; at 1 sample/s per register it is several days.
pub const DEFAULT_MAX_ROWS: u64 = 5_000_000;

pub struct BufferDb {
    conn: Arc<Mutex<Connection>>,
    /// High-water mark: max rows allowed in pending_samples.
    max_rows: u64,
    /// Total samples dropped by the drop-oldest policy since startup.
    dropped_total: Arc<AtomicU64>,
    /// Last time we emitted the "dropping samples" warn, in unix ms.
    /// Throttles logging so a sustained overflow doesn't log per enqueue.
    last_drop_log_ms: Arc<AtomicI64>,
}

impl BufferDb {
    pub fn open(path: impl AsRef<Path>, max_rows: u64) -> Result<Self> {
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
                batch_id     TEXT,
                -- Origin connector/datasource id (Phase 2). NULL = the agent's
                -- default datasource_id (legacy Modbus path). A batch carries a
                -- single origin so the streamer attributes it correctly.
                origin       TEXT
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

        // Add the `origin` column to buffers created before Phase 2. SQLite has
        // no ADD COLUMN IF NOT EXISTS; ignore the duplicate-column error.
        match conn.execute("ALTER TABLE pending_samples ADD COLUMN origin TEXT", []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
                if msg.contains("duplicate column") => {}
            Err(e) => return Err(e).context("adding origin column"),
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            max_rows,
            dropped_total: Arc::new(AtomicU64::new(0)),
            last_drop_log_ms: Arc::new(AtomicI64::new(0)),
        })
    }

    /// Total samples dropped by the drop-oldest policy since startup.
    pub fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }

    /// Append samples to the buffer under the agent's default origin (legacy
    /// Modbus path). Equivalent to `enqueue_with_origin(None, …)`. The
    /// high-water mark / drop-oldest cap is enforced by `enqueue_with_origin`.
    pub async fn enqueue(&self, samples: Vec<Sample>) -> Result<()> {
        self.enqueue_with_origin(None, samples).await
    }

    /// Append samples tagged with an origin connector id (Phase 2 collector),
    /// enforcing the high-water mark. `origin = None` uses the agent's default
    /// datasource at send time.
    ///
    /// If the insert would push the table past `max_rows`, the oldest PENDING
    /// rows are deleted first (drop-oldest) in chunks of ~1% of the limit, so a
    /// sustained overflow amortizes the deletes instead of trimming one row per
    /// insert. In-flight rows are never touched. This is the single INSERT path
    /// (the Modbus `enqueue` wrapper and every plugin-SDK push/subscribe ingest
    /// land here), so the cap holds for all producers.
    #[tracing::instrument(skip(self, samples), fields(sample_count = samples.len()))]
    pub async fn enqueue_with_origin(
        &self,
        origin: Option<String>,
        samples: Vec<Sample>,
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let conn = self.conn.clone();
        let max_rows = self.max_rows;
        let dropped_total = self.dropped_total.clone();
        let last_drop_log_ms = self.last_drop_log_ms.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = conn.lock().expect("buffer lock poisoned");
            let tx = conn.transaction()?;

            // Drop-oldest inside the same transaction as the insert so the
            // cap holds atomically even if the agent crashes mid-enqueue.
            let total: i64 =
                tx.query_row("SELECT count(*) FROM pending_samples", [], |row| row.get(0))?;
            let projected = total as u64 + samples.len() as u64;
            if projected > max_rows {
                // Trim at least the overshoot, rounded up to a ~1% chunk so a
                // steady overflow deletes in batches, not row-by-row.
                let overshoot = projected - max_rows;
                let chunk = (max_rows / 100).max(1);
                let to_drop = overshoot.max(chunk);
                let dropped = tx.execute(
                    "DELETE FROM pending_samples WHERE id IN ( \
                         SELECT id FROM pending_samples \
                         WHERE batch_id IS NULL \
                         ORDER BY id LIMIT ?1)",
                    [to_drop as i64],
                )? as u64;
                // If everything left is in-flight there may be nothing to
                // drop; we still insert (in-flight is bounded by the
                // streamer's batch size, so this overshoot stays small).
                if dropped > 0 {
                    let total_dropped =
                        dropped_total.fetch_add(dropped, Ordering::Relaxed) + dropped;
                    // Warn at most every 30s: at 10 polls/s an unthrottled
                    // warn would flood the (possibly remote) log sink.
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let last = last_drop_log_ms.load(Ordering::Relaxed);
                    if now_ms - last >= 30_000
                        && last_drop_log_ms
                            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                            .is_ok()
                    {
                        warn!(
                            dropped,
                            total_dropped,
                            max_rows,
                            "buffer over high-water mark; dropped oldest pending samples"
                        );
                    }
                }
            }

            {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO pending_samples \
                     (variable_id, ts_ms, value, quality, attrs_json, origin) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
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
                        origin,
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

            // A batch carries a single origin so the streamer attributes it to
            // the right connector. Pick the oldest unclaimed sample's origin,
            // then claim same-origin rows only (`IS` is NULL-safe).
            let origin: Option<String> = {
                let mut stmt = tx.prepare_cached(
                    "SELECT origin FROM pending_samples WHERE batch_id IS NULL ORDER BY id LIMIT 1",
                )?;
                let mut rows = stmt.query([])?;
                match rows.next()? {
                    Some(row) => row.get(0)?,
                    None => return Ok(None),
                }
            };

            let ids: Vec<i64> = {
                let mut stmt = tx.prepare_cached(
                    "SELECT id FROM pending_samples \
                     WHERE batch_id IS NULL AND origin IS ?1 \
                     ORDER BY id LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![origin, max_size as i64], |row| row.get(0))?;
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
                origin = ?origin,
                "claimed batch from buffer"
            );

            Ok(Some(ClaimedBatch {
                batch_id,
                samples,
                origin,
            }))
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
                // All rows in a batch share one origin; read it from the first.
                let origin: Option<String> = conn
                    .query_row(
                        "SELECT origin FROM pending_samples WHERE batch_id = ?1 LIMIT 1",
                        params![batch_id],
                        |row| row.get(0),
                    )
                    .optional()?
                    .flatten();
                batches.push(ClaimedBatch {
                    batch_id,
                    samples,
                    origin,
                });
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
        let buffer = BufferDb::open(dir.path().join("buf.db"), DEFAULT_MAX_ROWS).unwrap();

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
    async fn claim_batch_groups_by_origin() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db"), DEFAULT_MAX_ROWS).unwrap();

        // Interleave two origins + a legacy (None) origin.
        buffer
            .enqueue_with_origin(Some("con_a".into()), vec![sample("a1", 1, 1.0)])
            .await
            .unwrap();
        buffer
            .enqueue_with_origin(Some("con_b".into()), vec![sample("b1", 2, 2.0)])
            .await
            .unwrap();
        buffer
            .enqueue(vec![sample("legacy", 3, 3.0)])
            .await
            .unwrap();
        buffer
            .enqueue_with_origin(Some("con_a".into()), vec![sample("a2", 4, 4.0)])
            .await
            .unwrap();

        // First claim follows the oldest sample's origin (con_a) and pulls only
        // its rows, not con_b's or the legacy one.
        let first = buffer.claim_batch(10).await.unwrap().expect("batch");
        assert_eq!(first.origin.as_deref(), Some("con_a"));
        assert_eq!(first.samples.len(), 2);
        assert!(first.samples.iter().all(|s| s.variable_id.starts_with('a')));
        buffer.ack_ok(first.batch_id).await.unwrap();

        // Next is con_b.
        let second = buffer.claim_batch(10).await.unwrap().expect("batch");
        assert_eq!(second.origin.as_deref(), Some("con_b"));
        assert_eq!(second.samples.len(), 1);
        buffer.ack_ok(second.batch_id).await.unwrap();

        // Finally the legacy (None) origin.
        let third = buffer.claim_batch(10).await.unwrap().expect("batch");
        assert_eq!(third.origin, None);
        assert_eq!(third.samples[0].variable_id, "legacy");
    }

    #[tokio::test]
    async fn ack_retry_returns_samples_to_pending() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db"), DEFAULT_MAX_ROWS).unwrap();

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

        let buffer = BufferDb::open(&db_path, DEFAULT_MAX_ROWS).unwrap();
        buffer
            .enqueue(vec![sample("v1", 100, 1.0), sample("v2", 200, 2.0)])
            .await
            .unwrap();
        let claimed = buffer.claim_batch(10).await.unwrap().unwrap();
        let original_batch_id = claimed.batch_id.clone();
        // Simulate crash: drop the buffer without ack
        drop(buffer);

        // Re-open
        let buffer = BufferDb::open(&db_path, DEFAULT_MAX_ROWS).unwrap();
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
        let buffer = BufferDb::open(dir.path().join("buf.db"), DEFAULT_MAX_ROWS).unwrap();
        assert!(buffer.claim_batch(10).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn enqueue_over_high_water_drops_oldest_pending() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db"), 10).unwrap();

        // Fill exactly to the high-water mark.
        let first: Vec<Sample> = (0..10).map(|i| sample("v", i as i64, i as f64)).collect();
        buffer.enqueue(first).await.unwrap();
        assert_eq!(buffer.counts().await.unwrap(), (10, 0));
        assert_eq!(buffer.dropped_total(), 0);

        // One more sample → the single oldest pending row must go.
        buffer.enqueue(vec![sample("v", 100, 100.0)]).await.unwrap();
        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!((pending, in_flight), (10, 0));
        assert_eq!(buffer.dropped_total(), 1);

        // Oldest survivor is ts=1 (ts=0 was dropped) and the newest is the
        // sample that triggered the trim.
        let claimed = buffer.claim_batch(100).await.unwrap().unwrap();
        assert_eq!(claimed.samples.first().unwrap().ts_ms, 1);
        assert_eq!(claimed.samples.last().unwrap().ts_ms, 100);
    }

    #[tokio::test]
    async fn drop_oldest_never_touches_in_flight_rows() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db"), 5).unwrap();

        // 3 rows claimed in-flight + 2 pending = at the cap.
        buffer
            .enqueue((0..3).map(|i| sample("inflight", i as i64, 0.0)).collect())
            .await
            .unwrap();
        let claimed = buffer.claim_batch(3).await.unwrap().unwrap();
        buffer
            .enqueue((10..12).map(|i| sample("pend", i as i64, 0.0)).collect())
            .await
            .unwrap();
        assert_eq!(buffer.counts().await.unwrap(), (2, 3));

        // 2 more → must evict 2 pending rows, never the in-flight ones.
        buffer
            .enqueue((20..22).map(|i| sample("new", i as i64, 0.0)).collect())
            .await
            .unwrap();
        let (pending, in_flight) = buffer.counts().await.unwrap();
        assert_eq!(in_flight, 3, "in-flight rows must survive the trim");
        assert_eq!(pending, 2);
        assert_eq!(buffer.dropped_total(), 2);

        // The in-flight batch is still intact and ACKable.
        buffer.ack_ok(claimed.batch_id).await.unwrap();
        assert_eq!(buffer.counts().await.unwrap(), (2, 0));

        // Survivors are the newest enqueued rows.
        let survivors = buffer.claim_batch(100).await.unwrap().unwrap();
        let vars: Vec<_> = survivors
            .samples
            .iter()
            .map(|s| s.variable_id.as_str())
            .collect();
        assert_eq!(vars, vec!["new", "new"]);
    }

    #[tokio::test]
    async fn drop_oldest_when_all_rows_in_flight_still_inserts() {
        let dir = tempdir().unwrap();
        let buffer = BufferDb::open(dir.path().join("buf.db"), 2).unwrap();

        buffer
            .enqueue(vec![sample("v1", 1, 1.0), sample("v2", 2, 2.0)])
            .await
            .unwrap();
        buffer.claim_batch(10).await.unwrap().unwrap();
        assert_eq!(buffer.counts().await.unwrap(), (0, 2));

        // Cap reached with only in-flight rows: nothing droppable, but the
        // insert must still succeed (bounded overshoot, no data loss).
        buffer.enqueue(vec![sample("v3", 3, 3.0)]).await.unwrap();
        assert_eq!(buffer.counts().await.unwrap(), (1, 2));
        assert_eq!(buffer.dropped_total(), 0);
    }
}
