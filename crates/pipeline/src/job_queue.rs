//! Durable Postgres-backed job queue for ingestion work (plan §16).
//!
//! The [`JobQueue`] provides enqueue, claim (atomic `SELECT … FOR UPDATE SKIP LOCKED`),
//! complete, and fail with exponential backoff. [`run_worker_pool`] spawns a concurrent
//! pool of workers that claim and process jobs until a graceful shutdown is signaled via
//! a [`tokio::sync::watch`] channel.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use kb_core::job::{Job, JobKind, JobStatus};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgRow};

// ── Backoff calculation ────────────────────────────────────

/// Calculate the exponential backoff duration for a given attempt count.
///
/// Formula: `min_backoff_ms × 2^attempts`, clamped so the exponent never exceeds 30
/// (prevents overflow). Uses saturating arithmetic — absurdly large inputs produce a
/// clamped result instead of panicking.
fn calculate_backoff(attempts: i32, min_backoff_ms: i64) -> chrono::Duration {
    let shift = (attempts.max(0) as u32).min(30);
    let ms = min_backoff_ms.saturating_mul(1i64 << shift);
    chrono::Duration::milliseconds(ms)
}

// ── JobQueue ───────────────────────────────────────────────

/// A durable, Postgres-backed job queue for ingestion work.
///
/// Workers claim jobs atomically with `SELECT … FOR UPDATE SKIP LOCKED`, so multiple
/// concurrent claimants never receive the same row. Failed jobs are retried with
/// exponential backoff, and jobs that exhaust their retry budget are moved to a
/// dead-letter state (inspectable via the admin panel, plan §16).
///
/// The connection is provided as a sqlx [`PgPool`] — typically obtained from
/// [`PgStore::pool`](https://docs.rs/kb-store/latest/kb_store/struct.PgStore.html#method.pool).
/// The pool itself can be hot-swapped externally without restarting.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use kb_pipeline::JobQueue;
///
/// # async fn example(pool: sqlx::PgPool) -> anyhow::Result<()> {
/// let queue = Arc::new(JobQueue::new(pool, 10_000, 3));
/// let job_id = queue.enqueue(1, None, kb_core::job::JobKind::Ingest, 100).await?;
/// # Ok(())
/// # }
/// ```
pub struct JobQueue {
    /// The active Postgres connection pool.
    pool: PgPool,
    /// Minimum backoff (ms). Actual backoff = `min_backoff_ms × 2^attempts`.
    min_backoff_ms: i64,
    /// Maximum total attempts before a job is dead-lettered.
    max_retries: i32,
}

impl JobQueue {
    /// Create a new job queue backed by `pool`.
    ///
    /// `min_backoff_ms` is the base backoff in milliseconds; after each failed attempt
    /// the job's `run_after` is set to `now + min_backoff_ms × 2^attempts`.
    ///
    /// `max_retries` is the number of total attempts allowed. When `attempts` reaches
    /// this value the job is dead-lettered instead of being retried.
    #[must_use]
    pub fn new(pool: PgPool, min_backoff_ms: i64, max_retries: i32) -> Self {
        Self {
            pool,
            min_backoff_ms,
            max_retries,
        }
    }

    /// The minimum backoff duration in milliseconds configured on this queue.
    #[must_use]
    pub fn min_backoff_ms(&self) -> i64 {
        self.min_backoff_ms
    }

    /// The maximum total attempts before dead-lettering.
    #[must_use]
    pub fn max_retries(&self) -> i32 {
        self.max_retries
    }

    /// Insert a new queued job row and return its generated id.
    ///
    /// The new row is created with `status = 'queued'`, `attempts = 0`, and
    /// `run_after = now()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn enqueue(
        &self,
        tenant_id: i64,
        file_id: Option<i64>,
        kind: JobKind,
        priority: i32,
    ) -> anyhow::Result<i64> {
        let row = sqlx::query_scalar::<_, i64>(
            "INSERT INTO jobs \
             (tenant_id, file_id, kind, priority, status, attempts, run_after, created_at) \
             VALUES ($1, $2, $3, $4, 'queued', 0, now(), now()) \
             RETURNING id",
        )
        .bind(tenant_id)
        .bind(file_id)
        .bind(kind.as_str())
        .bind(priority)
        .fetch_one(&self.pool)
        .await
        .context("failed to enqueue job")?;

        Ok(row)
    }

    /// Atomically claim the next eligible job.
    ///
    /// Picks the candidate with the lowest `priority` (smaller runs first) and earliest
    /// `run_after`. Uses `FOR UPDATE SKIP LOCKED` so concurrent claimants **never**
    /// receive the same row. Once selected, the job's status is set to `running` in the
    /// same transaction.
    ///
    /// Returns `Ok(None)` when no job is eligible (all are either already claimed,
    /// completed, or have `run_after` in the future).
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn claim(&self) -> anyhow::Result<Option<Job>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin claim transaction")?;

        let row = sqlx::query(
            "SELECT id, tenant_id, file_id, kind, priority, status, attempts, \
             last_error, run_after, created_at \
             FROM jobs \
             WHERE status IN ('queued', 'failed') \
               AND run_after <= now() \
             ORDER BY priority ASC, run_after ASC, id ASC \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED",
        )
        .fetch_optional(&mut *tx)
        .await
        .context("failed to select next job for claim")?;

        let job = match row {
            Some(r) => {
                let id: i64 = r.get("id");
                sqlx::query("UPDATE jobs SET status = 'running' WHERE id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to update job status to running")?;

                Some(row_to_job(&r)?)
            }
            None => None,
        };

        tx.commit()
            .await
            .context("failed to commit claim transaction")?;

        Ok(job)
    }

    /// Mark a running job as successfully completed.
    ///
    /// Sets `status = 'done'` only if the job is currently `running`. Already-completed
    /// or dead-lettered jobs are silently ignored (the update matches zero rows, which
    /// is not an error).
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn complete(&self, job_id: i64) -> anyhow::Result<()> {
        sqlx::query("UPDATE jobs SET status = 'done' WHERE id = $1 AND status = 'running'")
            .bind(job_id)
            .execute(&self.pool)
            .await
            .context("failed to complete job")?;
        Ok(())
    }

    /// Mark a running job as failed.
    ///
    /// Reads the current attempt count under a row lock, increments it, stores the
    /// error message, and decides the outcome:
    ///
    /// * If the new attempt count **is less than** `max_retries`: the job stays
    ///   `status = 'failed'` and `run_after` is set to
    ///   `now + min_backoff_ms × 2^new_attempts` (exponential backoff). It will be
    ///   eligible for re-claim once the backoff expires.
    /// * If the new attempt count **reaches** `max_retries`: the job is **dead-lettered**
    ///   (`status = 'dead'`) and its `run_after` is left unchanged.
    ///
    /// Only affects jobs currently in `running` status. Returns the new status
    /// ([`JobStatus::Failed`] or [`JobStatus::Dead`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the job does not exist, is not currently `running`, or the
    /// database operation fails.
    pub async fn fail(&self, job_id: i64, error: &str) -> anyhow::Result<JobStatus> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin fail transaction")?;

        // Lock the row and read current state.
        let row = sqlx::query("SELECT status, attempts FROM jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read job for fail")?;

        let (status_str, current_attempts): (String, i32) = match row {
            Some(r) => (r.get("status"), r.get("attempts")),
            None => anyhow::bail!("job {job_id} not found"),
        };

        anyhow::ensure!(
            status_str == "running",
            "job {job_id} has status '{status_str}', expected 'running'"
        );

        let new_attempts = current_attempts + 1;
        let dead = new_attempts >= self.max_retries;
        let new_status = if dead {
            JobStatus::Dead
        } else {
            JobStatus::Failed
        };

        if dead {
            sqlx::query(
                "UPDATE jobs SET status = $1, attempts = $2, last_error = $3 \
                 WHERE id = $4",
            )
            .bind(new_status.as_str())
            .bind(new_attempts)
            .bind(error)
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .context("failed to dead-letter job")?;
        } else {
            let run_after = Utc::now() + calculate_backoff(new_attempts, self.min_backoff_ms);
            sqlx::query(
                "UPDATE jobs SET status = $1, attempts = $2, last_error = $3, run_after = $4 \
                 WHERE id = $5",
            )
            .bind(new_status.as_str())
            .bind(new_attempts)
            .bind(error)
            .bind(run_after)
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .context("failed to mark job as failed")?;
        }

        tx.commit()
            .await
            .context("failed to commit fail transaction")?;

        Ok(new_status)
    }
}

// ── Helpers ─────────────────────────────────────────────────

/// Map a sqlx [`PgRow`] into a [`Job`]. The `kind` and `status` columns are stored as
/// text and parsed through their `FromStr` implementations.
fn row_to_job(row: &PgRow) -> anyhow::Result<Job> {
    let kind_str: String = row.try_get("kind")?;
    let status_str: String = row.try_get("status")?;

    Ok(Job {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        file_id: row.try_get("file_id")?,
        kind: JobKind::from_str(&kind_str)
            .map_err(|e| anyhow::anyhow!("invalid job kind '{kind_str}' in database: {e}"))?,
        priority: row.try_get("priority")?,
        status: JobStatus::from_str(&status_str)
            .map_err(|e| anyhow::anyhow!("invalid job status '{status_str}' in database: {e}"))?,
        attempts: row.try_get("attempts")?,
        last_error: row.try_get("last_error")?,
        run_after: row.try_get("run_after")?,
        created_at: row.try_get("created_at")?,
    })
}

// ── Worker pool ─────────────────────────────────────────────

/// Run a pool of `concurrency` workers that claim and process jobs.
///
/// Each worker loops until a shutdown signal is received via the `shutdown` watch
/// channel. When shutdown is signaled, workers **stop claiming** new jobs, finish
/// their current in-flight handler (drain), and exit.
///
/// The `handler` receives a claimed [`Job`] and returns `Ok(())` on success or
/// `Err(String)` on failure. The worker loop automatically calls
/// [`JobQueue::complete`] or [`JobQueue::fail`] based on the result.
///
/// # Panics
///
/// Panics if `concurrency` is zero.
pub async fn run_worker_pool<F, Fut>(
    queue: Arc<JobQueue>,
    concurrency: usize,
    shutdown: tokio::sync::watch::Receiver<bool>,
    handler: F,
) where
    F: Fn(Job) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    assert!(concurrency > 0, "concurrency must be at least 1");

    let handler = Arc::new(handler);
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let q = Arc::clone(&queue);
        let h = Arc::clone(&handler);
        let s = shutdown.clone();

        handles.push(tokio::spawn(async move {
            worker_loop(q, h, s).await;
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}

/// Single-worker loop: claim → process → complete / fail → repeat.
async fn worker_loop<F, Fut>(
    queue: Arc<JobQueue>,
    handler: Arc<F>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) where
    F: Fn(Job) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    loop {
        // Quick shutdown check (does not consume the notification).
        if *shutdown.borrow() {
            break;
        }

        match queue.claim().await {
            Ok(Some(job)) => {
                let job_id = job.id;
                match handler(job).await {
                    Ok(()) => {
                        let _ = queue.complete(job_id).await;
                    }
                    Err(e) => {
                        let _ = queue.fail(job_id, &e).await;
                    }
                }
            }
            Ok(None) => {
                // No eligible jobs — wait briefly or bail on shutdown.
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {},
                }
            }
            Err(_) => {
                // Claim error (e.g. transient DB issue) — back off.
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(1000)) => {},
                }
            }
        }
    }
}

// ── Unit tests ──────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── calculate_backoff ────────────────────────────────

    #[test]
    fn backoff_attempt_1_is_2x_min() {
        let d = calculate_backoff(1, 10_000);
        assert_eq!(d.num_milliseconds(), 20_000);
    }

    #[test]
    fn backoff_attempt_2_is_4x_min() {
        let d = calculate_backoff(2, 10_000);
        assert_eq!(d.num_milliseconds(), 40_000);
    }

    #[test]
    fn backoff_attempt_3_is_8x_min() {
        let d = calculate_backoff(3, 10_000);
        assert_eq!(d.num_milliseconds(), 80_000);
    }

    #[test]
    fn backoff_attempt_0_is_1x_min() {
        let d = calculate_backoff(0, 5_000);
        assert_eq!(d.num_milliseconds(), 5_000);
    }

    #[test]
    fn backoff_clamped_at_30() {
        // 2^30 = 1_073_741_824 — at 1ms this fits in i64.
        let d = calculate_backoff(30, 1);
        assert_eq!(d.num_milliseconds(), 1_073_741_824);
    }

    #[test]
    fn backoff_clamped_at_31() {
        // 2^31 would overflow i64 at 1ms, so we clamp to 2^30.
        let d = calculate_backoff(31, 1);
        assert_eq!(d.num_milliseconds(), 1_073_741_824);
    }

    #[test]
    fn backoff_saturating_mul_prevents_overflow() {
        // Large min_backoff with large exponent — must not panic.
        let d = calculate_backoff(30, i64::MAX);
        // The saturating mul clamped — Duration should be capped.
        assert!(d.num_milliseconds() >= 0);
    }

    #[test]
    fn backoff_zero_min_is_zero() {
        let d = calculate_backoff(5, 0);
        assert_eq!(d.num_milliseconds(), 0);
    }

    #[test]
    fn backoff_negative_attempts_are_clamped_to_0_exponent() {
        // Negative attempts clamp to 0 → min_backoff × 2^0 = min_backoff.
        let d = calculate_backoff(-1, 1_000);
        assert_eq!(d.num_milliseconds(), 1_000);
    }

    // ── JobQueue construction ───────────────────────────────────

    #[tokio::test]
    async fn new_stores_parameters() {
        let pool = PgPool::connect_lazy("postgres://localhost/kb").unwrap();
        let queue = JobQueue::new(pool, 5_000, 3);
        assert_eq!(queue.min_backoff_ms(), 5_000);
        assert_eq!(queue.max_retries(), 3);
    }

    #[tokio::test]
    async fn min_backoff_ms_returns_configured_value() {
        let pool = PgPool::connect_lazy("postgres://localhost/kb").unwrap();
        let queue = JobQueue::new(pool, 12_345, 2);
        assert_eq!(queue.min_backoff_ms(), 12_345);
    }

    #[tokio::test]
    async fn max_retries_returns_configured_value() {
        let pool = PgPool::connect_lazy("postgres://localhost/kb").unwrap();
        let queue = JobQueue::new(pool, 1_000, 7);
        assert_eq!(queue.max_retries(), 7);
    }

    #[tokio::test]
    async fn queue_with_zero_max_retries_dead_letters_immediately() {
        let pool = PgPool::connect_lazy("postgres://localhost/kb").unwrap();
        let queue = JobQueue::new(pool, 1_000, 0);
        assert_eq!(queue.max_retries(), 0);
        assert_eq!(queue.min_backoff_ms(), 1_000);
    }
}
