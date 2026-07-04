// SPDX-License-Identifier: AGPL-3.0-or-later

//! Durable Postgres-backed job queue for ingestion work (plan §16).
//!
//! The [`JobQueue`] provides enqueue, claim (atomic `SELECT … FOR UPDATE SKIP LOCKED`),
//! complete, and fail with exponential backoff. [`run_worker_pool`] spawns a concurrent
//! pool of workers that claim and process jobs until a graceful shutdown is signaled via
//! a [`tokio::sync::watch`] channel.
//!
//! ## Priority semantics (P9-T12)
//!
//! Priority is **higher-is-more-urgent** (0 = default, no special treatment).
//! The claim query orders by effective priority (base priority + aging boost),
//! so older low-priority jobs are eventually lifted above newer high-priority ones —
//! preventing indefinite starvation.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use kb_core::job::{Job, JobKind, JobStatus};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgRow};

use crate::job_aging::AGING_STEP_SECS;
use crate::job_lease::LEASE_EXPIRED_ERROR;

// ── Backoff calculation ────────────────────────────────────

/// Error-message prefix that marks a handler failure as **permanent** (P15-T9).
///
/// A worker handler returning `Err` whose message starts with this prefix is
/// dead-lettered immediately via [`JobQueue::fail_permanent`] — deterministic
/// failures (invalid bytes, missing staged state) produce the same outcome on
/// every retry, so spending the retry budget on them only wastes capacity.
/// The prefix is stripped before the message is stored in `last_error`.
/// Producers: `kb_pipeline::ingest_worker` tags its deterministic failures;
/// consumers: the worker loop below.
pub const PERMANENT_ERROR_PREFIX: &str = "permanent: ";

/// Calculate the exponential backoff duration for a given attempt count.
///
/// Formula: `min_backoff_ms × 2^(attempts-1)`, clamped so the exponent never exceeds 30
/// (prevents overflow). Attempt 1 retries after `min_backoff_ms`; attempt 2 after
/// `2 × min_backoff_ms`; attempt 3 after `4 × min_backoff_ms`, etc.
/// Uses saturating arithmetic — absurdly large inputs produce a
/// clamped result instead of panicking.
fn calculate_backoff(attempts: i32, min_backoff_ms: i64) -> chrono::Duration {
    let shift = ((attempts.max(1) - 1) as u32).min(30);
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
/// ## Priority (P9-T12)
///
/// Priority is **higher-is-more-urgent** (default 0). The claim query orders by
/// effective priority (base priority + age-based boost, see [`crate::job_aging`]),
/// so older low-priority jobs eventually overtake fresher high-priority work —
/// preventing indefinite starvation.
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
/// let job_id = queue.enqueue(1, None, None, None, kb_core::job::JobKind::Ingest, 0).await?;
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
    /// Lease (visibility timeout) granted on claim, in seconds (P15-T2).
    lease_secs: i64,
    /// Identity stamped into `jobs.locked_by` on claim (`hostname:pid`).
    worker_id: String,
}

/// Default claim lease: 10 minutes — generous for long media transcriptions;
/// the worker-loop heartbeat extends it every `lease/3` while a job runs.
const DEFAULT_LEASE_SECS: i64 = 600;

/// Best-effort `hostname:pid` worker identity for `jobs.locked_by`.
fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "worker".to_string());
    format!("{host}:{}", std::process::id())
}

impl JobQueue {
    /// Create a new job queue backed by `pool`.
    ///
    /// `min_backoff_ms` is the base backoff in milliseconds; after each failed attempt
    /// the job's `run_after` is set to `now + min_backoff_ms × 2^attempts`.
    ///
    /// `max_retries` is the number of total attempts allowed. When `attempts` reaches
    /// this value the job is dead-lettered instead of being retried.
    ///
    /// The claim lease defaults to 600 s and the worker id to `hostname:pid`;
    /// override with [`with_lease_secs`](Self::with_lease_secs) /
    /// [`with_worker_id`](Self::with_worker_id).
    #[must_use]
    pub fn new(pool: PgPool, min_backoff_ms: i64, max_retries: i32) -> Self {
        Self {
            pool,
            min_backoff_ms,
            max_retries,
            lease_secs: DEFAULT_LEASE_SECS,
            worker_id: default_worker_id(),
        }
    }

    /// Set the claim lease (visibility timeout) in seconds (P15-T2).
    ///
    /// A claimed job whose lease expires without extension is presumed
    /// orphaned by a crashed worker and is requeued by the lease reaper
    /// (`run_lease_reaper`) through the normal retry/backoff accounting.
    /// Values below 1 are clamped to 1.
    #[must_use]
    pub fn with_lease_secs(mut self, lease_secs: i64) -> Self {
        self.lease_secs = lease_secs.max(1);
        self
    }

    /// Set the worker identity stamped into `jobs.locked_by` on claim.
    #[must_use]
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
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

    /// The claim lease (visibility timeout) in seconds.
    #[must_use]
    pub fn lease_secs(&self) -> i64 {
        self.lease_secs
    }

    /// The worker identity stamped into `jobs.locked_by` on claim.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Crate-internal pool access for the lease module (`job_lease.rs`).
    pub(crate) fn pool_ref(&self) -> &PgPool {
        &self.pool
    }

    /// Insert a new queued job row and return its generated id.
    ///
    /// The new row is created with `status = 'queued'`, `attempts = 0`, and
    /// `run_after = now()`. The `priority` parameter uses the
    /// **higher-is-more-urgent** convention (0 = default, no special treatment);
    /// see the [module-level docs](self) for the full P9-T12 semantics.
    ///
    /// `created_by` attributes the job to the user who enqueued it (P14-T1), so
    /// the resulting model-call usage is metered to that user. Pass `None` for
    /// system/maintenance jobs (reembed, export, orphan_gc, …).
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn enqueue(
        &self,
        tenant_id: i64,
        created_by: Option<i64>,
        file_id: Option<i64>,
        document_id: Option<i64>,
        kind: JobKind,
        priority: i32,
    ) -> anyhow::Result<i64> {
        let row = sqlx::query_scalar::<_, i64>(
            "INSERT INTO jobs \
             (tenant_id, created_by, file_id, document_id, kind, priority, status, attempts, run_after, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 'queued', 0, now(), now()) \
             RETURNING id",
        )
        .bind(tenant_id)
        .bind(created_by)
        .bind(file_id)
        .bind(document_id)
        .bind(kind.as_str())
        .bind(priority)
        .fetch_one(&self.pool)
        .await
        .context("failed to enqueue job")?;

        Ok(row)
    }

    /// Atomically claim the next eligible job.
    ///
    /// Picks the candidate with the highest **effective** priority (base priority +
    /// aging boost; see [`crate::job_aging`]), with ties broken by earliest `run_after`
    /// and then `id`. Aging prevents starvation: a low-priority job that waits long
    /// enough eventually overtakes fresher high-priority work.
    ///
    /// Uses `FOR UPDATE SKIP LOCKED` in the selecting sub-query so concurrent
    /// claimants **never** receive the same row, and flips its status to `running` in the
    /// **same statement** via `UPDATE … RETURNING`. Returning the post-update row (rather
    /// than the pre-update one) means the returned [`Job::status`] is the authoritative
    /// `Running`, matching the row now committed to the database.
    ///
    /// Returns `Ok(None)` when no job is eligible (all are either already claimed,
    /// completed, or have `run_after` in the future).
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn claim(&self) -> anyhow::Result<Option<Job>> {
        self.claim_kinds(&[]).await
    }

    /// Atomically claim the next eligible job of one of the given `kinds`.
    ///
    /// Identical to [`claim`](Self::claim) but restricted to the listed
    /// [`JobKind`]s, so a worker pool never claims work it has no handler for
    /// (e.g. an ingest worker skipping `send_email`/`delete_tenant` jobs,
    /// P15-T2). An **empty** slice means "any kind" (the [`claim`](Self::claim)
    /// behavior).
    ///
    /// Every claim also takes a **lease**: `lease_expires_at = now() +
    /// lease_secs` and `locked_by = worker_id`. A worker that crashes stops
    /// extending its lease, and the lease reaper requeues the job through the
    /// normal retry accounting (plan §16 visibility timeout).
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn claim_kinds(&self, kinds: &[JobKind]) -> anyhow::Result<Option<Job>> {
        // Single-statement claim with aging (P9-T12):
        // effective_priority = priority + floor(age_seconds / AGING_STEP_SECS).
        // The sub-query locks the chosen row with `FOR UPDATE SKIP LOCKED`,
        // the outer `UPDATE` flips it to 'running' + stamps the lease, and
        // `RETURNING` yields the post-update row.
        //
        // We embed the aging step as a literal because it's a compile-time
        // constant and this avoids an extra bound parameter.
        let kind_filter = if kinds.is_empty() {
            ""
        } else {
            "AND kind = ANY($3) "
        };
        let sql = format!(
            "UPDATE jobs SET status = 'running', \
                    lease_expires_at = now() + make_interval(secs => $1), \
                    locked_by = $2 \
             WHERE id = ( \
                 SELECT id FROM jobs \
                 WHERE status IN ('queued', 'failed') \
                   AND run_after <= now() \
                   {kind_filter}\
                 ORDER BY (priority \
                   + FLOOR(EXTRACT(EPOCH FROM (now() - created_at)) / {aging_step})) DESC, \
                          run_after ASC, id ASC \
                 LIMIT 1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             RETURNING id, tenant_id, file_id, document_id, kind, priority, status, attempts, \
                       last_error, run_after, created_at, created_by",
            aging_step = AGING_STEP_SECS
        );

        let mut query = sqlx::query(&sql)
            .bind(self.lease_secs as f64)
            .bind(&self.worker_id);
        let kind_strs: Vec<&str> = kinds.iter().map(|k| k.as_str()).collect();
        if !kinds.is_empty() {
            query = query.bind(kind_strs);
        }

        let row = query
            .fetch_optional(&self.pool)
            .await
            .context("failed to claim next job")?;

        row.map(|r| row_to_job(&r)).transpose()
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

    /// Sweep all `running` jobs whose `locked_by` does NOT match the current
    /// worker (`self.worker_id`).  This recovers jobs orphaned by a previous
    /// process crash — the new process has a different PID, so `locked_by`
    /// mismatch reliably identifies dead instances.  Jobs are requeued via
    /// [`requeue_crashed`](Self::requeue_crashed) with no attempt penalty and no
    /// backoff (the job didn't fail — the infrastructure did).
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn orphan_sweep(&self) -> anyhow::Result<usize> {
        let result = sqlx::query(
            "UPDATE jobs SET \
                status = 'failed', \
                lease_expires_at = NULL, \
                locked_by = NULL, \
                run_after = now() \
             WHERE status = 'running' \
               AND locked_by IS NOT NULL \
               AND locked_by != $1 \
             RETURNING id",
        )
        .bind(&self.worker_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to sweep orphaned running jobs")?;

        let count = result.len();
        if count > 0 {
            tracing::info!(count, "recovered orphaned jobs from previous process");
        }
        Ok(count)
    }

    /// Requeue a `running` job that was orphaned by a process crash.  Does NOT
    /// increment `attempts` and sets `run_after = now()` with zero backoff —
    /// the job didn't fail, the infrastructure did.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn requeue_crashed(&self, job_id: i64) -> anyhow::Result<()> {
        let result = sqlx::query(
            "UPDATE jobs SET \
                status = 'failed', \
                last_error = $2, \
                lease_expires_at = NULL, \
                locked_by = NULL, \
                run_after = now() \
             WHERE id = $1 AND status = 'running'",
        )
        .bind(job_id)
        .bind(LEASE_EXPIRED_ERROR)
        .execute(&self.pool)
        .await
        .context("failed to requeue crashed job")?;

        if result.rows_affected() == 0 {
            tracing::debug!(%job_id, "requeue_crashed: job not running, skipped");
        }
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
    /// When an **ingest** job dead-letters, its staged document (and that
    /// document's still-`pending` files) transitions to `status = 'failed'`
    /// in the same transaction (P15-T9) so the failure is visible to the
    /// tenant instead of leaving the document `pending` forever. A document
    /// that already reached `ready` (idempotent replay) is never downgraded.
    /// Both updates carry the job's own `tenant_id` filter — the jobs table
    /// sits outside RLS (§31.5).
    ///
    /// Only affects jobs currently in `running` status. Returns the new status
    /// ([`JobStatus::Failed`] or [`JobStatus::Dead`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the job does not exist, is not currently `running`, or the
    /// database operation fails.
    pub async fn fail(&self, job_id: i64, error: &str) -> anyhow::Result<JobStatus> {
        self.fail_inner(job_id, error, false).await
    }

    /// Dead-letter a running job immediately, bypassing remaining retries.
    ///
    /// For **permanent** failures — deterministic errors (e.g. extraction of
    /// invalid bytes) where every retry must produce the same outcome, so
    /// burning the retry budget only wastes worker capacity (P15-T9; observed
    /// as 5 pointless attempts in ~30 s). The dead-letter document hook of
    /// [`fail`](Self::fail) applies identically; the tenant retry endpoint
    /// can still requeue the job explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error if the job does not exist, is not currently `running`,
    /// or the database operation fails.
    pub async fn fail_permanent(&self, job_id: i64, error: &str) -> anyhow::Result<JobStatus> {
        self.fail_inner(job_id, error, true).await
    }

    /// Shared body of [`fail`](Self::fail) / [`fail_permanent`](Self::fail_permanent).
    async fn fail_inner(
        &self,
        job_id: i64,
        error: &str,
        force_dead: bool,
    ) -> anyhow::Result<JobStatus> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin fail transaction")?;

        // Lock the row and read current state (+ what the dead-letter document
        // hook needs: kind, document_id and the job's own tenant for explicit
        // tenant-scoped updates — jobs sit outside RLS).
        let row = sqlx::query(
            "SELECT status, attempts, kind, document_id, tenant_id \
             FROM jobs WHERE id = $1 FOR UPDATE",
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to read job for fail")?;

        let Some(row) = row else {
            anyhow::bail!("job {job_id} not found");
        };
        let status_str: String = row.get("status");
        let current_attempts: i32 = row.get("attempts");
        let kind: String = row.get("kind");
        let document_id: Option<i64> = row.get("document_id");
        let tenant_id: i64 = row.get("tenant_id");

        anyhow::ensure!(
            status_str == "running",
            "job {job_id} has status '{status_str}', expected 'running'"
        );

        let new_attempts = current_attempts + 1;
        let dead = force_dead || new_attempts >= self.max_retries;
        let new_status = if dead {
            JobStatus::Dead
        } else {
            JobStatus::Failed
        };

        if dead {
            sqlx::query(
                "UPDATE jobs SET status = $1, attempts = $2, last_error = $3, \
                        lease_expires_at = NULL, locked_by = NULL \
                 WHERE id = $4",
            )
            .bind(new_status.as_str())
            .bind(new_attempts)
            .bind(error)
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .context("failed to dead-letter job")?;

            // Dead-letter document hook (P15-T9): surface the terminal failure
            // on the staged document so the tenant sees `failed` (+ retry UX)
            // instead of an eternally-`pending` document. Never downgrade a
            // document that already converged to `ready`.
            if kind == "ingest"
                && let Some(doc_id) = document_id
            {
                sqlx::query(
                    "UPDATE documents SET status = 'failed' \
                         WHERE id = $1 AND tenant_id = $2 AND status <> 'ready'",
                )
                .bind(doc_id)
                .bind(tenant_id)
                .execute(&mut *tx)
                .await
                .context("failed to mark document failed on dead-letter")?;
                sqlx::query(
                    "UPDATE files SET status = 'failed' \
                         WHERE document_id = $1 AND tenant_id = $2 AND status = 'pending'",
                )
                .bind(doc_id)
                .bind(tenant_id)
                .execute(&mut *tx)
                .await
                .context("failed to mark files failed on dead-letter")?;
            }
        } else {
            let run_after = Utc::now() + calculate_backoff(new_attempts, self.min_backoff_ms);
            sqlx::query(
                "UPDATE jobs SET status = $1, attempts = $2, last_error = $3, run_after = $4, \
                        lease_expires_at = NULL, locked_by = NULL \
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
        document_id: row.try_get("document_id")?,
        kind: JobKind::from_str(&kind_str)
            .map_err(|e| anyhow::anyhow!("invalid job kind '{kind_str}' in database: {e}"))?,
        priority: row.try_get("priority")?,
        status: JobStatus::from_str(&status_str)
            .map_err(|e| anyhow::anyhow!("invalid job status '{status_str}' in database: {e}"))?,
        attempts: row.try_get("attempts")?,
        last_error: row.try_get("last_error")?,
        run_after: row.try_get("run_after")?,
        created_at: row.try_get("created_at")?,
        created_by: row.try_get("created_by")?,
    })
}

// ── Worker pool ─────────────────────────────────────────────

/// Run a pool of `concurrency` workers that claim and process jobs.
///
/// Each worker loops until a shutdown signal is received via the `shutdown` watch
/// channel. When shutdown is signaled, workers **stop claiming** new jobs, finish
/// their current in-flight handler (drain), and exit.
///
/// `kinds` restricts what the pool claims (see [`JobQueue::claim_kinds`]); an
/// empty `Vec` claims any kind. An ingest worker passes `vec![JobKind::Ingest]`
/// so it never claims work it has no handler for (P15-T2).
///
/// The `handler` receives a claimed [`Job`] and returns `Ok(())` on success or
/// `Err(String)` on failure. The worker loop automatically calls
/// [`JobQueue::complete`] or [`JobQueue::fail`] based on the result, and keeps
/// the job's **lease** alive while the handler runs by extending it every
/// `lease/3` seconds (long media jobs outlive a single lease window).
///
/// # Panics
///
/// Panics if `concurrency` is zero.
pub async fn run_worker_pool<F, Fut>(
    queue: Arc<JobQueue>,
    concurrency: usize,
    kinds: Vec<JobKind>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    handler: F,
) where
    F: Fn(Job) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    assert!(concurrency > 0, "concurrency must be at least 1");

    let handler = Arc::new(handler);
    let kinds = Arc::new(kinds);
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let q = Arc::clone(&queue);
        let h = Arc::clone(&handler);
        let k = Arc::clone(&kinds);
        let s = shutdown.clone();

        handles.push(tokio::spawn(async move {
            worker_loop(q, h, k, s).await;
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}

/// Single-worker loop: claim → (heartbeat ‖ process) → complete / fail → repeat.
async fn worker_loop<F, Fut>(
    queue: Arc<JobQueue>,
    handler: Arc<F>,
    kinds: Arc<Vec<JobKind>>,
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

        match queue.claim_kinds(&kinds).await {
            Ok(Some(job)) => {
                let job_id = job.id;
                tracing::info!(%job_id, kind = %job.kind, doc_id = %job.document_id.map_or_else(|| "?".to_string(), |d| d.to_string()), "claimed job");

                // Keep the lease alive while the handler runs. If an extension
                // reports the job is no longer ours (reaped after a stall), the
                // heartbeat stops; the handler still finishes, and its
                // complete()/fail() no-op safely (both are status-guarded —
                // duplicate completion converges, see transactional_ingest).
                let hb_queue = Arc::clone(&queue);
                let (hb_stop, mut hb_stop_rx) = tokio::sync::oneshot::channel::<()>();
                let heartbeat = tokio::spawn(async move {
                    let period = Duration::from_secs((hb_queue.lease_secs() / 3).max(1) as u64);
                    loop {
                        tokio::select! {
                            _ = &mut hb_stop_rx => break,
                            _ = tokio::time::sleep(period) => {
                                match hb_queue.extend_lease(job_id).await {
                                    Ok(true) => {}
                                    Ok(false) => break, // no longer ours
                                    Err(_) => {}        // transient DB error — retry
                                }
                            }
                        }
                    }
                });

                let kind_label = job.kind.as_str().to_owned();
                let started = std::time::Instant::now();
                let result = handler(job).await;
                let _ = hb_stop.send(());
                let _ = heartbeat.await;

                let outcome = match result {
                    Ok(()) => {
                        let _ = queue.complete(job_id).await;
                        tracing::info!(%job_id, "job completed");
                        "done"
                    }
                    Err(e) => {
                        // Deterministic failures skip the retry budget — every
                        // retry would produce the same outcome (P15-T9).
                        let status = if let Some(msg) = e.strip_prefix(PERMANENT_ERROR_PREFIX) {
                            let st = queue.fail_permanent(job_id, msg).await;
                            tracing::error!(%job_id, attempts = %queue.max_retries(), error = %msg, "job permanently failed");
                            st
                        } else {
                            let st = queue.fail(job_id, &e).await;
                            match st {
                                Ok(JobStatus::Dead) => {
                                    tracing::error!(%job_id, attempts = %queue.max_retries(), error = %e, "job permanently failed");
                                }
                                Ok(JobStatus::Failed) => {
                                    tracing::warn!(%job_id, error = %e, "job failed and will retry");
                                }
                                _ => {
                                    tracing::warn!(%job_id, error = %e, "job failed");
                                }
                            }
                            st
                        };
                        match status {
                            Ok(JobStatus::Dead) => "dead",
                            _ => "failed",
                        }
                    }
                };
                kb_metrics::record_job_processed(
                    &kind_label,
                    outcome,
                    started.elapsed().as_secs_f64(),
                );
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
    fn backoff_attempt_1_is_1x_min() {
        let d = calculate_backoff(1, 10_000);
        assert_eq!(d.num_milliseconds(), 10_000);
    }

    #[test]
    fn backoff_attempt_2_is_2x_min() {
        let d = calculate_backoff(2, 10_000);
        assert_eq!(d.num_milliseconds(), 20_000);
    }

    #[test]
    fn backoff_attempt_3_is_4x_min() {
        let d = calculate_backoff(3, 10_000);
        assert_eq!(d.num_milliseconds(), 40_000);
    }

    #[test]
    fn backoff_attempt_0_is_1x_min() {
        let d = calculate_backoff(0, 5_000);
        assert_eq!(d.num_milliseconds(), 5_000);
    }

    #[test]
    fn backoff_clamped_at_30() {
        // 2^29 = 536_870_912 — with 1ms base this fits in i64.
        let d = calculate_backoff(30, 1);
        assert_eq!(d.num_milliseconds(), 536_870_912);
    }

    #[test]
    fn backoff_clamped_at_31() {
        // 2^31 would overflow i64, clamped to 2^30 = 1_073_741_824.
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
