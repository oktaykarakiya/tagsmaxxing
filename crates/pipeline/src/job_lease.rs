//! Job lease (visibility timeout) maintenance for the [`JobQueue`] (P15-T2,
//! plan §16).
//!
//! Every claim stamps `lease_expires_at = now() + lease` and `locked_by`
//! (see [`JobQueue::claim_kinds`]); the worker loop extends the lease every
//! `lease/3` while a job runs. A worker that crashes (or loses its DB
//! connection) stops extending, so its job's lease expires — the **reaper**
//! ([`run_lease_reaper`]) then requeues it through the queue's normal
//! [`fail`](JobQueue::fail) accounting: attempts increment, exponential
//! backoff applies, and retry exhaustion dead-letters as usual. No parallel
//! bookkeeping; `fail()` stays the single arbiter of retry state.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

use crate::job_queue::JobQueue;

/// The error message recorded on a job whose lease expired.
pub const LEASE_EXPIRED_ERROR: &str = "lease expired — worker presumed crashed";

/// How many expired leases one reaper pass processes at most. Bounds the work
/// per tick; the next tick picks up any remainder.
const REAP_BATCH: i64 = 50;

impl JobQueue {
    /// Extend the lease of a running job this worker owns (P15-T2).
    ///
    /// Returns `Ok(true)` when the lease was extended, `Ok(false)` when the
    /// job is no longer this worker's running job — completed, failed, or
    /// reaped and re-claimed elsewhere — which tells the heartbeat to stop.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn extend_lease(&self, job_id: i64) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $1) \
             WHERE id = $2 AND status = 'running' AND locked_by = $3",
        )
        .bind(self.lease_secs() as f64)
        .bind(job_id)
        .bind(self.worker_id())
        .execute(self.pool_ref())
        .await
        .context("failed to extend job lease")?;
        Ok(res.rows_affected() > 0)
    }

    /// Requeue running jobs whose lease has expired (crashed-worker recovery).
    ///
    /// Selects up to [`REAP_BATCH`] candidates **without holding locks** and
    /// delegates each to [`fail`](Self::fail), which row-locks the job and
    /// verifies it is still `running` — so a race with the job's own worker
    /// (or another reaper) resolves safely: whoever loses the race simply
    /// skips. Returns the number of jobs actually reaped.
    ///
    /// # Errors
    /// Returns an error if the candidate query fails (individual `fail` races
    /// are skipped, not errors).
    pub async fn reap_expired_leases(&self) -> anyhow::Result<u32> {
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM jobs \
             WHERE status = 'running' \
               AND lease_expires_at IS NOT NULL \
               AND lease_expires_at < now() \
             ORDER BY lease_expires_at ASC \
             LIMIT $1",
        )
        .bind(REAP_BATCH)
        .fetch_all(self.pool_ref())
        .await
        .context("failed to list expired job leases")?;

        let mut reaped = 0u32;
        for id in ids {
            match self.fail(id, LEASE_EXPIRED_ERROR).await {
                Ok(status) => {
                    tracing::warn!(
                        job_id = id,
                        new_status = status.as_str(),
                        "reaped expired job lease (worker presumed crashed)"
                    );
                    reaped += 1;
                }
                // Lost the race (job completed / already failed / re-claimed
                // between SELECT and fail) — fail() refused; skip.
                Err(e) => {
                    tracing::debug!(job_id = id, error = %e, "lease reap skipped (raced)");
                }
            }
        }
        if reaped > 0 {
            // A reaped lease means a worker crashed or stalled mid-job
            // (heartbeats stopped) — operators alert on spikes (P15-T13).
            kb_metrics::record_leases_reaped(u64::from(reaped));
        }
        Ok(reaped)
    }
}

/// Run the lease reaper loop until `shutdown` signals (P15-T2).
///
/// Every `interval`, requeues running jobs whose lease expired via
/// [`JobQueue::reap_expired_leases`]. Errors are logged and the loop
/// continues (a transient DB outage must not kill crash recovery).
pub async fn run_lease_reaper(
    queue: Arc<JobQueue>,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        if let Err(e) = queue.reap_expired_leases().await {
            tracing::warn!(error = %e, "lease reaper pass failed; will retry");
        }
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(interval) => {},
        }
    }
    tracing::info!("lease reaper shut down");
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn lazy_queue() -> Arc<JobQueue> {
        // connect_lazy: errors surface on first use. A tiny acquire timeout
        // makes that failure fast (the default is 30 s, which would stall the
        // shutdown-responsiveness assertion below).
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://127.0.0.1:1/none?sslmode=disable")
            .unwrap();
        Arc::new(JobQueue::new(pool, 1_000, 3))
    }

    /// The reaper loop stops promptly on shutdown even when every pass errors
    /// (unreachable DB) — crash recovery must never wedge shutdown.
    #[tokio::test]
    async fn reaper_loop_stops_on_shutdown_despite_errors() {
        let queue = lazy_queue();
        let (tx, rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(run_lease_reaper(
            queue,
            Duration::from_secs(3600), // long interval: rely on the select arm
            rx,
        ));

        // Let the first (failing) pass run, then signal shutdown.
        tokio::task::yield_now().await;
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("reaper must stop on shutdown")
            .unwrap();
    }

    /// `extend_lease` surfaces DB errors (rather than silently reporting
    /// `false`, which would stop a healthy heartbeat).
    #[tokio::test]
    async fn extend_lease_errors_on_unreachable_db() {
        let queue = lazy_queue();
        let err = queue.extend_lease(1).await.unwrap_err();
        assert!(
            err.to_string().contains("failed to extend job lease"),
            "got: {err}"
        );
    }

    /// `reap_expired_leases` surfaces candidate-query DB errors.
    #[tokio::test]
    async fn reap_errors_on_unreachable_db() {
        let queue = lazy_queue();
        let err = queue.reap_expired_leases().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to list expired job leases"),
            "got: {err}"
        );
    }
}
