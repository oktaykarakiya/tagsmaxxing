//! Ingest-worker spawning shared by `kb serve` (embedded workers, P15-T6) and
//! `kb worker` (dedicated worker processes, P15-T8).
//!
//! Wires the queue plumbing built in P15-T2/T3 into running tasks: a pool of
//! claim-loop workers restricted to [`JobKind::Ingest`] whose handler resolves
//! the tenant's plan gate **at processing time** and runs
//! [`process_queued_ingest`], plus the lease reaper that requeues jobs
//! orphaned by crashed workers. Both shut down gracefully via the shared
//! `watch` channel (stop claiming, finish in-flight, exit).

use std::sync::Arc;
use std::time::Duration;

use kb_core::blob::Blob;
use kb_core::job::JobKind;
use kb_pipeline::ingest::IngestPipeline;
use kb_pipeline::job_queue::JobQueue;
use kb_pipeline::{process_queued_ingest, run_lease_reaper, run_worker_pool};
use kb_store::PgStore;

/// How often the lease reaper scans for expired claims.
pub const REAPER_INTERVAL: Duration = Duration::from_secs(30);

/// Spawn `concurrency` ingest workers claiming from `job_queue` (P15-T6).
///
/// Each claimed [`JobKind::Ingest`] job is processed by
/// [`process_queued_ingest`]: the staged document + blobs are loaded and the
/// pipeline finalizes the document to `ready`. The tenant's remote-models plan
/// gate is resolved per job at processing time (the hot-swap rule). The
/// returned handle resolves once every worker has drained after `shutdown`
/// signals.
///
/// # Panics
/// Panics if `concurrency` is zero (the underlying pool requires ≥ 1).
pub fn spawn_ingest_workers(
    job_queue: Arc<JobQueue>,
    pipeline: Arc<IngestPipeline>,
    pg_store: Arc<PgStore>,
    blob: Arc<dyn Blob>,
    concurrency: usize,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let handler = move |job: kb_core::job::Job| {
        let pipeline = Arc::clone(&pipeline);
        let pg_store = Arc::clone(&pg_store);
        let blob = Arc::clone(&blob);
        async move {
            // Resolve the plan gate now, not at enqueue time — plans can
            // change while a job waits in the queue (P14-T6 + hot-swap rule).
            let plan_local_only =
                crate::handlers::resolve_local_only(&pg_store, job.tenant_id).await;
            process_queued_ingest(
                &pipeline,
                pg_store.as_ref(),
                blob.as_ref(),
                &job,
                plan_local_only,
            )
            .await
        }
    };
    tokio::spawn(run_worker_pool(
        job_queue,
        concurrency,
        vec![JobKind::Ingest],
        shutdown,
        handler,
    ))
}

/// Spawn the lease reaper loop (P15-T2): every [`REAPER_INTERVAL`], running
/// jobs whose lease expired (crashed worker) are requeued through the queue's
/// normal retry/backoff accounting.
pub fn spawn_lease_reaper(
    job_queue: Arc<JobQueue>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_lease_reaper(job_queue, REAPER_INTERVAL, shutdown))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Both spawners come up and drain promptly on shutdown even against an
    /// unreachable database (claim/reap errors back off; shutdown still wins) —
    /// the graceful-drain contract `kb serve`'s shutdown path relies on.
    #[tokio::test]
    async fn spawned_tasks_drain_on_shutdown() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://127.0.0.1:1/none?sslmode=disable")
            .unwrap();
        let queue = Arc::new(JobQueue::new(pool, 1_000, 3));

        let (tx, rx) = tokio::sync::watch::channel(false);
        let reaper = spawn_lease_reaper(Arc::clone(&queue), rx.clone());

        // A worker pool needs full pipeline wiring; the reaper alone exercises
        // the spawn + drain seam (the pool's drain is covered by the
        // job_queue_pg worker-pool suite and the queued_worker_pg integration).
        tokio::task::yield_now().await;
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), reaper)
            .await
            .expect("reaper must drain on shutdown")
            .unwrap();
    }
}
