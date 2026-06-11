// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for `JobQueue` and `run_worker_pool` against a real pgvector
//! Postgres container.
//!
//! These tests need **Podman** (this project targets Podman exclusively) and network
//! access to pull `pgvector/pgvector`. They are gated `#[ignore]` so `just ci` stays
//! green when no container runtime is available. Run them explicitly with:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-pipeline --test job_queue_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use kb_core::job::{JobKind, JobStatus};
use kb_pipeline::{JobQueue, run_worker_pool};
use sqlx::PgPool;

// ── Fixtures ──────────────────────────────────────────────────────────────────

struct Setup {
    queue: Arc<JobQueue>,
    pool: PgPool,
    tenant_id: i64,
}

/// Provision a fresh DB in the shared container, run migrations, insert a tenant, and return a
/// connected `JobQueue` + direct `PgPool` for assertions.
///
/// The job queue is **intrinsically cross-tenant** (`claim` scans all tenants with no GUC), so
/// it runs on the **privileged** pool (admin_url) — under the RLS-enforced kb_app role it would
/// be denied every row. This is the two-role design (P6-T14).
async fn setup_queue(min_backoff_ms: i64, max_retries: i32) -> anyhow::Result<Setup> {
    let db = kb_testsupport::fresh_db().await?;

    // Create the privileged pool, run migrations.
    let pool = sqlx::PgPool::connect(&db.admin_url).await?;
    kb_store::MIGRATOR.run(&pool).await?;

    // Insert a tenant (tenants table has no RLS, so this always works).
    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'Test') RETURNING id")
            .fetch_one(&pool)
            .await?;

    let queue = Arc::new(JobQueue::new(pool.clone(), min_backoff_ms, max_retries));

    Ok(Setup {
        queue,
        pool,
        tenant_id,
    })
}

/// Shorthand for setup with default backoff params.
async fn setup() -> anyhow::Result<Setup> {
    setup_queue(1_000, 3).await
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Count rows in the jobs table matching a filter column/value pair.
async fn count_jobs(pool: &PgPool, col: &str, val: &str) -> anyhow::Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM jobs WHERE {col} = $1");
    let count: i64 = sqlx::query_scalar(&sql).bind(val).fetch_one(pool).await?;
    Ok(count)
}

/// Fetch a single job's status from the database.
async fn job_status(pool: &PgPool, job_id: i64) -> anyhow::Result<String> {
    let s: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    Ok(s)
}

/// Fetch a single job's attempts from the database.
async fn job_attempts(pool: &PgPool, job_id: i64) -> anyhow::Result<i32> {
    let a: i32 = sqlx::query_scalar("SELECT attempts FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    Ok(a)
}

/// Fetch a single job's run_after as seconds-since-epoch.
async fn job_run_after_epoch(pool: &PgPool, job_id: i64) -> anyhow::Result<f64> {
    let epoch: f64 =
        sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM run_after)::float8 FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(pool)
            .await?;
    Ok(epoch)
}

/// Get the current epoch from the database.
async fn now_epoch(pool: &PgPool) -> anyhow::Result<f64> {
    let epoch: f64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM now())::float8")
        .fetch_one(pool)
        .await?;
    Ok(epoch)
}

// ── enqueue ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn enqueue_creates_job_row() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;
    assert!(id > 0, "enqueued job id should be positive");

    let queued = count_jobs(&s.pool, "status", "queued").await?;
    assert_eq!(queued, 1);

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn enqueue_allows_file_id() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, Some(42), None, JobKind::Ingest, 50)
        .await?;

    let file_id: Option<i64> = sqlx::query_scalar("SELECT file_id FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(file_id, Some(42));

    Ok(())
}

/// P14-T1: `created_by` survives an enqueue → claim round-trip, both persisted
/// to the row and mapped back onto the claimed [`Job`]. A system enqueue
/// (`created_by = None`) round-trips as `None`.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn enqueue_claim_round_trips_created_by() -> anyhow::Result<()> {
    let s = setup().await?;

    // created_by FKs users(id), so seed a user in the tenant first.
    let user_id: i64 = sqlx::query_scalar(
        "INSERT INTO users (tenant_id, email, password_hash) \
         VALUES ($1, 'u@t', 'x') RETURNING id",
    )
    .bind(s.tenant_id)
    .fetch_one(&s.pool)
    .await?;

    // Attributed job: created_by is persisted and returned on claim.
    let attributed_id = s
        .queue
        .enqueue(s.tenant_id, Some(user_id), None, None, JobKind::Ingest, 100)
        .await?;
    let stored: Option<i64> = sqlx::query_scalar("SELECT created_by FROM jobs WHERE id = $1")
        .bind(attributed_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(stored, Some(user_id), "created_by persisted on the row");

    let claimed = s.queue.claim().await?.expect("a job is claimable");
    assert_eq!(claimed.id, attributed_id);
    assert_eq!(
        claimed.created_by,
        Some(user_id),
        "claim() maps created_by back onto the Job"
    );

    // System job: created_by None round-trips as None.
    let system_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Reembed, 100)
        .await?;
    let system_stored: Option<i64> =
        sqlx::query_scalar("SELECT created_by FROM jobs WHERE id = $1")
            .bind(system_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(system_stored, None);
    let claimed_system = s.queue.claim().await?.expect("system job claimable");
    assert_eq!(claimed_system.id, system_id);
    assert_eq!(claimed_system.created_by, None);

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn enqueue_preserves_kind_and_priority() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Reembed, 25)
        .await?;

    let kind_str: String = sqlx::query_scalar("SELECT kind FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&s.pool)
        .await?;
    let priority: i32 = sqlx::query_scalar("SELECT priority FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&s.pool)
        .await?;

    assert_eq!(kind_str, "reembed");
    assert_eq!(priority, 25);

    Ok(())
}

// ── claim ──────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claim_returns_next_eligible_job() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    let job = s.queue.claim().await?.expect("should claim a job");
    assert_eq!(job.id, id);
    assert_eq!(job.status, JobStatus::Running); // claim sets running
    assert_eq!(job.kind, JobKind::Ingest);

    // The DB row should now be 'running'.
    let db_status = job_status(&s.pool, id).await?;
    assert_eq!(db_status, "running");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claim_respects_priority_ordering() -> anyhow::Result<()> {
    let s = setup().await?;

    // Enqueue three jobs with different priorities. Since migration 0014
    // (P9-T12) priority is HIGHER-is-more-urgent: the claim query orders by
    // effective priority DESC. (This test predated 0014 and was stale — it
    // asserted the old lower-first semantics; first actually run in P15-T2.)
    let low_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 10)
        .await?;
    let mid_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 50)
        .await?;
    let high_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Claim three times — highest priority first.
    let j1 = s.queue.claim().await?.expect("first claim");
    let j2 = s.queue.claim().await?.expect("second claim");
    let j3 = s.queue.claim().await?.expect("third claim");

    assert_eq!(j1.id, high_id, "priority 100 (most urgent) should be first");
    assert_eq!(j2.id, mid_id, "priority 50 should be second");
    assert_eq!(j3.id, low_id, "priority 10 should be last");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claim_respects_run_after() -> anyhow::Result<()> {
    let s = setup().await?;

    // Insert two jobs: one available now, one in the future.
    let now_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Manually insert a job with future run_after.
    let future_id: i64 = sqlx::query_scalar(
        "INSERT INTO jobs (tenant_id, kind, priority, status, attempts, run_after, created_at) \
         VALUES ($1, 'ingest', 1, 'queued', 0, now() + interval '1 hour', now()) \
         RETURNING id",
    )
    .bind(s.tenant_id)
    .fetch_one(&s.pool)
    .await?;

    // Claim should return only the now-available job.
    let j1 = s
        .queue
        .claim()
        .await?
        .expect("should claim now-available job");
    assert_eq!(j1.id, now_id);

    // No more eligible jobs.
    let j2 = s.queue.claim().await?;
    assert!(j2.is_none(), "future job should not be claimable");

    // Manually update the future job's run_after to now.
    sqlx::query("UPDATE jobs SET run_after = now(), status = 'queued' WHERE id = $1")
        .bind(future_id)
        .execute(&s.pool)
        .await?;

    // Now it should be claimable.
    let j3 = s
        .queue
        .claim()
        .await?
        .expect("should now claim the reset job");
    assert_eq!(j3.id, future_id);

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claim_returns_none_when_no_eligible_jobs() -> anyhow::Result<()> {
    let s = setup().await?;

    let job = s.queue.claim().await?;
    assert!(job.is_none(), "empty queue should return None");

    Ok(())
}

// ── complete ───────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn complete_sets_status_to_done() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Claim it first (must be 'running' to complete).
    let _ = s.queue.claim().await?;
    s.queue.complete(id).await?;

    let status = job_status(&s.pool, id).await?;
    assert_eq!(status, "done");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn complete_does_not_affect_done_job() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Complete without claiming — shouldn't affect the queued job.
    s.queue.complete(id).await?;
    let status = job_status(&s.pool, id).await?;
    assert_eq!(status, "queued", "unclaimed job should remain queued");

    Ok(())
}

// ── fail ───────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn fail_increments_attempts_and_sets_backoff() -> anyhow::Result<()> {
    let s = setup_queue(5_000, 3).await?; // 5s min backoff, 3 max retries

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;
    let _ = s.queue.claim().await?;

    let before = now_epoch(&s.pool).await?;
    let new_status = s.queue.fail(id, "test error").await?;
    assert_eq!(new_status, JobStatus::Failed);

    // Attempts should be 1.
    let attempts = job_attempts(&s.pool, id).await?;
    assert_eq!(attempts, 1);

    // run_after should be in the future (~now + 2 × 5000ms = +10s).
    let run_after = job_run_after_epoch(&s.pool, id).await?;
    assert!(
        dbg!(run_after) > dbg!(before),
        "run_after {run_after} should be after now {before} for backoff"
    );

    // Status in DB should be 'failed'.
    let db_status = job_status(&s.pool, id).await?;
    assert_eq!(db_status, "failed");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn fail_stores_last_error() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;
    let _ = s.queue.claim().await?;
    s.queue.fail(id, "disk full").await?;

    let last_error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(last_error.as_deref(), Some("disk full"));

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn fail_dead_letters_after_max_retries() -> anyhow::Result<()> {
    let s = setup_queue(100, 2).await?; // max 2 total attempts

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // First attempt.
    let _ = s.queue.claim().await?;
    let status1 = s.queue.fail(id, "error 1").await?;
    assert_eq!(status1, JobStatus::Failed);
    assert_eq!(job_attempts(&s.pool, id).await?, 1);

    // Manually reset to queued so it can be claimed again.
    sqlx::query("UPDATE jobs SET status = 'queued', run_after = now() WHERE id = $1")
        .bind(id)
        .execute(&s.pool)
        .await?;

    // Second attempt — should dead-letter.
    let _ = s.queue.claim().await?;
    let status2 = s.queue.fail(id, "error 2").await?;
    assert_eq!(status2, JobStatus::Dead);
    assert_eq!(job_attempts(&s.pool, id).await?, 2);

    let db_status = job_status(&s.pool, id).await?;
    assert_eq!(db_status, "dead");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn fail_errors_on_non_running_job() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Try to fail without claiming (status is still 'queued').
    let err = s.queue.fail(id, "premature failure").await.unwrap_err();
    assert!(
        err.to_string().contains("expected 'running'"),
        "should reject fail on queued job: {err}"
    );

    Ok(())
}

// ── concurrent claim ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn concurrent_claimants_never_get_the_same_row() -> anyhow::Result<()> {
    let s = setup().await?;

    // Enqueue many jobs.
    let mut ids = Vec::new();
    for i in 0..50 {
        let id = s
            .queue
            .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, i)
            .await?;
        ids.push(id);
    }

    // Spawn multiple concurrent claimants.
    let queue = Arc::clone(&s.queue);
    let mut handles = Vec::new();
    for _ in 0..5 {
        let q = Arc::clone(&queue);
        handles.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            loop {
                match q.claim().await {
                    Ok(Some(job)) => claimed.push(job.id),
                    Ok(None) => break,
                    Err(_) => break,
                }
                tokio::task::yield_now().await;
            }
            claimed
        }));
    }

    // Collect all claimed ids.
    let mut all_claimed = Vec::new();
    for h in handles {
        let claimed = h.await.unwrap();
        all_claimed.extend(claimed);
    }

    // Every job should be claimed exactly once.
    all_claimed.sort_unstable();
    ids.sort_unstable();
    assert_eq!(all_claimed, ids, "each job claimed exactly once");

    // All 50 should now be 'running'.
    let running = count_jobs(&s.pool, "status", "running").await?;
    assert_eq!(running, 50);

    Ok(())
}

// ── worker pool ────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn worker_pool_processes_all_jobs() -> anyhow::Result<()> {
    let s = setup().await?;

    // Enqueue 10 jobs.
    for i in 0..10 {
        s.queue
            .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, i)
            .await?;
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let queue = Arc::clone(&s.queue);
    let rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        run_worker_pool(queue, 3, vec![], rx, |_job| async move {
            // Simulate successful processing.
            Ok(())
        })
        .await;
    });

    // Wait for all jobs to be done (poll).
    for _ in 0..50 {
        let done = count_jobs(&s.pool, "status", "done").await?;
        if done == 10 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Signal shutdown.
    shutdown_tx.send(true).ok();
    handle.await.unwrap();

    let done = count_jobs(&s.pool, "status", "done").await?;
    assert_eq!(done, 10, "all 10 jobs should be done");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn worker_pool_handles_failures_and_retries() -> anyhow::Result<()> {
    let s = setup_queue(100, 2).await?; // small backoff, max 1 retry

    // Enqueue one job.
    let job_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Counter: first attempt fails, second succeeds.
    let attempts = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let attempts2 = Arc::clone(&attempts);

    let queue = Arc::clone(&s.queue);
    let rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        run_worker_pool(queue, 1, vec![], rx, move |_job| {
            let a = attempts2.clone();
            async move {
                let n = a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    Err("first attempt fails".into())
                } else {
                    Ok(())
                }
            }
        })
        .await;
    });

    // After first fail, the job is 'failed' with run_after in the future.
    // Manually reset run_after so it can be retried immediately.
    for _ in 0..20 {
        let status = job_status(&s.pool, job_id).await?;
        if status == "failed" {
            sqlx::query(
                "UPDATE jobs SET status = 'queued', run_after = now() \
                 WHERE id = $1 AND status = 'failed'",
            )
            .bind(job_id)
            .execute(&s.pool)
            .await?;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait for done.
    for _ in 0..30 {
        let status = job_status(&s.pool, job_id).await?;
        if status == "done" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown_tx.send(true).ok();
    handle.await.unwrap();

    let status = job_status(&s.pool, job_id).await?;
    assert_eq!(status, "done", "job should eventually succeed");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn worker_pool_graceful_shutdown_does_not_claim_new_jobs() -> anyhow::Result<()> {
    let s = setup().await?;

    // Enqueue jobs.
    for i in 0..5 {
        s.queue
            .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, i)
            .await?;
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let queue = Arc::clone(&s.queue);
    // Signal shutdown immediately — worker should stop before claiming.
    shutdown_tx.send(true).ok();

    let rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        run_worker_pool(queue, 2, vec![], rx, |_job| async move { Ok(()) }).await;
    });

    handle.await.unwrap();

    // No jobs should have been claimed (all still queued).
    let queued = count_jobs(&s.pool, "status", "queued").await?;
    assert_eq!(
        queued, 5,
        "all jobs should remain queued after early shutdown"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn worker_pool_respects_concurrency_limit() -> anyhow::Result<()> {
    let s = setup().await?;

    // Enqueue 8 slow jobs.
    for i in 0..8 {
        s.queue
            .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, i)
            .await?;
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let max_concurrent = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let current = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let max_observed = Arc::clone(&max_concurrent);
    let current2 = Arc::clone(&current);

    let queue = Arc::clone(&s.queue);
    let rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        run_worker_pool(queue, 2, vec![], rx, move |_job| {
            let c = Arc::clone(&current2);
            let m = Arc::clone(&max_observed);
            async move {
                let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                m.fetch_max(n, std::sync::atomic::Ordering::SeqCst);
                // Simulate work.
                tokio::time::sleep(Duration::from_millis(50)).await;
                c.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .await;
    });

    // Wait for all done.
    for _ in 0..50 {
        let done = count_jobs(&s.pool, "status", "done").await?;
        if done == 8 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown_tx.send(true).ok();
    handle.await.unwrap();

    let max = max_concurrent.load(std::sync::atomic::Ordering::SeqCst);
    assert!(max <= 2, "concurrency {max} should not exceed pool size 2");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claimed_job_is_not_reclaimed_while_running() -> anyhow::Result<()> {
    let s = setup().await?;

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Claim it.
    let _ = s.queue.claim().await?;

    // Try to claim again — should return None (job is 'running').
    let second = s.queue.claim().await?;
    assert!(
        second.is_none(),
        "running job should not be claimable again"
    );

    // Complete it.
    s.queue.complete(id).await?;

    // Now it's done — should still not be claimable.
    let third = s.queue.claim().await?;
    assert!(third.is_none(), "done job should not be claimable");

    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn failed_job_becomes_claimable_after_backoff() -> anyhow::Result<()> {
    let s = setup_queue(100, 5).await?; // 100ms min backoff

    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 100)
        .await?;

    // Claim and fail it.
    let _ = s.queue.claim().await?;
    s.queue.fail(id, "transient").await?;

    // Immediately — should not be claimable (run_after is ~now + 200ms).
    let now = s.queue.claim().await?;
    assert!(now.is_none(), "failed job should wait for backoff");

    // Manually bump run_after to now.
    sqlx::query("UPDATE jobs SET run_after = now() WHERE id = $1")
        .bind(id)
        .execute(&s.pool)
        .await?;

    // Now it should be claimable.
    let retry = s
        .queue
        .claim()
        .await?
        .expect("should claim after backoff reset");
    assert_eq!(retry.id, id);
    assert_eq!(retry.attempts, 1);

    Ok(())
}

// ── lease / reaper / kind filter (P15-T2) ──────────────────────────────────────

/// Fetch a job's lease as (epoch_or_none, locked_by_or_none).
async fn job_lease(pool: &PgPool, job_id: i64) -> anyhow::Result<(Option<f64>, Option<String>)> {
    let row: (Option<f64>, Option<String>) = sqlx::query_as(
        "SELECT EXTRACT(EPOCH FROM lease_expires_at)::float8, locked_by \
         FROM jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Force a running job's lease into the past (simulates a crashed worker).
async fn expire_lease(pool: &PgPool, job_id: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE jobs SET lease_expires_at = now() - interval '5 seconds' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claim_stamps_lease_and_locked_by() -> anyhow::Result<()> {
    let s = setup().await?;
    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;

    let before = now_epoch(&s.pool).await?;
    let job = s.queue.claim().await?.expect("claimable");
    assert_eq!(job.id, id);

    let (lease, locked_by) = job_lease(&s.pool, id).await?;
    let lease = lease.expect("claim must set lease_expires_at");
    // Default lease is 600 s; allow generous slack for clock movement.
    assert!(
        lease > before + 500.0 && lease < before + 700.0,
        "lease ≈ now+600s, got delta {}",
        lease - before
    );
    assert_eq!(
        locked_by.as_deref(),
        Some(s.queue.worker_id()),
        "claim must stamp the claimant's worker id"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn claim_kinds_filters_kinds() -> anyhow::Result<()> {
    let s = setup().await?;
    s.queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let retag_id = s
        .queue
        .enqueue(s.tenant_id, None, None, Some(1), JobKind::Retag, 100)
        .await?;

    // An ingest-only claimant never receives the (higher-priority) retag job.
    let job = s
        .queue
        .claim_kinds(&[JobKind::Ingest])
        .await?
        .expect("ingest job claimable");
    assert_eq!(job.kind, JobKind::Ingest);

    // No more ingest jobs → None, even though a retag job is eligible.
    assert!(s.queue.claim_kinds(&[JobKind::Ingest]).await?.is_none());

    // A retag claimant gets it.
    let job = s
        .queue
        .claim_kinds(&[JobKind::Retag])
        .await?
        .expect("retag job claimable");
    assert_eq!(job.id, retag_id);
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn extend_lease_extends_own_running_job_only() -> anyhow::Result<()> {
    let s = setup().await?;
    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let _ = s.queue.claim().await?.expect("claimable");

    // Shrink the lease, then extend — it must move forward again.
    expire_lease(&s.pool, id).await?;
    assert!(s.queue.extend_lease(id).await?, "own running job extends");
    let (lease, _) = job_lease(&s.pool, id).await?;
    let now = now_epoch(&s.pool).await?;
    assert!(
        lease.expect("lease set") > now,
        "lease moved into the future"
    );

    // A different worker id cannot extend it.
    let other = JobQueue::new(s.pool.clone(), 1_000, 3).with_worker_id("other:1");
    assert!(
        !other.extend_lease(id).await?,
        "foreign worker must not extend"
    );

    // After completion, extension reports false (job no longer ours/running).
    s.queue.complete(id).await?;
    assert!(!s.queue.extend_lease(id).await?, "done job must not extend");
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn reaper_requeues_expired_lease_with_backoff() -> anyhow::Result<()> {
    let s = setup().await?; // max_retries = 3
    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let _ = s.queue.claim().await?.expect("claimable");
    expire_lease(&s.pool, id).await?;

    let reaped = s.queue.reap_expired_leases().await?;
    assert_eq!(reaped, 1, "exactly the expired job is reaped");

    // Normal fail() accounting applied: failed + attempts=1 + backoff + error.
    assert_eq!(job_status(&s.pool, id).await?, "failed");
    assert_eq!(job_attempts(&s.pool, id).await?, 1);
    let run_after = job_run_after_epoch(&s.pool, id).await?;
    assert!(
        run_after > now_epoch(&s.pool).await?,
        "backoff pushes run_after into the future"
    );
    let err: Option<String> = sqlx::query_scalar("SELECT last_error FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(err.as_deref(), Some(kb_pipeline::LEASE_EXPIRED_ERROR));

    // A second pass finds nothing (the job is no longer running) — the
    // SELECT-then-fail race resolves as a skip, never a double-increment.
    assert_eq!(s.queue.reap_expired_leases().await?, 0);
    assert_eq!(job_attempts(&s.pool, id).await?, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn reaper_dead_letters_at_max_retries() -> anyhow::Result<()> {
    let s = setup_queue(10, 1).await?; // max_retries = 1: first failure is fatal
    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let _ = s.queue.claim().await?.expect("claimable");
    expire_lease(&s.pool, id).await?;

    assert_eq!(s.queue.reap_expired_leases().await?, 1);
    assert_eq!(
        job_status(&s.pool, id).await?,
        "dead",
        "retry budget exhausted → dead-letter"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn reaper_ignores_live_leases() -> anyhow::Result<()> {
    let s = setup().await?;
    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let _ = s.queue.claim().await?.expect("claimable");

    // Lease is fresh (now + 600 s) — the reaper must not touch it.
    assert_eq!(s.queue.reap_expired_leases().await?, 0);
    assert_eq!(job_status(&s.pool, id).await?, "running");
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn reaped_job_is_reclaimable_after_backoff() -> anyhow::Result<()> {
    let s = setup_queue(10, 3).await?; // 10 ms base backoff
    let id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let _ = s.queue.claim().await?.expect("claimable");
    expire_lease(&s.pool, id).await?;
    assert_eq!(s.queue.reap_expired_leases().await?, 1);

    // Skip the (tiny) backoff deterministically, then re-claim: the fresh
    // claim stamps a fresh lease — full crashed-worker recovery round trip.
    sqlx::query("UPDATE jobs SET run_after = now() WHERE id = $1")
        .bind(id)
        .execute(&s.pool)
        .await?;
    let job = s.queue.claim().await?.expect("reaped job reclaimable");
    assert_eq!(job.id, id);
    assert_eq!(job.attempts, 1);
    let (lease, _) = job_lease(&s.pool, id).await?;
    assert!(
        lease.expect("fresh lease") > now_epoch(&s.pool).await?,
        "re-claim stamps a fresh lease"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn worker_pool_kind_filter_leaves_other_kinds_queued() -> anyhow::Result<()> {
    let s = setup().await?;
    s.queue
        .enqueue(s.tenant_id, None, None, None, JobKind::Ingest, 0)
        .await?;
    let email_id = s
        .queue
        .enqueue(s.tenant_id, None, None, None, JobKind::SendEmail, 100)
        .await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let queue = Arc::clone(&s.queue);
    let handle = tokio::spawn(async move {
        run_worker_pool(queue, 2, vec![JobKind::Ingest], shutdown_rx, |_job| async {
            Ok(())
        })
        .await;
    });

    // Wait until the ingest job is done.
    for _ in 0..50 {
        if count_jobs(&s.pool, "status", "done").await? == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown_tx.send(true).ok();
    handle.await.unwrap();

    assert_eq!(count_jobs(&s.pool, "status", "done").await?, 1);
    assert_eq!(
        job_status(&s.pool, email_id).await?,
        "queued",
        "the ingest-only pool must never claim a send_email job"
    );
    Ok(())
}

// ── P15-T9: dead-letter document hook + permanent failures ─────────────────────

/// Insert a pending document + one pending file, returning (doc_id, file_id).
async fn staged_doc(pool: &PgPool, tenant_id: i64, tag: &str) -> anyhow::Result<(i64, i64)> {
    let doc_id: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, kind, status) \
         VALUES ($1, 'document', 'pending') RETURNING id",
    )
    .bind(tenant_id)
    .fetch_one(pool)
    .await?;
    let file_id: i64 = sqlx::query_scalar(
        "INSERT INTO files (tenant_id, document_id, sha256, blob_key, status) \
         VALUES ($1, $2, $3, $4, 'pending') RETURNING id",
    )
    .bind(tenant_id)
    .bind(doc_id)
    .bind(tag.as_bytes().to_vec())
    .bind(format!("t{tenant_id}/{tag}"))
    .fetch_one(pool)
    .await?;
    Ok((doc_id, file_id))
}

async fn doc_status(pool: &PgPool, doc_id: i64) -> anyhow::Result<String> {
    Ok(
        sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
            .bind(doc_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn file_status(pool: &PgPool, file_id: i64) -> anyhow::Result<String> {
    Ok(sqlx::query_scalar("SELECT status FROM files WHERE id = $1")
        .bind(file_id)
        .fetch_one(pool)
        .await?)
}

/// Exhausting the retry budget on an ingest job marks its staged document —
/// and the document's pending files — `failed` in the same transaction.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn dead_letter_marks_document_and_files_failed() -> anyhow::Result<()> {
    let s = setup_queue(10, 2).await?;
    let (doc_id, file_id) = staged_doc(&s.pool, s.tenant_id, "dlhook").await?;
    let job_id = s
        .queue
        .enqueue(s.tenant_id, None, None, Some(doc_id), JobKind::Ingest, 0)
        .await?;

    // Attempt 1: failed (retry scheduled) — document untouched.
    let _ = s.queue.claim().await?;
    assert_eq!(s.queue.fail(job_id, "boom").await?, JobStatus::Failed);
    assert_eq!(doc_status(&s.pool, doc_id).await?, "pending");

    // Attempt 2 (= max_retries): dead — document + pending file flip failed.
    sqlx::query("UPDATE jobs SET status = 'queued', run_after = now() WHERE id = $1")
        .bind(job_id)
        .execute(&s.pool)
        .await?;
    let _ = s.queue.claim().await?;
    assert_eq!(s.queue.fail(job_id, "boom again").await?, JobStatus::Dead);
    assert_eq!(job_status(&s.pool, job_id).await?, "dead");
    assert_eq!(doc_status(&s.pool, doc_id).await?, "failed");
    assert_eq!(file_status(&s.pool, file_id).await?, "failed");
    Ok(())
}

/// A document that already converged to `ready` (idempotent replay race) is
/// never downgraded by a late dead-letter.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn dead_letter_never_downgrades_ready_document() -> anyhow::Result<()> {
    let s = setup_queue(10, 1).await?;
    let (doc_id, _file_id) = staged_doc(&s.pool, s.tenant_id, "dlready").await?;
    sqlx::query("UPDATE documents SET status = 'ready' WHERE id = $1")
        .bind(doc_id)
        .execute(&s.pool)
        .await?;
    let job_id = s
        .queue
        .enqueue(s.tenant_id, None, None, Some(doc_id), JobKind::Ingest, 0)
        .await?;

    let _ = s.queue.claim().await?;
    assert_eq!(s.queue.fail(job_id, "late failure").await?, JobStatus::Dead);
    assert_eq!(
        doc_status(&s.pool, doc_id).await?,
        "ready",
        "ready documents must never be downgraded by the dead-letter hook"
    );
    Ok(())
}

/// `fail_permanent` dead-letters on the FIRST attempt (deterministic failures
/// skip the retry budget) and fires the same document hook.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn fail_permanent_dead_letters_immediately_with_hook() -> anyhow::Result<()> {
    let s = setup_queue(10, 5).await?;
    let (doc_id, file_id) = staged_doc(&s.pool, s.tenant_id, "dlperm").await?;
    let job_id = s
        .queue
        .enqueue(s.tenant_id, None, None, Some(doc_id), JobKind::Ingest, 0)
        .await?;

    let _ = s.queue.claim().await?;
    assert_eq!(
        s.queue.fail_permanent(job_id, "invalid utf-8").await?,
        JobStatus::Dead
    );
    assert_eq!(job_status(&s.pool, job_id).await?, "dead");
    assert_eq!(job_attempts(&s.pool, job_id).await?, 1, "one attempt only");
    assert_eq!(doc_status(&s.pool, doc_id).await?, "failed");
    assert_eq!(file_status(&s.pool, file_id).await?, "failed");
    Ok(())
}

/// The worker pool routes `permanent: `-prefixed handler errors straight to
/// dead (prefix stripped from `last_error`), and plain errors to backoff.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn worker_pool_routes_permanent_errors_to_immediate_dead() -> anyhow::Result<()> {
    let s = setup_queue(60_000, 5).await?; // long backoff: a retried job stays parked
    let (doc_id, _file_id) = staged_doc(&s.pool, s.tenant_id, "dlroute").await?;
    let permanent_id = s
        .queue
        .enqueue(s.tenant_id, None, None, Some(doc_id), JobKind::Ingest, 0)
        .await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let queue = Arc::clone(&s.queue);
    let handle = tokio::spawn(kb_pipeline::run_worker_pool(
        queue,
        1,
        vec![JobKind::Ingest],
        shutdown_rx,
        move |_job| async move {
            Err(format!(
                "{}extraction failed for page 1 (x.bin): invalid utf-8",
                kb_pipeline::PERMANENT_ERROR_PREFIX
            ))
        },
    ));

    for _ in 0..100 {
        if job_status(&s.pool, permanent_id).await? == "dead" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown_tx.send(true).ok();
    handle.await.unwrap();

    assert_eq!(job_status(&s.pool, permanent_id).await?, "dead");
    assert_eq!(job_attempts(&s.pool, permanent_id).await?, 1);
    let last_error: String = sqlx::query_scalar("SELECT last_error FROM jobs WHERE id = $1")
        .bind(permanent_id)
        .fetch_one(&s.pool)
        .await?;
    assert!(
        last_error.starts_with("extraction failed"),
        "prefix must be stripped from last_error: {last_error}"
    );
    assert_eq!(doc_status(&s.pool, doc_id).await?, "failed");
    Ok(())
}
