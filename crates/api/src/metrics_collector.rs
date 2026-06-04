//! Periodic metrics collector that polls live state and updates Prometheus gauges.
//!
//! The collector runs as a background task and updates backend-level metrics
//! (health, free/total slots) from the scheduler [`kb_scheduler::Pool`] and
//! optionally updates queue and storage metrics when the relevant components are
//! available.
//!
//! Call [`start_metrics_collector`] once at startup. The collector runs until
//! the shutdown watch channel signals.

use std::sync::Arc;

use kb_metrics as metrics;
use kb_scheduler::Pool;
use kb_store::PgStore;
use tracing::{info, warn};

/// Run a metrics collection loop that polls state every `interval` and updates
/// Prometheus gauges.
///
/// The collector stops when `shutdown` signals. Each tick polls:
/// - Backend health, free/total slots, in-flight (from `pool`)
/// - Global queue depth and per-tenant storage usage (from `pg_store`)
///
/// The first tick runs immediately (before the first sleep) so the
/// `kb_backend_*`, `kb_queue_depth`, and `kb_storage_bytes_used` families appear
/// on `/metrics` from startup rather than only after `interval` has elapsed.
///
/// # Panics
///
/// Does not panic. Collection errors are logged and the loop continues.
pub async fn start_collector(
    pool: Arc<Pool>,
    pg_store: Arc<PgStore>,
    interval: std::time::Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    run_collector_loop(pool, pg_store, interval, shutdown).await;
}

/// Internal loop: poll → sleep → repeat until shutdown.
async fn run_collector_loop(
    pool: Arc<Pool>,
    pg_store: Arc<PgStore>,
    interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        collect_backends(&pool);
        collect_runtime(&pg_store).await;

        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(interval) => {},
        }
    }

    info!("metrics collector shut down");
}

/// Update the DB-derived runtime gauges from `pg_store` (BUG-OBS-06).
///
/// Publishes the global `kb_queue_depth` gauge from
/// [`PgStore::count_pending_jobs`] and one `kb_storage_bytes_used` gauge per
/// tenant from [`PgStore::get_storage_usage`]. Each query is best-effort: a
/// failure (e.g. transient DB error) is logged and skipped so the collector
/// loop keeps running and the other gauges still update.
async fn collect_runtime(pg_store: &PgStore) {
    match pg_store.count_pending_jobs().await {
        Ok(depth) => metrics::record_queue_depth(depth.max(0) as u64),
        Err(e) => warn!(error = %e, "metrics collector: failed to read queue depth"),
    }

    match pg_store.admin_list_tenants().await {
        Ok(tenants) => {
            for tenant in tenants {
                match pg_store.get_storage_usage(tenant.id).await {
                    Ok(bytes) => metrics::record_storage_bytes(tenant.id, bytes.max(0) as u64),
                    Err(e) => warn!(
                        error = %e,
                        tenant_id = tenant.id,
                        "metrics collector: failed to read storage usage"
                    ),
                }
            }
        }
        Err(e) => warn!(error = %e, "metrics collector: failed to list tenants"),
    }
}

/// Update backend-level gauges from the current pool state.
fn collect_backends(pool: &Pool) {
    use std::sync::atomic::Ordering;

    for backend in pool.all_backends() {
        let healthy = backend.healthy.load(Ordering::Acquire);
        let free = backend.free();
        let total = backend.max_concurrency;
        let in_flight = total.saturating_sub(free);

        metrics::record_backend(&backend.id, healthy, free, total, in_flight);
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use kb_core::role::Role;
    use kb_scheduler::test_backend;

    use super::*;

    /// Ensure the global metrics recorder is initialised before any test
    /// that reads metrics output.
    fn ensure_init() {
        let _ = kb_metrics::init_metrics();
    }

    #[test]
    fn collect_backends_updates_all_gauges() {
        ensure_init();

        let backend = Arc::new(test_backend(
            "test-gpu",
            "http://localhost:8001",
            vec![Role::Text, Role::Embed],
            10,
            4,
        ));
        // Consume one slot to test free vs total.
        let _permit = backend.capacity.try_acquire().unwrap();

        let pool = Pool::new(vec![Arc::clone(&backend)], Duration::from_secs(5));

        collect_backends(&pool);

        let text = kb_metrics::render();
        assert!(text.contains("kb_backend_healthy{backend_id=\"test-gpu\"} 1"));
        // 4 total, 1 held = 3 free.
        assert!(text.contains("kb_backend_free_slots{backend_id=\"test-gpu\"} 3"));
        assert!(text.contains("kb_backend_total_slots{backend_id=\"test-gpu\"} 4"));
        assert!(text.contains("kb_backend_in_flight{backend_id=\"test-gpu\"} 1"));

        drop(_permit);
    }

    #[test]
    fn collect_backends_unhealthy() {
        ensure_init();

        let backend = Arc::new(test_backend(
            "sick",
            "http://localhost:8001",
            vec![Role::Text],
            0,
            2,
        ));
        backend.healthy.store(false, Ordering::Release);

        let pool = Pool::new(vec![Arc::clone(&backend)], Duration::from_secs(5));

        collect_backends(&pool);

        let text = kb_metrics::render();
        assert!(text.contains("kb_backend_healthy{backend_id=\"sick\"} 0"));
    }

    #[test]
    fn collect_backends_multiple() {
        ensure_init();

        let a = Arc::new(test_backend(
            "gpu-a",
            "http://a:8001",
            vec![Role::Text],
            0,
            3,
        ));
        let b = Arc::new(test_backend(
            "gpu-b",
            "http://b:8002",
            vec![Role::Embed],
            1,
            2,
        ));

        let pool = Pool::new(vec![Arc::clone(&a), Arc::clone(&b)], Duration::from_secs(5));

        collect_backends(&pool);

        let text = kb_metrics::render();
        assert!(text.contains("kb_backend_healthy{backend_id=\"gpu-a\"} 1"));
        assert!(text.contains("kb_backend_healthy{backend_id=\"gpu-b\"} 1"));
        assert!(text.contains("kb_backend_total_slots{backend_id=\"gpu-a\"} 3"));
        assert!(text.contains("kb_backend_total_slots{backend_id=\"gpu-b\"} 2"));
    }

    #[tokio::test]
    async fn collector_stops_on_shutdown() {
        let backend = Arc::new(test_backend(
            "g",
            "http://localhost:8001",
            vec![Role::Text],
            0,
            2,
        ));

        let pool = Arc::new(Pool::new(vec![backend], Duration::from_secs(5)));
        // PgStore at a closed port → its queries error fast; the collector logs
        // and keeps running, so shutdown still works.
        let pg_store = Arc::new(PgStore::new("postgres://127.0.0.1:1/none?sslmode=disable"));
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Spawn the collector, then immediately shut down.
        let handle = tokio::spawn(async move {
            start_collector(pool, pg_store, Duration::from_secs(60), rx).await;
        });

        // Small sleep to let the collector start.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Signal shutdown.
        let _ = tx.send(true);
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "collector should shut down when signalled");
    }

    #[tokio::test]
    async fn collect_runtime_swallows_db_errors() {
        // PgStore pointed at a closed port: count_pending_jobs and
        // admin_list_tenants both error. collect_runtime must log and skip each,
        // returning normally (never panicking) so the loop keeps running.
        let pg_store = PgStore::new("postgres://127.0.0.1:1/none?sslmode=disable");
        // Completes without panic — exercises the error branches.
        collect_runtime(&pg_store).await;
    }
}
