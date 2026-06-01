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
use tracing::info;

/// Run a metrics collection loop that polls state every `interval` and updates
/// Prometheus gauges.
///
/// The collector stops when `shutdown` signals. It polls:
/// - Backend health, free slots, total slots, in-flight count (from `pool`)
/// - Queue depth (from `pg_store`, when available)
///
/// # Panics
///
/// Does not panic. Collection errors are logged and the loop continues.
pub async fn start_collector(
    pool: Arc<Pool>,
    interval: std::time::Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    run_collector_loop(pool, interval, shutdown).await;
}

/// Internal loop: poll → sleep → repeat until shutdown.
async fn run_collector_loop(
    pool: Arc<Pool>,
    interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        collect_backends(&pool);

        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(interval) => {},
        }
    }

    info!("metrics collector shut down");
}

/// Update backend-level gauges from the current pool state.
fn collect_backends(pool: &Pool) {
    use std::sync::atomic::Ordering;

    for backend in pool.all_backends() {
        let healthy = backend.healthy.load(Ordering::Acquire);
        let free = backend.free();
        let total = backend.max_slots;
        let in_flight = backend.in_flight.load(Ordering::Acquire);

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
    use kb_scheduler::Backend;

    use super::*;

    /// Ensure the global metrics recorder is initialised before any test
    /// that reads metrics output.
    fn ensure_init() {
        let _ = kb_metrics::init_metrics();
    }

    #[test]
    fn collect_backends_updates_all_gauges() {
        ensure_init();

        let backend = Arc::new(Backend::new(
            "test-gpu",
            "http://localhost:8001",
            vec![Role::Text, Role::Embed],
            10,
            4,
        ));
        // Consume one slot to test free vs total.
        let _permit = backend.capacity.try_acquire().unwrap();
        backend.in_flight.store(1, Ordering::Release);

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

        let backend = Arc::new(Backend::new(
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

        let a = Arc::new(Backend::new(
            "gpu-a",
            "http://a:8001",
            vec![Role::Text],
            0,
            3,
        ));
        let b = Arc::new(Backend::new(
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
        let backend = Arc::new(Backend::new(
            "g",
            "http://localhost:8001",
            vec![Role::Text],
            0,
            2,
        ));
        let pool = Arc::new(Pool::new(vec![backend], Duration::from_secs(5)));
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Spawn collector and shut it down immediately.
        let handle =
            tokio::spawn(async move { start_collector(pool, Duration::from_secs(60), rx).await });

        // Give it a moment to start, then signal shutdown.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = tx.send(true);

        // Collector must exit within a reasonable timeout.
        match tokio::time::timeout(Duration::from_secs(2), handle).await {
            Ok(Ok(())) => {} // collector exited cleanly
            Ok(Err(e)) => panic!("collector task panicked: {e}"),
            Err(_) => panic!("collector did not shut down within timeout"),
        }
    }
}
