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

use kb_core::budget::{BudgetStatus, check_budget};
use kb_metrics as metrics;
use kb_scheduler::Pool;
use kb_store::PgStore;
use tracing::{info, warn};

/// Run a metrics collection loop that polls state every `interval` and updates
/// Prometheus gauges.
///
/// The collector stops when `shutdown` signals. Each tick polls:
/// - Backend health, free/total slots, in-flight (from `pool`)
/// - Global queue depth (from `pg_store`)
/// - Per-tenant storage usage, active users, monthly spend, budget cap +
///   exceeded flag, and monthly token usage (from `pg_store`), plus the
///   rollup↔`usage_events` reconciliation (P14-T8)
///
/// The first tick runs immediately (before the first sleep) so the
/// `kb_backend_*`, `kb_queue_depth`, `kb_storage_bytes_used`, and the per-tenant
/// `kb_active_users` / `kb_tenant_*` families appear on `/metrics` from startup
/// rather than only after `interval` has elapsed.
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

/// Update the DB-derived runtime gauges from `pg_store` (BUG-OBS-06, P14-T8).
///
/// Publishes the global `kb_queue_depth` gauge from
/// [`PgStore::count_pending_jobs`], then iterates every tenant and publishes the
/// per-tenant gauges (storage, active users, monthly spend, budget cap +
/// exceeded flag, monthly tokens). It also reconciles each tenant's O(1) token
/// rollup against the authoritative `usage_events` SUM, self-healing drift
/// (P14-T8).
///
/// Every query is **best-effort**: a failure for one tenant (or one gauge) is
/// logged and skipped so the collector loop keeps running and the other gauges
/// still update. This is a slow background loop, so the per-tenant work stays
/// O(tenants) per poll.
async fn collect_runtime(pg_store: &PgStore) {
    match pg_store.count_pending_jobs().await {
        Ok(depth) => metrics::record_queue_depth(depth.max(0) as u64),
        Err(e) => warn!(error = %e, "metrics collector: failed to read queue depth"),
    }

    match pg_store.admin_list_tenants().await {
        Ok(tenants) => {
            for tenant in tenants {
                collect_tenant(pg_store, tenant.id, tenant.budget_monthly_cents).await;
            }
        }
        Err(e) => warn!(error = %e, "metrics collector: failed to list tenants"),
    }
}

/// Publish all per-tenant gauges and reconcile the token rollup for one tenant.
///
/// Each underlying read is independent and best-effort: a failure logs a warning
/// and that gauge is simply not updated this tick, leaving the others to publish.
/// `budget_cents` is the tenant's `budget_monthly_cents` (already loaded from the
/// `tenants` listing), used to derive the budget gauges without a second query.
async fn collect_tenant(pg_store: &PgStore, tenant_id: i64, budget_cents: Option<i64>) {
    match pg_store.get_storage_usage(tenant_id).await {
        Ok(bytes) => metrics::record_storage_bytes(tenant_id, bytes.max(0) as u64),
        Err(e) => warn!(
            error = %e,
            tenant_id,
            "metrics collector: failed to read storage usage"
        ),
    }

    match pg_store.admin_user_count(tenant_id).await {
        Ok(n) => metrics::record_active_users(tenant_id, n.max(0) as u64),
        Err(e) => warn!(
            error = %e,
            tenant_id,
            "metrics collector: failed to read active-user count"
        ),
    }

    // Monthly spend feeds both the spend gauge and the budget-exceeded flag, so
    // only publish the budget gauges when the spend read succeeds.
    match pg_store.get_monthly_cost(tenant_id).await {
        Ok(spend_micros) => publish_budget_gauges(tenant_id, budget_cents, spend_micros),
        Err(e) => warn!(
            error = %e,
            tenant_id,
            "metrics collector: failed to read monthly spend"
        ),
    }

    // Reconcile the rollup against the authoritative SUM and publish the healed
    // value as the monthly-tokens gauge in one step (reconcile returns it).
    match pg_store.reconcile_month_token_rollup(tenant_id).await {
        Ok(tokens) => metrics::record_tenant_tokens_monthly(tenant_id, tokens.max(0) as u64),
        Err(e) => warn!(
            error = %e,
            tenant_id,
            "metrics collector: failed to reconcile monthly token rollup"
        ),
    }
}

/// Publish the per-tenant budget gauges from a tenant's monthly spend and budget
/// cap (pure: only `record_*` calls, no I/O).
///
/// Sets `kb_tenant_spend_monthly_micros` (always), `kb_tenant_budget_cents` (the
/// cap, when one is configured), and `kb_tenant_budget_exceeded` (1 when spend
/// exceeds the cap, else 0) — the exceeded flag derived via the shared pure
/// [`check_budget`] so the gauge agrees with the enforcement path.
fn publish_budget_gauges(tenant_id: i64, budget_cents: Option<i64>, spend_micros: u64) {
    metrics::record_tenant_spend_monthly(tenant_id, spend_micros);

    // Publish the cap only when it is a real positive limit; a None/≤0 cap means
    // "unlimited" and has no meaningful gauge value.
    if let Some(cents) = budget_cents
        && cents > 0
    {
        metrics::record_tenant_budget_cents(tenant_id, cents as u64);
    }

    let exceeded = matches!(
        check_budget(spend_micros, budget_cents),
        BudgetStatus::OverBudget { .. }
    );
    metrics::record_tenant_budget_exceeded(tenant_id, exceeded);
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

    #[tokio::test]
    async fn collect_tenant_swallows_db_errors() {
        // Every per-tenant read errors against a closed port; collect_tenant must
        // log+skip each (storage, users, spend, reconcile) and return normally so
        // one bad tenant never aborts the loop (P14-T8 best-effort contract).
        let pg_store = PgStore::new("postgres://127.0.0.1:1/none?sslmode=disable");
        collect_tenant(&pg_store, 1, Some(1000)).await;
    }

    // ── Per-tenant budget-gauge publishing (P14-T8) ──────────────────────────

    #[test]
    fn publish_budget_gauges_within_budget() {
        ensure_init();
        // $5.00 spend (5_000_000 micros) vs a $10.00 cap (1000 cents) → under.
        // Use a distinct tenant id so other tests' gauges don't collide.
        publish_budget_gauges(9_001, Some(1000), 5_000_000);

        let text = kb_metrics::render();
        assert!(
            text.contains("kb_tenant_spend_monthly_micros{tenant_id=\"9001\"} 5000000"),
            "spend gauge missing/wrong: {text}"
        );
        assert!(
            text.contains("kb_tenant_budget_cents{tenant_id=\"9001\"} 1000"),
            "budget-cents gauge missing/wrong: {text}"
        );
        assert!(
            text.contains("kb_tenant_budget_exceeded{tenant_id=\"9001\"} 0"),
            "within-budget tenant must report exceeded=0: {text}"
        );
    }

    #[test]
    fn publish_budget_gauges_over_budget() {
        ensure_init();
        // $12.00 spend vs a $10.00 cap → exceeded.
        publish_budget_gauges(9_002, Some(1000), 12_000_000);

        let text = kb_metrics::render();
        assert!(
            text.contains("kb_tenant_spend_monthly_micros{tenant_id=\"9002\"} 12000000"),
            "spend gauge missing/wrong: {text}"
        );
        assert!(
            text.contains("kb_tenant_budget_exceeded{tenant_id=\"9002\"} 1"),
            "over-budget tenant must report exceeded=1: {text}"
        );
    }

    #[test]
    fn publish_budget_gauges_unlimited_has_no_cap_and_never_exceeds() {
        ensure_init();
        // No budget cap → no kb_tenant_budget_cents line for this tenant, and
        // exceeded is always 0 even for a huge spend.
        publish_budget_gauges(9_003, None, u64::MAX);

        let text = kb_metrics::render();
        assert!(
            text.contains("kb_tenant_budget_exceeded{tenant_id=\"9003\"} 0"),
            "unlimited tenant must report exceeded=0: {text}"
        );
        assert!(
            !text.contains("kb_tenant_budget_cents{tenant_id=\"9003\"}"),
            "unlimited tenant must not emit a budget-cents gauge: {text}"
        );
    }
}
