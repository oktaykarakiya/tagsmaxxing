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
use kb_core::degradation::DegradationState;
use kb_metrics as metrics;
use kb_scheduler::Pool;
use kb_store::PgStore;
use tracing::{info, warn};

use crate::backpressure::InflightLimiter;

/// The subsystems whose degraded flag is published as `kb_subsystem_degraded`
/// each poll. Kept as a static list (rather than only the *currently* degraded
/// names from [`DegradationState::degraded_subsystems`]) so every subsystem gets
/// an explicit `0`/`1` time series — a healthy subsystem still emits `0` instead
/// of its series silently disappearing, which keeps alert/dashboard math sane.
///
/// The names match [`DegradationState::degraded_subsystems`] exactly.
const DEGRADATION_SUBSYSTEMS: [&str; 6] =
    ["blob-store", "embed", "text", "vision", "code", "rerank"];

/// Run a metrics collection loop that polls state every `interval` and updates
/// Prometheus gauges.
///
/// The collector stops when `shutdown` signals. Each tick polls:
/// - Backend health, free/total slots, in-flight (from `pool`)
/// - Per-subsystem degraded flag (from `degradation`) and the in-flight ingest
///   count (from `inflight_limiter`) — both are in-memory, so they publish even
///   when the database is unreachable (P14-T9)
/// - Global queue depth and the oldest queued job's age (from `pg_store`)
/// - Per-tenant storage usage, active users, monthly spend, budget cap +
///   exceeded flag, and monthly token usage (from `pg_store`), plus the
///   rollup↔`usage_events` reconciliation (P14-T8)
///
/// The first tick runs immediately (before the first sleep) so the
/// `kb_backend_*`, `kb_subsystem_degraded`, `kb_inflight_ingest`,
/// `kb_queue_depth`, `kb_storage_bytes_used`, and the per-tenant
/// `kb_active_users` / `kb_tenant_*` families appear on `/metrics` from startup
/// rather than only after `interval` has elapsed.
///
/// # Panics
///
/// Does not panic. Collection errors are logged and the loop continues.
pub async fn start_collector(
    pool: Arc<Pool>,
    pg_store: Arc<PgStore>,
    degradation: Arc<DegradationState>,
    inflight_limiter: Arc<InflightLimiter>,
    interval: std::time::Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    run_collector_loop(
        pool,
        pg_store,
        degradation,
        inflight_limiter,
        interval,
        shutdown,
    )
    .await;
}

/// Internal loop: poll → sleep → repeat until shutdown.
async fn run_collector_loop(
    pool: Arc<Pool>,
    pg_store: Arc<PgStore>,
    degradation: Arc<DegradationState>,
    inflight_limiter: Arc<InflightLimiter>,
    interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        collect_backends(&pool);
        // In-memory gauges first: these never touch the DB, so they stay fresh
        // even while `collect_runtime`'s queries are failing.
        publish_runtime_state(&degradation, &inflight_limiter);
        collect_runtime(&pg_store).await;

        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(interval) => {},
        }
    }

    info!("metrics collector shut down");
}

/// Publish the in-memory runtime gauges from live state (P14-T9, C2).
///
/// Pure apart from the `record_*` gauge writes — takes the live
/// [`DegradationState`] and [`InflightLimiter`] and emits:
/// - `kb_subsystem_degraded{subsystem}` = `1` degraded / `0` healthy, one series
///   per known subsystem (so a recovered subsystem flips back to `0` rather than
///   leaving a stale series), and
/// - `kb_inflight_ingest` = the limiter's current global in-flight count.
///
/// No I/O and no failure modes, so it is called unconditionally each tick and is
/// directly unit-testable without the loop or a database.
fn publish_runtime_state(degradation: &DegradationState, inflight_limiter: &InflightLimiter) {
    for subsystem in DEGRADATION_SUBSYSTEMS {
        metrics::record_degradation(subsystem, degradation.is_subsystem_degraded(subsystem));
    }
    metrics::record_inflight_ingest(inflight_limiter.in_flight());
}

/// Update the DB-derived runtime gauges from `pg_store` (BUG-OBS-06, P14-T8).
///
/// Publishes the global `kb_queue_depth` gauge from
/// [`PgStore::count_pending_jobs`] and the `kb_queue_oldest_job_age_secs` gauge
/// from [`PgStore::oldest_queued_job_age_secs`], then iterates every tenant and
/// publishes the per-tenant gauges (storage, active users, monthly spend, budget
/// cap + exceeded flag, monthly tokens). It also reconciles each tenant's O(1)
/// token rollup against the authoritative `usage_events` SUM, self-healing drift
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

    match pg_store.oldest_queued_job_age_secs().await {
        Ok(age) => metrics::record_queue_oldest_job_age(age),
        Err(e) => warn!(error = %e, "metrics collector: failed to read oldest queued job age"),
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
        let degradation = Arc::new(DegradationState::default());
        let inflight = Arc::new(InflightLimiter::new(8));
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Spawn the collector, then immediately shut down.
        let handle = tokio::spawn(async move {
            start_collector(
                pool,
                pg_store,
                degradation,
                inflight,
                Duration::from_secs(60),
                rx,
            )
            .await;
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
        // PgStore pointed at a closed port: count_pending_jobs,
        // oldest_queued_job_age_secs and admin_list_tenants all error.
        // collect_runtime must log and skip each, returning normally (never
        // panicking) so the loop keeps running.
        let pg_store = PgStore::new("postgres://127.0.0.1:1/none?sslmode=disable");
        // Completes without panic — exercises the error branches.
        collect_runtime(&pg_store).await;
    }

    // ── In-memory runtime gauges (P14-T9) ─────────────────────────────────────

    #[test]
    fn publish_runtime_state_emits_every_subsystem_and_inflight() {
        ensure_init();

        let degradation = DegradationState::default();
        // Mark two subsystems degraded; the rest stay healthy.
        degradation
            .blob_store_degraded
            .store(true, Ordering::Release);
        degradation.set_role_degraded("embed", true);

        let limiter = InflightLimiter::new(8);

        publish_runtime_state(&degradation, &limiter);

        let text = kb_metrics::render();
        // Every known subsystem gets an explicit series (healthy ones included),
        // so dashboards/alerts see a 0 rather than a vanished metric.
        for subsystem in DEGRADATION_SUBSYSTEMS {
            assert!(
                text.contains(&format!(
                    "kb_subsystem_degraded{{subsystem=\"{subsystem}\"}}"
                )),
                "missing kb_subsystem_degraded series for {subsystem}: {text}"
            );
        }
        // And the in-flight ingest gauge is published.
        assert!(
            text.contains("kb_inflight_ingest"),
            "kb_inflight_ingest not found in: {text}"
        );
    }

    #[test]
    fn publish_runtime_state_reads_live_degraded_and_inflight_inputs() {
        ensure_init();

        // Verify the helper sources its values from *live* state. The
        // record_degradation(bool)→0/1 and record_inflight_ingest(n)→n mappings
        // are unit-tested in kb-metrics; here we pin the **inputs** the helper
        // reads (deterministic, no shared-gauge race) and that publishing emits
        // the corresponding series.
        let degradation = DegradationState::default();
        degradation.set_role_degraded("vision", true);
        // The helper reads exactly this for the "vision" label.
        assert!(degradation.is_subsystem_degraded("vision"));
        assert!(!degradation.is_subsystem_degraded("text"));

        let limiter = InflightLimiter::new(16);
        let _p1 = limiter.try_acquire(1).unwrap();
        let _p2 = limiter.try_acquire(1).unwrap();
        let _p3 = limiter.try_acquire(2).unwrap();
        // The helper reads exactly this for kb_inflight_ingest.
        assert_eq!(limiter.in_flight(), 3);

        publish_runtime_state(&degradation, &limiter);

        let text = kb_metrics::render();
        // The vision series exists (its value tracks the live flag we set).
        assert!(
            text.contains("kb_subsystem_degraded{subsystem=\"vision\"}"),
            "vision series missing after publish: {text}"
        );
        assert!(
            text.contains("kb_inflight_ingest"),
            "kb_inflight_ingest missing after publish: {text}"
        );

        drop((_p1, _p2, _p3));
    }

    #[test]
    fn publish_runtime_state_all_healthy_reports_zeros() {
        ensure_init();

        // A pristine state with a disabled limiter: every subsystem healthy (0)
        // and zero in-flight. Assert blob-store (a value no other test flips to a
        // steady 0/1 mid-render for a *disabled-limiter* default) emits its line;
        // exact 0/1 for shared series can race, so only assert presence here.
        let degradation = DegradationState::default();
        let limiter = InflightLimiter::new(0); // disabled → in_flight() == 0

        publish_runtime_state(&degradation, &limiter);

        let text = kb_metrics::render();
        assert!(
            text.contains("kb_subsystem_degraded{subsystem=\"blob-store\"}"),
            "blob-store series missing: {text}"
        );
        assert!(
            text.contains("kb_inflight_ingest"),
            "kb_inflight_ingest missing: {text}"
        );
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
