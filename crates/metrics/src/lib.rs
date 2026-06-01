//! `kb-metrics`: Prometheus metrics + optional OpenTelemetry tracing (plan §15, §18).
//!
//! This crate provides the metrics infrastructure for the Local File Knowledge Base:
//!
//! - [`init_metrics`] — installs the global Prometheus recorder, registers all metric
//!   descriptions, and returns a static handle for rendering. Call once at startup.
//! - [`render`] — renders all currently collected metrics in the Prometheus text
//!   exposition format (`GET /metrics`).
//! - [`record_request`] — records an LLM request with outcome, duration, role, and model
//!   labels (call from the LLM client / API handlers).
//! - [`record_storage_bytes`] — sets the per-tenant storage gauge.
//! - [`record_active_users`] — sets the per-tenant active-users gauge.
//! - [`record_queue_depth`] — sets the queue depth and oldest-job-age gauges.
//! - [`record_degradation`] — sets the per-subsystem degradation gauge (P8-T9).
//! - [`record_circuit_breaker`] — sets the per-dependency circuit-breaker gauge (P8-T9).
//! - [`record_inflight_ingest`] — sets the in-flight ingest gauge (P8-T9).
//! - [`record_ingest_throttled`] — increments the throttled-ingest counter (P8-T9).
//! - [`record_orphan_gc_result`] — sets the orphan GC result gauges (P8-T10).
//! - [`record_integrity_scan_result`] — sets the integrity scan result gauges (P8-T10).
//!
//! # Optional features
//!
//! - `otlp`: enables [`init_otlp_tracing`] for exporting spans via OpenTelemetry OTLP
//!   (plan §18). Off by default to keep the dependency graph lean.

pub mod telemetry;

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// The singleton Prometheus handle, installed once at startup.
static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

// ── Initialisation ───────────────────────────────────────────────────────────────

/// Install the global Prometheus metrics recorder and register all metric
/// descriptions.
///
/// **Must be called once at startup.** Subsequent calls return the existing handle
/// without side effects. All [`metrics`] macros (`counter!`, `gauge!`, `histogram!`)
/// target the installed recorder.
///
/// # Panics
///
/// Panics if the Prometheus recorder cannot be installed — this is a fatal
/// startup error (the metrics endpoint would be useless without a recorder).
#[must_use]
pub fn init_metrics() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        let handle = match PrometheusBuilder::new().install_recorder() {
            Ok(h) => h,
            Err(e) => {
                panic!("failed to install prometheus metrics recorder: {e}");
            }
        };
        describe_all();
        handle
    })
}

/// Render all currently collected metrics in the Prometheus text exposition format.
///
/// Returns an empty string when [`init_metrics`] has not been called yet (the
/// metrics endpoint is harmless but empty before initialisation).
#[must_use]
pub fn render() -> String {
    match HANDLE.get() {
        Some(h) => h.render(),
        None => String::new(),
    }
}

// ── Metric descriptions ──────────────────────────────────────────────────────────

/// Register every metric name + help text with the globally installed recorder.
fn describe_all() {
    // Backend (per-backend, label = backend_id)
    metrics::describe_gauge!(
        "kb_backend_healthy",
        "1 if the backend is healthy (eligible for routing), 0 otherwise"
    );
    metrics::describe_gauge!(
        "kb_backend_free_slots",
        "Number of free concurrency slots on this backend"
    );
    metrics::describe_gauge!(
        "kb_backend_total_slots",
        "Total concurrency slots configured on this backend"
    );
    metrics::describe_gauge!(
        "kb_backend_in_flight",
        "Best-effort count of in-flight requests on this backend"
    );

    // Queue (global, no labels)
    metrics::describe_gauge!(
        "kb_queue_depth",
        "Number of jobs currently queued or waiting for retry"
    );
    metrics::describe_gauge!(
        "kb_queue_oldest_job_age_secs",
        "Age in seconds of the oldest queued/waiting job"
    );

    // Requests (labels: role, model)
    metrics::describe_counter!("kb_requests_total", "Total number of LLM requests made");
    metrics::describe_histogram!(
        "kb_request_duration_seconds",
        "LLM request duration in seconds"
    );
    metrics::describe_counter!(
        "kb_request_errors_total",
        "Total number of failed LLM requests"
    );

    // Storage (per-tenant, label = tenant_id)
    metrics::describe_gauge!(
        "kb_storage_bytes_used",
        "Total bytes used by blobs for this tenant"
    );

    // Active users (per-tenant, label = tenant_id)
    metrics::describe_gauge!("kb_active_users", "Number of active users in this tenant");

    // Restore-test (plan §21, P8-T7)
    metrics::describe_gauge!(
        "kb_restore_test_success",
        "1 if the most recent restore test passed, 0 if it failed"
    );
    metrics::describe_gauge!(
        "kb_backup_age_hours",
        "Age in hours of the latest backup (set by restore-test job)"
    );
    metrics::describe_gauge!(
        "kb_backup_stale",
        "1 if the latest backup is older than the configured maximum age, 0 otherwise"
    );

    // Degradation (plan §22, P8-T9)
    metrics::describe_gauge!(
        "kb_subsystem_degraded",
        "1 when the subsystem is degraded (label: subsystem)"
    );
    metrics::describe_gauge!(
        "kb_circuit_breaker_open",
        "1 when the circuit breaker is open for a dependency (label: dependency)"
    );
    metrics::describe_gauge!(
        "kb_inflight_ingest",
        "Number of currently in-flight ingest requests"
    );
    metrics::describe_counter!(
        "kb_ingest_throttled_total",
        "Number of ingest requests rejected due to backpressure (429)"
    );

    // Orphan GC (plan §23, P8-T10)
    metrics::describe_gauge!(
        "kb_orphan_gc_blobs_deleted",
        "Number of orphaned blobs deleted in the most recent GC run"
    );
    metrics::describe_gauge!(
        "kb_orphan_gc_blobs_found",
        "Number of orphaned blobs found in the most recent GC run"
    );
    metrics::describe_gauge!(
        "kb_missing_blobs_found",
        "Number of DB rows referencing blobs that do not exist in B2 (data-loss events)"
    );

    // Integrity scan (plan §23, P8-T10)
    metrics::describe_gauge!(
        "kb_integrity_scan_verified",
        "Number of blobs verified in the most recent integrity scan"
    );
    metrics::describe_gauge!(
        "kb_integrity_scan_failed",
        "Number of integrity check failures in the most recent scan"
    );
}

// ── Request-level recording helpers ──────────────────────────────────────────────

/// Outcome of an LLM request for metrics recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// The request completed successfully.
    Success,
    /// The request failed (transport error, HTTP 5xx, timeout, etc.).
    Error,
}

/// Record a completed LLM request in the metrics counters and histograms.
///
/// Call this from the LLM client after every chat / embed / rerank attempt
/// (per backend call, not per logical retry).
///
/// `duration_secs` is the wall-clock duration of the HTTP call.
/// `role` and `model` are the labels used for aggregation
/// (e.g. `role="text"`, `model="qwen3-vl"`).
pub fn record_request(outcome: RequestOutcome, duration_secs: f64, role: &str, model: &str) {
    metrics::counter!("kb_requests_total", "role" => role.to_owned(), "model" => model.to_owned())
        .increment(1);
    metrics::histogram!("kb_request_duration_seconds", "role" => role.to_owned(), "model" => model.to_owned())
        .record(duration_secs);

    if outcome == RequestOutcome::Error {
        metrics::counter!(
            "kb_request_errors_total",
            "role" => role.to_owned(),
            "model" => model.to_owned()
        )
        .increment(1);
    }
}

// ── Gauge update helpers ─────────────────────────────────────────────────────────

/// Set the per-tenant storage gauge.
///
/// `tenant_id` is converted to its string representation for the Prometheus label.
pub fn record_storage_bytes(tenant_id: i64, bytes: u64) {
    metrics::gauge!("kb_storage_bytes_used", "tenant_id" => tenant_id.to_string())
        .set(bytes as f64);
}

/// Set the per-tenant active-users gauge.
pub fn record_active_users(tenant_id: i64, count: u64) {
    metrics::gauge!("kb_active_users", "tenant_id" => tenant_id.to_string()).set(count as f64);
}

/// Set the queue depth gauge.
pub fn record_queue_depth(depth: u64) {
    metrics::gauge!("kb_queue_depth").set(depth as f64);
}

/// Set the oldest-job-age gauge in seconds.
pub fn record_queue_oldest_job_age(age_secs: f64) {
    metrics::gauge!("kb_queue_oldest_job_age_secs").set(age_secs);
}

/// Set all per-backend gauges at once for a single backend.
///
/// Called from the periodic metrics collector (in `kb-api` or wherever the
/// [`kb_scheduler::Pool`] is available).
pub fn record_backend(
    backend_id: &str,
    healthy: bool,
    free_slots: usize,
    total_slots: usize,
    in_flight: usize,
) {
    // Owned copies needed: the metrics macros require label values that live
    // long enough to populate the Prometheus registry key set.
    let bid = backend_id.to_owned();
    let healthy_val: f64 = if healthy { 1.0 } else { 0.0 };
    metrics::gauge!("kb_backend_healthy", "backend_id" => bid.clone()).set(healthy_val);
    metrics::gauge!("kb_backend_free_slots", "backend_id" => bid.clone()).set(free_slots as f64);
    metrics::gauge!("kb_backend_total_slots", "backend_id" => bid.clone()).set(total_slots as f64);
    metrics::gauge!("kb_backend_in_flight", "backend_id" => bid.clone()).set(in_flight as f64);
}

/// Set the restore-test success gauge.
///
/// Called after each restore-test run. `success` = 1.0 on pass, 0.0 on failure.
pub fn record_restore_test_result(success: bool) {
    let val: f64 = if success { 1.0 } else { 0.0 };
    metrics::gauge!("kb_restore_test_success").set(val);
}

/// Set the backup age gauge (hours since the latest backup completed).
pub fn record_backup_age_hours(age_hours: f64) {
    metrics::gauge!("kb_backup_age_hours").set(age_hours);
}

/// Set the backup-stale alert gauge.
///
/// `stale` = 1.0 when the latest backup is older than the configured maximum age.
pub fn record_backup_stale(stale: bool) {
    let val: f64 = if stale { 1.0 } else { 0.0 };
    metrics::gauge!("kb_backup_stale").set(val);
}

// ── Degradation metrics (plan §22, P8-T9) ────────────────────────────────────────

/// Set the per-subsystem degradation gauge.
///
/// `subsystem` is the human-readable name (e.g. `"blob-store"`, `"embed"`).
/// `degraded` = 1.0 when the subsystem is in a degraded state.
pub fn record_degradation(subsystem: &str, degraded: bool) {
    let val: f64 = if degraded { 1.0 } else { 0.0 };
    metrics::gauge!("kb_subsystem_degraded", "subsystem" => subsystem.to_owned()).set(val);
}

/// Set the per-dependency circuit-breaker gauge.
///
/// `dependency` identifies the external dependency (e.g. `"b2"`).
/// `open` = 1.0 when the circuit breaker is tripped (open).
pub fn record_circuit_breaker(dependency: &str, open: bool) {
    let val: f64 = if open { 1.0 } else { 0.0 };
    metrics::gauge!("kb_circuit_breaker_open", "dependency" => dependency.to_owned()).set(val);
}

/// Set the in-flight ingest gauge.
pub fn record_inflight_ingest(count: usize) {
    metrics::gauge!("kb_inflight_ingest").set(count as f64);
}

/// Increment the throttled-ingest counter.
///
/// Call when an ingest request is rejected with 429 due to backpressure.
pub fn record_ingest_throttled() {
    metrics::counter!("kb_ingest_throttled_total").increment(1);
}

// ── Orphan GC metrics (plan §23, P8-T10) ─────────────────────────────────────────────

/// Set the orphan GC result gauges.
///
/// Called after each orphan GC run. `blobs_found` is the total number of orphaned
/// blobs detected; `blobs_deleted` is how many were actually deleted (excluding
/// those still within the grace period). `missing_rows` counts DB rows whose
/// referenced blob does not exist in B2.
pub fn record_orphan_gc_result(blobs_found: u64, blobs_deleted: u64, missing_rows: u64) {
    metrics::gauge!("kb_orphan_gc_blobs_found").set(blobs_found as f64);
    metrics::gauge!("kb_orphan_gc_blobs_deleted").set(blobs_deleted as f64);
    metrics::gauge!("kb_missing_blobs_found").set(missing_rows as f64);
}

// ── Integrity scan metrics (plan §23, P8-T10) ────────────────────────────────────────

/// Set the integrity scan result gauges.
///
/// Called after each integrity scan run. `verified` is the number of blobs that
/// passed the hash check; `failed` is the number whose computed SHA-256 did not
/// match the stored value.
pub fn record_integrity_scan_result(verified: u64, failed: u64) {
    metrics::gauge!("kb_integrity_scan_verified").set(verified as f64);
    metrics::gauge!("kb_integrity_scan_failed").set(failed as f64);
}

// ── Tests ────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Ensure `init_metrics()` is called before any test that reads metrics.
    fn ensure_init() -> &'static PrometheusHandle {
        init_metrics()
    }

    #[test]
    fn init_is_idempotent() {
        let h1 = init_metrics();
        let h2 = init_metrics();
        // Same handle, no panic on second call.
        assert!(std::ptr::eq(h1, h2));
    }

    #[test]
    fn render_returns_text_before_init() {
        // Without init, render returns empty (no panic).
        // (init may already have been called by another test; we just assert
        // the function never panics.)
        let text = render();
        // Either empty (no init yet) or populated (init already called).
        // Both are valid — we just assert no panic.
        let _ = text;
    }

    #[test]
    fn request_counter_and_histogram() {
        let _h = ensure_init();
        record_request(RequestOutcome::Success, 0.42, "text", "qwen3");
        record_request(RequestOutcome::Error, 1.23, "embed", "bge-m3");

        let text = render();
        // Counter for successful request.
        assert!(text.contains("kb_requests_total{role=\"text\",model=\"qwen3\"} 1"));
        // Counter for failed request.
        assert!(text.contains("kb_requests_total{role=\"embed\",model=\"bge-m3\"} 1"));
        // Error counter.
        assert!(text.contains("kb_request_errors_total{role=\"embed\",model=\"bge-m3\"} 1"));
        // Histogram renders as summary with quantiles (metrics-exporter-prometheus v0.16).
        assert!(text.contains("kb_request_duration_seconds{role=\"text\",model=\"qwen3\""));
        assert!(
            text.contains("kb_request_duration_seconds_sum{role=\"text\",model=\"qwen3\"} 0.42")
        );
        assert!(
            text.contains("kb_request_duration_seconds_count{role=\"text\",model=\"qwen3\"} 1")
        );
        assert!(
            text.contains("kb_request_duration_seconds_sum{role=\"embed\",model=\"bge-m3\"} 1.23")
        );
    }

    #[test]
    fn storage_bytes_gauge() {
        let _h = ensure_init();
        record_storage_bytes(1, 1024);
        record_storage_bytes(2, 2048);

        let text = render();
        assert!(text.contains("kb_storage_bytes_used{tenant_id=\"1\"} 1024"));
        assert!(text.contains("kb_storage_bytes_used{tenant_id=\"2\"} 2048"));
    }

    #[test]
    fn active_users_gauge() {
        let _h = ensure_init();
        record_active_users(1, 5);

        let text = render();
        assert!(text.contains("kb_active_users{tenant_id=\"1\"} 5"));
    }

    #[test]
    fn queue_depth_gauge() {
        let _h = ensure_init();
        record_queue_depth(42);
        record_queue_oldest_job_age(300.0);

        let text = render();
        assert!(text.contains("kb_queue_depth 42"));
        assert!(text.contains("kb_queue_oldest_job_age_secs 300"));
    }

    #[test]
    fn backend_gauge() {
        let _h = ensure_init();
        record_backend("gpu-a", true, 3, 4, 1);
        record_backend("gpu-b", false, 0, 2, 2);

        let text = render();
        assert!(text.contains("kb_backend_healthy{backend_id=\"gpu-a\"} 1"));
        assert!(text.contains("kb_backend_free_slots{backend_id=\"gpu-a\"} 3"));
        assert!(text.contains("kb_backend_total_slots{backend_id=\"gpu-a\"} 4"));
        assert!(text.contains("kb_backend_in_flight{backend_id=\"gpu-a\"} 1"));

        assert!(text.contains("kb_backend_healthy{backend_id=\"gpu-b\"} 0"));
        assert!(text.contains("kb_backend_free_slots{backend_id=\"gpu-b\"} 0"));
        assert!(text.contains("kb_backend_total_slots{backend_id=\"gpu-b\"} 2"));
        assert!(text.contains("kb_backend_in_flight{backend_id=\"gpu-b\"} 2"));
    }

    #[test]
    fn render_includes_help_and_type_lines() {
        let _h = ensure_init();

        // The Prometheus exporter only includes metrics that have been
        // observed at least once — record a data-point for each checked metric.
        // Use distinct labels so other tests' counters are unaffected.
        record_backend("help-test", true, 0, 4, 0);
        record_request(
            RequestOutcome::Success,
            0.001,
            "help-test-role",
            "help-test-model",
        );

        let text = render();
        // Prometheus exposition format requires HELP and TYPE lines.
        assert!(text.contains("# HELP kb_backend_healthy"));
        assert!(text.contains("# TYPE kb_backend_healthy gauge"));
        assert!(text.contains("# HELP kb_requests_total"));
        assert!(text.contains("# TYPE kb_requests_total counter"));
        assert!(text.contains("# HELP kb_request_duration_seconds"));
        assert!(text.contains("# TYPE kb_request_duration_seconds summary"));
    }

    #[test]
    fn record_request_success_does_not_increment_errors() {
        let _h = ensure_init();
        // Render baseline first (other tests may have left error counts).
        let baseline = render();
        let baseline_has_errors = baseline.contains("kb_request_errors_total{role=\"rerank\"");

        record_request(RequestOutcome::Success, 0.1, "rerank", "bge-reranker");

        let text = render();
        // Success should NOT increment error counter beyond baseline.
        // We check that no NEW error line appeared for this specific combo
        // if it wasn't there before.
        if !baseline_has_errors {
            assert!(!text.contains("kb_request_errors_total{role=\"rerank\""));
        }
    }

    #[test]
    fn restore_test_success_gauge() {
        let _h = ensure_init();
        record_restore_test_result(true);
        let text = render();
        assert!(
            text.lines()
                .any(|l| l.starts_with("kb_restore_test_success ")),
            "kb_restore_test_success not found in: {text}"
        );

        record_restore_test_result(false);
        let text2 = render();
        assert!(
            text2
                .lines()
                .any(|l| l.starts_with("kb_restore_test_success ")),
            "kb_restore_test_success not found in: {text2}"
        );
    }

    #[test]
    fn backup_age_and_stale_gauges() {
        let _h = ensure_init();
        // Record distinct values so the gauge family is populated.
        record_backup_age_hours(3.5);
        record_backup_stale(false);

        let text = render();
        // The gauges exist in the output with numeric values.
        assert!(
            text.lines().any(|l| l.starts_with("kb_backup_age_hours ")),
            "kb_backup_age_hours not found in: {text}"
        );
        assert!(
            text.lines().any(|l| l.starts_with("kb_backup_stale ")),
            "kb_backup_stale not found in: {text}"
        );

        // Update values and verify the metric families still render.
        record_backup_age_hours(26.0);
        record_backup_stale(true);

        let text2 = render();
        assert!(text2.lines().any(|l| l.starts_with("kb_backup_age_hours ")));
        assert!(text2.lines().any(|l| l.starts_with("kb_backup_stale ")));
    }

    #[test]
    fn restore_test_gauge_help_and_type() {
        let _h = ensure_init();
        record_restore_test_result(true);
        record_backup_age_hours(1.0);
        record_backup_stale(false);

        let text = render();
        assert!(text.contains("# HELP kb_restore_test_success"));
        assert!(text.contains("# TYPE kb_restore_test_success gauge"));
        assert!(text.contains("# HELP kb_backup_age_hours"));
        assert!(text.contains("# TYPE kb_backup_age_hours gauge"));
        assert!(text.contains("# HELP kb_backup_stale"));
        assert!(text.contains("# TYPE kb_backup_stale gauge"));
    }

    // ── Degradation metrics tests (P8-T9) ────────────────────────────────────

    #[test]
    fn subsystem_degraded_gauge_present() {
        let _h = ensure_init();
        record_degradation("blob-store", true);
        record_degradation("embed", false);

        let text = render();
        // Both label variants are present in the output (exact values may
        // be overwritten by concurrent tests, so we only assert the metric
        // families and labels exist).
        assert!(
            text.contains("kb_subsystem_degraded{subsystem=\"blob-store\"}"),
            "blob-store subsystem not found in: {text}"
        );
        assert!(
            text.contains("kb_subsystem_degraded{subsystem=\"embed\"}"),
            "embed subsystem not found in: {text}"
        );
    }

    #[test]
    fn circuit_breaker_open_gauge_present() {
        let _h = ensure_init();
        record_circuit_breaker("b2", true);
        record_circuit_breaker("embed-backend", false);

        let text = render();
        // Assert the metric families exist (values may be racy under
        // parallel test execution since gauges are global).
        assert!(
            text.contains("kb_circuit_breaker_open{dependency=\"b2\"}"),
            "b2 circuit breaker not found in: {text}"
        );
        assert!(
            text.contains("kb_circuit_breaker_open{dependency=\"embed-backend\"}"),
            "embed-backend circuit breaker not found in: {text}"
        );
    }

    #[test]
    fn inflight_ingest_gauge_present() {
        let _h = ensure_init();
        record_inflight_ingest(7);

        let text = render();
        // Assert the metric name is present (value may be racy under
        // parallel execution since the gauge is global and unlabelled).
        assert!(
            text.contains("kb_inflight_ingest"),
            "kb_inflight_ingest not found in: {text}"
        );
    }

    #[test]
    fn ingest_throttled_counter_present() {
        let _h = ensure_init();
        record_ingest_throttled();

        let text = render();
        // Counter values accumulate; assert the counter exists.
        assert!(
            text.contains("kb_ingest_throttled_total"),
            "kb_ingest_throttled_total not found in: {text}"
        );
    }

    #[test]
    fn degradation_metrics_have_help_and_type() {
        let _h = ensure_init();
        record_degradation("blob-store", false);
        record_circuit_breaker("b2", false);
        record_inflight_ingest(0);
        record_ingest_throttled();

        let text = render();
        assert!(text.contains("# HELP kb_subsystem_degraded"));
        assert!(text.contains("# TYPE kb_subsystem_degraded gauge"));
        assert!(text.contains("# HELP kb_circuit_breaker_open"));
        assert!(text.contains("# TYPE kb_circuit_breaker_open gauge"));
        assert!(text.contains("# HELP kb_inflight_ingest"));
        assert!(text.contains("# TYPE kb_inflight_ingest gauge"));
        assert!(text.contains("# HELP kb_ingest_throttled_total"));
        assert!(text.contains("# TYPE kb_ingest_throttled_total counter"));
    }

    // ── Orphan GC metrics tests (P8-T10) ────────────────────────────────────

    #[test]
    fn orphan_gc_result_gauges() {
        let _h = ensure_init();
        record_orphan_gc_result(10, 7, 3);

        let text = render();
        // All three gauges should be present (values may be overwritten by
        // parallel tests — gauges are global and unlabelled).
        assert!(
            text.contains("kb_orphan_gc_blobs_found"),
            "kb_orphan_gc_blobs_found not found in: {text}"
        );
        assert!(
            text.contains("kb_orphan_gc_blobs_deleted"),
            "kb_orphan_gc_blobs_deleted not found in: {text}"
        );
        assert!(
            text.contains("kb_missing_blobs_found"),
            "kb_missing_blobs_found not found in: {text}"
        );
    }

    #[test]
    fn orphan_gc_metrics_have_help_and_type() {
        let _h = ensure_init();
        record_orphan_gc_result(1, 1, 0);

        let text = render();
        assert!(text.contains("# HELP kb_orphan_gc_blobs_found"));
        assert!(text.contains("# TYPE kb_orphan_gc_blobs_found gauge"));
        assert!(text.contains("# HELP kb_orphan_gc_blobs_deleted"));
        assert!(text.contains("# TYPE kb_orphan_gc_blobs_deleted gauge"));
        assert!(text.contains("# HELP kb_missing_blobs_found"));
        assert!(text.contains("# TYPE kb_missing_blobs_found gauge"));
    }

    // ── Integrity scan metrics tests (P8-T10) ───────────────────────────────

    #[test]
    fn integrity_scan_result_gauges() {
        let _h = ensure_init();
        record_integrity_scan_result(42, 0);

        let text = render();
        // Gauges should be present (values may be overwritten by parallel tests).
        assert!(
            text.contains("kb_integrity_scan_verified"),
            "kb_integrity_scan_verified not found in: {text}"
        );
        assert!(
            text.contains("kb_integrity_scan_failed"),
            "kb_integrity_scan_failed not found in: {text}"
        );
    }

    #[test]
    fn integrity_scan_failure_gauges() {
        let _h = ensure_init();
        record_integrity_scan_result(38, 4);

        let text = render();
        // Gauges should be present (values may be overwritten by parallel tests).
        assert!(
            text.contains("kb_integrity_scan_verified"),
            "kb_integrity_scan_verified not found in: {text}"
        );
        assert!(
            text.contains("kb_integrity_scan_failed"),
            "kb_integrity_scan_failed not found in: {text}"
        );
    }

    #[test]
    fn integrity_scan_metrics_have_help_and_type() {
        let _h = ensure_init();
        record_integrity_scan_result(1, 0);

        let text = render();
        assert!(text.contains("# HELP kb_integrity_scan_verified"));
        assert!(text.contains("# TYPE kb_integrity_scan_verified gauge"));
        assert!(text.contains("# HELP kb_integrity_scan_failed"));
        assert!(text.contains("# TYPE kb_integrity_scan_failed gauge"));
    }
}
