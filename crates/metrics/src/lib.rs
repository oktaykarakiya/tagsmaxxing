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
}
