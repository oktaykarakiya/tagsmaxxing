//! `kb-metrics`: Prometheus metrics + optional OpenTelemetry tracing (plan §15, §18).
//!
//! This crate provides the metrics infrastructure for the Local File Knowledge Base:
//!
//! - [`init_metrics`] — installs the global Prometheus recorder, registers all metric
//!   descriptions, seeds the backup/DR metric families to a baseline so they appear on
//!   `/metrics` from startup, and returns a static handle for rendering. Call once at startup.
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
//! - [`record_maintenance_success`] — sets the maintenance success gauge (P8-T11).
//! - [`record_maintenance_failure`] — sets the maintenance failure gauge (P8-T11).
//! - [`record_tenant_tokens_monthly`] — sets the per-tenant monthly token-usage gauge (P14-T8).
//! - [`record_tokens`] — counts tokens at the metering seam, labelled by role + model (P14-T10).
//! - [`record_metering_write_failure`] — counts lost usage-accounting writes (P14-T10).
//! - [`record_rate_limit_rejection`] — counts 429 rate-limit rejections by kind (P14-T10).
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
        seed_backup_dr_metrics();
        seed_runtime_metrics();
        handle
    })
}

/// Render all currently collected metrics in the Prometheus text exposition format.
///
/// Returns an empty string when [`init_metrics`] has not been called yet (the
/// metrics endpoint is harmless but empty before initialisation).
///
/// The output is **canonicalised** ([`canonicalize_exposition`]): metric families
/// are ordered by name and the samples within each family are sorted, so repeated
/// calls produce byte-identical output. The underlying exporter renders families
/// and label-sets in nondeterministic hash-map order; stable output keeps
/// `/metrics` diffable and lets callers compare snapshots without spurious churn.
#[must_use]
pub fn render() -> String {
    match HANDLE.get() {
        Some(h) => canonicalize_exposition(&h.render()),
        None => String::new(),
    }
}

/// Reorder a Prometheus text-exposition document into a deterministic form.
///
/// The input is split into metric-family blocks (separated by blank lines, as the
/// exporter emits them). Within each block the leading `# HELP` / `# TYPE` comment
/// lines are kept in place and the sample lines are sorted; blocks are then ordered
/// by metric name. This preserves the exposition format (comments stay grouped with
/// their samples) while removing the exporter's run-to-run ordering nondeterminism.
fn canonicalize_exposition(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let mut blocks: Vec<(String, String)> = trimmed
        .split("\n\n")
        .filter_map(|block| {
            let mut comments: Vec<&str> = Vec::new();
            let mut samples: Vec<&str> = Vec::new();
            for line in block.lines() {
                if line.starts_with('#') {
                    comments.push(line);
                } else if !line.trim().is_empty() {
                    samples.push(line);
                }
            }
            if comments.is_empty() && samples.is_empty() {
                return None;
            }
            let key = block_sort_key(&comments, &samples);
            samples.sort_unstable();

            let mut out = String::new();
            for line in comments.iter().chain(samples.iter()) {
                out.push_str(line);
                out.push('\n');
            }
            Some((key, out.trim_end().to_owned()))
        })
        .collect();

    blocks.sort_by(|a, b| a.0.cmp(&b.0));
    blocks
        .into_iter()
        .map(|(_, body)| body)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Determine the metric name used to order a family block.
///
/// Prefers the name in a `# HELP <name> …` or `# TYPE <name> …` comment; if a block
/// has no such comment, falls back to the metric name of its first sample line (the
/// text before the first `{` or space). Returns an empty string for an empty block.
fn block_sort_key(comments: &[&str], samples: &[&str]) -> String {
    for comment in comments {
        let mut parts = comment.split_whitespace();
        let _hash = parts.next(); // "#"
        if matches!(parts.next(), Some("HELP" | "TYPE"))
            && let Some(name) = parts.next()
        {
            return name.to_owned();
        }
    }
    if let Some(sample) = samples.first() {
        let name_end = sample.find(['{', ' ']).unwrap_or(sample.len());
        return sample[..name_end].to_owned();
    }
    String::new()
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

    // HTTP RED metrics — per-route rate/errors/duration (plan §15, BUG-OBS-05).
    // Labels: method, path (matched route template), status.
    metrics::describe_counter!(
        "kb_http_requests_total",
        "Total HTTP requests handled, labelled by method, matched-route path, and status"
    );
    metrics::describe_histogram!(
        "kb_http_request_duration_seconds",
        "HTTP request handling duration in seconds, labelled by method, matched-route path, and status"
    );
    metrics::describe_counter!(
        "kb_http_errors_total",
        "Total HTTP responses with a 5xx status, labelled by method, matched-route path, and status"
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

    // Maintenance scheduler (plan §25, P8-T11)
    metrics::describe_gauge!(
        "kb_maintenance_last_success",
        "Unix timestamp of the last successful run, per maintenance job kind"
    );
    metrics::describe_gauge!(
        "kb_maintenance_last_failure",
        "Unix timestamp of the last failed run, per maintenance job kind"
    );

    // Decrypt-access audit (plan §28, P10-T5)
    metrics::describe_counter!(
        "kb_decrypt_audit_failed_total",
        "Number of failed key-unwrap operations (DEK or provider key) — spike signals possible attack or key corruption"
    );

    // Quota rejections (plan §29, P14-T4) — counter, label = limit.
    metrics::describe_counter!(
        "kb_quota_rejections_total",
        "Number of requests rejected because a plan quota was exceeded (label: limit = storage|tokens|users)"
    );

    // Tokens metered (plan §15, §29, P14-T10) — counter, labels = role, model.
    metrics::describe_counter!(
        "kb_tokens_total",
        "Total tokens (prompt + completion) accepted at the metering seam, labelled by role and model — counts every metered AI call (ingest, search, vision)"
    );

    // Metering write failures (plan §15, §29, P14-T10) — counter, no labels.
    metrics::describe_counter!(
        "kb_metering_write_failures_total",
        "Number of usage-accounting writes lost: fail-open buffer drops (channel full/closed) plus durable-sink write errors in the drain task"
    );

    // Rate-limit rejections (plan §15, §29, P14-T10) — counter, label = kind.
    metrics::describe_counter!(
        "kb_rate_limit_rejections_total",
        "Number of requests rejected with 429 by a rate limiter (label: kind = plan|login)"
    );

    // Budget / cost tracking (plan §26.6, P9-T10)
    metrics::describe_gauge!(
        "kb_tenant_spend_monthly_micros",
        "Current month's total spend in micro-dollars for this tenant"
    );
    metrics::describe_gauge!(
        "kb_tenant_budget_exceeded",
        "1 if the tenant's monthly spend exceeds their budget, 0 otherwise"
    );
    metrics::describe_gauge!(
        "kb_tenant_budget_cents",
        "Monthly spend budget in cents (USD) for this tenant"
    );
    metrics::describe_gauge!(
        "kb_tenant_tokens_monthly",
        "Tokens consumed by this tenant in the current UTC calendar month (from the per-tenant monthly rollup)"
    );
}

// ── Backup / disaster-recovery seeding ───────────────────────────────────────────

/// String labels for the maintenance job kinds, mirrored from
/// `kb_pipeline::MaintenanceJobKind`.
///
/// Duplicated here (rather than depending on `kb-pipeline`) to keep `kb-metrics`
/// a leaf crate with no upward dependencies. The production scheduler emits the
/// authoritative series via [`record_maintenance_success`] /
/// [`record_maintenance_failure`] once it runs; these labels only drive the
/// startup seed so the family is scrapeable before the first run.
const MAINTENANCE_JOB_KINDS: [&str; 5] = [
    "vacuum",
    "log_prune",
    "blob_cache_eviction",
    "b2_lifecycle",
    "reembed_check",
];

/// Seed the backup / disaster-recovery metric families with baseline values so
/// they are present in `/metrics` output from process start — before the backup,
/// restore-test, integrity-scan, orphan-GC and maintenance jobs have run for the
/// first time.
///
/// The Prometheus exporter only renders a metric family once a data point has
/// been recorded for it. Without this seed, an operator scraping a freshly
/// started instance would see no backup/DR signals at all and could not write
/// alerting rules against backup staleness, restore-test outcomes, or integrity
/// failures — "an untested backup is not a backup". Each seeded value is
/// overwritten by the corresponding `record_*` helper once the real job runs.
///
/// Timestamp gauges are seeded to `0` (the Unix-epoch "never ran" sentinel, so a
/// `time() - last_success > threshold` rule fires correctly); boolean and count
/// gauges are seeded to `0` (the neutral "no observation yet" baseline).
fn seed_backup_dr_metrics() {
    // Backup freshness / restore-test (plan §21, P8-T7).
    metrics::gauge!("kb_restore_test_success").set(0.0);
    metrics::gauge!("kb_backup_age_hours").set(0.0);
    metrics::gauge!("kb_backup_stale").set(0.0);

    // Orphan GC + integrity scan (plan §23, P8-T10).
    metrics::gauge!("kb_orphan_gc_blobs_found").set(0.0);
    metrics::gauge!("kb_orphan_gc_blobs_deleted").set(0.0);
    metrics::gauge!("kb_missing_blobs_found").set(0.0);
    metrics::gauge!("kb_integrity_scan_verified").set(0.0);
    metrics::gauge!("kb_integrity_scan_failed").set(0.0);

    // Maintenance scheduler (plan §25, P8-T11) — one series per job kind.
    for kind in MAINTENANCE_JOB_KINDS {
        metrics::gauge!("kb_maintenance_last_success", "kind" => kind).set(0.0);
        metrics::gauge!("kb_maintenance_last_failure", "kind" => kind).set(0.0);
    }
}

/// Seed the runtime gauges published by the periodic metrics collector so they
/// are present on `/metrics` from process start — before the collector's first
/// poll (BUG-OBS-06).
///
/// Only the global, label-free `kb_queue_depth` is seeded, to `0` (the neutral
/// "nothing queued yet" baseline). The per-tenant `kb_storage_bytes_used` and
/// per-backend `kb_backend_*` families cannot be seeded without inventing
/// synthetic label values, so they are left to the collector — whose first tick
/// runs immediately at startup. The collector overwrites the seeded value with
/// the live queue depth on that first tick.
fn seed_runtime_metrics() {
    metrics::gauge!("kb_queue_depth").set(0.0);
    // Queue/worker families (P15-T13): present from boot so dashboards and
    // alert rules never see an absent family. The duration histogram is NOT
    // seeded — a synthetic observation would pollute its quantiles; its
    // family appears with the first processed job.
    metrics::gauge!("kb_jobs_running").set(0.0);
    metrics::counter!("kb_job_leases_reaped_total").absolute(0);
    metrics::counter!("kb_queue_full_rejections_total").absolute(0);
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

/// Record one completed HTTP request for the RED (rate / errors / duration)
/// metrics (plan §15, BUG-OBS-05).
///
/// Emits, all labelled by `method`, matched-route `path`, and `status`:
/// - `kb_http_requests_total` — request **rate** counter;
/// - `kb_http_request_duration_seconds` — request **duration** histogram;
/// - `kb_http_errors_total` — server-**error** counter, incremented only for
///   `5xx` responses (client `4xx` errors are not service errors and are
///   excluded so error-budget alerting tracks server faults).
///
/// `path` must be the *matched route template* (e.g. `/api/documents/{id}`),
/// not the raw URI, so that dynamic path segments do not explode label
/// cardinality. Called from the API's HTTP-metrics middleware after the
/// response status is known.
pub fn record_http_request(method: &str, path: &str, status: u16, duration_secs: f64) {
    let status_label = status.to_string();
    metrics::counter!(
        "kb_http_requests_total",
        "method" => method.to_owned(),
        "path" => path.to_owned(),
        "status" => status_label.clone()
    )
    .increment(1);
    metrics::histogram!(
        "kb_http_request_duration_seconds",
        "method" => method.to_owned(),
        "path" => path.to_owned(),
        "status" => status_label.clone()
    )
    .record(duration_secs);
    if status >= 500 {
        metrics::counter!(
            "kb_http_errors_total",
            "method" => method.to_owned(),
            "path" => path.to_owned(),
            "status" => status_label
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

// ── Queue/worker metrics (plan §16, P15-T13) ─────────────────────────────────────

/// Record one processed queue job in `kb_job_duration_seconds{kind,outcome}`.
///
/// Called from the worker loop after the handler + completion bookkeeping.
/// `kind` is the job kind wire string (`ingest`, `retag`, …); `outcome` is
/// the job's resulting state: `done`, `failed` (backoff retry scheduled), or
/// `dead` (terminal). The histogram's `_count` doubles as a processed-jobs
/// counter per (kind, outcome).
pub fn record_job_processed(kind: &str, outcome: &str, duration_secs: f64) {
    metrics::histogram!(
        "kb_job_duration_seconds",
        "kind" => kind.to_owned(),
        "outcome" => outcome.to_owned()
    )
    .record(duration_secs);
}

/// Set the cluster-wide running-jobs gauge (`kb_jobs_running`).
///
/// Published by the metrics collector from the database (`status='running'`
/// count) — the database is the only truthful cross-process source when
/// multiple workers drain the same queue.
pub fn record_jobs_running(count: u64) {
    metrics::gauge!("kb_jobs_running").set(count as f64);
}

/// Add reaped (expired-lease) jobs to `kb_job_leases_reaped_total`.
///
/// Called by the lease reaper after each pass that requeued anything. A spike
/// means workers are crashing or stalling mid-job (their heartbeats stopped).
pub fn record_leases_reaped(count: u64) {
    metrics::counter!("kb_job_leases_reaped_total").increment(count);
}

/// Increment `kb_queue_full_rejections_total` — an upload was refused 429
/// `queue_full` by bounded-queue admission (per-tenant or global cap).
///
/// Deliberately label-free: a per-tenant label would be unbounded-cardinality;
/// the 429 response body already carries the tenant's own counts.
pub fn record_queue_full_rejection() {
    metrics::counter!("kb_queue_full_rejections_total").increment(1);
}

/// Increment the throttled-ingest counter.
///
/// Call when an ingest request is rejected with 429 due to backpressure.
pub fn record_ingest_throttled() {
    metrics::counter!("kb_ingest_throttled_total").increment(1);
}

// ── Quota-rejection metrics (plan §29, P14-T4) ───────────────────────────────────

/// Increment the quota-rejection counter for a given limit.
///
/// Call when a request is rejected because a plan quota was exceeded.
/// `limit` is the quota that was hit — `"tokens"` for the monthly token
/// budget (429 + upsell), `"storage"` for the storage cap (413 + upsell),
/// or `"users"` for the per-plan user limit (403 + upsell). Operators can
/// alert on a rising rate to spot tenants who should be prompted to upgrade.
pub fn record_quota_rejection(limit: &str) {
    metrics::counter!("kb_quota_rejections_total", "limit" => limit.to_owned()).increment(1);
}

// ── Metering-seam counters (plan §15, §29, P14-T10) ──────────────────────────────

/// Add the tokens consumed by one metered AI call to `kb_tokens_total`.
///
/// Called at the single metering chokepoint
/// (`kb_store::BufferedUsageRecorder::record_usage`) as each [`UsageEvent`] is
/// accepted, so **every** metered call — ingest tagging/embedding, search query
/// embedding + reranking, and vision captioning — is counted exactly once.
///
/// `tokens` is `prompt_tokens + completion_tokens` for the event. `role` is the
/// model capability (a small fixed enum: `text|vision|code|embed|rerank`) and
/// `model` is the model id (a small configured set), so label cardinality stays
/// bounded.
///
/// [`UsageEvent`]: kb_core-style usage record carrying role/model/token counts.
pub fn record_tokens(role: &str, model: &str, tokens: u64) {
    metrics::counter!(
        "kb_tokens_total",
        "role" => role.to_owned(),
        "model" => model.to_owned()
    )
    .increment(tokens);
}

/// Increment `kb_metering_write_failures_total` — one accounting write was lost.
///
/// Called from `kb_store::BufferedUsageRecorder` on a fail-open drop (the bounded
/// channel was full or the drain task had stopped) and from its drain task when
/// the inner durable sink returns an error. Both are real "we lost a usage write"
/// events; a sustained rate signals back-pressure or a broken sink. The metric is
/// unlabelled — operators alert on its total rate.
pub fn record_metering_write_failure() {
    metrics::counter!("kb_metering_write_failures_total").increment(1);
}

/// Increment `kb_rate_limit_rejections_total` for a 429 from a rate limiter.
///
/// `kind` is a fixed string identifying which limiter rejected the request:
/// `"plan"` for the per-tenant plan per-minute cap, `"login"` for the per-IP
/// login brute-force limiter. Called from the API middleware at each limiter's
/// 429 site, so operators can distinguish abuse (login) from plan-cap throttling
/// (plan) on a single counter.
pub fn record_rate_limit_rejection(kind: &str) {
    metrics::counter!("kb_rate_limit_rejections_total", "kind" => kind.to_owned()).increment(1);
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

// ── Maintenance scheduler metrics (plan §25, P8-T11) ──────────────────────────────

/// Record a successful maintenance job run.
///
/// Sets the `kb_maintenance_last_success` gauge to the current Unix timestamp
/// (seconds) and clears the `kb_maintenance_last_failure` gauge for the given
/// `kind` label.
pub fn record_maintenance_success(kind: &str) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    metrics::gauge!("kb_maintenance_last_success", "kind" => kind.to_owned()).set(now);
    metrics::gauge!("kb_maintenance_last_failure", "kind" => kind.to_owned()).set(0.0);
}

/// Record a failed maintenance job run.
///
/// Sets the `kb_maintenance_last_failure` gauge to the current Unix timestamp
/// (seconds) for the given `kind` label.
pub fn record_maintenance_failure(kind: &str) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    metrics::gauge!("kb_maintenance_last_failure", "kind" => kind.to_owned()).set(now);
}

// ── Budget / cost tracking metrics (plan §26.6, P9-T10) ──────────────────────

/// Set the per-tenant monthly spend gauge (micro-dollars).
///
/// Also records the tenant's budget in cents as a parallel gauge for
/// Prometheus alert-rule consumption (computing percentage from two
/// gauges).
pub fn record_tenant_spend_monthly(tenant_id: i64, spend_micros: u64) {
    let tid = tenant_id.to_string();
    metrics::gauge!("kb_tenant_spend_monthly_micros", "tenant_id" => tid).set(spend_micros as f64);
}

/// Set the per-tenant budget-exceeded alert gauge.
///
/// `exceeded` = true → gauge = 1.0 (the alert should fire).
pub fn record_tenant_budget_exceeded(tenant_id: i64, exceeded: bool) {
    let val: f64 = if exceeded { 1.0 } else { 0.0 };
    metrics::gauge!("kb_tenant_budget_exceeded", "tenant_id" => tenant_id.to_string()).set(val);
}

/// Set the per-tenant budget cap in cents.
///
/// This exists so Prometheus can compute spend-percentage relative
/// to the cap without an extra data source.
pub fn record_tenant_budget_cents(tenant_id: i64, budget_cents: u64) {
    metrics::gauge!("kb_tenant_budget_cents", "tenant_id" => tenant_id.to_string())
        .set(budget_cents as f64);
}

/// Set the per-tenant monthly token-usage gauge (P14-T8).
///
/// `tokens` is the tenant's token total for the current UTC calendar month, read
/// from the O(1) `tenant_monthly_usage` rollup. Published per poll by the metrics
/// collector so an operator can graph monthly token consumption per tenant
/// alongside the spend and budget gauges.
pub fn record_tenant_tokens_monthly(tenant_id: i64, tokens: u64) {
    metrics::gauge!("kb_tenant_tokens_monthly", "tenant_id" => tenant_id.to_string())
        .set(tokens as f64);
}

// ── Decrypt-access audit metrics (plan §28, P10-T5) ─────────────────────────

/// Increment the decrypt-audit-failed counter.
///
/// Called from [`PgStore::insert_decrypt_audit_event`] when a key-unwrap
/// operation fails. A spike in this counter signals a possible attack
/// (brute-force attempts on wrapped DEKs) or key corruption.
///
/// `operation` is the [`DecryptAuditAction`](kb_core::audit::DecryptAuditAction)
/// as a string (e.g. `"unwrap_dek"`, `"unwrap_provider_key"`).
pub fn record_decrypt_audit_failed(operation: &str) {
    metrics::counter!(
        "kb_decrypt_audit_failed_total",
        "operation" => operation.to_owned()
    )
    .increment(1);
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
    fn http_red_metrics_emit_families_with_status_label() {
        let _h = ensure_init();
        // A successful request and a server error on distinct routes.
        record_http_request("GET", "/api/red-ok", 200, 0.012);
        record_http_request("POST", "/api/red-fail", 503, 0.5);

        let text = render();
        // Rate counter — present with method/path/status labels.
        assert!(
            text.contains("kb_http_requests_total")
                && text.lines().any(|l| l.contains("kb_http_requests_total")
                    && l.contains("path=\"/api/red-ok\"")
                    && l.contains("status=\"200\"")),
            "kb_http_requests_total missing status-labelled line in: {text}"
        );
        // Duration histogram — also carries the status label.
        assert!(
            text.lines()
                .any(|l| l.contains("kb_http_request_duration_seconds")
                    && l.contains("status=\"200\"")),
            "kb_http_request_duration_seconds missing status label in: {text}"
        );
        // Error counter — only the 5xx response is counted, not any 2xx.
        assert!(
            text.lines().any(|l| l.contains("kb_http_errors_total")
                && l.contains("path=\"/api/red-fail\"")
                && l.contains("status=\"503\"")),
            "kb_http_errors_total missing 5xx line in: {text}"
        );
        assert!(
            !text
                .lines()
                .any(|l| l.contains("kb_http_errors_total") && l.contains("path=\"/api/red-ok\"")),
            "2xx request must not increment kb_http_errors_total: {text}"
        );
    }

    #[test]
    fn http_red_error_counter_skips_4xx() {
        let _h = ensure_init();
        record_http_request("GET", "/api/red-clienterr", 404, 0.001);
        let text = render();
        // A 4xx is a client error, not a server fault — kb_http_errors_total
        // must not gain a line for it.
        assert!(
            !text
                .lines()
                .any(|l| l.contains("kb_http_errors_total")
                    && l.contains("path=\"/api/red-clienterr\"")),
            "4xx must not increment kb_http_errors_total: {text}"
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
    fn queue_depth_seeded_present_after_init() {
        // BUG-OBS-06: kb_queue_depth is seeded at init so the family is present
        // on /metrics before the collector's first poll.
        let _h = ensure_init();
        let text = render();
        assert!(
            text.lines()
                .any(|l| l.trim_start().starts_with("kb_queue_depth ")),
            "kb_queue_depth must be present after init (seeded): {text}"
        );
    }

    // ── Queue/worker metrics (P15-T13) ─────────────────────────────────────

    #[test]
    fn queue_worker_families_seeded_after_init() {
        // Same BUG-OBS-06 rationale: gauges/counters exist from boot so
        // dashboards and alert rules never see an absent family.
        let _h = ensure_init();
        let text = render();
        for family in [
            "kb_jobs_running ",
            "kb_job_leases_reaped_total ",
            "kb_queue_full_rejections_total ",
        ] {
            assert!(
                text.lines().any(|l| l.trim_start().starts_with(family)),
                "{family} must be present after init (seeded): {text}"
            );
        }
    }

    #[test]
    fn job_duration_histogram_records_labeled_observation() {
        let _h = ensure_init();
        record_job_processed("ingest", "done", 1.25);
        record_job_processed("ingest", "dead", 0.05);
        let text = render();
        assert!(
            text.contains("kb_job_duration_seconds")
                && text.contains("kind=\"ingest\"")
                && text.contains("outcome=\"done\"")
                && text.contains("outcome=\"dead\""),
            "histogram with kind+outcome labels must render: {text}"
        );
    }

    #[test]
    fn jobs_running_gauge_sets_value() {
        let _h = ensure_init();
        record_jobs_running(7);
        assert!(render().contains("kb_jobs_running 7"));
        record_jobs_running(0);
        assert!(render().contains("kb_jobs_running 0"));
    }

    #[test]
    fn leases_reaped_counter_accumulates() {
        let _h = ensure_init();
        let before = counter_value(&render(), "kb_job_leases_reaped_total");
        record_leases_reaped(3);
        let after = counter_value(&render(), "kb_job_leases_reaped_total");
        assert_eq!(
            after - before,
            3,
            "counter must accumulate by the batch size"
        );
    }

    #[test]
    fn queue_full_rejection_counter_increments() {
        let _h = ensure_init();
        let before = counter_value(&render(), "kb_queue_full_rejections_total");
        record_queue_full_rejection();
        let after = counter_value(&render(), "kb_queue_full_rejections_total");
        assert_eq!(after - before, 1);
    }

    /// Parse a label-free counter's value out of rendered Prometheus text.
    fn counter_value(text: &str, family: &str) -> i64 {
        text.lines()
            .find_map(|l| {
                let l = l.trim_start();
                l.strip_prefix(family)
                    .and_then(|rest| rest.trim().parse::<i64>().ok())
            })
            .unwrap_or(0)
    }

    #[test]
    fn queue_depth_gauge() {
        let _h = ensure_init();
        record_queue_depth(42);
        record_queue_oldest_job_age(300.0);

        let text = render();
        assert!(text.contains("kb_queue_depth 42"));
        assert!(text.contains("kb_queue_oldest_job_age_secs 300"));
        // Exposition metadata for the oldest-job-age gauge (P14-T9). Asserted
        // here, alongside the single writer of this global unlabelled gauge, so a
        // second test does not race the exact-value assertion above.
        assert!(text.contains("# HELP kb_queue_oldest_job_age_secs"));
        assert!(text.contains("# TYPE kb_queue_oldest_job_age_secs gauge"));
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
    fn backup_dr_families_present_after_init() {
        // `init_metrics` must seed every backup/DR family so it renders even
        // before any backup/restore/integrity/maintenance job has run. This
        // mirrors the e2e contract (test_backup_and_dr_metrics_exposed): the
        // family is "present" if a non-comment line begins with `name ` or
        // `name{` (a real data line, not just a HELP/TYPE comment).
        let _h = ensure_init();
        let text = render();

        let families = [
            "kb_backup_stale",
            "kb_backup_age_hours",
            "kb_restore_test_success",
            "kb_integrity_scan_failed",
            "kb_missing_blobs_found",
            "kb_maintenance_last_success",
        ];
        for fam in families {
            let present = text.lines().any(|l| {
                let l = l.trim_start();
                l.starts_with(&format!("{fam} ")) || l.starts_with(&format!("{fam}{{"))
            });
            assert!(present, "backup/DR family {fam} missing after init: {text}");
        }
    }

    #[test]
    fn seed_backup_dr_metrics_emits_per_kind_maintenance_series() {
        let _h = ensure_init();
        let text = render();
        // The seed must emit one maintenance series per known job kind so the
        // labelled family is scrapeable from startup.
        for kind in MAINTENANCE_JOB_KINDS {
            assert!(
                text.contains(&format!("kb_maintenance_last_success{{kind=\"{kind}\"}}")),
                "missing seeded kb_maintenance_last_success for kind {kind} in: {text}"
            );
        }
    }

    #[test]
    fn canonicalize_orders_families_and_samples_deterministically() {
        // Two inputs with the same content but different family/sample order
        // must canonicalise to the same string.
        let a = "# HELP kb_b help b\n# TYPE kb_b gauge\nkb_b{x=\"2\"} 2\nkb_b{x=\"1\"} 1\n\n# HELP kb_a help a\n# TYPE kb_a gauge\nkb_a 0";
        let b = "# HELP kb_a help a\n# TYPE kb_a gauge\nkb_a 0\n\n# HELP kb_b help b\n# TYPE kb_b gauge\nkb_b{x=\"1\"} 1\nkb_b{x=\"2\"} 2";

        let ca = canonicalize_exposition(a);
        let cb = canonicalize_exposition(b);
        assert_eq!(
            ca, cb,
            "differently-ordered inputs must canonicalise equally"
        );

        // kb_a family sorts before kb_b, and HELP/TYPE stay with their samples.
        let expected = "# HELP kb_a help a\n# TYPE kb_a gauge\nkb_a 0\n\n# HELP kb_b help b\n# TYPE kb_b gauge\nkb_b{x=\"1\"} 1\nkb_b{x=\"2\"} 2";
        assert_eq!(ca, expected);
    }

    #[test]
    fn canonicalize_is_idempotent_and_handles_empty() {
        assert_eq!(canonicalize_exposition(""), "");
        assert_eq!(canonicalize_exposition("   \n  \n"), "");

        let raw =
            "# HELP kb_z z\n# TYPE kb_z gauge\nkb_z 3\n\n# HELP kb_a a\n# TYPE kb_a gauge\nkb_a 1";
        let once = canonicalize_exposition(raw);
        let twice = canonicalize_exposition(&once);
        assert_eq!(once, twice, "canonicalisation must be idempotent");
    }

    #[test]
    fn block_sort_key_prefers_help_type_then_sample() {
        assert_eq!(
            block_sort_key(&["# HELP kb_foo some help text"], &["kb_foo 1"]),
            "kb_foo"
        );
        assert_eq!(
            block_sort_key(&["# TYPE kb_bar gauge"], &["kb_bar{a=\"1\"} 1"]),
            "kb_bar"
        );
        // No usable comment → fall back to the sample's metric name.
        assert_eq!(block_sort_key(&[], &["kb_baz{label=\"x\"} 9"]), "kb_baz");
        assert_eq!(block_sort_key(&[], &["kb_qux 7"]), "kb_qux");
        assert_eq!(block_sort_key(&[], &[]), "");
    }

    #[test]
    fn render_orders_families_alphabetically() {
        // Canonicalised render output lists metric families in name order, so
        // `/metrics` is byte-stable for a fixed metric set (the exporter itself
        // orders families and label-sets nondeterministically). Values may be
        // mutated by other tests sharing the global recorder, so we assert only
        // on the *ordering* of family HELP lines, not their values.
        let _h = ensure_init();
        record_backend("order-test", true, 1, 2, 1);
        seed_backup_dr_metrics();

        let text = render();
        let help_names: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("# HELP "))
            .filter_map(|rest| rest.split_whitespace().next())
            .collect();
        let mut sorted = help_names.clone();
        sorted.sort_unstable();
        assert_eq!(
            help_names, sorted,
            "metric families must render in name order"
        );
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
    fn quota_rejection_counter_present_and_labelled() {
        let _h = ensure_init();
        // Use distinct labels so the lines are unambiguous regardless of what
        // other tests recorded.
        record_quota_rejection("tokens");
        record_quota_rejection("storage");

        let text = render();
        assert!(
            text.contains("kb_quota_rejections_total{limit=\"tokens\"}"),
            "kb_quota_rejections_total tokens line missing in: {text}"
        );
        assert!(
            text.contains("kb_quota_rejections_total{limit=\"storage\"}"),
            "kb_quota_rejections_total storage line missing in: {text}"
        );
    }

    #[test]
    fn quota_rejection_counter_increments() {
        let _h = ensure_init();
        // A label nobody else uses, so the count is exactly what we record.
        record_quota_rejection("p14t4-unit");
        record_quota_rejection("p14t4-unit");
        record_quota_rejection("p14t4-unit");

        let text = render();
        assert!(
            text.contains("kb_quota_rejections_total{limit=\"p14t4-unit\"} 3"),
            "expected count 3 for p14t4-unit label in: {text}"
        );
    }

    #[test]
    fn quota_rejection_has_help_and_type() {
        let _h = ensure_init();
        record_quota_rejection("tokens");

        let text = render();
        assert!(text.contains("# HELP kb_quota_rejections_total"));
        assert!(text.contains("# TYPE kb_quota_rejections_total counter"));
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

    // ── Budget metrics tests (P9-T10) ────────────────────────────────────

    #[test]
    fn tenant_spend_monthly_gauge() {
        let _h = ensure_init();
        record_tenant_spend_monthly(1, 5_000_000);
        record_tenant_spend_monthly(2, 250_000);

        let text = render();
        assert!(text.contains("kb_tenant_spend_monthly_micros{tenant_id=\"1\"} 5000000"));
        assert!(text.contains("kb_tenant_spend_monthly_micros{tenant_id=\"2\"} 250000"));
    }

    #[test]
    fn tenant_budget_exceeded_gauge() {
        let _h = ensure_init();
        record_tenant_budget_exceeded(1, false);
        record_tenant_budget_exceeded(2, true);

        let text = render();
        // Both label variants present in output.
        assert!(
            text.contains("kb_tenant_budget_exceeded{tenant_id=\"1\"}"),
            "tenant 1 budget exceeded not found in: {text}"
        );
        assert!(
            text.contains("kb_tenant_budget_exceeded{tenant_id=\"2\"}"),
            "tenant 2 budget exceeded not found in: {text}"
        );
    }

    #[test]
    fn tenant_budget_cents_gauge() {
        let _h = ensure_init();
        record_tenant_budget_cents(1, 1000); // $10.00

        let text = render();
        assert!(text.contains("kb_tenant_budget_cents{tenant_id=\"1\"} 1000"));
    }

    #[test]
    fn budget_metrics_have_help_and_type() {
        let _h = ensure_init();
        record_tenant_spend_monthly(1, 0);
        record_tenant_budget_exceeded(1, false);
        record_tenant_budget_cents(1, 0);

        let text = render();
        assert!(text.contains("# HELP kb_tenant_spend_monthly_micros"));
        assert!(text.contains("# TYPE kb_tenant_spend_monthly_micros gauge"));
        assert!(text.contains("# HELP kb_tenant_budget_exceeded"));
        assert!(text.contains("# TYPE kb_tenant_budget_exceeded gauge"));
        assert!(text.contains("# HELP kb_tenant_budget_cents"));
        assert!(text.contains("# TYPE kb_tenant_budget_cents gauge"));
    }

    // ── Per-tenant monthly token gauge test (P14-T8) ──────────────────────

    #[test]
    fn tenant_tokens_monthly_gauge_emits_with_label_help_and_type() {
        let _h = ensure_init();
        record_tenant_tokens_monthly(1, 4242);
        record_tenant_tokens_monthly(2, 7);

        let text = render();
        // Per-tenant label + value.
        assert!(
            text.contains("kb_tenant_tokens_monthly{tenant_id=\"1\"} 4242"),
            "tenant 1 monthly-tokens gauge missing/wrong in: {text}"
        );
        assert!(
            text.contains("kb_tenant_tokens_monthly{tenant_id=\"2\"} 7"),
            "tenant 2 monthly-tokens gauge missing/wrong in: {text}"
        );
        // Exposition HELP/TYPE lines.
        assert!(text.contains("# HELP kb_tenant_tokens_monthly"));
        assert!(text.contains("# TYPE kb_tenant_tokens_monthly gauge"));
    }

    // ── Decrypt audit metrics tests (P10-T5) ──────────────────────────────

    #[test]
    fn decrypt_audit_failed_counter_present() {
        let _h = ensure_init();
        record_decrypt_audit_failed("unwrap_dek");

        let text = render();
        assert!(
            text.contains("kb_decrypt_audit_failed_total{operation=\"unwrap_dek\"}"),
            "kb_decrypt_audit_failed_total not found in: {text}"
        );
    }

    #[test]
    fn decrypt_audit_failed_counter_increments() {
        let _h = ensure_init();
        // Use a unique label so other tests don't interfere.
        record_decrypt_audit_failed("unwrap_provider_key");
        record_decrypt_audit_failed("unwrap_provider_key");

        let text = render();
        assert!(
            text.contains("kb_decrypt_audit_failed_total{operation=\"unwrap_provider_key\"}"),
            "kb_decrypt_audit_failed_total counter missing for unwrap_provider_key in: {text}"
        );
    }

    #[test]
    fn decrypt_audit_help_and_type_present() {
        let _h = ensure_init();
        record_decrypt_audit_failed("unwrap_dek");

        let text = render();
        assert!(text.contains("# HELP kb_decrypt_audit_failed_total"));
        assert!(text.contains("# TYPE kb_decrypt_audit_failed_total counter"));
    }

    // ── Metering-seam counter tests (P14-T10) ─────────────────────────────

    #[test]
    fn tokens_counter_labelled_by_role_and_model_and_sums_tokens() {
        let _h = ensure_init();
        // A label combo nobody else uses so the count is exactly what we add.
        record_tokens("p14t10-role", "p14t10-model", 30);
        record_tokens("p14t10-role", "p14t10-model", 12);

        let text = render();
        assert!(
            text.contains("kb_tokens_total{role=\"p14t10-role\",model=\"p14t10-model\"} 42"),
            "expected summed token count 42 for the unique label combo in: {text}"
        );
    }

    #[test]
    fn tokens_counter_has_help_and_type() {
        let _h = ensure_init();
        record_tokens("text", "qwen3", 1);

        let text = render();
        assert!(text.contains("# HELP kb_tokens_total"));
        assert!(text.contains("# TYPE kb_tokens_total counter"));
    }

    #[test]
    fn metering_write_failure_counter_present_and_has_help_and_type() {
        let _h = ensure_init();
        record_metering_write_failure();

        let text = render();
        // Unlabelled counter — assert the family is present as a data line.
        assert!(
            text.lines().any(|l| l
                .trim_start()
                .starts_with("kb_metering_write_failures_total ")),
            "kb_metering_write_failures_total data line missing in: {text}"
        );
        assert!(text.contains("# HELP kb_metering_write_failures_total"));
        assert!(text.contains("# TYPE kb_metering_write_failures_total counter"));
    }

    #[test]
    fn rate_limit_rejection_counter_labelled_by_kind() {
        let _h = ensure_init();
        record_rate_limit_rejection("plan");
        record_rate_limit_rejection("login");

        let text = render();
        assert!(
            text.contains("kb_rate_limit_rejections_total{kind=\"plan\"}"),
            "kb_rate_limit_rejections_total plan line missing in: {text}"
        );
        assert!(
            text.contains("kb_rate_limit_rejections_total{kind=\"login\"}"),
            "kb_rate_limit_rejections_total login line missing in: {text}"
        );
    }

    #[test]
    fn rate_limit_rejection_counter_increments_and_has_help_and_type() {
        let _h = ensure_init();
        // A kind nobody else uses so the count is exactly what we record.
        record_rate_limit_rejection("p14t10-kind");
        record_rate_limit_rejection("p14t10-kind");

        let text = render();
        assert!(
            text.contains("kb_rate_limit_rejections_total{kind=\"p14t10-kind\"} 2"),
            "expected count 2 for p14t10-kind label in: {text}"
        );
        assert!(text.contains("# HELP kb_rate_limit_rejections_total"));
        assert!(text.contains("# TYPE kb_rate_limit_rejections_total counter"));
    }
}
