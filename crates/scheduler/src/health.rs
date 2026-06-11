// SPDX-License-Identifier: AGPL-3.0-or-later

//! Background health-check loop that polls every backend's `/health` endpoint
//! and updates [`Backend::healthy`] (plan §6.5).
//!
//! The local semaphore remains the source of truth for in-flight load; health
//! is liveness only — a recovered host becomes eligible again on the next
//! [`crate::Pool::acquire`] call.
//!
//! # Debounced liveness (plan §6.4, §26)
//!
//! A backend is only marked unhealthy after a **run of consecutive failed
//! probes** (the rolling-error-window intent of plan §326/§938) — never on a
//! single transient blip. Under heavy-but-healthy load an occasional dropped
//! connection or slow `/health` must not flap a live host out of rotation. A
//! single successful probe immediately restores health and resets the counter.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use reqwest::Client;
use tokio::sync::watch;

use crate::backend::Backend;

/// Consecutive failed probes before a backend is marked unhealthy when
/// [`FAILURE_THRESHOLD_ENV`] is unset or invalid.
const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

/// Per-probe HTTP timeout (seconds) when [`PROBE_TIMEOUT_ENV`] is unset or
/// invalid. Generous enough that a busy-but-live `/health` still answers, but
/// bounded so a hung host cannot stall the poll loop.
const DEFAULT_PROBE_TIMEOUT_SECS: u64 = 10;

/// Env var (hot-swappable, read per cycle) overriding the consecutive-failure
/// threshold before a backend is marked unhealthy.
const FAILURE_THRESHOLD_ENV: &str = "KB_HEALTH_FAILURE_THRESHOLD";

/// Env var (hot-swappable, read per probe) overriding the per-probe timeout.
const PROBE_TIMEOUT_ENV: &str = "KB_HEALTH_PROBE_TIMEOUT_SECS";

/// A background task that polls each backend's `/health` endpoint periodically
/// and updates [`Backend::healthy`] (plan §6.5).
///
/// The local semaphore remains the source of truth for in-flight load; health
/// is liveness only — a recovered host becomes eligible again on the next
/// [`crate::Pool::acquire`] call.
///
/// # Graceful shutdown
///
/// Call [`HealthLoop::shutdown_tx`] and then `.await` the [`tokio::task::JoinHandle`]
/// returned by [`HealthLoop::spawn`].
///
/// # Deduplication
///
/// The same backend registered under several roles shares one `Arc<Backend>`;
/// the loop deduplicates by pointer so each physical host is probed once.
pub struct HealthLoop {
    /// Unique backends to poll (deduplicated at construction).
    backends: Vec<Arc<Backend>>,
    /// Consecutive-failed-probe count per backend (index-aligned with
    /// [`backends`](Self::backends)). Reset to 0 on any successful probe; a
    /// backend is only marked unhealthy once its count reaches the threshold.
    failures: Vec<AtomicU32>,
    /// Shared HTTP client for all health probes.
    client: Client,
    /// Delay between full poll cycles.
    interval: Duration,
}

impl HealthLoop {
    /// Create a health loop that will poll every unique backend.
    ///
    /// Duplicate backends (same `Arc` pointer — a backend registered under
    /// several roles) are collapsed so each physical host is checked once.
    pub fn new(backends: Vec<Arc<Backend>>, client: Client, interval: Duration) -> Self {
        let mut seen = HashSet::new();
        let unique: Vec<Arc<Backend>> = backends
            .into_iter()
            .filter(|b| seen.insert(Arc::as_ptr(b) as usize))
            .collect();
        let failures = unique.iter().map(|_| AtomicU32::new(0)).collect();
        Self {
            backends: unique,
            failures,
            client,
            interval,
        }
    }

    /// Spawn the health loop on a background task.
    ///
    /// Returns the `JoinHandle` (for awaiting graceful completion) and a
    /// [`watch::Sender`] that signals shutdown. Dropping the sender (or
    /// calling [`watch::Sender::send`]) is enough to stop the loop.
    pub fn spawn(self) -> (tokio::task::JoinHandle<()>, watch::Sender<()>) {
        let (tx, rx) = watch::channel(());
        let handle = tokio::spawn(async move { self.run(rx).await });
        (handle, tx)
    }

    /// Run the poll loop. Returns when `shutdown` is notified.
    ///
    /// On each tick every backend's `/health` endpoint is probed and
    /// `Backend::healthy` is set to the result. The loop then sleeps for
    /// [`self.interval`](Self::interval) (or exits early if shutdown arrives).
    async fn run(self, mut shutdown: watch::Receiver<()>) {
        loop {
            self.poll_all().await;

            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {},
                _ = shutdown.changed() => return,
            }
        }
    }

    /// Probe every tracked backend and update its healthy flag with debounce.
    ///
    /// A failed probe increments the backend's consecutive-failure counter and
    /// only flips `healthy` to `false` once the counter reaches the
    /// (hot-swappable) threshold; a successful probe resets the counter and
    /// marks the backend healthy. This prevents a single transient probe
    /// failure under heavy-but-healthy load from evicting a live backend.
    async fn poll_all(&self) {
        let threshold = current_failure_threshold();
        for (backend, failures) in self.backends.iter().zip(self.failures.iter()) {
            let probe_ok = self.probe_one(backend).await;
            let prev_healthy = backend.healthy.load(Ordering::Acquire);
            let prev_failures = failures.load(Ordering::Acquire);
            let (new_healthy, new_failures) =
                next_health(prev_healthy, probe_ok, prev_failures, threshold);
            failures.store(new_failures, Ordering::Release);
            backend.healthy.store(new_healthy, Ordering::Release);
        }
    }

    /// Single-backend health probe: `GET {endpoint}/health`.
    ///
    /// Returns `true` on a 2xx response, `false` on any error or non-2xx
    /// status, or if the backend has no endpoint (native-SDK backends are
    /// considered healthy unless their adapter reports otherwise).
    ///
    /// The probe carries a bounded, hot-swappable timeout (read per call, plan
    /// CLAUDE.md) so it answers quickly under load yet cannot stall the loop.
    async fn probe_one(&self, backend: &Backend) -> bool {
        let Some(endpoint) = backend.endpoint.as_deref() else {
            // Native-SDK backends (no HTTP endpoint) are assumed healthy;
            // their adapter handles health internally.
            return true;
        };
        let url = format!("{endpoint}/health");
        match self
            .client
            .get(&url)
            .timeout(current_probe_timeout())
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}

/// Compute a backend's next health state from a single probe outcome, applying
/// the consecutive-failure debounce.
///
/// Returns `(new_healthy, new_consecutive_failures)`:
/// - a successful probe (`probe_ok = true`) resets the counter to 0 and reports
///   healthy;
/// - a failed probe increments the counter and only reports unhealthy once it
///   reaches `threshold` — below the threshold the previous health is retained,
///   so a transient blip never evicts a live backend.
///
/// `threshold` is clamped to at least 1 (a threshold of 0 would otherwise never
/// trip, hiding a genuinely dead host).
fn next_health(
    prev_healthy: bool,
    probe_ok: bool,
    consecutive_failures: u32,
    threshold: u32,
) -> (bool, u32) {
    if probe_ok {
        return (true, 0);
    }
    let failures = consecutive_failures.saturating_add(1);
    let healthy = failures < threshold.max(1) && prev_healthy;
    (healthy, failures)
}

/// Resolve the consecutive-failure threshold, read fresh from the environment
/// each poll cycle so an operator can retune it on a live system (CLAUDE.md
/// hot-swappable rule).
fn current_failure_threshold() -> u32 {
    parse_failure_threshold(std::env::var(FAILURE_THRESHOLD_ENV).ok())
}

/// Parse a failure-threshold override; falls back to
/// [`DEFAULT_FAILURE_THRESHOLD`] when absent, unparseable, or zero.
fn parse_failure_threshold(raw: Option<String>) -> u32 {
    raw.and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_FAILURE_THRESHOLD)
}

/// Resolve the per-probe timeout, read fresh from the environment each probe so
/// it is hot-swappable on a live system (CLAUDE.md).
fn current_probe_timeout() -> Duration {
    parse_probe_timeout(std::env::var(PROBE_TIMEOUT_ENV).ok())
}

/// Parse a probe-timeout override (seconds); falls back to
/// [`DEFAULT_PROBE_TIMEOUT_SECS`] when absent, unparseable, or zero.
fn parse_probe_timeout(raw: Option<String>) -> Duration {
    let secs = raw
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use kb_mock_backend::{MockBackend, ResponseMode};

    // ── Debounce decision (`next_health`) ──────────────────────────────

    /// A successful probe always reports healthy and resets the failure count.
    #[test]
    fn next_health_success_resets() {
        assert_eq!(next_health(true, true, 0, 3), (true, 0));
        assert_eq!(next_health(false, true, 2, 3), (true, 0));
        assert_eq!(next_health(true, true, 99, 3), (true, 0));
    }

    /// A single (sub-threshold) failed probe keeps the prior health but counts up.
    #[test]
    fn next_health_single_failure_does_not_flip() {
        // Healthy backend stays healthy after one blip; counter advances.
        assert_eq!(next_health(true, false, 0, 3), (true, 1));
        assert_eq!(next_health(true, false, 1, 3), (true, 2));
        // An already-unhealthy backend stays unhealthy below threshold.
        assert_eq!(next_health(false, false, 0, 3), (false, 1));
    }

    /// Reaching the threshold of consecutive failures marks the backend unhealthy.
    #[test]
    fn next_health_threshold_marks_unhealthy() {
        assert_eq!(next_health(true, false, 2, 3), (false, 3));
        assert_eq!(next_health(true, false, 5, 3), (false, 6));
    }

    /// A threshold of 1 trips on the first failure; a threshold of 0 is clamped to 1.
    #[test]
    fn next_health_threshold_edges() {
        assert_eq!(next_health(true, false, 0, 1), (false, 1));
        // 0 would never trip, so it is clamped to 1.
        assert_eq!(next_health(true, false, 0, 0), (false, 1));
    }

    // ── Env-override parsing ───────────────────────────────────────────

    /// Threshold parsing falls back to the default for absent/invalid/zero input.
    #[test]
    fn parse_failure_threshold_defaults() {
        assert_eq!(parse_failure_threshold(None), DEFAULT_FAILURE_THRESHOLD);
        assert_eq!(
            parse_failure_threshold(Some("not-a-number".into())),
            DEFAULT_FAILURE_THRESHOLD
        );
        assert_eq!(
            parse_failure_threshold(Some("0".into())),
            DEFAULT_FAILURE_THRESHOLD
        );
        assert_eq!(parse_failure_threshold(Some("  5 ".into())), 5);
    }

    /// Probe-timeout parsing falls back to the default for absent/invalid/zero input.
    #[test]
    fn parse_probe_timeout_defaults() {
        assert_eq!(
            parse_probe_timeout(None),
            Duration::from_secs(DEFAULT_PROBE_TIMEOUT_SECS)
        );
        assert_eq!(
            parse_probe_timeout(Some("oops".into())),
            Duration::from_secs(DEFAULT_PROBE_TIMEOUT_SECS)
        );
        assert_eq!(
            parse_probe_timeout(Some("0".into())),
            Duration::from_secs(DEFAULT_PROBE_TIMEOUT_SECS)
        );
        assert_eq!(
            parse_probe_timeout(Some("30".into())),
            Duration::from_secs(30)
        );
    }

    /// Helper: build a backend that points at a running mock.
    fn backend_for_mock(mock: &MockBackend, id: &str, priority: u8, slots: usize) -> Arc<Backend> {
        Arc::new(crate::backend::test_backend(
            id,
            mock.url("/v1"),
            vec![kb_core::role::Role::Text],
            priority,
            slots,
        ))
    }

    /// The health loop marks a backend unhealthy when its `/health` returns 503.
    #[tokio::test]
    async fn marks_unhealthy_on_503() {
        let mock = MockBackend::start().await;
        let b = backend_for_mock(&mock, "b1", 0, 2);
        assert!(b.healthy.load(Ordering::Acquire), "starts healthy");

        // Make the mock respond 503 on /health.
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;

        let client = reqwest::Client::new();
        let loop_ = HealthLoop::new(vec![Arc::clone(&b)], client, Duration::from_millis(5));
        let (handle, shutdown_tx) = loop_.spawn();

        // Wait for at least one poll cycle to land.
        tokio::time::sleep(Duration::from_millis(40)).await;

        assert!(
            !b.healthy.load(Ordering::Acquire),
            "health loop must mark unhealthy after 503"
        );

        drop(shutdown_tx);
        handle.await.unwrap();
        mock.shutdown().await;
    }

    /// After a backend recovers (503 → 200), the health loop sets it healthy again.
    #[tokio::test]
    async fn marks_healthy_after_recovery() {
        let mock = MockBackend::start().await;
        let b = backend_for_mock(&mock, "b1", 0, 2);

        // Start unhealthy.
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;

        let client = reqwest::Client::new();
        let loop_ = HealthLoop::new(vec![Arc::clone(&b)], client, Duration::from_millis(5));
        let (handle, shutdown_tx) = loop_.spawn();

        // Wait for the loop to detect unhealthy.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !b.healthy.load(Ordering::Acquire),
            "should be unhealthy now"
        );

        // Recover the backend.
        mock.scenario().lock().await.health = ResponseMode::Healthy;

        // Wait for the next poll to notice the recovery.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            b.healthy.load(Ordering::Acquire),
            "must recover after 200 returns"
        );

        drop(shutdown_tx);
        handle.await.unwrap();
        mock.shutdown().await;
    }

    /// Backends registered under multiple roles are polled only once (dedup by Arc ptr).
    #[tokio::test]
    async fn deduplicates_by_arc_pointer() {
        let mock = MockBackend::start().await;
        let b = backend_for_mock(&mock, "b1", 0, 2);

        // Pass the same Arc three times.
        let client = reqwest::Client::new();
        let loop_ = HealthLoop::new(
            vec![Arc::clone(&b), Arc::clone(&b), Arc::clone(&b)],
            client,
            Duration::from_millis(5),
        );
        // Internal dedup means only one backend tracked.
        assert_eq!(loop_.backends.len(), 1);

        // Mark unhealthy and verify the single probe detects it.
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;

        let (handle, shutdown_tx) = loop_.spawn();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!b.healthy.load(Ordering::Acquire));

        drop(shutdown_tx);
        handle.await.unwrap();
        mock.shutdown().await;
    }

    /// The health loop can be shut down gracefully via the watch channel.
    #[tokio::test]
    async fn graceful_shutdown() {
        let mock = MockBackend::start().await;
        let b = backend_for_mock(&mock, "b1", 0, 2);

        let client = reqwest::Client::new();
        let loop_ = HealthLoop::new(vec![Arc::clone(&b)], client, Duration::from_secs(60));
        let (handle, shutdown_tx) = loop_.spawn();

        // Shut down almost immediately — must not hang.
        drop(shutdown_tx);
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "health loop must shut down promptly");
        mock.shutdown().await;
    }

    /// Integration: acquire skips a backend that the health loop has marked unhealthy.
    #[tokio::test]
    async fn unhealthy_backend_skipped_in_acquire() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let b = Arc::new(crate::backend::test_backend(
            "mock-1",
            &base_url,
            vec![kb_core::role::Role::Text],
            0,
            2,
        ));
        let pool = crate::Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Start the health loop.
        let client = reqwest::Client::new();
        let loop_ = HealthLoop::new(pool.all_backends(), client, Duration::from_millis(5));
        let (hl_handle, hl_shutdown) = loop_.spawn();

        // First, healthy → acquire works.
        let l1 = pool
            .acquire(kb_core::role::Role::Text, false, 0)
            .await
            .unwrap();
        assert_eq!(l1.backend_id, "mock-1");
        drop(l1);

        // Now mark unhealthy and wait for the health loop to notice.
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Acquire must return NoBackend (the only backend is now unhealthy).
        let err = pool
            .acquire(kb_core::role::Role::Text, false, 0)
            .await
            .unwrap_err();
        match err {
            crate::AcquireError::NoBackend { role } => {
                assert_eq!(role, kb_core::role::Role::Text);
            }
            other => panic!("expected NoBackend, got {other:?}"),
        }

        drop(hl_shutdown);
        hl_handle.await.unwrap();
        mock.shutdown().await;
    }

    /// Integration: after the health loop detects recovery, acquire picks the backend
    /// again.
    #[tokio::test]
    async fn recovered_backend_used_in_acquire() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let b = Arc::new(crate::backend::test_backend(
            "mock-1",
            &base_url,
            vec![kb_core::role::Role::Text],
            0,
            2,
        ));
        // Start unhealthy.
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;
        b.healthy.store(false, Ordering::Release);

        let pool = crate::Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Health loop will see the backend as unhealthy at first, then detect recovery.
        let client = reqwest::Client::new();
        let loop_ = HealthLoop::new(pool.all_backends(), client, Duration::from_millis(5));
        let (hl_handle, hl_shutdown) = loop_.spawn();

        // Acquire must fail while the backend is unhealthy.
        let err = pool
            .acquire(kb_core::role::Role::Text, false, 0)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::AcquireError::NoBackend { .. }));

        // Recover.
        mock.scenario().lock().await.health = ResponseMode::Healthy;
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Now acquire must succeed.
        let lease = pool
            .acquire(kb_core::role::Role::Text, false, 0)
            .await
            .unwrap();
        assert_eq!(lease.backend_id, "mock-1");

        drop(lease);
        drop(hl_shutdown);
        hl_handle.await.unwrap();
        mock.shutdown().await;
    }
}
