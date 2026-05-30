//! Background health-check loop that polls every backend's `/health` endpoint
//! and updates [`Backend::healthy`] (plan §6.5).
//!
//! The local semaphore remains the source of truth for in-flight load; health
//! is liveness only — a recovered host becomes eligible again on the next
//! [`crate::Pool::acquire`] call.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use reqwest::Client;
use tokio::sync::watch;

use crate::backend::Backend;

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
        Self {
            backends: unique,
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

    /// Probe every tracked backend and set its healthy flag.
    async fn poll_all(&self) {
        for backend in &self.backends {
            let is_healthy = self.probe_one(backend).await;
            backend.healthy.store(is_healthy, Ordering::Release);
        }
    }

    /// Single-backend health probe: `GET {base_url}/health`.
    ///
    /// Returns `true` on a 2xx response, `false` on any error or non-2xx
    /// status.
    async fn probe_one(&self, backend: &Backend) -> bool {
        let url = format!("{}/health", backend.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use kb_mock_backend::{MockBackend, ResponseMode};

    /// Helper: build a backend that points at a running mock.
    fn backend_for_mock(mock: &MockBackend, id: &str, priority: u8, slots: usize) -> Arc<Backend> {
        Arc::new(Backend::new(
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
        let b = Arc::new(Backend::new(
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
        let l1 = pool.acquire(kb_core::role::Role::Text).await.unwrap();
        assert_eq!(l1.backend_id, "mock-1");
        drop(l1);

        // Now mark unhealthy and wait for the health loop to notice.
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Acquire must return NoBackend (the only backend is now unhealthy).
        let err = pool.acquire(kb_core::role::Role::Text).await.unwrap_err();
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
        let b = Arc::new(Backend::new(
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
        let err = pool.acquire(kb_core::role::Role::Text).await.unwrap_err();
        assert!(matches!(err, crate::AcquireError::NoBackend { .. }));

        // Recover.
        mock.scenario().lock().await.health = ResponseMode::Healthy;
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Now acquire must succeed.
        let lease = pool.acquire(kb_core::role::Role::Text).await.unwrap();
        assert_eq!(lease.backend_id, "mock-1");

        drop(lease);
        drop(hl_shutdown);
        hl_handle.await.unwrap();
        mock.shutdown().await;
    }
}
