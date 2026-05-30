//! [`Pool`]: the role-indexed registry of backends (plan §6.2, §6.6) and
//! its acquire algorithm (§6.3).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use kb_config::Config;
use kb_core::role::Role;

use crate::backend::Backend;
use crate::error::AcquireError;
use crate::lease::Lease;

/// The scheduler's registry of backends, indexed by the role they serve.
///
/// A backend that serves several roles is registered under each — by a shared
/// `Arc`, so all roles see one slot pool. The pool owns live semaphores and is a
/// long-lived component: build it once at startup with [`Pool::from_config`].
///
/// Hot-reload note (CLAUDE.md): because the semaphores hold in-flight
/// [`crate::Lease`]s, a config change should *reconcile* the registry and timeout
/// in place rather than rebuild the pool wholesale. The acquire path therefore
/// reads the timeout per call via [`Pool::acquire_timeout`] (the live seam); the
/// reconcile step lands with the acquisition algorithm in a later P1 task
/// (plan §6.3, §6.6).
#[derive(Clone)]
pub struct Pool {
    by_role: Arc<DashMap<Role, Vec<Arc<Backend>>>>,
    acquire_timeout: Duration,
}

impl Pool {
    /// Build a pool from explicit backends and an acquire timeout.
    ///
    /// Each backend is registered (by shared `Arc`) under every role it serves.
    pub fn new(backends: Vec<Arc<Backend>>, acquire_timeout: Duration) -> Self {
        let by_role: DashMap<Role, Vec<Arc<Backend>>> = DashMap::new();
        for backend in backends {
            for role in &backend.roles {
                by_role.entry(*role).or_default().push(Arc::clone(&backend));
            }
        }
        Self {
            by_role: Arc::new(by_role),
            acquire_timeout,
        }
    }

    /// Build a pool from the typed configuration (plan §6.6).
    ///
    /// Every `[[backend]]` becomes a [`Backend`] registered under each role it
    /// serves; `acquire_timeout` comes from `scheduler.acquire_timeout_secs`.
    pub fn from_config(cfg: &Config) -> Self {
        let backends = cfg
            .backends
            .iter()
            .map(|b| Arc::new(Backend::from_config(b)))
            .collect();
        Self::new(
            backends,
            Duration::from_secs(cfg.scheduler.acquire_timeout_secs),
        )
    }

    /// The timeout bounding a single acquire wait — read per call (plan §6.3).
    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Acquire a slot on any healthy backend serving `role` (plan §6.3).
    ///
    /// **Fast path:** probe candidates sorted by `(priority, free-slots↓)`
    /// with [`Backend::try_acquire`](#);
    /// **wait path:** await all candidates concurrently via
    /// [`FuturesUnordered`], first freed slot wins, bounded by the pool's
    /// [`acquire_timeout`](Self::acquire_timeout).
    ///
    /// Unhealthy backends are filtered before both steps (dead-host skip).
    ///
    /// # Errors
    ///
    /// - [`AcquireError::NoBackend`] — no healthy backend serves `role`.
    /// - [`AcquireError::Timeout`] — every candidate stayed busy for the full
    ///   acquire-timeout.
    /// - [`AcquireError::Closed`] — all candidate semaphores are closed.
    pub async fn acquire(&self, role: Role) -> Result<Lease, AcquireError> {
        // 1. Gather healthy candidates (dead-host skip).
        let mut cands: Vec<Arc<Backend>> = self
            .backends_for(role)
            .into_iter()
            .filter(|b| b.healthy.load(Ordering::Acquire))
            .collect();

        if cands.is_empty() {
            return Err(AcquireError::NoBackend { role });
        }

        // 2. Sort by priority (lower = preferred), then by most free slots.
        cands.sort_by_key(|b| (b.priority, usize::MAX.saturating_sub(b.free())));

        // 3. FAST PATH — try a free slot on the best candidate (plan §6.3).
        for b in &cands {
            if let Ok(permit) = b.slots.clone().try_acquire_owned() {
                return Ok(Lease::new(b.id.clone(), b.base_url.clone(), permit));
            }
        }

        // 4. WAIT PATH — whichever frees first wins, bounded by timeout.
        let mut waiters = FuturesUnordered::new();
        for b in &cands {
            let slots = Arc::clone(&b.slots);
            let id = b.id.clone();
            let url = b.base_url.clone();
            waiters
                .push(async move { slots.acquire_owned().await.map(|permit| (id, url, permit)) });
        }

        match tokio::time::timeout(self.acquire_timeout, waiters.next()).await {
            Ok(Some(Ok((id, url, permit)))) => Ok(Lease::new(id, url, permit)),
            Ok(None) | Ok(Some(Err(_))) => Err(AcquireError::Closed),
            Err(_elapsed) => Err(AcquireError::Timeout(self.acquire_timeout)),
        }
    }

    /// Candidate backends registered for `role`.
    ///
    /// Returns a snapshot clone (cheap `Arc` clones); an unknown role yields an
    /// empty vec. Health filtering and priority ordering happen in the acquire
    /// path, not here (plan §6.3).
    pub fn backends_for(&self, role: Role) -> Vec<Arc<Backend>> {
        self.by_role
            .get(&role)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Distinct roles that at least one backend serves.
    pub fn roles(&self) -> Vec<Role> {
        self.by_role.iter().map(|entry| *entry.key()).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use kb_config::{Backend as BackendCfg, Config, Scheduler};

    fn backend_cfg(
        id: &str,
        base_url: &str,
        roles: Vec<Role>,
        slots: u32,
        priority: u8,
    ) -> BackendCfg {
        BackendCfg {
            id: id.into(),
            base_url: base_url.into(),
            roles,
            slots,
            priority,
        }
    }

    /// gpu-a serves text+embed (4 slots); gpu-b serves embed only (2 slots).
    fn two_backend_config() -> Config {
        Config {
            scheduler: Scheduler {
                acquire_timeout_secs: 7,
                ..Default::default()
            },
            backends: vec![
                backend_cfg(
                    "gpu-a",
                    "http://localhost:8001",
                    vec![Role::Text, Role::Embed],
                    4,
                    10,
                ),
                backend_cfg("gpu-b", "http://localhost:8002", vec![Role::Embed], 2, 20),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn from_config_indexes_backends_by_role() {
        let pool = Pool::from_config(&two_backend_config());
        assert_eq!(pool.acquire_timeout(), Duration::from_secs(7));

        let text = pool.backends_for(Role::Text);
        assert_eq!(text.len(), 1);
        assert_eq!(text[0].id, "gpu-a");
        assert_eq!(text[0].free(), 4);

        let embed = pool.backends_for(Role::Embed);
        assert_eq!(embed.len(), 2);

        // A role no backend serves yields an empty candidate set, not a panic.
        assert!(pool.backends_for(Role::Rerank).is_empty());
    }

    #[test]
    fn multi_role_backend_shares_one_slot_pool() {
        let pool = Pool::from_config(&two_backend_config());
        let text = pool.backends_for(Role::Text);
        let text_a = &text[0];
        let embed_a = pool
            .backends_for(Role::Embed)
            .into_iter()
            .find(|b| b.id == "gpu-a")
            .unwrap();

        // gpu-a under `text` and under `embed` is the very same Arc.
        assert!(Arc::ptr_eq(text_a, &embed_a));

        let _permit = text_a.slots.clone().try_acquire_owned().unwrap();
        assert_eq!(embed_a.free(), 3, "one shared semaphore across roles");
    }

    #[test]
    fn roles_lists_every_served_role() {
        // `Role` has no `Ord`, so compare as an unordered set.
        let roles = Pool::from_config(&two_backend_config()).roles();
        assert_eq!(roles.len(), 2);
        assert!(roles.contains(&Role::Text));
        assert!(roles.contains(&Role::Embed));
    }

    #[test]
    fn new_with_no_backends_is_empty() {
        let pool = Pool::new(vec![], Duration::from_secs(1));
        assert!(pool.backends_for(Role::Text).is_empty());
        assert!(pool.roles().is_empty());
        assert_eq!(pool.acquire_timeout(), Duration::from_secs(1));
    }

    // ── P1-T2: acquire algorithm tests (§6.3) ──────────────────────────

    /// Fast path: a backend with a free slot returns a lease immediately.
    #[tokio::test]
    async fn acquire_free_slot_returns_lease() {
        let b = Arc::new(Backend::new("b1", "http://x:8001", vec![Role::Text], 0, 2));
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(lease.backend_id, "b1");
        assert_eq!(lease.base_url, "http://x:8001");
        assert_eq!(b.free(), 1, "one slot consumed");

        drop(lease);
        assert_eq!(b.free(), 2, "slot returned on drop");
    }

    /// Lower-priority (preferred) backend is picked first when both have free slots.
    #[tokio::test]
    async fn acquire_prefers_lower_priority() {
        let b_pri0 = Arc::new(Backend::new("pri0", "http://x:0", vec![Role::Text], 0, 2));
        let b_pri10 = Arc::new(Backend::new(
            "pri10",
            "http://x:10",
            vec![Role::Text],
            10,
            2,
        ));
        let pool = Pool::new(
            vec![Arc::clone(&b_pri10), Arc::clone(&b_pri0)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(
            lease.backend_id, "pri0",
            "priority 0 must be picked before priority 10 when both free"
        );
    }

    /// When two backends have the same priority, the one with more free slots wins.
    #[tokio::test]
    async fn acquire_same_priority_picks_most_free_slots() {
        let b_full = Arc::new(Backend::new(
            "full",
            "http://x:full",
            vec![Role::Text],
            5,
            1,
        ));
        // Exhaust b_full's only slot so the fast-path skips it.
        let _busy = b_full.slots.clone().try_acquire_owned().unwrap();

        let b_free = Arc::new(Backend::new(
            "free",
            "http://x:free",
            vec![Role::Text],
            5,
            3,
        ));
        let pool = Pool::new(
            vec![Arc::clone(&b_full), Arc::clone(&b_free)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(
            lease.backend_id, "free",
            "backend with free slots must be picked"
        );
    }

    /// When all backends are busy (no free slots), acquire times out.
    #[tokio::test]
    async fn acquire_all_busy_then_timeout() {
        let b = Arc::new(Backend::new("b1", "http://x:8001", vec![Role::Text], 0, 1));
        // Hold the only slot — never release it.
        let _hold = b.slots.clone().try_acquire_owned().unwrap();
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_millis(10));

        let err = pool.acquire(Role::Text).await.unwrap_err();
        match err {
            AcquireError::Timeout(d) => {
                assert_eq!(d, Duration::from_millis(10));
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    /// An unhealthy backend is skipped even if it has free slots.
    #[tokio::test]
    async fn dead_host_skipped() {
        let b_dead = Arc::new(Backend::new(
            "dead",
            "http://x:dead",
            vec![Role::Text],
            0,
            4,
        ));
        b_dead.healthy.store(false, Ordering::Release); // mark dead

        let b_alive = Arc::new(Backend::new(
            "alive",
            "http://x:alive",
            vec![Role::Text],
            10,
            2,
        ));
        let pool = Pool::new(
            vec![Arc::clone(&b_dead), Arc::clone(&b_alive)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(
            lease.backend_id, "alive",
            "unhealthy backend must be skipped regardless of free slots"
        );
    }

    /// NoBackend when no healthy backend serves the requested role.
    #[tokio::test]
    async fn no_backend_role_unknown() {
        let pool = Pool::new(vec![], Duration::from_secs(5));
        let err = pool.acquire(Role::Embed).await.unwrap_err();
        match err {
            AcquireError::NoBackend { role } => assert_eq!(role, Role::Embed),
            other => panic!("expected NoBackend, got {other:?}"),
        }
    }

    /// NoBackend when every backend serving the role is unhealthy.
    #[tokio::test]
    async fn no_backend_all_unhealthy() {
        let b = Arc::new(Backend::new("b1", "http://x:8001", vec![Role::Text], 0, 2));
        b.healthy.store(false, Ordering::Release);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        let err = pool.acquire(Role::Text).await.unwrap_err();
        match err {
            AcquireError::NoBackend { role } => assert_eq!(role, Role::Text),
            other => panic!("expected NoBackend, got {other:?}"),
        }
    }

    /// Wait path: acquire blocks until a held slot is released, then succeeds.
    #[tokio::test]
    async fn wait_path_succeeds_when_slot_frees() {
        let b = Arc::new(Backend::new("b1", "http://x:8001", vec![Role::Text], 0, 1));
        // Hold the only slot.
        let hold = b.slots.clone().try_acquire_owned().unwrap();
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Spawn a task that releases the slot after a short delay.
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(hold);
        });

        // acquire should block briefly then succeed once the slot is freed.
        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(lease.backend_id, "b1");

        release.await.unwrap();
        drop(lease);
        assert_eq!(b.free(), 1);
    }

    /// RAII: dropping the Lease returns the slot to the backend.
    #[tokio::test]
    async fn lease_drop_frees_slot_for_next_acquire() {
        let b = Arc::new(Backend::new("b1", "http://x:8001", vec![Role::Text], 0, 1));
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Acquire and immediately drop.
        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(b.free(), 0);
        drop(lease);
        assert_eq!(b.free(), 1);

        // A subsequent acquire must succeed (slot was returned).
        let lease2 = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(lease2.backend_id, "b1");
        assert_eq!(b.free(), 0);
    }

    /// Pool is `Clone`-able and clones share the same registries/timeout.
    #[tokio::test]
    async fn cloned_pool_shares_backend_state() {
        let b = Arc::new(Backend::new("b1", "http://x:8001", vec![Role::Text], 0, 2));
        let pool1 = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let pool2 = pool1.clone();

        // Acquire through pool1 consumes a slot visible through pool2.
        let _l1 = pool1.acquire(Role::Text).await.unwrap();
        assert_eq!(b.free(), 1);

        // pool2 sees the same exhaustion.
        let _l2 = pool2.acquire(Role::Text).await.unwrap();
        assert_eq!(b.free(), 0);
    }

    // ── P1-T3: mock-backend harness smoke test ────────────────────────

    /// Prove the mock-backend harness works end-to-end with the scheduler:
    /// start a mock, register it as a backend, acquire a slot, drop it.
    #[tokio::test]
    async fn acquire_from_mock_backend() {
        let mock = kb_mock_backend::MockBackend::start().await;
        let base_url = format!("http://{}/v1", mock.addr());

        let b = Arc::new(Backend::new(
            "mock-1",
            base_url,
            vec![Role::Text],
            0, /* priority */
            2, /* slots */
        ));
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Fast-path: mock is healthy (default), slots are free.
        let lease = pool.acquire(Role::Text).await.unwrap();
        assert_eq!(lease.backend_id, "mock-1");
        assert!(lease.base_url.starts_with("http://127.0.0.1:"));
        assert!(lease.base_url.ends_with("/v1"));
        assert_eq!(b.free(), 1);

        drop(lease);
        assert_eq!(b.free(), 2);

        // Prove the mock is truly alive: issue an actual health request.
        let health_url = mock.url("/health");
        let status = reqwest::get(&health_url).await.unwrap().status();
        assert_eq!(status, 200);

        mock.shutdown().await;
    }
}
