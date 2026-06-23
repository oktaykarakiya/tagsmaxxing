// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`Pool`]: the role-indexed registry of backends (plan §6.2, §6.6) and
//! its acquire algorithm (§6.3, §26.4).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use kb_config::Config;
use kb_core::role::Role;
use kb_core::routing::RoutingTable;

use crate::backend::Backend;
use crate::error::AcquireError;
use crate::lease::Lease;
use crate::priority_wait::PriorityWaiterQueue;
use crate::tiered;

/// Minimum interval between tiered→legacy fallback warnings per role
/// (BUG-SCHED-03). The fallback itself is silent and hot-path-cheap; the
/// warning is operator-facing diagnostics only.
const FALLBACK_WARN_EVERY: Duration = Duration::from_secs(30);

/// The scheduler's registry of backends, indexed by the role they serve.
///
/// A backend that serves several roles (via its [`CapSet`](kb_core::capability::CapSet))
/// is registered under each — by a shared `Arc`, so all roles see one capacity
/// pool. The pool owns live capacity guards and is a long-lived component: build
/// it once at startup with [`Pool::from_config`].
///
/// # Tiered routing (P9-T6)
///
/// When a [`RoutingTable`] is set via [`set_routing`](Self::set_routing),
/// [`acquire`](Self::acquire) uses the tiered failover algorithm from plan
/// §26.4 (primary tier → wait `spill_after` → next tier → … → union wait →
/// capacity exhausted).  If no routing table is active, it falls back to the
/// legacy flat priority-based candidate list (§6.3).
///
/// # Priority-aware waiting (P9-T12)
///
/// The embedded [`PriorityWaiterQueue`] ensures that when multiple callers
/// wait for capacity on the same role, the one with the highest `priority`
/// (higher = more urgent) is woken first when a slot frees. The slot-free
/// notification is triggered by [`Lease::drop`].
///
/// Hot-reload note (CLAUDE.md): the routing table is stored behind an
/// [`ArcSwap`] so hot-reload (P9-T7) is lock-free for readers.
#[derive(Clone)]
pub struct Pool {
    /// Backends indexed by role (legacy + current — always populated).
    by_role: Arc<DashMap<Role, Vec<Arc<Backend>>>>,
    /// Flat registry of every backend by its string ID, for fast lookup
    /// from routing-table model references.
    backends: Arc<DashMap<String, Arc<Backend>>>,
    /// The global acquire timeout.
    acquire_timeout: Duration,
    /// Tiered routing table (P9-T6).  When `Some`, `acquire` uses the
    /// tiered algorithm; when `None`, it falls back to the legacy flat pool.
    /// Wrapped in [`ArcSwap`] for lock-free hot-reload (P9-T7).
    routing: Arc<ArcSwap<Option<Arc<RoutingTable>>>>,
    /// Per-(role, tier) round-robin counters for the `RoundRobin` strategy.
    rr_counters: Arc<DashMap<(u8, i32), AtomicUsize>>,
    /// Priority-ordered waiter queue for P9-T12. When a caller waits for
    /// capacity, it registers here; when a [`Lease`] is dropped, the queue
    /// is notified and the highest-priority waiter is woken.
    wait_queue: Arc<PriorityWaiterQueue>,
    /// Per-role timestamp of the last tiered→legacy fallback warning, so the
    /// log is not flooded when a role has no usable routing entries
    /// (BUG-SCHED-03). Keyed by the compact role bit value.
    fallback_warned: Arc<DashMap<u8, std::time::Instant>>,
}

impl Pool {
    /// Build a pool from explicit backends and an acquire timeout.
    ///
    /// Each backend is registered (by shared `Arc`) under every role it serves,
    /// as determined by [`Backend::supports`].
    pub fn new(backends: Vec<Arc<Backend>>, acquire_timeout: Duration) -> Self {
        let by_role: DashMap<Role, Vec<Arc<Backend>>> = DashMap::new();
        let backends_map: DashMap<String, Arc<Backend>> = DashMap::new();
        for backend in backends {
            // Register in the flat ID→Backend map.
            backends_map.insert(backend.id.clone(), Arc::clone(&backend));
            // Register the backend under every role its CapSet supports.
            for role in Role::all() {
                if backend.supports(*role) {
                    by_role.entry(*role).or_default().push(Arc::clone(&backend));
                }
            }
        }
        Self {
            by_role: Arc::new(by_role),
            backends: Arc::new(backends_map),
            acquire_timeout,
            routing: Arc::new(ArcSwap::new(Arc::new(None))),
            rr_counters: Arc::new(DashMap::new()),
            wait_queue: Arc::new(PriorityWaiterQueue::default()),
            fallback_warned: Arc::new(DashMap::new()),
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

    /// Apply a freshly fetched routing snapshot (BUG-SCHED-03).
    ///
    /// Materializes/refreshes the DB-keyed backends in the flat map via
    /// [`crate::materialize`] **before** swapping the table in, so no acquire
    /// ever observes a routing table whose backends are missing. An empty
    /// snapshot clears the table (legacy flat-priority mode) and prunes all
    /// DB-materialized backends.
    ///
    /// Concurrency: production has a single reload task, so applies are
    /// serial. A hypothetical concurrent apply interleaves per-key
    /// atomically; worst case an acquire resolves zero candidates for one
    /// role momentarily and falls back to the legacy pool.
    pub fn apply_routing(&self, entries: Vec<kb_core::routing::RoutingEntry>) {
        if entries.is_empty() {
            self.clear_routing();
            crate::materialize::prune_all(&self.backends);
            return;
        }
        crate::materialize::sync_backends(&self.backends, &entries);
        self.set_routing(Arc::new(RoutingTable::from_entries(entries)));
    }

    /// Look up any backend — config or DB-materialized — by its flat-map id.
    ///
    /// This is the id carried by [`Lease::backend_id`](crate::Lease); the
    /// LLM client uses it to flip the pool-side health flag on transport
    /// failures, which is what lets tiered routing skip a dead routed
    /// backend and fail over to the next tier.
    pub fn find_backend(&self, id: &str) -> Option<Arc<Backend>> {
        self.backends.get(id).map(|e| Arc::clone(e.value()))
    }

    /// Set or replace the tiered routing table (P9-T6).
    ///
    /// Prefer [`apply_routing`](Self::apply_routing), which also materializes
    /// DB-backed entries; this raw setter is kept for tests and for callers
    /// that manage the backend map themselves.
    ///
    /// After setting, subsequent [`acquire`](Self::acquire) calls use the
    /// tiered failover algorithm (§26.4) instead of the legacy flat-priority
    /// candidate list.  The swap is wait-free for readers (ArcSwap).
    pub fn set_routing(&self, table: Arc<RoutingTable>) {
        self.routing.store(Arc::new(Some(table)));
    }

    /// Remove the routing table, reverting to the legacy flat-priority
    /// acquire algorithm.
    pub fn clear_routing(&self) {
        self.routing.store(Arc::new(None));
    }

    /// Whether a routing table is currently active.
    pub fn has_routing(&self) -> bool {
        self.routing.load().is_some()
    }

    /// Acquire capacity on any eligible backend serving `role`.
    ///
    /// When a [`RoutingTable`] is active (set via [`set_routing`](Self::set_routing)),
    /// this uses the **tiered failover algorithm** from plan §26.4: walk tiers
    /// in order, filter + order candidates by strategy, try each, wait
    /// `spill_after`, then spill to the next tier.  [`Role::Embed`] is locked
    /// to tier 0 only (plan §11 — no cross-model failover for embeddings).
    ///
    /// **Per-role fallback (BUG-SCHED-03):** a routing table covers only the
    /// roles it has entries for. When the tiered path reports
    /// [`AcquireError::NoBackend`] — the role has no tiers, or every entry
    /// resolved to zero live backends — acquire falls back to the legacy
    /// flat-priority pool (with a rate-limited warning) instead of failing
    /// the role outright. Real capacity pressure is **not** masked:
    /// [`AcquireError::CapacityExhausted`]/[`Timeout`](AcquireError::Timeout)
    /// from resolvable tiers are returned as-is.
    ///
    /// When no routing table is set, this falls back to the **legacy
    /// flat-priority algorithm** (§6.3): candidates sorted by
    /// `(priority, free-slots↓)` with fast-path `try_acquire` + bounded
    /// wait path via [`FuturesUnordered`].
    ///
    /// `priority` (P9-T12) makes the acquire priority-aware: when multiple
    /// callers wait for a slot, the one with the highest priority (higher =
    /// more urgent) is woken first. Default is 0.
    ///
    /// `local_only` enforces data-residency policy (plan §26.6, P9-T9):
    /// when `true`, backends with [`DataClass::Remote`](kb_core::data_class::DataClass)
    /// are excluded — the call must never reach a remote provider.
    ///
    /// # Errors
    ///
    /// - [`AcquireError::NoBackend`] — no healthy, usable backend serves `role`.
    /// - [`AcquireError::Timeout`] — legacy path: every candidate stayed busy
    ///   for the full acquire-timeout.
    /// - [`AcquireError::CapacityExhausted`] — tiered path: all tiers
    ///   exhausted without acquiring capacity.
    /// - [`AcquireError::Closed`] — all candidate capacity guards are closed.
    pub async fn acquire(
        &self,
        role: Role,
        local_only: bool,
        priority: i32,
    ) -> Result<Lease, AcquireError> {
        // Check for an active routing table — use tiered path if available.
        let routing_guard = self.routing.load();
        if let Some(table) = (*routing_guard).as_ref() {
            match tiered::acquire_tiered(
                role,
                priority,
                None, // tenant_id — global routes for now (per-tenant acquire is a recorded gap).
                table,
                &self.backends,
                self.acquire_timeout,
                &self.rr_counters,
                local_only, // P9-T9: data-residency enforcement (§26.6)
                &self.wait_queue,
            )
            .await
            {
                // Both NoBackend causes (the role has no tiers in the table;
                // every entry resolved to zero live candidates) mean tiered
                // routing has nothing usable for `role`. Fall back to the
                // legacy config-backend pool so a partial routing table never
                // takes unrelated roles down (BUG-SCHED-03). Both causes
                // return without waiting, so the fallback adds no latency.
                Err(AcquireError::NoBackend { .. }) => {
                    if self.should_warn_fallback(role) {
                        tracing::warn!(
                            role = role.as_str(),
                            "tiered routing has no usable candidates for this role; \
                             falling back to legacy config backends"
                        );
                    }
                }
                // CapacityExhausted / Timeout / Closed come from resolvable
                // candidates — real pressure must not be masked by silently
                // switching pools.
                other => return other,
            }
        }

        // Legacy flat-priority path (§6.3).
        self.acquire_legacy(role, local_only, priority).await
    }

    /// Whether the tiered→legacy fallback warning for `role` should be
    /// emitted now. Updates the per-role timestamp when it returns `true`;
    /// at most one warning per role per [`FALLBACK_WARN_EVERY`].
    fn should_warn_fallback(&self, role: Role) -> bool {
        let key = tiered::role_to_rr_key(role);
        let now = std::time::Instant::now();
        let mut entry = self.fallback_warned.entry(key).or_insert(
            // First sighting: backdate so the first warning always fires.
            now - FALLBACK_WARN_EVERY,
        );
        if now.duration_since(*entry) >= FALLBACK_WARN_EVERY {
            *entry = now;
            true
        } else {
            false
        }
    }

    /// Legacy flat-priority acquire (plan §6.3).
    ///
    /// Used when no [`RoutingTable`] is active.  See [`acquire`](Self::acquire)
    /// for the combined documentation.
    async fn acquire_legacy(
        &self,
        role: Role,
        local_only: bool,
        priority: i32,
    ) -> Result<Lease, AcquireError> {
        // 1. Gather healthy candidates with endpoints (dead-host + cooldown skip
        //    + skip Rated backends with exhausted token buckets).
        //    P9-T9: when `local_only`, exclude Remote backends (§26.6).
        let mut cands: Vec<Arc<Backend>> = self
            .backends_for(role)
            .into_iter()
            .filter(|b| b.healthy.load(Ordering::Acquire))
            .filter(|b| !b.cooldown_active())
            .filter(|b| b.capacity.is_usable())
            .filter(|b| b.endpoint.is_some())
            .filter(|b| b.is_in_time_window()) // P9-T13: time-of-day gate
            .filter(|b| b.bandwidth_available()) // P9-T13: bandwidth throttle
            .filter(|b| !local_only || b.data_class == kb_core::data_class::DataClass::Local)
            .collect();

        if cands.is_empty() {
            return Err(AcquireError::NoBackend { role });
        }

        // 2. Sort by priority (lower = preferred), then by most free slots.
        cands.sort_by_key(|b| (b.priority, usize::MAX.saturating_sub(b.free())));

        // 3. FAST PATH — try capacity on the best candidate (plan §6.3, §26.2).
        // P9-T12: attach wait-queue notification so a waiter is woken when
        // this lease is dropped.
        if let Some(lease) = Self::try_fast_path(&cands, role) {
            return Ok(lease.with_notify(role, Arc::clone(&self.wait_queue)));
        }

        // 4. WAIT PATH — priority-ordered (P9-T12).
        //    Register in the wait queue. Race between:
        //      a) A slot freeing (we are the highest-priority waiter — notified by lease drop)
        //      b) The global acquire timeout
        self.wait_on_candidates(&cands, role, priority).await
    }

    /// Fast-path helper: try `try_acquire` on each candidate, returning the
    /// first success with a lease that notifies the wait queue on drop.
    ///
    /// Public within the crate so tiered routing can reuse it for its union-wait
    /// retry loop (P9-T12).
    pub(crate) fn try_fast_path(candidates: &[Arc<Backend>], _role: Role) -> Option<Lease> {
        for b in candidates {
            if let Some(guard) = b.capacity.try_acquire() {
                // Safety: candidates are pre-filtered for endpoint presence.
                let endpoint = b.endpoint.clone().unwrap_or_default();
                // Note: fast-path does NOT attach wait-queue notification
                // because no one was waiting — this is the immediate success case.
                return Some(Lease::new(b.id.clone(), endpoint, guard));
            }
        }
        None
    }

    /// Wait path: register in the priority wait queue and race between slot-free
    /// notification and the global timeout.
    async fn wait_on_candidates(
        &self,
        candidates: &[Arc<Backend>],
        role: Role,
        priority: i32,
    ) -> Result<Lease, AcquireError> {
        // Register in the priority wait queue.
        let mut ticket = self.wait_queue.register(role, priority).await;

        // Race between: 1) being woken (slot freed), 2) global timeout.
        loop {
            tokio::select! {
                _ = ticket.wait() => {
                    // We were woken — a slot was freed. Retry the fast path.
                    if let Some(lease) = Self::try_fast_path(candidates, role) {
                        return Ok(lease);
                    }
                    // The freed slot was taken by someone else.
                    // Re-register and keep waiting.
                    ticket = self.wait_queue.register(role, priority).await;
                }
                _ = tokio::time::sleep(self.acquire_timeout) => {
                    return Err(AcquireError::Timeout(self.acquire_timeout));
                }
            }
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

    /// Every unique backend registered in this pool, deduplicated.
    ///
    /// A backend that serves several roles still appears once. Useful for the
    /// health loop and other components that need to operate on all physical
    /// hosts without double-counting.
    pub fn all_backends(&self) -> Vec<Arc<Backend>> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for entry in self.by_role.iter() {
            for b in entry.value() {
                if seen.insert(Arc::as_ptr(b) as usize) {
                    out.push(Arc::clone(b));
                }
            }
        }
        out
    }

    /// Minimum context window size (in tokens) among healthy backends for a role.
    ///
    /// Returns `None` when no healthy backend for `role` has a known `ctx_tokens`
    /// (all are `None`, or no backend serves the role).
    pub fn role_context_tokens(&self, role: Role) -> Option<usize> {
        let backends = self.backends_for(role);
        if backends.is_empty() {
            return None;
        }
        let mut min_ctx: Option<i64> = None;
        for b in &backends {
            if b.healthy.load(Ordering::Acquire)
                && let Some(ctx) = b.ctx_tokens
            {
                match min_ctx {
                    None => min_ctx = Some(ctx),
                    Some(current) => min_ctx = Some(current.min(ctx)),
                }
            }
        }
        min_ctx.map(|t| t as usize)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::Capacity;
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

    /// Shortcut for building a test backend via the test helper.
    fn tb(id: &str, endpoint: &str, roles: Vec<Role>, priority: u8, slots: usize) -> Arc<Backend> {
        Arc::new(crate::backend::test_backend(
            id, endpoint, roles, priority, slots,
        ))
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

        let _permit = text_a.capacity.try_acquire().unwrap();
        assert_eq!(embed_a.free(), 3, "one shared capacity across roles");
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
    fn all_backends_deduplicates_multi_role() {
        let pool = Pool::from_config(&two_backend_config());
        let all = pool.all_backends();
        // gpu-a serves two roles but appears once.
        assert_eq!(all.len(), 2, "two unique backends, not three");
        let ids: Vec<&str> = all.iter().map(|b| b.id.as_str()).collect();
        assert!(ids.contains(&"gpu-a"));
        assert!(ids.contains(&"gpu-b"));
    }

    #[test]
    fn new_with_no_backends_is_empty() {
        let pool = Pool::new(vec![], Duration::from_secs(1));
        assert!(pool.backends_for(Role::Text).is_empty());
        assert!(pool.roles().is_empty());
        assert_eq!(pool.acquire_timeout(), Duration::from_secs(1));
    }

    #[test]
    fn new_registers_only_for_supported_roles() {
        let b = tb("b1", "http://x", vec![Role::Text, Role::Embed], 0, 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Registered under Text and Embed.
        assert_eq!(pool.backends_for(Role::Text).len(), 1);
        assert_eq!(pool.backends_for(Role::Embed).len(), 1);
        // NOT registered under other roles.
        assert!(pool.backends_for(Role::Vision).is_empty());
        assert!(pool.backends_for(Role::Code).is_empty());
        assert!(pool.backends_for(Role::Rerank).is_empty());
    }

    // ── P1-T2: acquire algorithm tests (§6.3) ──────────────────────────

    /// Fast path: a backend with a free slot returns a lease immediately.
    #[tokio::test]
    async fn acquire_free_slot_returns_lease() {
        let b = tb("b1", "http://x:8001", vec![Role::Text], 0, 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "b1");
        assert_eq!(lease.endpoint, "http://x:8001");
        assert_eq!(b.free(), 1, "one slot consumed");

        drop(lease);
        assert_eq!(b.free(), 2, "slot returned on drop");
    }

    /// Lower-priority (preferred) backend is picked first when both have free slots.
    #[tokio::test]
    async fn acquire_prefers_lower_priority() {
        let b_pri0 = tb("pri0", "http://x:0", vec![Role::Text], 0, 2);
        let b_pri10 = tb("pri10", "http://x:10", vec![Role::Text], 10, 2);
        let pool = Pool::new(
            vec![Arc::clone(&b_pri10), Arc::clone(&b_pri0)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(
            lease.backend_id, "pri0",
            "priority 0 must be picked before priority 10 when both free"
        );
    }

    /// When two backends have the same priority, the one with more free slots wins.
    #[tokio::test]
    async fn acquire_same_priority_picks_most_free_slots() {
        let b_full = tb("full", "http://x:full", vec![Role::Text], 5, 1);
        // Exhaust b_full's only slot so the fast-path skips it.
        let _busy = b_full.capacity.try_acquire().unwrap();

        let b_free = tb("free", "http://x:free", vec![Role::Text], 5, 3);
        let pool = Pool::new(
            vec![Arc::clone(&b_full), Arc::clone(&b_free)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(
            lease.backend_id, "free",
            "backend with free slots must be picked"
        );
    }

    /// When all backends are busy (no free slots), acquire times out.
    #[tokio::test]
    async fn acquire_all_busy_then_timeout() {
        let b = tb("b1", "http://x:8001", vec![Role::Text], 0, 1);
        // Hold the only slot — never release it.
        let _hold = b.capacity.try_acquire().unwrap();
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_millis(10));

        let err = pool.acquire(Role::Text, false, 0).await.unwrap_err();
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
        let b_dead = tb("dead", "http://x:dead", vec![Role::Text], 0, 4);
        b_dead.healthy.store(false, Ordering::Release); // mark dead

        let b_alive = tb("alive", "http://x:alive", vec![Role::Text], 10, 2);
        let pool = Pool::new(
            vec![Arc::clone(&b_dead), Arc::clone(&b_alive)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(
            lease.backend_id, "alive",
            "unhealthy backend must be skipped regardless of free slots"
        );
    }

    /// NoBackend when no healthy backend serves the requested role.
    #[tokio::test]
    async fn no_backend_role_unknown() {
        let pool = Pool::new(vec![], Duration::from_secs(5));
        let err = pool.acquire(Role::Embed, false, 0).await.unwrap_err();
        match err {
            AcquireError::NoBackend { role } => assert_eq!(role, Role::Embed),
            other => panic!("expected NoBackend, got {other:?}"),
        }
    }

    /// NoBackend when every backend serving the role is unhealthy.
    #[tokio::test]
    async fn no_backend_all_unhealthy() {
        let b = tb("b1", "http://x:8001", vec![Role::Text], 0, 2);
        b.healthy.store(false, Ordering::Release);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        let err = pool.acquire(Role::Text, false, 0).await.unwrap_err();
        match err {
            AcquireError::NoBackend { role } => assert_eq!(role, Role::Text),
            other => panic!("expected NoBackend, got {other:?}"),
        }
    }

    /// Wait path: acquire blocks until a held slot is released, then succeeds.
    #[tokio::test]
    async fn wait_path_succeeds_when_slot_frees() {
        let b = tb("b1", "http://x:8001", vec![Role::Text], 0, 1);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Hold the only slot via a Lease (which notifies the wait queue on drop —
        // required for P9-T12 priority-aware waiting).
        let hold_lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(b.free(), 0);

        // Spawn a task that releases the slot after a short delay.
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(hold_lease);
        });

        // acquire should block briefly then succeed once the slot is freed.
        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "b1");

        release.await.unwrap();
        drop(lease);
        assert_eq!(b.free(), 1);
    }

    /// RAII: dropping the Lease returns the slot to the backend.
    #[tokio::test]
    async fn lease_drop_frees_slot_for_next_acquire() {
        let b = tb("b1", "http://x:8001", vec![Role::Text], 0, 1);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Acquire and immediately drop.
        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(b.free(), 0);
        drop(lease);
        assert_eq!(b.free(), 1);

        // A subsequent acquire must succeed (slot was returned).
        let lease2 = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease2.backend_id, "b1");
        assert_eq!(b.free(), 0);
    }

    /// Pool is `Clone`-able and clones share the same registries/timeout.
    #[tokio::test]
    async fn cloned_pool_shares_backend_state() {
        let b = tb("b1", "http://x:8001", vec![Role::Text], 0, 2);
        let pool1 = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let pool2 = pool1.clone();

        // Acquire through pool1 consumes a slot visible through pool2.
        let _l1 = pool1.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(b.free(), 1);

        // pool2 sees the same exhaustion.
        let _l2 = pool2.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(b.free(), 0);
    }

    // ── P9-T3: cooldown + capacity-is-usable filtering ─────────────────

    /// A backend in cooldown is skipped during acquire.
    #[tokio::test]
    async fn cooldown_backend_skipped() {
        let b_cooldown = tb("cooldown", "http://x:c", vec![Role::Text], 0, 4);
        // Set cooldown far in the future.
        b_cooldown.set_cooldown(std::time::Instant::now() + Duration::from_secs(300));

        let b_alive = tb("alive", "http://x:a", vec![Role::Text], 10, 2);
        let pool = Pool::new(
            vec![Arc::clone(&b_cooldown), Arc::clone(&b_alive)],
            Duration::from_secs(5),
        );

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(
            lease.backend_id, "alive",
            "cooldown backend must be skipped"
        );
    }

    /// A Rated backend with exhausted RPM is skipped (is_usable returns false).
    #[tokio::test]
    async fn rated_exhausted_rpm_skipped() {
        use crate::capacity::{Capacity, FakeClock};
        use std::sync::Arc as StdArc;
        use std::time::Instant;

        let clock: StdArc<dyn crate::capacity::Clock> = StdArc::new(FakeClock::new(Instant::now()));
        // 10 concurrency, 1 RPM, large TPM.
        let rated_cap = Capacity::new_rated(10, 1.0, 1000.0, clock);

        // Build a Rated backend manually.
        let b_rated = Arc::new(Backend {
            id: "rated".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: Some("http://x:rated".into()),
            key: None,
            model_id: "rated-model".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: rated_cap.clone(),
            max_concurrency: 10,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Local,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: None,
        });

        // Exhaust the single RPM token.
        let _hold = rated_cap.try_acquire().unwrap();
        assert!(!rated_cap.is_usable());

        let pool = Pool::new(vec![Arc::clone(&b_rated)], Duration::from_secs(5));
        let err = pool.acquire(Role::Text, false, 0).await.unwrap_err();
        // No usable backends → NoBackend.
        assert!(matches!(err, AcquireError::NoBackend { .. }));
    }

    // ── P9-T4: NoBackend when backend has no endpoint ──────────────────

    /// A backend without an endpoint is skipped during acquire (native SDK backends
    /// manage their own connection pools).
    #[tokio::test]
    async fn no_endpoint_skipped_in_acquire() {
        let b = Arc::new(Backend {
            id: "no-ep".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: None, /* no endpoint */
            key: None,
            model_id: "model".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: Capacity::new_slots(5),
            max_concurrency: 5,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Remote,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: None,
        });

        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let err = pool.acquire(Role::Text, false, 0).await.unwrap_err();
        assert!(
            matches!(err, AcquireError::NoBackend { .. }),
            "backend without endpoint must be skipped"
        );
    }

    // ── BUG-SCHED-03: routing-table resolution + per-role fallback ─────

    /// Build one DB-shaped routing entry: numeric model id (a Postgres PK),
    /// the way `PgStore::get_routing_table` returns them. Deliberately does
    /// NOT match any config backend id.
    fn db_entry(role: Role, model_id: i64, endpoint: &str) -> kb_core::routing::RoutingEntry {
        use kb_core::routing::{
            ModelRow, ProviderKind, ProviderRow, RoutingEntry, RoutingStrategy,
        };
        RoutingEntry {
            tenant_id: None,
            role,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 100,
            provider: ProviderRow {
                id: 1000 + model_id,
                name: format!("db-prov-{model_id}"),
                kind: ProviderKind::OpenAiCompat,
                endpoint: Some(endpoint.to_string()),
                headers: serde_json::Value::Object(Default::default()),
                enabled: true,
                api_key_enc: None,
            },
            model: ModelRow {
                id: model_id,
                provider_id: 1000 + model_id,
                model_id: format!("db-model-{model_id}"),
                caps: vec![role.as_str().to_string()],
                ctx_tokens: Some(8192),
                max_conc: Some(2),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: kb_core::data_class::DataClass::Local,
                enabled: true,
            },
        }
    }

    /// The original BUG-SCHED-03 shape: a pool keyed by config string ids and
    /// a routing table whose entries carry numeric DB model ids. The table is
    /// unresolvable (no materialization happened — e.g. a stale snapshot), so
    /// acquire must fall back to the legacy config backends instead of
    /// returning `NoBackend` for every role.
    #[tokio::test]
    async fn regression_config_keyed_pool_with_db_entries_still_acquires() {
        let b = tb("llama-text", "http://x:8001", vec![Role::Text], 0, 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        let table = kb_core::routing::RoutingTable::from_entries(vec![db_entry(
            Role::Text,
            12,
            "http://db:12",
        )]);
        pool.set_routing(Arc::new(table));

        let lease = pool
            .acquire(Role::Text, false, 0)
            .await
            .expect("unresolvable routing entries must fall back to legacy, not NoBackend");
        assert_eq!(lease.backend_id, "llama-text");
    }

    /// Real capacity pressure on resolvable tiered candidates must surface as
    /// `CapacityExhausted` — never silently served by the legacy pool.
    #[tokio::test]
    async fn capacity_exhausted_is_not_masked_by_fallback() {
        // A routed backend, registered under the key the tiered resolver uses.
        let routed = tb("routed", "http://db:12", vec![Role::Text], 0, 1);
        // A legacy config backend with free slots that must NOT be used.
        let legacy = tb("cfg-fallback", "http://x:1", vec![Role::Text], 0, 4);
        let pool = Pool::new(vec![Arc::clone(&legacy)], Duration::from_millis(200));
        pool.backends
            .insert(crate::materialize::db_backend_key(12), Arc::clone(&routed));

        let table = kb_core::routing::RoutingTable::from_entries(vec![db_entry(
            Role::Text,
            12,
            "http://db:12",
        )]);
        pool.set_routing(Arc::new(table));

        // Exhaust the routed backend's only slot.
        let _hold = routed.capacity.try_acquire().unwrap();

        let err = pool.acquire(Role::Text, false, 0).await.unwrap_err();
        assert!(
            matches!(err, AcquireError::CapacityExhausted { .. }),
            "busy resolvable tier must stay CapacityExhausted, got {err:?}"
        );
        assert_eq!(legacy.free(), 4, "legacy pool must not have been touched");
    }

    /// The fallback warning fires immediately, then is suppressed within the
    /// rate-limit window (per role).
    #[test]
    fn should_warn_fallback_is_rate_limited() {
        let pool = Pool::new(vec![], Duration::from_secs(1));
        assert!(pool.should_warn_fallback(Role::Text), "first call warns");
        assert!(
            !pool.should_warn_fallback(Role::Text),
            "second call within the window is suppressed"
        );
        // A different role has its own window.
        assert!(pool.should_warn_fallback(Role::Embed));
    }

    /// The ledger acceptance: creating ONE real DB route (here: Text) must not
    /// break every other role's acquire — roles with no tiers in the table
    /// fall back to the legacy flat-priority pool.
    #[tokio::test]
    async fn one_route_for_one_role_does_not_break_other_roles() {
        let b_text = tb("cfg-text", "http://x:1", vec![Role::Text], 0, 2);
        let b_embed = tb("cfg-embed", "http://x:2", vec![Role::Embed], 0, 2);
        let pool = Pool::new(
            vec![Arc::clone(&b_text), Arc::clone(&b_embed)],
            Duration::from_secs(5),
        );

        let table = kb_core::routing::RoutingTable::from_entries(vec![db_entry(
            Role::Text,
            7,
            "http://db:7",
        )]);
        pool.set_routing(Arc::new(table));

        // Embed has no routing tiers at all → must fall back to legacy.
        let lease = pool
            .acquire(Role::Embed, false, 0)
            .await
            .expect("a role absent from the routing table must use the legacy pool");
        assert_eq!(lease.backend_id, "cfg-embed");
    }

    // ── P1-T3: mock-backend harness smoke test ────────────────────────

    /// Prove the mock-backend harness works end-to-end with the scheduler:
    /// start a mock, register it as a backend, acquire a slot, drop it.
    #[tokio::test]
    async fn acquire_from_mock_backend() {
        let mock = kb_mock_backend::MockBackend::start().await;
        let base_url = format!("http://{}/v1", mock.addr());

        let b = tb(
            "mock-1",
            &base_url,
            vec![Role::Text],
            0, /* priority */
            2, /* slots */
        );
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Fast-path: mock is healthy (default), slots are free.
        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "mock-1");
        assert!(lease.endpoint.starts_with("http://127.0.0.1:"));
        assert!(lease.endpoint.ends_with("/v1"));
        assert_eq!(b.free(), 1);

        drop(lease);
        assert_eq!(b.free(), 2);

        // Prove the mock is truly alive: issue an actual health request.
        let health_url = mock.url("/health");
        let status = reqwest::get(&health_url).await.unwrap().status();
        assert_eq!(status, 200);

        mock.shutdown().await;
    }

    // ── role_context_tokens ──────────────────────────────────────────

    #[test]
    fn role_context_tokens_returns_none_when_no_backends() {
        let pool = Pool::new(vec![], Duration::from_secs(5));
        assert_eq!(pool.role_context_tokens(Role::Text), None);
    }

    #[test]
    fn role_context_tokens_returns_none_when_all_unknown() {
        let b = tb("b1", "http://x", vec![Role::Text], 0, 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        assert_eq!(pool.role_context_tokens(Role::Text), None);
    }

    #[test]
    fn role_context_tokens_returns_min_of_healthy() {
        // Build backends with different ctx_tokens via direct construction.
        let b1 = Arc::new(Backend {
            id: "b1".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: Some("http://x:1".into()),
            key: None,
            model_id: "m1".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: Capacity::new_slots(2),
            max_concurrency: 2,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Local,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: Some(65536),
        });
        let b2 = Arc::new(Backend {
            id: "b2".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: Some("http://x:2".into()),
            key: None,
            model_id: "m2".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: Capacity::new_slots(2),
            max_concurrency: 2,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Local,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: Some(131072),
        });
        let pool = Pool::new(
            vec![Arc::clone(&b1), Arc::clone(&b2)],
            Duration::from_secs(5),
        );
        assert_eq!(pool.role_context_tokens(Role::Text), Some(65536));
    }

    #[test]
    fn role_context_tokens_skips_unhealthy() {
        let b1 = Arc::new(Backend {
            id: "b1".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: Some("http://x:1".into()),
            key: None,
            model_id: "m1".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: Capacity::new_slots(2),
            max_concurrency: 2,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Local,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: Some(65536),
        });
        let b2 = Arc::new(Backend {
            id: "b2".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: Some("http://x:2".into()),
            key: None,
            model_id: "m2".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: Capacity::new_slots(2),
            max_concurrency: 2,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Local,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: Some(131072),
        });
        let pool = Pool::new(
            vec![Arc::clone(&b1), Arc::clone(&b2)],
            Duration::from_secs(5),
        );
        // b1 is unhealthy, so only b2's ctx_tokens count.
        assert_eq!(pool.role_context_tokens(Role::Text), Some(131072));
    }

    #[test]
    fn role_context_tokens_some_backends_unknown() {
        // One backend with known ctx_tokens, one with None — the None
        // backend is skipped; the known value is returned.
        let b_known = Arc::new(Backend {
            id: "known".into(),
            adapter: Arc::new(kb_core::provider::NoopAdapter),
            endpoint: Some("http://x:1".into()),
            key: None,
            model_id: "mk".into(),
            caps: kb_core::capability::CapSet::from(Role::Text),
            capacity: Capacity::new_slots(2),
            max_concurrency: 2,
            pricing: None,
            data_class: kb_core::data_class::DataClass::Local,
            priority: 0,
            healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            time_window: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            bandwidth_limiter: Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(None))),
            ctx_tokens: Some(128000),
        });
        let pool = Pool::new(
            vec![
                Arc::clone(&b_known),
                tb("unk", "http://u", vec![Role::Text], 0, 2),
            ],
            Duration::from_secs(5),
        );
        assert_eq!(pool.role_context_tokens(Role::Text), Some(128000));
    }
}
