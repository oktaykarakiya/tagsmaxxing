//! Tiered failover routing algorithm (plan §26.4).
//!
//! Implements the per-tier spill-and-fallback acquire strategy that replaces
//! the flat priority-based candidate list. When a [`crate::Pool`] has an active
//! [`RoutingTable`](kb_core::routing::RoutingTable), [`Pool::acquire`](crate::Pool::acquire)
//! delegates to the functions in this module instead of the legacy flat algorithm.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use kb_core::pricing::Pricing;
use kb_core::role::Role;
use kb_core::routing::{RoutingEntry, RoutingStrategy, RoutingTable};

use crate::backend::Backend;
use crate::error::AcquireError;
use crate::lease::Lease;
use crate::priority_wait::PriorityWaiterQueue;

/// The per-(role, tier) round-robin counter key.
type RrKey = (u8, i32);

/// Execute the tiered acquire algorithm for the given role against a routing table.
///
/// # Algorithm (plan §26.4)
///
/// 1. Look up tiers for `(tenant_id, role)` from the routing table.
/// 2. For each tier in order:
///    a. Resolve routing entries to live backends (via `backends` map).
///    b. Filter: healthy, not in cooldown, capacity usable, has endpoint.
///    c. Order by the tier's strategy ([`RoutingStrategy`]).
///    d. Fast path: [`super::capacity::Capacity::try_acquire`] on each candidate.
///    e. Wait path: await all candidates up to `spill_ms` → first free wins.
///    f. On timeout: spill to the next tier.
/// 3. [`Role::Embed`] role: only tier 0 is used (plan §11 lock — no failover
///    across different embedding models).
/// 4. All tiers exhausted without success: wait on the union of all candidates
///    up to the global `timeout`, then return [`AcquireError::CapacityExhausted`].
///
/// `priority` (P9-T12) is the caller's priority (higher = more urgent) for the
/// priority-ordered waiter queue.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn acquire_tiered(
    role: Role,
    priority: i32,
    tenant_id: Option<i64>,
    table: &RoutingTable,
    backends: &DashMap<String, Arc<Backend>>,
    global_timeout: Duration,
    rr_counters: &DashMap<RrKey, AtomicUsize>,
    local_only: bool,
    wait_queue: &Arc<PriorityWaiterQueue>,
) -> Result<Lease, AcquireError> {
    let tiers = table.tiers_for(tenant_id, role);

    // Embed lock (plan §11): only tier 0 is eligible — no cross-model spill.
    let effective_tiers = if role == Role::Embed {
        if tiers.is_empty() { &[] } else { &tiers[..1] }
    } else {
        tiers
    };

    if effective_tiers.is_empty() {
        return Err(AcquireError::NoBackend { role });
    }

    // Collect all eligible candidates across every tier for the union wait
    // (the last-resort, global-timeout-bounded wait in §26.4 step 4).
    let mut union_candidates: Vec<Arc<Backend>> = Vec::new();

    for (tier_index, tier_entries) in effective_tiers.iter().enumerate() {
        if tier_entries.is_empty() {
            continue;
        }

        // Strategy: every entry in a tier shares the same strategy
        // (enforced by the DB schema). Use the first entry's strategy.
        let strategy = tier_entries[0].strategy;
        let spill_ms = tier_entries[0].spill_ms;
        let spill_after = Duration::from_millis(spill_ms as u64);

        // Resolve routing entries → live backends, with filtering.
        let mut candidates: Vec<Arc<Backend>> =
            resolve_and_filter(tier_entries, backends, local_only);

        if candidates.is_empty() {
            continue;
        }

        // Order by the tier's strategy.
        order_by_strategy(
            &mut candidates,
            strategy,
            role,
            tier_index as i32,
            rr_counters,
        );

        // Fast path: try_acquire on each candidate in order.
        // P9-T12: wrap with wait-queue notification so waiters are woken on drop.
        if let Some(lease) = fast_path_try_acquire(&candidates) {
            return Ok(lease.with_notify(role, Arc::clone(wait_queue)));
        }

        // Wait path: await any free capacity in this tier, bounded by spill_after.
        if let Some(lease) = wait_on_tier(&candidates, spill_after).await {
            return Ok(lease.with_notify(role, Arc::clone(wait_queue)));
        }

        // Spill to next tier — collect candidates for the union wait.
        union_candidates.extend(candidates);
    }

    if union_candidates.is_empty() {
        return Err(AcquireError::NoBackend { role });
    }

    // Step 4 (§26.4): all tiers exhausted → wait on the union up to global timeout,
    // using the priority-ordered waiter queue (P9-T12).
    wait_on_union(
        &union_candidates,
        global_timeout,
        role,
        effective_tiers.len(),
        priority,
        wait_queue,
    )
    .await
}

/// Resolve routing entries to live backends, filtering out ineligible ones.
///
/// When `local_only` is `true`, backends with [`DataClass::Remote`] are
/// excluded regardless of health or capacity (plan §26.6, P9-T9).
fn resolve_and_filter(
    entries: &[RoutingEntry],
    backends: &DashMap<String, Arc<Backend>>,
    local_only: bool,
) -> Vec<Arc<Backend>> {
    let mut candidates = Vec::with_capacity(entries.len());
    for entry in entries {
        let backend_id = entry.model.id.to_string();
        if let Some(entry_ref) = backends.get(&backend_id) {
            let b = entry_ref.value().clone();
            if b.healthy.load(Ordering::Acquire)
                && !b.cooldown_active()
                && b.capacity.is_usable()
                && b.endpoint.is_some()
                && !(local_only && b.data_class == kb_core::data_class::DataClass::Remote)
            {
                candidates.push(b);
            }
        }
    }
    candidates
}

/// Order candidates by the routing strategy (plan §26.4).
fn order_by_strategy(
    candidates: &mut [Arc<Backend>],
    strategy: RoutingStrategy,
    role: Role,
    tier_index: i32,
    rr_counters: &DashMap<RrKey, AtomicUsize>,
) {
    match strategy {
        RoutingStrategy::LeastLoaded => {
            // Most free capacity first.
            candidates.sort_by_key(|b| std::cmp::Reverse(b.free()));
        }
        RoutingStrategy::RoundRobin => {
            // Rotate candidates so the next-in-line is at position 0.
            if candidates.len() > 1 {
                let key = (role_to_rr_key(role), tier_index);
                let counter = rr_counters
                    .entry(key)
                    .or_insert_with(|| AtomicUsize::new(0));
                let idx = counter.fetch_add(1, Ordering::Relaxed) % candidates.len();
                candidates.rotate_left(idx);
            }
        }
        RoutingStrategy::CostAsc => {
            // Cheapest first (free = cheapest). Sort by combined input+output price.
            candidates.sort_by_key(|b| {
                b.pricing.map_or(0u64, |p| {
                    // Convert f64 prices to integer micro-dollars for ordering.
                    // We multiply by 1e6 to preserve fractional ordering.
                    ordered_cost(p)
                })
            });
        }
        RoutingStrategy::Priority => {
            // Natural order — the admin-specified sequence in the tier.
            // The candidates vector already reflects the DB row order
            // (entries are iterated in the tier's natural order).
            // No sort needed.
        }
    }
}

/// Convert pricing to a sortable integer (cheapest = lowest).
///
/// Multiplies the combined input+output per-Mtok price by 1e6 to produce
/// an integer suitable for comparison, avoiding floating-point issues
/// in sort keys.
fn ordered_cost(p: Pricing) -> u64 {
    ((p.input_per_mtok + p.output_per_mtok) * 1_000_000.0) as u64
}

/// Map a role to its compact round-robin key (the raw bit value).
///
/// This is smaller and cheaper to hash than the full enum discriminant.
fn role_to_rr_key(role: Role) -> u8 {
    match role {
        Role::Text => 0b00001,
        Role::Vision => 0b00010,
        Role::Code => 0b00100,
        Role::Embed => 0b01000,
        Role::Rerank => 0b10000,
    }
}

/// Fast path: try `try_acquire` on each candidate, returning the first success.
fn fast_path_try_acquire(candidates: &[Arc<Backend>]) -> Option<Lease> {
    for b in candidates {
        if let Some(guard) = b.capacity.try_acquire() {
            let endpoint = b.endpoint.clone().unwrap_or_default();
            return Some(Lease::new(b.id.clone(), endpoint, guard));
        }
    }
    None
}

/// Wait path for a single tier: await any free capacity, bounded by `spill_after`.
async fn wait_on_tier(candidates: &[Arc<Backend>], spill_after: Duration) -> Option<Lease> {
    let mut waiters = FuturesUnordered::new();
    for b in candidates {
        let capacity = b.capacity.clone();
        let id = b.id.clone();
        let endpoint = b.endpoint.clone().unwrap_or_default();
        waiters.push(async move { capacity.acquire().await.map(|guard| (id, endpoint, guard)) });
    }

    match tokio::time::timeout(spill_after, waiters.next()).await {
        Ok(Some(Some((id, endpoint, guard)))) => Some(Lease::new(id, endpoint, guard)),
        Ok(None) | Ok(Some(None)) => {
            // All semaphores closed — no capacity available in this tier.
            None
        }
        Err(_elapsed) => {
            // Spill timeout expired.
            None
        }
    }
}

/// Final union wait (plan §26.4 step 4): all tiers exhausted, wait on ALL
/// candidates up to the global timeout, with priority-ordered waiter wake
/// (P9-T12).
async fn wait_on_union(
    candidates: &[Arc<Backend>],
    global_timeout: Duration,
    role: Role,
    tiers_attempted: usize,
    priority: i32,
    wait_queue: &Arc<PriorityWaiterQueue>,
) -> Result<Lease, AcquireError> {
    // Register in the priority wait queue.
    let mut ticket = wait_queue.register(role, priority).await;

    loop {
        tokio::select! {
            _ = ticket.wait() => {
                // Woken — a slot might be free. Try fast-path on any candidate.
                if let Some(lease) =
                    super::pool::Pool::try_fast_path(candidates, role)
                {
                    // Attach wait-queue notification so the next waiter is woken
                    // when this lease is dropped.
                    return Ok(lease.with_notify(role, Arc::clone(wait_queue)));
                }
                // Slot was taken — re-register.
                ticket = wait_queue.register(role, priority).await;
            }
            _ = tokio::time::sleep(global_timeout) => {
                return Err(AcquireError::CapacityExhausted {
                    role,
                    tiers_attempted,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::capacity::Capacity;
    use crate::pool::Pool;
    use kb_core::capability::CapSet;
    use kb_core::data_class::DataClass;
    use kb_core::provider::NoopAdapter;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Create a test backend with the given id and capacity.
    fn tb(id: &str, slots: usize, priority: u8) -> Arc<Backend> {
        Arc::new(Backend::new(
            id,
            Arc::new(NoopAdapter),
            format!("http://x:{id}"),
            None,
            format!("model-{id}"),
            CapSet::from(Role::Text),
            Capacity::new_slots(slots),
            slots,
            None, /* pricing */
            DataClass::Local,
            priority,
        ))
    }

    /// Create a test backend with pricing.
    fn tb_priced(id: &str, slots: usize, input_price: f64, output_price: f64) -> Arc<Backend> {
        Arc::new(Backend::new(
            id,
            Arc::new(NoopAdapter),
            format!("http://x:{id}"),
            None,
            format!("model-{id}"),
            CapSet::from(Role::Text),
            Capacity::new_slots(slots),
            slots,
            Some(Pricing::new(input_price, output_price)),
            DataClass::Remote,
            0,
        ))
    }

    /// Build a minimal RoutingTable from backends: all backends in tier 0, LeastLoaded.
    fn simple_routing(
        tenant_id: Option<i64>,
        role: Role,
        tier: i32,
        strategy: RoutingStrategy,
        spill_ms: i32,
        backends: &[Arc<Backend>],
    ) -> (Arc<RoutingTable>, DashMap<String, Arc<Backend>>) {
        use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};

        let entries: Vec<RoutingEntry> = backends
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let model_id_num: i64 = b.id.parse().unwrap_or(i as i64);
                RoutingEntry {
                    tenant_id,
                    role,
                    tier,
                    strategy,
                    spill_ms,
                    provider: ProviderRow {
                        id: 100 + model_id_num,
                        name: format!("provider-{model_id_num}"),
                        kind: kb_core::routing::ProviderKind::Local,
                        endpoint: Some(format!("http://x:{model_id_num}")),
                        headers: Default::default(),
                        enabled: true,
                    },
                    model: ModelRow {
                        id: model_id_num,
                        provider_id: 100 + model_id_num,
                        model_id: format!("model-{model_id_num}"),
                        caps: vec!["text".into()],
                        ctx_tokens: Some(32768),
                        max_conc: Some(b.max_concurrency as i32),
                        rpm: None,
                        tpm: None,
                        price_in: b.pricing.map(|p| p.input_per_mtok),
                        price_out: b.pricing.map(|p| p.output_per_mtok),
                        data_class: b.data_class,
                        enabled: true,
                    },
                }
            })
            .collect();

        let table = Arc::new(RoutingTable::from_entries(entries));
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        for b in backends {
            map.insert(b.id.clone(), Arc::clone(b));
        }
        (table, map)
    }

    /// Build a two-tier routing table.
    fn two_tier_routing(
        tier0_backends: &[Arc<Backend>],
        tier1_backends: &[Arc<Backend>],
    ) -> (Arc<RoutingTable>, DashMap<String, Arc<Backend>>) {
        use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};

        let mut entries = Vec::new();

        for b in tier0_backends {
            let mid: i64 = b.id.parse().expect("backend id must be numeric");
            entries.push(RoutingEntry {
                tenant_id: None,
                role: Role::Text,
                tier: 0,
                strategy: RoutingStrategy::LeastLoaded,
                spill_ms: 100,
                provider: ProviderRow {
                    id: 100 + mid,
                    name: format!("p-{mid}"),
                    kind: kb_core::routing::ProviderKind::Local,
                    endpoint: Some(format!("http://x:{mid}")),
                    headers: Default::default(),
                    enabled: true,
                },
                model: ModelRow {
                    id: mid,
                    provider_id: 100 + mid,
                    model_id: format!("model-{mid}"),
                    caps: vec!["text".into()],
                    ctx_tokens: Some(32768),
                    max_conc: Some(b.max_concurrency as i32),
                    rpm: None,
                    tpm: None,
                    price_in: None,
                    price_out: None,
                    data_class: DataClass::Local,
                    enabled: true,
                },
            });
        }
        for b in tier1_backends {
            let mid: i64 = b.id.parse().expect("backend id must be numeric");
            entries.push(RoutingEntry {
                tenant_id: None,
                role: Role::Text,
                tier: 1,
                strategy: RoutingStrategy::LeastLoaded,
                spill_ms: 100,
                provider: ProviderRow {
                    id: 100 + mid,
                    name: format!("p-{mid}"),
                    kind: kb_core::routing::ProviderKind::Local,
                    endpoint: Some(format!("http://x:{mid}")),
                    headers: Default::default(),
                    enabled: true,
                },
                model: ModelRow {
                    id: mid,
                    provider_id: 100 + mid,
                    model_id: format!("model-{mid}"),
                    caps: vec!["text".into()],
                    ctx_tokens: Some(32768),
                    max_conc: Some(b.max_concurrency as i32),
                    rpm: None,
                    tpm: None,
                    price_in: None,
                    price_out: None,
                    data_class: DataClass::Local,
                    enabled: true,
                },
            });
        }

        let table = Arc::new(RoutingTable::from_entries(entries));
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        for b in tier0_backends.iter().chain(tier1_backends.iter()) {
            map.insert(b.id.clone(), Arc::clone(b));
        }
        (table, map)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Create a fresh test wait queue (P9-T12).
    fn test_wq() -> Arc<PriorityWaiterQueue> {
        Arc::new(PriorityWaiterQueue::default())
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// Tier 0 candidate available → acquired immediately.
    #[tokio::test]
    async fn tier0_candidate_available_acquired() {
        let b0 = tb("1", 2, 0);
        let b0_id = b0.id.clone();
        let (table, backends) = simple_routing(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            500,
            &[Arc::clone(&b0)],
        );
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        assert_eq!(lease.backend_id, b0_id);
        assert_eq!(b0.free(), 1);
        drop(lease);
        assert_eq!(b0.free(), 2);
    }

    /// Tier 0 all busy → waits spill_after → spills to tier 1.
    #[tokio::test]
    async fn tier0_busy_spills_to_tier1() {
        let b0 = tb("1", 1, 0); // 1 slot
        let b1 = tb("2", 2, 0); // 2 slots

        // Exhaust tier 0's only slot.
        let _hold = b0.capacity.try_acquire().unwrap();

        let (table, backends) = two_tier_routing(&[Arc::clone(&b0)], &[Arc::clone(&b1)]);
        let rr = DashMap::new();

        // tier 0's spill_after is 100ms → after that, spills to tier 1.
        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        assert_eq!(
            lease.backend_id, "2",
            "should spill to tier 1 when tier 0 is exhausted"
        );
    }

    /// Embed role → only tier 0 used (tier 1+ ignored, plan §11 lock).
    #[tokio::test]
    async fn embed_role_only_tier0() {
        let b_embed0 = Arc::new(Backend::new(
            "3",
            Arc::new(NoopAdapter),
            "http://x:3",
            None,
            "model-3",
            CapSet::from(Role::Embed),
            Capacity::new_slots(1),
            1,
            None,
            DataClass::Local,
            0,
        ));
        let b_embed1 = Arc::new(Backend::new(
            "4",
            Arc::new(NoopAdapter),
            "http://x:4",
            None,
            "model-4",
            CapSet::from(Role::Embed),
            Capacity::new_slots(2),
            2,
            None,
            DataClass::Local,
            0,
        ));

        // Exhaust tier 0.
        let _hold = b_embed0.capacity.try_acquire().unwrap();

        // Build routing: tier 0 = b_embed0, tier 1 = b_embed1.
        let (table, backends) = {
            use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};
            let entries = vec![
                RoutingEntry {
                    tenant_id: None,
                    role: Role::Embed,
                    tier: 0,
                    strategy: RoutingStrategy::LeastLoaded,
                    spill_ms: 100,
                    provider: ProviderRow {
                        id: 103,
                        name: "p-3".into(),
                        kind: kb_core::routing::ProviderKind::Local,
                        endpoint: Some("http://x:3".into()),
                        headers: Default::default(),
                        enabled: true,
                    },
                    model: ModelRow {
                        id: 3,
                        provider_id: 103,
                        model_id: "model-3".into(),
                        caps: vec!["embed".into()],
                        ctx_tokens: None,
                        max_conc: Some(1),
                        rpm: None,
                        tpm: None,
                        price_in: None,
                        price_out: None,
                        data_class: DataClass::Local,
                        enabled: true,
                    },
                },
                RoutingEntry {
                    tenant_id: None,
                    role: Role::Embed,
                    tier: 1,
                    strategy: RoutingStrategy::LeastLoaded,
                    spill_ms: 100,
                    provider: ProviderRow {
                        id: 104,
                        name: "p-4".into(),
                        kind: kb_core::routing::ProviderKind::Local,
                        endpoint: Some("http://x:4".into()),
                        headers: Default::default(),
                        enabled: true,
                    },
                    model: ModelRow {
                        id: 4,
                        provider_id: 104,
                        model_id: "model-4".into(),
                        caps: vec!["embed".into()],
                        ctx_tokens: None,
                        max_conc: Some(2),
                        rpm: None,
                        tpm: None,
                        price_in: None,
                        price_out: None,
                        data_class: DataClass::Local,
                        enabled: true,
                    },
                },
            ];
            let table = Arc::new(RoutingTable::from_entries(entries));
            let map: DashMap<String, Arc<Backend>> = DashMap::new();
            map.insert("3".into(), Arc::clone(&b_embed0));
            map.insert("4".into(), Arc::clone(&b_embed1));
            (table, map)
        };
        let rr = DashMap::new();

        let err = acquire_tiered(
            Role::Embed,
            0,
            None,
            &table,
            &backends,
            Duration::from_millis(200), // short global timeout
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap_err();

        // Should be CapacityExhausted (tier 0 exhausted, no spill to tier 1 for embed).
        assert!(
            matches!(
                err,
                AcquireError::CapacityExhausted {
                    role: Role::Embed,
                    ..
                }
            ),
            "embed must not spill to tier 1, got {err:?}"
        );
    }

    /// CostAsc prefers cheapest backend.
    #[tokio::test]
    async fn cost_asc_prefers_cheapest() {
        let b_expensive = tb_priced("5", 2, 10.0, 20.0); // $30/Mtok combined
        let b_cheap = tb_priced("6", 2, 1.0, 2.0); // $3/Mtok combined
        let b_free = tb("7", 2, 0); // free

        let (table, backends) = {
            use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};
            let all = [
                Arc::clone(&b_expensive),
                Arc::clone(&b_cheap),
                Arc::clone(&b_free),
            ];
            let entries: Vec<RoutingEntry> = all
                .iter()
                .map(|b| {
                    let mid: i64 = b.id.parse().unwrap();
                    RoutingEntry {
                        tenant_id: None,
                        role: Role::Text,
                        tier: 0,
                        strategy: RoutingStrategy::CostAsc,
                        spill_ms: 500,
                        provider: ProviderRow {
                            id: 100 + mid,
                            name: format!("p-{mid}"),
                            kind: kb_core::routing::ProviderKind::Local,
                            endpoint: Some(format!("http://x:{mid}")),
                            headers: Default::default(),
                            enabled: true,
                        },
                        model: ModelRow {
                            id: mid,
                            provider_id: 100 + mid,
                            model_id: format!("model-{mid}"),
                            caps: vec!["text".into()],
                            ctx_tokens: Some(32768),
                            max_conc: Some(b.max_concurrency as i32),
                            rpm: None,
                            tpm: None,
                            price_in: b.pricing.map(|p| p.input_per_mtok),
                            price_out: b.pricing.map(|p| p.output_per_mtok),
                            data_class: b.data_class,
                            enabled: true,
                        },
                    }
                })
                .collect();
            let table = Arc::new(RoutingTable::from_entries(entries));
            let map: DashMap<String, Arc<Backend>> = DashMap::new();
            for b in &all {
                map.insert(b.id.clone(), Arc::clone(b));
            }
            (table, map)
        };
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        // Free backend (id "7") should be picked first under CostAsc.
        assert_eq!(
            lease.backend_id, "7",
            "CostAsc must prefer free/cheapest backend"
        );
    }

    /// 429 cooldown: backend in cooldown is skipped during acquire.
    #[tokio::test]
    async fn cooldown_backend_skipped_in_tiered() {
        let b_cooldown = tb("8", 4, 0);
        let b_alive = tb("9", 2, 10);

        // Put b_cooldown into cooldown for 5 minutes.
        b_cooldown.set_cooldown(std::time::Instant::now() + Duration::from_secs(300));

        let (table, backends) = simple_routing(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            500,
            &[Arc::clone(&b_cooldown), Arc::clone(&b_alive)],
        );
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        assert_eq!(
            lease.backend_id, "9",
            "cooldown backend must be skipped in tiered acquire"
        );
    }

    /// Unhealthy backend is skipped (5xx marks suspect pattern).
    #[tokio::test]
    async fn unhealthy_backend_skipped_in_tiered() {
        let b_unhealthy = tb("10", 4, 0);
        b_unhealthy.healthy.store(false, Ordering::Release);

        let b_healthy = tb("11", 2, 10);
        let (table, backends) = simple_routing(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            500,
            &[Arc::clone(&b_unhealthy), Arc::clone(&b_healthy)],
        );
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        assert_eq!(lease.backend_id, "11");
    }

    /// Priority strategy: explicit tier-entry order determines selection.
    #[tokio::test]
    async fn priority_strategy_respects_explicit_order() {
        let b_first = tb("12", 2, 0);
        let b_second = tb("13", 2, 0); // same capacity, later in tier → picked second

        let (table, backends) = {
            use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};
            let all = [Arc::clone(&b_first), Arc::clone(&b_second)];
            let entries: Vec<RoutingEntry> = all
                .iter()
                .map(|b| {
                    let mid: i64 = b.id.parse().unwrap();
                    RoutingEntry {
                        tenant_id: None,
                        role: Role::Text,
                        tier: 0,
                        strategy: RoutingStrategy::Priority,
                        spill_ms: 500,
                        provider: ProviderRow {
                            id: 100 + mid,
                            name: format!("p-{mid}"),
                            kind: kb_core::routing::ProviderKind::Local,
                            endpoint: Some(format!("http://x:{mid}")),
                            headers: Default::default(),
                            enabled: true,
                        },
                        model: ModelRow {
                            id: mid,
                            provider_id: 100 + mid,
                            model_id: format!("model-{mid}"),
                            caps: vec!["text".into()],
                            ctx_tokens: Some(32768),
                            max_conc: Some(b.max_concurrency as i32),
                            rpm: None,
                            tpm: None,
                            price_in: None,
                            price_out: None,
                            data_class: DataClass::Local,
                            enabled: true,
                        },
                    }
                })
                .collect();
            let table = Arc::new(RoutingTable::from_entries(entries));
            let map: DashMap<String, Arc<Backend>> = DashMap::new();
            for b in &all {
                map.insert(b.id.clone(), Arc::clone(b));
            }
            (table, map)
        };
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        // Under Priority strategy, "12" is first in the tier, so it should be picked.
        assert_eq!(
            lease.backend_id, "12",
            "Priority strategy must respect explicit tier order"
        );
    }

    /// LeastLoaded: backend with most free capacity is preferred.
    #[tokio::test]
    async fn least_loaded_prefers_most_free() {
        let b_fewer = tb("14", 1, 0); // 1 free slot
        let b_more = tb("15", 5, 0); // 5 free slots

        let (table, backends) = {
            let all = [Arc::clone(&b_fewer), Arc::clone(&b_more)];
            // Put both in the same tier with LeastLoaded strategy, but in
            // reverse order (fewer first) to prove strategy overrides entry order.
            use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};
            let entries: Vec<RoutingEntry> = all
                .iter()
                .map(|b| {
                    let mid: i64 = b.id.parse().unwrap();
                    RoutingEntry {
                        tenant_id: None,
                        role: Role::Text,
                        tier: 0,
                        strategy: RoutingStrategy::LeastLoaded,
                        spill_ms: 500,
                        provider: ProviderRow {
                            id: 100 + mid,
                            name: format!("p-{mid}"),
                            kind: kb_core::routing::ProviderKind::Local,
                            endpoint: Some(format!("http://x:{mid}")),
                            headers: Default::default(),
                            enabled: true,
                        },
                        model: ModelRow {
                            id: mid,
                            provider_id: 100 + mid,
                            model_id: format!("model-{mid}"),
                            caps: vec!["text".into()],
                            ctx_tokens: Some(32768),
                            max_conc: Some(b.max_concurrency as i32),
                            rpm: None,
                            tpm: None,
                            price_in: None,
                            price_out: None,
                            data_class: DataClass::Local,
                            enabled: true,
                        },
                    }
                })
                .collect();
            let table = Arc::new(RoutingTable::from_entries(entries));
            let map: DashMap<String, Arc<Backend>> = DashMap::new();
            for b in &all {
                map.insert(b.id.clone(), Arc::clone(b));
            }
            (table, map)
        };
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        assert_eq!(
            lease.backend_id, "15",
            "LeastLoaded must prefer backend with more free slots"
        );
    }

    /// All tiers exhausted → CapacityExhausted on global timeout.
    #[tokio::test]
    async fn all_tiers_exhausted_capacity_exhausted() {
        let b0 = tb("16", 1, 0);
        let b1 = tb("17", 1, 0);

        // Exhaust both backends' only slots.
        let _hold0 = b0.capacity.try_acquire().unwrap();
        let _hold1 = b1.capacity.try_acquire().unwrap();

        let (table, backends) = two_tier_routing(&[Arc::clone(&b0)], &[Arc::clone(&b1)]);
        let rr = DashMap::new();

        // Short spill_after (100ms each tier) + short global timeout.
        let err = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_millis(200),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                AcquireError::CapacityExhausted {
                    role: Role::Text,
                    ..
                }
            ),
            "all tiers exhausted must return CapacityExhausted, got {err:?}"
        );
    }

    /// No routing entries for the role → NoBackend.
    #[tokio::test]
    async fn empty_routing_returns_no_backend() {
        let table = RoutingTable::default();
        let backends: DashMap<String, Arc<Backend>> = DashMap::new();
        let rr = DashMap::new();

        let err = acquire_tiered(
            Role::Rerank,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            AcquireError::NoBackend { role: Role::Rerank }
        ));
    }

    /// RoundRobin distributes across backends in the tier.
    #[tokio::test]
    async fn round_robin_distributes() {
        let b_a = tb("18", 2, 0);
        let b_b = tb("19", 2, 0);

        let (table, backends) = {
            use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};
            let all = [Arc::clone(&b_a), Arc::clone(&b_b)];
            let entries: Vec<RoutingEntry> = all
                .iter()
                .map(|b| {
                    let mid: i64 = b.id.parse().unwrap();
                    RoutingEntry {
                        tenant_id: None,
                        role: Role::Text,
                        tier: 0,
                        strategy: RoutingStrategy::RoundRobin,
                        spill_ms: 500,
                        provider: ProviderRow {
                            id: 100 + mid,
                            name: format!("p-{mid}"),
                            kind: kb_core::routing::ProviderKind::Local,
                            endpoint: Some(format!("http://x:{mid}")),
                            headers: Default::default(),
                            enabled: true,
                        },
                        model: ModelRow {
                            id: mid,
                            provider_id: 100 + mid,
                            model_id: format!("model-{mid}"),
                            caps: vec!["text".into()],
                            ctx_tokens: Some(32768),
                            max_conc: Some(b.max_concurrency as i32),
                            rpm: None,
                            tpm: None,
                            price_in: None,
                            price_out: None,
                            data_class: DataClass::Local,
                            enabled: true,
                        },
                    }
                })
                .collect();
            let table = Arc::new(RoutingTable::from_entries(entries));
            let map: DashMap<String, Arc<Backend>> = DashMap::new();
            for b in &all {
                map.insert(b.id.clone(), Arc::clone(b));
            }
            (table, map)
        };
        let rr: DashMap<RrKey, AtomicUsize> = DashMap::new();

        // Acquire several times; both backends should be used.
        let mut hits_a = 0usize;
        let mut hits_b = 0usize;
        for _ in 0..4 {
            let lease = acquire_tiered(
                Role::Text,
                0,
                None,
                &table,
                &backends,
                Duration::from_secs(5),
                &rr,
                false,
                &test_wq(),
            )
            .await
            .unwrap();
            match lease.backend_id.as_str() {
                "18" => hits_a += 1,
                "19" => hits_b += 1,
                other => panic!("unexpected backend {other}"),
            }
            drop(lease); // return slots for next iteration
        }

        assert!(
            hits_a >= 1,
            "RR must use backend 18 at least once, got {hits_a}"
        );
        assert!(
            hits_b >= 1,
            "RR must use backend 19 at least once, got {hits_b}"
        );
    }

    /// Backend without endpoint is skipped in tiered routing.
    #[tokio::test]
    async fn no_endpoint_skipped_in_tiered() {
        let b_no_ep = Arc::new(Backend {
            id: "20".into(),
            adapter: Arc::new(NoopAdapter),
            endpoint: None,
            key: None,
            model_id: "no-ep-model".into(),
            caps: CapSet::from(Role::Text),
            capacity: Capacity::new_slots(5),
            max_concurrency: 5,
            pricing: None,
            data_class: DataClass::Local,
            priority: 0,
            healthy: Arc::new(AtomicBool::new(true)),
            cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        });
        let b_ok = tb("21", 2, 0);

        let (table, backends) = simple_routing(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            500,
            &[Arc::clone(&b_no_ep), Arc::clone(&b_ok)],
        );
        let rr = DashMap::new();

        let lease = acquire_tiered(
            Role::Text,
            0,
            None,
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();

        assert_eq!(
            lease.backend_id, "21",
            "no-endpoint backend must be skipped"
        );
    }

    /// When a tier has multiple entries with different strategies, the first
    /// entry's strategy is used (as enforced by DB schema).
    #[tokio::test]
    async fn tenant_specific_routing_preferred() {
        // Global route for tenant=None with one backend.
        let b_global = tb("22", 2, 0);
        // Tenant-specific route for tenant_id=Some(7) with a different backend.
        let b_tenant = tb("23", 2, 0);

        let (table, backends) = {
            use kb_core::routing::{ModelRow, ProviderRow, RoutingEntry};
            let entries = vec![
                RoutingEntry {
                    tenant_id: None,
                    role: Role::Text,
                    tier: 0,
                    strategy: RoutingStrategy::LeastLoaded,
                    spill_ms: 500,
                    provider: ProviderRow {
                        id: 122,
                        name: "p-global".into(),
                        kind: kb_core::routing::ProviderKind::Local,
                        endpoint: Some("http://x:22".into()),
                        headers: Default::default(),
                        enabled: true,
                    },
                    model: ModelRow {
                        id: 22,
                        provider_id: 122,
                        model_id: "model-22".into(),
                        caps: vec!["text".into()],
                        ctx_tokens: Some(32768),
                        max_conc: Some(2),
                        rpm: None,
                        tpm: None,
                        price_in: None,
                        price_out: None,
                        data_class: DataClass::Local,
                        enabled: true,
                    },
                },
                RoutingEntry {
                    tenant_id: Some(7),
                    role: Role::Text,
                    tier: 0,
                    strategy: RoutingStrategy::LeastLoaded,
                    spill_ms: 500,
                    provider: ProviderRow {
                        id: 123,
                        name: "p-tenant".into(),
                        kind: kb_core::routing::ProviderKind::Local,
                        endpoint: Some("http://x:23".into()),
                        headers: Default::default(),
                        enabled: true,
                    },
                    model: ModelRow {
                        id: 23,
                        provider_id: 123,
                        model_id: "model-23".into(),
                        caps: vec!["text".into()],
                        ctx_tokens: Some(32768),
                        max_conc: Some(2),
                        rpm: None,
                        tpm: None,
                        price_in: None,
                        price_out: None,
                        data_class: DataClass::Local,
                        enabled: true,
                    },
                },
            ];
            let table = Arc::new(RoutingTable::from_entries(entries));
            let map: DashMap<String, Arc<Backend>> = DashMap::new();
            map.insert("22".into(), Arc::clone(&b_global));
            map.insert("23".into(), Arc::clone(&b_tenant));
            (table, map)
        };
        let rr = DashMap::new();

        // Tenant 7 should use its specific route (backend 23).
        let lease = acquire_tiered(
            Role::Text,
            0,
            Some(7),
            &table,
            &backends,
            Duration::from_secs(5),
            &rr,
            false,
            &test_wq(),
        )
        .await
        .unwrap();
        assert_eq!(lease.backend_id, "23", "tenant-specific route must be used");
    }

    /// Existing P1-T2 pattern: Pool::acquire with no routing set falls back to legacy.
    #[tokio::test]
    async fn pool_acquire_falls_back_to_legacy_when_no_routing() {
        let b = tb("24", 2, 0);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "24");
        assert_eq!(b.free(), 1);
        drop(lease);
        assert_eq!(b.free(), 2);
    }

    /// Pool routing: acquire uses tiered routing when routing table is set.
    #[tokio::test]
    async fn pool_acquire_uses_tiered_when_routing_set() {
        let b = tb("25", 2, 0);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));

        // Build routing table pointing to backend "25".
        let (table, _backends) = simple_routing(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            500,
            &[Arc::clone(&b)],
        );
        pool.set_routing(table);

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "25");
        drop(lease);

        // Clear routing and verify fallback.
        pool.clear_routing();
        let lease2 = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease2.backend_id, "25"); // still works via legacy path
    }

    /// ordered_cost helper provides correct ordering.
    #[test]
    fn ordered_cost_cheapest_first() {
        let free = ordered_cost(Pricing::new(0.0, 0.0));
        let cheap = ordered_cost(Pricing::new(1.0, 2.0));
        let expensive = ordered_cost(Pricing::new(100.0, 200.0));

        assert!(free < cheap);
        assert!(cheap < expensive);
        assert_eq!(free, 0);
    }
}
