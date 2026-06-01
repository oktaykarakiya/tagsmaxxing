//! DB-driven routing types: provider kinds, strategies, and resolved routing-table
//! entries (plan §26.5).
//!
//! The `providers`/`models`/`routes` tables are defined in migration `0011_providers.sql`
//! (kb-store). This module holds the pure-data types that kb-store and kb-scheduler
//! share: string enums for provider kind and routing strategy, and resolved row
//! structs that [`PgStore::get_routing_table`](https://docs.rs/kb-store) builds from
//! a JOIN of the three tables.
//!
//! These types carry **no live adapters or capacity guards** — those are constructed
//! by the scheduler when it materializes a [`RoutingTable`] (P9-T6). Here we only
//! represent what is stored in the database.

use std::collections::HashMap;

use crate::data_class::DataClass;
use crate::macros::str_enum;
use crate::role::Role;

str_enum! {
    /// The protocol / SDK family a provider uses (plan §26.5).
    ///
    /// The scheduler instantiates the correct [`ProviderAdapter`](crate::provider::ProviderAdapter)
    /// based on this kind when materializing a live backend from the routing table.
    pub enum ProviderKind {
        /// OpenAI-compatible HTTP API (`llama-server`, vLLM, Groq, Together, …).
        OpenAiCompat = "openai_compat",
        /// Anthropic Messages API (plan §26.1, P9-T2).
        Anthropic = "anthropic",
        /// Google Gemini native SDK/API.
        GeminiNative = "gemini_native",
        /// Local llama.cpp or other self-hosted server.
        Local = "local",
    }
}

str_enum! {
    /// How the scheduler orders candidates within a routing tier (plan §26.4, §26.5).
    pub enum RoutingStrategy {
        /// Prefer the backend with the most free capacity.
        LeastLoaded = "least_loaded",
        /// Distribute across backends in round-robin order.
        RoundRobin = "round_robin",
        /// Prefer the cheapest backend by per-token pricing.
        CostAsc = "cost_asc",
        /// Use the explicit priority order from the `models` or `routes` row.
        Priority = "priority",
    }
}

// ── DB row types (what PgStore CRUD returns) ──────────────────────────────────

/// A row from the `providers` table.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderRow {
    /// Primary key.
    pub id: i64,
    /// Human-readable name (e.g. "llama-gpu", "openai-paid").
    pub name: String,
    /// Adapter kind the scheduler uses to talk to this provider.
    pub kind: ProviderKind,
    /// Base URL for HTTP providers; `None` for native-SDK providers.
    pub endpoint: Option<String>,
    /// Extra HTTP headers (JSON object, e.g. `{"anthropic-version":"2023-06-01"}`).
    pub headers: serde_json::Value,
    /// Whether this provider is eligible for routing.
    pub enabled: bool,
}

/// A row from the `models` table.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelRow {
    /// Primary key.
    pub id: i64,
    /// FK to `providers.id`.
    pub provider_id: i64,
    /// Provider's model name or local alias (e.g. "Qwen3-VL-30B-A3B-Instruct").
    pub model_id: String,
    /// Capability tags stored as `TEXT[]` (e.g. `{text,vision}`).
    pub caps: Vec<String>,
    /// Maximum context window in tokens, if known.
    pub ctx_tokens: Option<i32>,
    /// Maximum concurrent requests (slots for local, concurrency for remote).
    pub max_conc: Option<i32>,
    /// Remote requests-per-minute limit.
    pub rpm: Option<i32>,
    /// Remote tokens-per-minute limit.
    pub tpm: Option<i32>,
    /// USD per million input tokens.
    pub price_in: Option<f64>,
    /// USD per million output tokens.
    pub price_out: Option<f64>,
    /// Where inference runs (local or remote cloud).
    pub data_class: DataClass,
    /// Whether this model is eligible for routing.
    pub enabled: bool,
}

/// A row from the `routes` table.
#[derive(Debug, Clone, PartialEq)]
pub struct RouteRow {
    /// Primary key.
    pub id: i64,
    /// `NULL` = global default; set for tenant-specific override.
    pub tenant_id: Option<i64>,
    /// Which capability role this route entry serves.
    pub role: Role,
    /// Tier order (0 = primary, 1 = first fallback, …).
    pub tier: i32,
    /// Per-tier candidate-ordering strategy.
    pub strategy: RoutingStrategy,
    /// Millisecond delay before spilling to the next tier.
    pub spill_ms: i32,
    /// FK to `models.id`.
    pub model_id: i64,
}

// ── Resolved routing entry (the product of the JOIN) ──────────────────────────

/// One fully-resolved entry in the routing table — the product of a
/// `providers ⋈ models ⋈ routes` JOIN filtered to enabled rows only.
///
/// The scheduler uses a `Vec<RoutingEntry>` returned by
/// [`PgStore::get_routing_table`](https://docs.rs/kb-store) to materialize
/// live [`Backend`](https://docs.rs/kb-scheduler) instances (P9-T6).
///
/// # Tenant-specific override semantics
///
/// When both a global (`tenant_id IS NULL`) and a tenant-specific
/// (`tenant_id = 7`) route exist for the same `(role, tier, model_id)`,
/// the tenant-specific entry **replaces** the global one for that tenant.
/// This is enforced at query time in `get_routing_table`.
#[derive(Debug, Clone)]
pub struct RoutingEntry {
    /// The tenant this entry applies to (same as `routes.tenant_id`).
    /// `None` = global default.
    pub tenant_id: Option<i64>,
    /// Capability role this route entry serves.
    pub role: Role,
    /// Tier order (lower = tried first).
    pub tier: i32,
    /// Per-tier candidate-ordering strategy.
    pub strategy: RoutingStrategy,
    /// Millisecond delay before spilling to the next tier.
    pub spill_ms: i32,
    /// The resolved provider row (joined via `models.provider_id`).
    pub provider: ProviderRow,
    /// The resolved model row.
    pub model: ModelRow,
}

/// The in-memory routing table, keyed by `(Option<tenant_id>, Role)` and holding
/// tiers in order.
///
/// This is a pure-data snapshot of the `providers ⋈ models ⋈ routes` join
/// with tenant-specific overrides already applied. The scheduler materializes
/// live [`Backend`](https://docs.rs/kb-scheduler) instances from these entries
/// (P9-T6) and hot-swaps the whole table under an
/// [`arc_swap::ArcSwap`](https://docs.rs/arc-swap).
#[derive(Debug, Clone, Default)]
pub struct RoutingTable {
    /// Resolved entries, grouped and tier-ordered. The outer key is
    /// `(Option<tenant_id>, Role)`; the value is a list of tiers (sorted
    /// by `tier`), each tier holding one or more entries.
    pub entries: HashMap<(Option<i64>, Role), Vec<Vec<RoutingEntry>>>,
}

impl RoutingTable {
    /// Build a `RoutingTable` from the resolved entries returned by the DB join.
    ///
    /// Entries are grouped by `(tenant_id, role)`, then by `tier`. Within each
    /// tier, entries retain their natural order (the DB defines the ordering).
    /// Tenant-specific entries (`tenant_id IS NOT NULL`) are physically stored
    /// in separate buckets — the query layer already applies override semantics,
    /// so this constructor simply groups.
    ///
    /// # Panics
    ///
    /// Panics on a malformed entry (e.g., a `role` that doesn't parse).
    /// Production callers should ensure the DB returns valid data.
    pub fn from_entries(entries: Vec<RoutingEntry>) -> Self {
        let mut map: HashMap<(Option<i64>, Role), Vec<Vec<RoutingEntry>>> = HashMap::new();
        for entry in entries {
            let key = (entry.tenant_id, entry.role);
            let tiers = map.entry(key).or_default();
            let tier_idx = entry.tier as usize;
            while tiers.len() <= tier_idx {
                tiers.push(Vec::new());
            }
            tiers[tier_idx].push(entry);
        }
        // Remove empty trailing tiers (shouldn't happen, but be defensive).
        for (_key, tiers) in map.iter_mut() {
            while tiers.last().is_some_and(Vec::is_empty) {
                tiers.pop();
            }
        }
        Self { entries: map }
    }

    /// Whether the table is empty (no routes defined).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of distinct `(tenant_id, role)` groups in the table.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.entries.len()
    }

    /// Tiers for a specific `(tenant_id, role)` pair, or an empty slice.
    #[must_use]
    pub fn tiers_for(&self, tenant_id: Option<i64>, role: Role) -> &[Vec<RoutingEntry>] {
        // First try tenant-specific, then fall back to global.
        if let Some(tiers) = self.entries.get(&(tenant_id, role)) {
            return tiers.as_slice();
        }
        self.entries
            .get(&(None, role))
            .map_or(&[], |v| v.as_slice())
    }
}

// ── Hot-reload notification trait (plan §26.5, P9-T7) ──────────────────────

/// A notification mechanism that signals when the routing table may have changed.
///
/// The first call to [`wait_for_change`](Self::wait_for_change) must return
/// immediately so the reloader performs its initial load. Subsequent calls block
/// until an external signal (e.g. Postgres `NOTIFY` or a polling timer) indicates
/// that the routing table should be re-fetched.
///
/// Implementations:
/// - [`PgRoutingNotifier`](https://docs.rs/kb-store) — Postgres `LISTEN`/`NOTIFY`
///   with polling fallback.
/// - Mock implementations in scheduler tests for unit-testing the reloader.
#[async_trait::async_trait]
pub trait RoutingChangeNotifier: Send + Sync {
    /// Wait until the routing table should be re-fetched.
    ///
    /// The **first call** must return immediately (startup load).
    /// Subsequent calls block until a change is signaled.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying notification mechanism fails
    /// (e.g. Postgres connection lost).
    async fn wait_for_change(&self) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    // ── ProviderKind ─────────────────────────────────────────────────────────

    #[test]
    fn provider_kind_wire_strings_are_locked() {
        assert_eq!(ProviderKind::OpenAiCompat.as_str(), "openai_compat");
        assert_eq!(ProviderKind::Anthropic.as_str(), "anthropic");
        assert_eq!(ProviderKind::GeminiNative.as_str(), "gemini_native");
        assert_eq!(ProviderKind::Local.as_str(), "local");
        assert_eq!(ProviderKind::all().len(), 4);
    }

    #[test]
    fn provider_kind_round_trips_every_variant() {
        for k in ProviderKind::all() {
            assert_eq!(ProviderKind::from_str(k.as_str()).unwrap(), *k);
        }
    }

    #[test]
    fn provider_kind_rejects_unknown() {
        assert!(ProviderKind::from_str("aws_bedrock").is_err());
    }

    // ── RoutingStrategy ──────────────────────────────────────────────────────

    #[test]
    fn routing_strategy_wire_strings_are_locked() {
        assert_eq!(RoutingStrategy::LeastLoaded.as_str(), "least_loaded");
        assert_eq!(RoutingStrategy::RoundRobin.as_str(), "round_robin");
        assert_eq!(RoutingStrategy::CostAsc.as_str(), "cost_asc");
        assert_eq!(RoutingStrategy::Priority.as_str(), "priority");
        assert_eq!(RoutingStrategy::all().len(), 4);
    }

    #[test]
    fn routing_strategy_round_trips_every_variant() {
        for s in RoutingStrategy::all() {
            assert_eq!(RoutingStrategy::from_str(s.as_str()).unwrap(), *s);
        }
    }

    #[test]
    fn routing_strategy_rejects_unknown() {
        assert!(RoutingStrategy::from_str("random").is_err());
    }

    // ── RoutingTable ─────────────────────────────────────────────────────────

    fn make_entry(
        tenant_id: Option<i64>,
        role: Role,
        tier: i32,
        strategy: RoutingStrategy,
        spill_ms: i32,
        provider_name: &str,
        model_name: &str,
    ) -> RoutingEntry {
        RoutingEntry {
            tenant_id,
            role,
            tier,
            strategy,
            spill_ms,
            provider: ProviderRow {
                id: 1,
                name: provider_name.into(),
                kind: ProviderKind::Local,
                endpoint: Some("http://localhost:8001/v1".into()),
                headers: Default::default(),
                enabled: true,
            },
            model: ModelRow {
                id: 1,
                provider_id: 1,
                model_id: model_name.into(),
                caps: vec!["text".into(), "vision".into()],
                ctx_tokens: Some(32768),
                max_conc: Some(4),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        }
    }

    #[test]
    fn empty_table_is_empty() {
        let table = RoutingTable::from_entries(vec![]);
        assert!(table.is_empty());
        assert_eq!(table.group_count(), 0);
        assert!(table.tiers_for(None, Role::Text).is_empty());
    }

    #[test]
    fn single_entry_groups_correctly() {
        let e = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "p",
            "m",
        );
        let table = RoutingTable::from_entries(vec![e]);
        assert!(!table.is_empty());
        assert_eq!(table.group_count(), 1);

        let tiers = table.tiers_for(None, Role::Text);
        assert_eq!(tiers.len(), 1, "one tier");
        assert_eq!(tiers[0].len(), 1, "one entry in tier 0");
        assert_eq!(tiers[0][0].tier, 0);
        assert_eq!(tiers[0][0].role, Role::Text);
    }

    #[test]
    fn two_entries_same_tier_grouped() {
        let e1 = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "p1",
            "m1",
        );
        let e2 = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "p2",
            "m2",
        );
        let table = RoutingTable::from_entries(vec![e1, e2]);
        let tiers = table.tiers_for(None, Role::Text);
        assert_eq!(tiers.len(), 1, "one tier");
        assert_eq!(tiers[0].len(), 2, "two entries in tier 0");
    }

    #[test]
    fn two_tiers_ordered() {
        let e0 = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "primary",
            "model-a",
        );
        let e1 = make_entry(
            None,
            Role::Text,
            1,
            RoutingStrategy::CostAsc,
            1500,
            "fallback",
            "model-b",
        );
        let table = RoutingTable::from_entries(vec![e0, e1]);
        let tiers = table.tiers_for(None, Role::Text);
        assert_eq!(tiers.len(), 2, "two tiers");
        assert_eq!(tiers[0].len(), 1);
        assert_eq!(tiers[0][0].strategy, RoutingStrategy::LeastLoaded);
        assert_eq!(tiers[1].len(), 1);
        assert_eq!(tiers[1][0].strategy, RoutingStrategy::CostAsc);
        assert_eq!(tiers[1][0].spill_ms, 1500);
    }

    #[test]
    fn tenant_specific_and_global_separate() {
        let global = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "g",
            "gm",
        );
        let tenant = make_entry(
            Some(7),
            Role::Text,
            0,
            RoutingStrategy::RoundRobin,
            500,
            "t",
            "tm",
        );
        let table = RoutingTable::from_entries(vec![global, tenant]);
        assert_eq!(table.group_count(), 2);

        // Global lookup.
        let gt = table.tiers_for(None, Role::Text);
        assert_eq!(gt[0][0].provider.name, "g");

        // Tenant-specific lookup.
        let tt = table.tiers_for(Some(7), Role::Text);
        assert_eq!(tt[0][0].provider.name, "t");
    }

    #[test]
    fn tiers_for_tenant_falls_back_to_global() {
        let global = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "g",
            "gm",
        );
        let table = RoutingTable::from_entries(vec![global]);

        // Tenant 99 has no override → fallback to global.
        let tiers = table.tiers_for(Some(99), Role::Text);
        assert_eq!(tiers.len(), 1);
        assert_eq!(tiers[0][0].provider.name, "g");
    }

    #[test]
    fn tiers_for_unknown_role_empty() {
        let table = RoutingTable::from_entries(vec![]);
        assert!(table.tiers_for(None, Role::Rerank).is_empty());
    }

    #[test]
    fn different_roles_separate_groups() {
        let e_text = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "pt",
            "mt",
        );
        let e_embed = make_entry(
            None,
            Role::Embed,
            0,
            RoutingStrategy::Priority,
            200,
            "pe",
            "me",
        );
        let table = RoutingTable::from_entries(vec![e_text, e_embed]);
        assert_eq!(table.group_count(), 2);

        assert_eq!(table.tiers_for(None, Role::Text)[0][0].provider.name, "pt");
        assert_eq!(table.tiers_for(None, Role::Embed)[0][0].provider.name, "pe");
    }

    #[test]
    fn noncontiguous_tiers_work() {
        // Tier 0 and tier 3 (skipping 1, 2) — a valid config.
        let e0 = make_entry(
            None,
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            "p0",
            "m0",
        );
        let e3 = make_entry(
            None,
            Role::Text,
            3,
            RoutingStrategy::CostAsc,
            2000,
            "p3",
            "m3",
        );
        let table = RoutingTable::from_entries(vec![e0, e3]);
        let tiers = table.tiers_for(None, Role::Text);
        assert_eq!(tiers.len(), 4, "tiers 0..=3 (intermediate tiers empty)");
        assert_eq!(tiers[0][0].tier, 0);
        assert!(tiers[1].is_empty());
        assert!(tiers[2].is_empty());
        assert_eq!(tiers[3][0].tier, 3);
    }

    #[test]
    fn default_table_is_empty() {
        let table = RoutingTable::default();
        assert!(table.is_empty());
        assert_eq!(table.group_count(), 0);
    }

    // ── RoutingChangeNotifier mock ─────────────────────────────────────────

    /// A mock notifier that returns immediately on first call, then on a
    /// shared semaphore thereafter.
    struct MockNotifier {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl MockNotifier {
        fn new() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl RoutingChangeNotifier for MockNotifier {
        async fn wait_for_change(&self) -> anyhow::Result<()> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                // First call: return immediately (startup load).
                Ok(())
            } else {
                // Subsequent calls: just return OK (unit tests don't need
                // real blocking — the reloader test controls the fetch).
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn mock_notifier_first_call_is_immediate() {
        let notifier = MockNotifier::new();
        // First call must not block.
        notifier.wait_for_change().await.unwrap();
        assert_eq!(notifier.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn mock_notifier_second_call_also_succeeds() {
        let notifier = MockNotifier::new();
        notifier.wait_for_change().await.unwrap();
        notifier.wait_for_change().await.unwrap();
        assert_eq!(notifier.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
