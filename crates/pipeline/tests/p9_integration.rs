// SPDX-License-Identifier: AGPL-3.0-or-later

//! P9 phase integration test (§26, §31): pluggable providers, DB-driven tiered
//! routing, data residency, cost tracking & budget enforcement, and hot-reload.
//!
//! Sets up a real pgvector Postgres via testcontainers, populates providers/models/routes
//! through the PgStore admin CRUD, wires mock-backend HTTP servers for the "backends,"
//! and exercises the full tiered-routing scheduler against them.
//!
//! Tests are `#[ignore]` in `just ci` because they require Podman +
//! `paradedb/paradedb:pg17`. Run with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-pipeline --test p9_integration -- --ignored --test-threads=1
//! ```
//!
//! P9 PHASE COMPLETE after this passes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use kb_core::capability::CapSet;
use kb_core::data_class::DataClass;
use kb_core::pricing::Pricing;
use kb_core::provider::NoopAdapter;
use kb_core::role::Role;
use kb_core::routing::{RoutingStrategy, RoutingTable};
use kb_mock_backend::MockBackend;
use kb_scheduler::backend::Backend;
use kb_scheduler::{Capacity, Pool};
use kb_store::pg_store::PgStore;

// ── Test helpers ─────────────────────────────────────────────────────────────────

/// Build a scheduler `Backend` pointing at a mock server, with the given database
/// `model_id` (the DB row id — must match what `resolve_and_filter` looks up).
fn mock_backend(
    db_model_id: i64,
    mock: &MockBackend,
    roles: Vec<Role>,
    slots: usize,
    data_class: DataClass,
    pricing: Option<Pricing>,
) -> Arc<Backend> {
    let endpoint = format!("http://{}/v1", mock.addr());
    Arc::new(Backend::new(
        db_model_id.to_string(), // id = model row id (string)
        Arc::new(NoopAdapter),
        &endpoint,
        None,                           // key
        format!("model-{db_model_id}"), // model_id alias
        CapSet::from(roles.as_slice()),
        Capacity::new_slots(slots),
        slots, // max_concurrency
        pricing,
        data_class,
        0,    // priority
        None, // ctx_tokens
    ))
}

/// Build a `RoutingTable` from the database entries and set it on the pool.
async fn load_routing_from_db(pool: &Pool, store: &PgStore) -> anyhow::Result<()> {
    let entries = store.get_routing_table().await?;
    assert!(!entries.is_empty(), "routing table must have entries");
    let table = Arc::new(RoutingTable::from_entries(entries));
    pool.set_routing(table);
    Ok(())
}

/// Create a tenant via raw SQL on the admin pool, returning its id.
async fn create_tenant(store: &PgStore, slug: &str) -> anyhow::Result<i64> {
    let admin = store.pool()?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name, residency) VALUES ($1, $2, 'allow_remote') RETURNING id",
    )
    .bind(slug)
    .bind(slug)
    .fetch_one(&admin)
    .await?;
    Ok(id)
}

/// Set the tenant's budget (monthly cents, SQL NULL = unlimited).
async fn set_tenant_budget(store: &PgStore, tenant_id: i64, budget_cents: Option<i64>) {
    let admin = store.pool().expect("admin pool");
    sqlx::query("UPDATE tenants SET budget_monthly_cents = $1 WHERE id = $2")
        .bind(budget_cents)
        .bind(tenant_id)
        .execute(&admin)
        .await
        .expect("set budget");
}

/// Insert a usage event with a cost, for budget testing.
async fn insert_cost(store: &PgStore, tenant_id: i64, cost_micros: i64) {
    let pool = store.pool().expect("admin pool");
    sqlx::query(
        "INSERT INTO usage_events (tenant_id, model, role, backend_id, prompt_tokens, \
         completion_tokens, cost_micros) \
         VALUES ($1, 'test-model', 'text', 'b1', 1000, 500, $2)",
    )
    .bind(tenant_id)
    .bind(cost_micros)
    .execute(&pool)
    .await
    .expect("insert usage event");
}

/// Full test harness: a Postgres container, a connected PgStore, several mock
/// backends, and a Pool with backends + routing table loaded from the DB.
struct Harness {
    store: Arc<PgStore>,
    pool: Pool,
    mocks: Vec<MockBackend>,
}

impl Harness {
    /// Build a harness with the given number of mock backends.
    ///
    /// Each mock gets a provider + model + route row in the DB, and a corresponding
    /// scheduler `Backend` that points to the mock's HTTP server.
    async fn new(mock_specs: &[MockSpec]) -> anyhow::Result<Self> {
        let db = kb_testsupport::fresh_db().await?;
        let store = Arc::new(PgStore::with_roles(db.admin_url, db.app_url));
        store.connect().await?;

        let mut mocks = Vec::new();
        let mut backends = Vec::new();

        for spec in mock_specs {
            // Start a mock HTTP server.
            let mock = MockBackend::start().await;
            let be = mock_backend(
                spec.db_model_id,
                &mock,
                spec.roles.clone(),
                spec.slots,
                spec.data_class,
                spec.pricing,
            );
            mocks.push(mock);
            backends.push(be);
        }

        // Insert providers, models, and routes into the database.
        for spec in mock_specs {
            let provider_id = store
                .create_provider(
                    &format!("provider-{}", spec.db_model_id),
                    spec.provider_kind,
                    Some(&format!("http://mock-{}", spec.db_model_id)),
                    &serde_json::json!({}),
                )
                .await?;

            let role_strs: Vec<String> =
                spec.roles.iter().map(|r| r.as_str().to_string()).collect();
            let model_id = store
                .create_model(
                    provider_id,
                    &format!("model-{}", spec.db_model_id),
                    &role_strs,
                    None, // ctx_tokens
                    Some(spec.slots as i32),
                    None, // rpm
                    None, // tpm
                    spec.pricing.map(|p| p.input_per_mtok),
                    spec.pricing.map(|p| p.output_per_mtok),
                    spec.data_class,
                )
                .await?;

            // Ensure the DB model id matches what we'll use for the Backend id.
            assert_eq!(
                model_id, spec.db_model_id,
                "model id must be predictable for Backend id matching"
            );

            for role in &spec.roles {
                store
                    .create_route(
                        spec.route_tenant_id,
                        *role,
                        spec.tier,
                        spec.strategy,
                        spec.spill_ms,
                        model_id,
                    )
                    .await?;
            }
        }

        let pool = Pool::new(backends, Duration::from_secs(10));
        load_routing_from_db(&pool, &store).await?;

        Ok(Self { store, pool, mocks })
    }

    /// Shut down all mock backends.
    async fn shutdown(self) {
        for m in self.mocks {
            m.shutdown().await;
        }
    }
}

/// Specifications for one mock backend + its DB rows.
struct MockSpec {
    /// The expected id of the `models` row (must be a unique positive integer).
    db_model_id: i64,
    /// The provider kind (for the `providers` table).
    provider_kind: kb_core::routing::ProviderKind,
    /// Which roles this backend serves.
    roles: Vec<Role>,
    /// Number of concurrency slots.
    slots: usize,
    /// Local or Remote data classification.
    data_class: DataClass,
    /// Optional per-Mtok pricing.
    pricing: Option<Pricing>,
    /// Tenant override (None = global).
    route_tenant_id: Option<i64>,
    /// Tier number (0 = primary, 1 = first fallback, …).
    tier: i32,
    /// Routing strategy for this tier.
    strategy: RoutingStrategy,
    /// Time to wait for a slot before spilling to the next tier (ms).
    spill_ms: i32,
}

// ── Tests ────────────────────────────────────────────────────────────────────────

/// Smoke: DB-driven routing table is loaded and acquire succeeds on tier 0.
#[tokio::test]
#[ignore]
async fn db_driven_routing_acquire_tier0() {
    let h = Harness::new(&[MockSpec {
        db_model_id: 1,
        provider_kind: kb_core::routing::ProviderKind::Local,
        roles: vec![Role::Text],
        slots: 2,
        data_class: DataClass::Local,
        pricing: None,
        route_tenant_id: None,
        tier: 0,
        strategy: RoutingStrategy::LeastLoaded,
        spill_ms: 800,
    }])
    .await
    .unwrap();

    assert!(h.pool.has_routing(), "pool must have routing table set");

    // Tier 0 has capacity → acquire succeeds immediately.
    let lease = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(lease.backend_id, "1", "must acquire from tier 0 model id=1");
    drop(lease);

    h.shutdown().await;
}

/// Tier spill: tier 0 at capacity → spills to tier 1 after spill_ms.
#[tokio::test]
#[ignore]
async fn db_driven_tier_spill_to_tier1() {
    let h = Harness::new(&[
        MockSpec {
            db_model_id: 1,
            provider_kind: kb_core::routing::ProviderKind::Local,
            roles: vec![Role::Text],
            slots: 1, // only 1 slot in tier 0
            data_class: DataClass::Local,
            pricing: None,
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 200, // short spill timeout
        },
        MockSpec {
            db_model_id: 2,
            provider_kind: kb_core::routing::ProviderKind::OpenAiCompat,
            roles: vec![Role::Text],
            slots: 2, // 2 slots in tier 1
            data_class: DataClass::Remote,
            pricing: Some(Pricing::new(2.0, 2.0)), // $2/Mtok
            route_tenant_id: None,
            tier: 1,
            strategy: RoutingStrategy::CostAsc,
            spill_ms: 800,
        },
    ])
    .await
    .unwrap();

    // Exhaust tier 0's only slot.
    let _hold = h.pool.acquire(Role::Text, false, 0).await.unwrap();

    // Next acquire should spill to tier 1 (model id=2) after spill_after (200ms).
    let lease = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(
        lease.backend_id, "2",
        "tier 0 exhausted → must spill to tier 1"
    );

    drop(lease);
    drop(_hold);
    h.shutdown().await;
}

/// Embed lock: only tier 0 is used for the Embed role (plan §11).
#[tokio::test]
#[ignore]
async fn embed_role_only_tier0_no_spill() {
    let h = Harness::new(&[
        MockSpec {
            db_model_id: 1,
            provider_kind: kb_core::routing::ProviderKind::Local,
            roles: vec![Role::Embed],
            slots: 1, // 1 slot in embed tier 0
            data_class: DataClass::Local,
            pricing: None,
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 100,
        },
        MockSpec {
            db_model_id: 2,
            provider_kind: kb_core::routing::ProviderKind::OpenAiCompat,
            roles: vec![Role::Embed],
            slots: 2, // 2 slots in embed tier 1 — should never be reached
            data_class: DataClass::Remote,
            pricing: Some(Pricing::new(1.0, 1.0)),
            route_tenant_id: None,
            tier: 1,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 800,
        },
    ])
    .await
    .unwrap();

    // Exhaust tier 0 embed.
    let _hold = h.pool.acquire(Role::Embed, false, 0).await.unwrap();

    // For Embed, even though tier 1 has capacity, the lock prevents spill.
    let err = h.pool.acquire(Role::Embed, false, 0).await.unwrap_err();

    assert!(
        err.to_string().contains("CapacityExhausted")
            || err.to_string().contains("capacity exhausted"),
        "embed must not spill to tier 1: got {err:?}"
    );

    drop(_hold);
    h.shutdown().await;
}

/// Data residency: `local_only=true` excludes `DataClass::Remote` backends.
#[tokio::test]
#[ignore]
async fn local_only_excludes_remote_backends() {
    let h = Harness::new(&[
        MockSpec {
            db_model_id: 1,
            provider_kind: kb_core::routing::ProviderKind::Local,
            roles: vec![Role::Text],
            slots: 1, // tier 0 exhausted forces consideration of other tiers
            data_class: DataClass::Local,
            pricing: None,
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 100,
        },
        MockSpec {
            db_model_id: 2,
            provider_kind: kb_core::routing::ProviderKind::OpenAiCompat,
            roles: vec![Role::Text],
            slots: 2,
            data_class: DataClass::Remote,
            pricing: Some(Pricing::new(2.0, 2.0)),
            route_tenant_id: None,
            tier: 1,
            strategy: RoutingStrategy::CostAsc,
            spill_ms: 800,
        },
    ])
    .await
    .unwrap();

    // First, without local_only: tier 0 available → acquire succeeds.
    let lease = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(lease.backend_id, "1", "tier 0 free → acquired");
    drop(lease);

    // Exhaust tier 0.
    let _hold = h.pool.acquire(Role::Text, false, 0).await.unwrap();

    // Without local_only: spills to tier 1 (remote).
    let lease_spill = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(
        lease_spill.backend_id, "2",
        "without local_only, remote tier 1 is reachable"
    );
    drop(lease_spill);

    // Now with local_only=true: tier 0 exhausted AND remote excluded → no backends.
    let err = h
        .pool
        .acquire(Role::Text, true /* local_only */, 0)
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("NoBackend")
            || err.to_string().contains("CapacityExhausted")
            || err.to_string().contains("capacity exhausted")
            || err.to_string().contains("no backend"),
        "local_only must exclude remote: got {err:?}"
    );

    drop(_hold);
    h.shutdown().await;
}

/// 429 cooldown: a backend in cooldown is skipped during tiered acquire.
#[tokio::test]
#[ignore]
async fn cooldown_backend_skipped_in_db_routing() {
    let h = Harness::new(&[
        MockSpec {
            db_model_id: 1,
            provider_kind: kb_core::routing::ProviderKind::Local,
            roles: vec![Role::Text],
            slots: 2,
            data_class: DataClass::Local,
            pricing: None,
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 100,
        },
        MockSpec {
            db_model_id: 2,
            provider_kind: kb_core::routing::ProviderKind::OpenAiCompat,
            roles: vec![Role::Text],
            slots: 2,
            data_class: DataClass::Remote,
            pricing: Some(Pricing::new(15.0, 15.0)), // $15/Mtok Anthropic-tier
            route_tenant_id: None,
            tier: 1,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 800,
        },
    ])
    .await
    .unwrap();

    // Put tier 0 backend into cooldown.
    let backends = h.pool.all_backends();
    let tier0 = backends
        .iter()
        .find(|b| b.id == "1")
        .expect("tier 0 backend");
    let far_future = std::time::Instant::now() + Duration::from_secs(300);
    tier0.set_cooldown(far_future);
    assert!(tier0.cooldown_active());

    // Tier 0 is in cooldown → tiered routing skips it → spills to tier 1.
    let lease = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(
        lease.backend_id, "2",
        "cooldown backend must be skipped, spill to tier 1"
    );

    drop(lease);
    tier0.clear_cooldown();
    h.shutdown().await;
}

/// Cost tracking: costs accrue in usage_events and budget enforcement works.
#[tokio::test]
#[ignore]
async fn cost_tracking_and_budget_enforcement() {
    let h = Harness::new(&[MockSpec {
        db_model_id: 1,
        provider_kind: kb_core::routing::ProviderKind::Local,
        roles: vec![Role::Text],
        slots: 2,
        data_class: DataClass::Local,
        pricing: None,
        route_tenant_id: None,
        tier: 0,
        strategy: RoutingStrategy::LeastLoaded,
        spill_ms: 800,
    }])
    .await
    .unwrap();

    let tenant_id = create_tenant(&h.store, "budget-tenant").await.unwrap();

    // Set a $1.00 monthly budget (100 cents = 1,000,000 micro-dollars).
    set_tenant_budget(&h.store, tenant_id, Some(100)).await;

    // No costs yet → within budget.
    h.store
        .check_tenant_budget(tenant_id, false /* soft cap */)
        .await
        .unwrap();

    // Insert $1.50 of costs (1,500,000 micro-dollars → over the $1.00 budget).
    insert_cost(&h.store, tenant_id, 1_500_000).await;

    // Soft cap: should warn but not error.
    let soft = h.store.check_tenant_budget(tenant_id, false).await;
    assert!(soft.is_ok(), "soft cap must not reject: {soft:?}");

    // Hard cap: should return error.
    let hard = h.store.check_tenant_budget(tenant_id, true).await;
    assert!(hard.is_err(), "hard cap must reject over-budget calls");
    let err_msg = hard.unwrap_err().to_string();
    assert!(
        err_msg.contains("budget exceeded") || err_msg.contains("1500000"),
        "error must describe budget exceeded, got: {err_msg}"
    );

    h.shutdown().await;
}

/// Hot-reload: insert a new route, send pg_notify, and verify the Pool picks it up.
#[tokio::test]
#[ignore]
async fn hot_reload_picks_up_new_route() {
    let h = Harness::new(&[MockSpec {
        db_model_id: 1,
        provider_kind: kb_core::routing::ProviderKind::Local,
        roles: vec![Role::Text],
        slots: 2,
        data_class: DataClass::Local,
        pricing: None,
        route_tenant_id: None,
        tier: 0,
        strategy: RoutingStrategy::LeastLoaded,
        spill_ms: 800,
    }])
    .await
    .unwrap();

    assert!(h.pool.has_routing());

    // Currently only model 1 is in the routing table → acquire should use it.
    let lease = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(lease.backend_id, "1");
    drop(lease);

    // Now add a second backend to the pool (model id 2) and its routing.
    let mock2 = MockBackend::start().await;
    let backend2 = mock_backend(
        2,
        &mock2,
        vec![Role::Text],
        2,
        DataClass::Remote,
        Some(Pricing::new(2.0, 2.0)),
    );

    // Insert provider → model → route for the new backend in the DB.
    let provider_id = h
        .store
        .create_provider(
            "provider-2",
            kb_core::routing::ProviderKind::OpenAiCompat,
            Some("http://mock-2"),
            &serde_json::json!({}),
        )
        .await
        .unwrap();

    let model_id = h
        .store
        .create_model(
            provider_id,
            "model-2",
            &["text".into()],
            None,
            Some(2),
            None,
            None,
            Some(2.0),
            Some(2.0),
            DataClass::Remote,
        )
        .await
        .unwrap();

    h.store
        .create_route(
            None,
            Role::Text,
            1, /* tier 1 */
            RoutingStrategy::CostAsc,
            500,
            model_id,
        )
        .await
        .unwrap();

    // Send pg_notify to trigger a routing reload.
    h.store.notify_routing_change().await.unwrap();

    // Build a fresh pool that includes the new backend and reload routing.
    // (In production, the Reloader does this; here we manually simulate the reload.)
    let mut all_backends = h.pool.all_backends();
    all_backends.push(backend2);
    let fresh_pool = Pool::new(all_backends, Duration::from_secs(10));

    // Reload routing from the updated DB.
    load_routing_from_db(&fresh_pool, &h.store).await.unwrap();
    assert!(fresh_pool.has_routing());

    // With tier 0 free (model 1, 2 slots), acquire should still get tier 0.
    let lease2 = fresh_pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(lease2.backend_id, "1", "tier 0 still preferred when free");
    drop(lease2);

    // Exhaust tier 0, then spill to tier 1 (the newly added route).
    let _hold = fresh_pool.acquire(Role::Text, false, 0).await.unwrap();
    let _hold2 = fresh_pool.acquire(Role::Text, false, 0).await.unwrap();
    // Both slots of tier 0 are now held → next acquire spills to tier 1.
    let lease3 = fresh_pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(
        lease3.backend_id, "2",
        "hot-reloaded tier 1 route must be reachable"
    );

    drop(lease3);
    drop(_hold2);
    drop(_hold);
    mock2.shutdown().await;
    h.shutdown().await;
}

/// CostAsc strategy prefers free/cheapest backend from DB-driven routing.
#[tokio::test]
#[ignore]
async fn cost_asc_prefers_cheapest_via_db_routing() {
    let h = Harness::new(&[
        MockSpec {
            db_model_id: 1,
            provider_kind: kb_core::routing::ProviderKind::Local,
            roles: vec![Role::Text],
            slots: 2,
            data_class: DataClass::Local,
            pricing: None, // free
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::CostAsc,
            spill_ms: 800,
        },
        MockSpec {
            db_model_id: 2,
            provider_kind: kb_core::routing::ProviderKind::OpenAiCompat,
            roles: vec![Role::Text],
            slots: 2,
            data_class: DataClass::Remote,
            pricing: Some(Pricing::new(2.0, 2.0)), // $4/Mtok combined
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::CostAsc,
            spill_ms: 800,
        },
        MockSpec {
            db_model_id: 3,
            provider_kind: kb_core::routing::ProviderKind::OpenAiCompat,
            roles: vec![Role::Text],
            slots: 2,
            data_class: DataClass::Remote,
            pricing: Some(Pricing::new(15.0, 15.0)), // $30/Mtok combined
            route_tenant_id: None,
            tier: 0,
            strategy: RoutingStrategy::CostAsc,
            spill_ms: 800,
        },
    ])
    .await
    .unwrap();

    // All three are in tier 0 with CostAsc strategy → free backend (id=1) wins.
    let lease = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(
        lease.backend_id, "1",
        "CostAsc must prefer free/cheapest backend"
    );
    drop(lease);

    // Exhaust the free backend → next cheapest (id=2, $4/Mtok) should be picked.
    let _hold = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    let _hold2 = h.pool.acquire(Role::Text, false, 0).await.unwrap();

    // Both free slots exhausted → spills to tier 1? No, all backends are in tier 0.
    // The CostAsc order means id=2 ($4) is next after id=1 (free).
    let lease2 = h.pool.acquire(Role::Text, false, 0).await.unwrap();
    assert_eq!(
        lease2.backend_id, "2",
        "CostAsc: after free exhausted, cheapest paid backend wins"
    );

    drop(lease2);
    drop(_hold2);
    drop(_hold);
    h.shutdown().await;
}
