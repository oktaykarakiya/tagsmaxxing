//! Integration tests for plan-driven quota enforcement (P11-T5) against a real
//! pgvector Postgres container.
//!
//! These tests need **Podman** and network access to pull `pgvector/pgvector`.
//! They are gated `#[ignore]` so `just ci` stays green when no container runtime
//! is available. Run them explicitly against the Podman socket:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test plan_enforcement_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::Utc;
use kb_core::quota;
use kb_core::role::Role;
use kb_core::usage::UsageEvent;
use kb_store::PgStore;

/// Helper: create a connected PgStore against a fresh testcontainers Postgres.
async fn connected_store() -> PgStore {
    let db = kb_testsupport::fresh_db().await.expect("fresh_db");
    let store = PgStore::with_roles(&db.admin_url, &db.app_url);
    store.connect().await.expect("connect");
    store
}

/// Build a usage event for the current UTC instant with `prompt` tokens, so it
/// lands in this month's rollup bucket.
fn usage_event_now(tenant_id: i64, prompt: i32) -> UsageEvent {
    UsageEvent {
        id: 0,
        tenant_id,
        user_id: None,
        model: "test-model".into(),
        role: Role::Embed,
        backend_id: None,
        prompt_tokens: Some(prompt),
        completion_tokens: None,
        latency_ms: None,
        cost_micros: None,
        created_at: Utc::now(),
    }
}

/// Real Postgres: check_plan_storage_quota enforces the free plan (50 MB).
#[tokio::test]
#[ignore]
async fn plan_storage_quota_enforces_free_plan_50mb() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("quota-test", "Quota Test")
        .await
        .expect("create_tenant");

    // Assign the free plan (50 MB = 52_428_800 bytes).
    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get plan")
        .expect("free plan exists");

    store
        .apply_plan_quotas_to_tenant(tenant_id, free.id, 0)
        .await
        .expect("apply quotas");

    // 60 MB should exceed the 50 MB limit.
    let sixty_mb: i64 = 60 * 1024 * 1024;
    let err = store
        .check_plan_storage_quota(tenant_id, sixty_mb)
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("storage quota exceeded"),
        "expected storage quota error, got: {msg}"
    );

    // The error should contain upsell info (free → pro).
    let quota_err: &quota::QuotaError = err.downcast_ref().expect("should be QuotaError");
    let upsell = quota_err.upsell_message().expect("should have upsell");
    assert!(
        upsell.contains("Upgrade from free to pro"),
        "expected free→pro upsell, got: {upsell}"
    );
}

/// Real Postgres: pro plan (5 GB) passes a 3 GB upload.
#[tokio::test]
#[ignore]
async fn pro_plan_allows_3gb_upload() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("pro-test", "Pro Test")
        .await
        .expect("create_tenant");

    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get plan")
        .expect("pro plan exists");

    store
        .apply_plan_quotas_to_tenant(tenant_id, pro.id, 0)
        .await
        .expect("apply quotas");

    // 3 GB — should be under the 5 GB pro limit.
    let three_gb: i64 = 3 * 1024 * 1024 * 1024;
    store
        .check_plan_storage_quota(tenant_id, three_gb)
        .await
        .expect("3 GB should fit in 5 GB plan");
}

/// Real Postgres: NULL plan_id → grandfathered (unlimited).
#[tokio::test]
#[ignore]
async fn grandfathered_tenant_unlimited() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("grandfathered", "Grandfathered")
        .await
        .expect("create_tenant");

    // No plan assigned → NULL plan_id → unlimited.
    store
        .check_plan_storage_quota(tenant_id, 100_000_000_000)
        .await
        .expect("grandfathered tenant must be unlimited");

    store
        .check_plan_token_budget(tenant_id, 1_000_000)
        .await
        .expect("grandfathered token budget must be unlimited");
}

/// Real Postgres: admin quota override takes precedence over plan.
#[tokio::test]
#[ignore]
async fn admin_quota_override_takes_precedence() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("override-test", "Override Test")
        .await
        .expect("create_tenant");

    // Assign pro plan (5 GB).
    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get plan")
        .expect("pro plan exists");

    store
        .apply_plan_quotas_to_tenant(tenant_id, pro.id, 0)
        .await
        .expect("apply quotas");

    // Admin overrides storage to only 10 MB.
    let ten_mb: i64 = 10 * 1024 * 1024;
    store
        .set_admin_quota_override(tenant_id, Some(ten_mb), None, 42)
        .await
        .expect("set override");

    // 15 MB should now be rejected (override of 10 MB < plan's 5 GB).
    let fifteen_mb: i64 = 15 * 1024 * 1024;
    let err = store
        .check_plan_storage_quota(tenant_id, fifteen_mb)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("storage quota exceeded"),
        "admin override of 10 MB should reject 15 MB"
    );

    // Clear the override — plan value should be used again.
    store
        .set_admin_quota_override(tenant_id, None, None, 42)
        .await
        .expect("clear override");

    // 15 MB should now pass (plan allows 5 GB).
    store
        .check_plan_storage_quota(tenant_id, fifteen_mb)
        .await
        .expect("15 MB should pass after clearing override");
}

/// Real Postgres: max_users enforcement (free plan → max 1 user).
#[tokio::test]
#[ignore]
async fn max_users_enforcement_free_plan() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("maxusers-test", "Max Users Test")
        .await
        .expect("create_tenant");

    // Assign free plan (max_users = 1).
    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get plan")
        .expect("free plan exists");

    store
        .update_tenant_plan(tenant_id, Some(free.id), None, None, Some("active"), None)
        .await
        .expect("assign free plan");

    // 1 user already exists (the tenant creator).
    // Adding 1 more should fail.
    let err = store
        .check_tenant_max_users(tenant_id, 1)
        .await
        .unwrap_err();

    let quota_err: &quota::QuotaError = err.downcast_ref().expect("should be QuotaError");
    assert!(
        matches!(quota_err, quota::QuotaError::UserLimitExceeded { .. }),
        "expected UserLimitExceeded, got: {err}"
    );
}

/// Real Postgres: team plan (max 20) allows user creation.
#[tokio::test]
#[ignore]
async fn max_users_team_plan_allows_users() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("team-users", "Team Users")
        .await
        .expect("create_tenant");

    let team = store
        .get_plan_by_code("team")
        .await
        .expect("get plan")
        .expect("team plan exists");

    store
        .update_tenant_plan(tenant_id, Some(team.id), None, None, Some("active"), None)
        .await
        .expect("assign team plan");

    // 1 user exists, plan allows 20 — adding 5 more should pass.
    store
        .check_tenant_max_users(tenant_id, 5)
        .await
        .expect("team plan should allow up to 20 users");
}

/// Real Postgres: free plan denies remote models.
#[tokio::test]
#[ignore]
async fn free_plan_denies_remote_models() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("remote-free", "Remote Free")
        .await
        .expect("create_tenant");

    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get plan")
        .expect("free plan exists");

    store
        .update_tenant_plan(tenant_id, Some(free.id), None, None, Some("active"), None)
        .await
        .expect("assign free plan");

    let allowed = store
        .is_remote_models_allowed(tenant_id)
        .await
        .expect("check remote models");
    assert!(!allowed, "free plan must deny remote models");
}

/// Real Postgres: pro plan allows remote models.
#[tokio::test]
#[ignore]
async fn pro_plan_allows_remote_models() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("remote-pro", "Remote Pro")
        .await
        .expect("create_tenant");

    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get plan")
        .expect("pro plan exists");

    store
        .update_tenant_plan(tenant_id, Some(pro.id), None, None, Some("active"), None)
        .await
        .expect("assign pro plan");

    let allowed = store
        .is_remote_models_allowed(tenant_id)
        .await
        .expect("check remote models");
    assert!(allowed, "pro plan must allow remote models");
}

/// Real Postgres: grandfathered tenant allows remote models.
#[tokio::test]
#[ignore]
async fn grandfathered_tenant_allows_remote_models() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("remote-gf", "Remote Grandfathered")
        .await
        .expect("create_tenant");

    // No plan — grandfathered.
    let allowed = store
        .is_remote_models_allowed(tenant_id)
        .await
        .expect("check remote models");
    assert!(allowed, "grandfathered tenant must allow remote models");
}

// ── Rollup-based token-budget enforcement (P14-T4) ───────────────────────────

/// Real Postgres: a tenant whose current-month rollup is at/over the plan's
/// token budget is rejected by the rollup-based hot-path check, with an upsell.
#[tokio::test]
#[ignore]
async fn token_budget_rollup_rejects_over_budget_tenant() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("tok-over", "Token Over Budget")
        .await
        .expect("create_tenant");

    // Free plan: 10_000 token budget.
    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get plan")
        .expect("free plan exists");
    store
        .update_tenant_plan(tenant_id, Some(free.id), None, None, Some("active"), None)
        .await
        .expect("assign free plan");

    // Seed the current-month rollup over budget via the production write path.
    // Two events of 6_000 prompt tokens ⇒ 12_000 this month > 10_000 budget.
    store
        .insert_usage_event(&usage_event_now(tenant_id, 6_000))
        .await
        .expect("insert usage 1");
    store
        .insert_usage_event(&usage_event_now(tenant_id, 6_000))
        .await
        .expect("insert usage 2");

    // The O(1) rollup read agrees we are over budget.
    let rollup = store
        .get_current_month_token_rollup(tenant_id)
        .await
        .expect("rollup");
    assert_eq!(rollup, 12_000, "rollup should reflect both events");

    // Hot-path enforcement rejects with a TokensExceeded carrying an upsell.
    let err = store
        .check_plan_token_budget_rollup(tenant_id)
        .await
        .unwrap_err();
    let quota_err: &quota::QuotaError = err.downcast_ref().expect("should be QuotaError");
    assert!(
        matches!(quota_err, quota::QuotaError::TokensExceeded { .. }),
        "expected TokensExceeded, got: {err}"
    );
    let upsell = quota_err.upsell_message().expect("should have upsell");
    assert!(
        upsell.contains("Upgrade from free to pro"),
        "expected free→pro upsell, got: {upsell}"
    );
}

/// Real Postgres: a tenant under its monthly token budget passes the
/// rollup-based check (no usage yet, and partial usage below the cap).
#[tokio::test]
#[ignore]
async fn token_budget_rollup_allows_under_budget_tenant() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("tok-under", "Token Under Budget")
        .await
        .expect("create_tenant");

    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get plan")
        .expect("free plan exists");
    store
        .update_tenant_plan(tenant_id, Some(free.id), None, None, Some("active"), None)
        .await
        .expect("assign free plan");

    // No usage yet → rollup is 0 → must pass.
    store
        .check_plan_token_budget_rollup(tenant_id)
        .await
        .expect("zero usage must pass");

    // Use 9_999 of the 10_000 budget — still strictly under, so the next
    // ingest is allowed (boundary: current < budget passes).
    store
        .insert_usage_event(&usage_event_now(tenant_id, 9_999))
        .await
        .expect("insert usage");
    store
        .check_plan_token_budget_rollup(tenant_id)
        .await
        .expect("9_999 < 10_000 budget must pass");
}

/// Real Postgres: a grandfathered tenant (no plan → no budget) is never
/// rejected by the rollup-based check, regardless of usage.
#[tokio::test]
#[ignore]
async fn token_budget_rollup_grandfathered_unlimited() {
    let store = connected_store().await;

    let tenant_id = store
        .create_tenant("tok-gf", "Token Grandfathered")
        .await
        .expect("create_tenant");

    // No plan assigned → None budget → unlimited.
    store
        .insert_usage_event(&usage_event_now(tenant_id, 1_000_000))
        .await
        .expect("insert large usage");

    store
        .check_plan_token_budget_rollup(tenant_id)
        .await
        .expect("grandfathered tenant must never be token-budget rejected");
}
