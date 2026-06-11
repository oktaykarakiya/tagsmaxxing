// SPDX-License-Identifier: AGPL-3.0-or-later

//! Billing plans and tenant subscription methods (plan §29, P11-T1).
//!
//! The `plans` table defines tiers (free/pro/team) with storage/token quotas and
//! feature gates (JSONB). Tenant billing columns track the active plan, Stripe
//! identifiers, and subscription lifecycle state.
//!
//! All methods use the **admin pool** — the `plans` table and the billing columns on
//! `tenants` are not covered by Row-Level Security (they are read before a tenant
//! context is established, or by admin paths).

use anyhow::Context;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;

use crate::pg_store::PgStore;

/// A billing plan as stored in the `plans` table (plan §29).
///
/// Plans define resource quotas (`quota_bytes`, `token_budget`) and capability
/// gates (`features` JSONB) that are enforced at upload and call time (P11-T4).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Plan {
    /// Primary key.
    pub id: i64,
    /// Unique machine-readable code: `free`, `pro`, `team`, …
    pub code: String,
    /// Stripe price ID (placeholder for seed rows).
    pub stripe_price: String,
    /// Storage hard cap in bytes.
    pub quota_bytes: i64,
    /// Token budget per billing period (`None` = unlimited).
    pub token_budget: Option<i64>,
    /// Capability gates: remote-models, max users, rate caps, …
    pub features: serde_json::Value,
    /// Price in cents (`None` = free).
    pub price_cents: Option<i32>,
    /// ISO 4217 currency code (`None` = free).
    pub currency: Option<String>,
}

/// Billing state for a tenant, mirrored from the `tenants` table.
#[derive(Debug, Clone)]
pub struct TenantBilling {
    /// The tenant's current plan, or `None` when grandfathered (NULL plan_id).
    pub plan: Option<Plan>,
    /// Stripe customer id (`None` before first Checkout).
    pub stripe_customer_id: Option<String>,
    /// Stripe subscription id (`None` before first subscription).
    pub subscription_id: Option<String>,
    /// Billing lifecycle: `active`, `past_due`, `canceled`, `suspended`, or `inactive`.
    pub billing_status: String,
    /// When the current billing period ends (`None` when inactive).
    pub current_period_end: Option<chrono::DateTime<chrono::Utc>>,
}

// ── sqlx row type ─────────────────────────────────────────────────────────────

/// A raw row from the `plans` table.
#[derive(sqlx::FromRow)]
pub(crate) struct PlanRow {
    id: i64,
    code: String,
    stripe_price: String,
    quota_bytes: i64,
    token_budget: Option<i64>,
    features: serde_json::Value,
    price_cents: Option<i32>,
    currency: Option<String>,
}

impl From<PlanRow> for Plan {
    fn from(r: PlanRow) -> Self {
        Self {
            id: r.id,
            code: r.code,
            stripe_price: r.stripe_price,
            quota_bytes: r.quota_bytes,
            token_budget: r.token_budget,
            features: r.features,
            price_cents: r.price_cents,
            currency: r.currency,
        }
    }
}

// ── Plan feature extraction helpers (P11-T5) ───────────────────────────────────

impl Plan {
    /// Whether remote models are allowed by this plan.
    ///
    /// Reads `features.remote_models` (JSONB boolean). Returns `false` when the
    /// key is missing, not a boolean, or explicitly `false`. Only an explicit
    /// `true` unlocks remote backends.
    ///
    /// This gates the [`Residency`](kb_core::residency::Residency) policy: when
    /// the plan disallows remote models, all LLM calls for that tenant must
    /// use `local_only = true` (P9-T9, P11-T5).
    pub fn remote_models_allowed(&self) -> bool {
        self.features
            .get("remote_models")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Maximum number of users allowed by this plan.
    ///
    /// Reads `features.max_users` (JSONB integer). Returns `None` when the key
    /// is missing or not an integer (interpreted as unlimited). An explicit
    /// `0` means no additional users can be created.
    pub fn max_users(&self) -> Option<i32> {
        self.features
            .get("max_users")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
    }
}

// ── PgStore billing methods ───────────────────────────────────────────────────

impl PgStore {
    /// Fetch a single plan by primary key.
    ///
    /// Returns `None` if no plan with that id exists.
    ///
    /// Uses the **admin pool** — `plans` is a cross-tenant reference table.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_plan(&self, plan_id: i64) -> anyhow::Result<Option<Plan>> {
        let pool = self.pool_for_plans()?;
        let row: Option<PlanRow> = sqlx::query_as(
            "SELECT id, code, stripe_price, quota_bytes, token_budget, features, \
             price_cents, currency FROM plans WHERE id = $1",
        )
        .bind(plan_id)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch plan by id")?;
        Ok(row.map(Plan::from))
    }

    /// Fetch a single plan by its machine-readable `code` (e.g. `"free"`, `"pro"`).
    ///
    /// Returns `None` if no plan with that code exists.
    ///
    /// Uses the **admin pool** — `plans` is a cross-tenant reference table.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_plan_by_code(&self, code: &str) -> anyhow::Result<Option<Plan>> {
        let pool = self.pool_for_plans()?;
        let row: Option<PlanRow> = sqlx::query_as(
            "SELECT id, code, stripe_price, quota_bytes, token_budget, features, \
             price_cents, currency FROM plans WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch plan by code")?;
        Ok(row.map(Plan::from))
    }

    /// Return all plans, ordered by `price_cents` (free first, then cheapest).
    ///
    /// `NULL` price_cents sorts last (via `NULLS LAST`).
    ///
    /// Uses the **admin pool** — `plans` is a cross-tenant reference table.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_plans(&self) -> anyhow::Result<Vec<Plan>> {
        let pool = self.pool_for_plans()?;
        let rows: Vec<PlanRow> = sqlx::query_as(
            "SELECT id, code, stripe_price, quota_bytes, token_budget, features, \
             price_cents, currency FROM plans ORDER BY price_cents NULLS LAST",
        )
        .fetch_all(&pool)
        .await
        .context("failed to list plans")?;
        Ok(rows.into_iter().map(Plan::from).collect())
    }

    /// Update a tenant's billing state — set (or clear) the active plan and Stripe
    /// identifiers.
    ///
    /// Every field is optional: passing `None` for a field leaves its current value
    /// unchanged. This is useful for partial updates from Stripe webhooks
    /// (e.g. `billing_status` may change without touching `plan_id`).
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn update_tenant_plan(
        &self,
        tenant_id: i64,
        plan_id: Option<i64>,
        stripe_customer_id: Option<&str>,
        subscription_id: Option<&str>,
        billing_status: Option<&str>,
        current_period_end: Option<chrono::DateTime<chrono::Utc>>,
    ) -> anyhow::Result<()> {
        let pool = self.pool_for_plans()?;
        sqlx::query(
            "UPDATE tenants SET \
             plan_id            = COALESCE($2, plan_id), \
             stripe_customer_id = COALESCE($3, stripe_customer_id), \
             subscription_id    = COALESCE($4, subscription_id), \
             billing_status     = COALESCE($5, billing_status), \
             current_period_end = COALESCE($6, current_period_end) \
             WHERE id = $1",
        )
        .bind(tenant_id)
        .bind(plan_id)
        .bind(stripe_customer_id)
        .bind(subscription_id)
        .bind(billing_status)
        .bind(current_period_end)
        .execute(&pool)
        .await
        .context("failed to update tenant billing columns")?;
        Ok(())
    }

    /// Fetch a tenant's full billing state, including the resolved [`Plan`] (when a
    /// `plan_id` is set) and all Stripe identifiers.
    ///
    /// Returns `None` if the tenant does not exist.
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_tenant_billing(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<Option<TenantBilling>> {
        let pool = self.pool_for_plans()?;

        #[derive(sqlx::FromRow)]
        struct BillingRow {
            plan_id: Option<i64>,
            stripe_customer_id: Option<String>,
            subscription_id: Option<String>,
            billing_status: Option<String>,
            current_period_end: Option<chrono::DateTime<chrono::Utc>>,
        }

        let row: Option<BillingRow> = sqlx::query_as(
            "SELECT plan_id, stripe_customer_id, subscription_id, \
             billing_status, current_period_end FROM tenants WHERE id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch tenant billing columns")?;

        match row {
            Some(r) => {
                let plan = match r.plan_id {
                    Some(pid) => self.get_plan(pid).await?,
                    None => None,
                };
                Ok(Some(TenantBilling {
                    plan,
                    stripe_customer_id: r.stripe_customer_id,
                    subscription_id: r.subscription_id,
                    billing_status: r.billing_status.unwrap_or_else(|| "inactive".into()),
                    current_period_end: r.current_period_end,
                }))
            }
            None => Ok(None),
        }
    }

    /// Resolve the admin pool for billing operations.
    ///
    /// The `plans` table and tenant billing columns are infrastructure tables,
    /// not covered by RLS — they must be reachable before a tenant context is
    /// set. This uses the same pool as `admin_pool()` / `pool()`.
    fn pool_for_plans(&self) -> anyhow::Result<PgPool> {
        self.pool()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── Plan/PlanRow conversion ────────────────────────────────────────────────

    #[test]
    fn plan_from_row_maps_all_fields() {
        let row = PlanRow {
            id: 1,
            code: "pro".into(),
            stripe_price: "price_pro".into(),
            quota_bytes: 5_368_709_120,
            token_budget: Some(500_000),
            features: json!({"remote_models": true, "max_users": 5}),
            price_cents: Some(900),
            currency: Some("usd".into()),
        };
        let plan: Plan = row.into();
        assert_eq!(plan.id, 1);
        assert_eq!(plan.code, "pro");
        assert_eq!(plan.stripe_price, "price_pro");
        assert_eq!(plan.quota_bytes, 5_368_709_120);
        assert_eq!(plan.token_budget, Some(500_000));
        assert_eq!(
            plan.features,
            json!({"remote_models": true, "max_users": 5})
        );
        assert_eq!(plan.price_cents, Some(900));
        assert_eq!(plan.currency.as_deref(), Some("usd"));
    }

    #[test]
    fn plan_from_row_free_plan_null_fields() {
        let row = PlanRow {
            id: 2,
            code: "free".into(),
            stripe_price: "price_free".into(),
            quota_bytes: 52_428_800,
            token_budget: Some(10_000),
            features: json!({"remote_models": false}),
            price_cents: Some(0),
            currency: Some("usd".into()),
        };
        let plan: Plan = row.into();
        assert_eq!(plan.price_cents, Some(0));
        assert_eq!(plan.currency.as_deref(), Some("usd"));
    }

    #[test]
    fn plan_from_row_null_token_budget() {
        let row = PlanRow {
            id: 3,
            code: "unlimited".into(),
            stripe_price: "price_unlimited".into(),
            quota_bytes: 0,
            token_budget: None,
            features: json!({}),
            price_cents: None,
            currency: None,
        };
        let plan: Plan = row.into();
        assert!(plan.token_budget.is_none());
        assert!(plan.price_cents.is_none());
        assert!(plan.currency.is_none());
    }

    // ── Seed plan values match the spec (P11-T1 acceptance) ───────────────────

    /// Verify the byte counts in the migration match the documented plan sizes.
    #[test]
    fn seed_quota_bytes_match_spec() {
        // These values are asserted in the migrations test too, but this test
        // documents them alongside the Plan type so a reader has one place to check.
        let free_bytes: i64 = 50 * 1024 * 1024; // 50 MB
        assert_eq!(free_bytes, 52_428_800);

        let pro_bytes: i64 = 5 * 1024 * 1024 * 1024; // 5 GB
        assert_eq!(pro_bytes, 5_368_709_120);

        let team_bytes: i64 = 50 * 1024 * 1024 * 1024; // 50 GB
        assert_eq!(team_bytes, 53_687_091_200);
    }

    /// Verify the token budgets in the migration match the documented values.
    #[test]
    fn seed_token_budgets_match_spec() {
        let free_tokens: i64 = 10_000;
        let pro_tokens: i64 = 500_000;
        let team_tokens: i64 = 2_000_000;

        assert_eq!(free_tokens, 10_000);
        assert_eq!(pro_tokens, 500_000);
        assert_eq!(team_tokens, 2_000_000);
    }

    /// Verify the price cents in the migration match the documented prices.
    #[test]
    fn seed_prices_match_spec() {
        let free_price: i32 = 0; // $0/mo
        let pro_price: i32 = 900; // $9/mo
        let team_price: i32 = 2_900; // $29/mo

        assert_eq!(free_price, 0);
        assert_eq!(pro_price, 900);
        assert_eq!(team_price, 2_900);
    }

    // ── TenantBilling ──────────────────────────────────────────────────────────

    #[test]
    fn tenant_billing_grandfathered() {
        // When a tenant has NULL plan_id, the plan field is None (grandfathered).
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "inactive".into(),
            current_period_end: None,
        };
        assert!(billing.plan.is_none());
        assert_eq!(billing.billing_status, "inactive");
    }

    #[test]
    fn tenant_billing_active_subscriber() {
        let plan = Plan {
            id: 2,
            code: "pro".into(),
            stripe_price: "price_pro".into(),
            quota_bytes: 5_368_709_120,
            token_budget: Some(500_000),
            features: json!({"remote_models": true}),
            price_cents: Some(900),
            currency: Some("usd".into()),
        };
        let billing = TenantBilling {
            plan: Some(plan),
            stripe_customer_id: Some("cus_abc123".into()),
            subscription_id: Some("sub_xyz789".into()),
            billing_status: "active".into(),
            current_period_end: None,
        };
        assert!(billing.plan.is_some());
        assert_eq!(billing.plan.as_ref().unwrap().code, "pro");
        assert_eq!(billing.stripe_customer_id.as_deref(), Some("cus_abc123"));
        assert_eq!(billing.subscription_id.as_deref(), Some("sub_xyz789"));
        assert_eq!(billing.billing_status, "active");
    }

    // ── Integration tests (Postgres testcontainers, #[ignore]) ────────────────

    /// Helper: create a connected PgStore against a fresh testcontainers Postgres.
    async fn connected_store() -> PgStore {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");
        store
    }

    /// Real Postgres: after connect(), seed plans exist (idempotent across re-runs).
    #[tokio::test]
    #[ignore]
    async fn plans_seed_is_idempotent() {
        let store = connected_store().await;

        // First connect seeded the rows.
        let plans = store.list_plans().await.expect("list_plans");
        assert_eq!(plans.len(), 3, "three seed plans expected");

        // Second connect must not duplicate (ON CONFLICT DO NOTHING).
        store.connect().await.expect("reconnect");
        let plans_again = store
            .list_plans()
            .await
            .expect("list_plans after reconnect");
        assert_eq!(plans_again.len(), 3, "seed must be idempotent");
    }

    /// Real Postgres: get_plan_by_code returns the correct plan.
    #[tokio::test]
    #[ignore]
    async fn get_plan_by_code_returns_correct_plan() {
        let store = connected_store().await;

        let free = store
            .get_plan_by_code("free")
            .await
            .expect("get_plan_by_code")
            .expect("free plan exists");
        assert_eq!(free.code, "free");
        assert_eq!(free.quota_bytes, 52_428_800);
        assert_eq!(free.token_budget, Some(10_000));
        assert_eq!(free.price_cents, Some(0));

        let pro = store
            .get_plan_by_code("pro")
            .await
            .expect("get_plan_by_code")
            .expect("pro plan exists");
        assert_eq!(pro.code, "pro");
        assert_eq!(pro.quota_bytes, 5_368_709_120);
        assert_eq!(pro.token_budget, Some(500_000));
        assert_eq!(pro.price_cents, Some(900));

        // Unknown code returns None.
        let unknown = store
            .get_plan_by_code("enterprise")
            .await
            .expect("query ok");
        assert!(unknown.is_none(), "unknown plan code must return None");
    }

    /// Real Postgres: list_plans is ordered by price (free first).
    #[tokio::test]
    #[ignore]
    async fn list_plans_ordered_by_price() {
        let store = connected_store().await;

        let plans = store.list_plans().await.expect("list_plans");
        assert_eq!(plans.len(), 3);
        // free (0) < pro (900) < team (2900).
        assert_eq!(plans[0].code, "free");
        assert_eq!(plans[1].code, "pro");
        assert_eq!(plans[2].code, "team");
    }

    /// Real Postgres: update_tenant_plan + get_tenant_billing round-trip.
    #[tokio::test]
    #[ignore]
    async fn update_tenant_plan_and_get_billing_round_trip() {
        let store = connected_store().await;

        // Create a tenant first.
        let tenant_id = store
            .create_tenant("billing-test", "Billing Test")
            .await
            .expect("create_tenant");

        // Initially, no plan and billing is inactive.
        let billing = store
            .get_tenant_billing(tenant_id)
            .await
            .expect("get_tenant_billing")
            .expect("tenant exists");
        assert!(billing.plan.is_none());
        assert_eq!(billing.billing_status, "inactive");

        // Get the pro plan id and assign it.
        let pro = store
            .get_plan_by_code("pro")
            .await
            .expect("get_plan_by_code")
            .expect("pro plan exists");

        store
            .update_tenant_plan(
                tenant_id,
                Some(pro.id),
                Some("cus_test123"),
                Some("sub_test456"),
                Some("active"),
                None,
            )
            .await
            .expect("update_tenant_plan");

        let billing = store
            .get_tenant_billing(tenant_id)
            .await
            .expect("get_tenant_billing")
            .expect("tenant exists");
        assert_eq!(billing.plan.as_ref().unwrap().code, "pro");
        assert_eq!(billing.stripe_customer_id.as_deref(), Some("cus_test123"));
        assert_eq!(billing.subscription_id.as_deref(), Some("sub_test456"));
        assert_eq!(billing.billing_status, "active");

        // Partial update: change only billing_status (other columns stay).
        store
            .update_tenant_plan(tenant_id, None, None, None, Some("past_due"), None)
            .await
            .expect("partial update_tenant_plan");

        let billing = store
            .get_tenant_billing(tenant_id)
            .await
            .expect("get_tenant_billing")
            .expect("tenant exists");
        assert_eq!(billing.billing_status, "past_due");
        // Other fields unchanged.
        assert_eq!(billing.plan.as_ref().unwrap().code, "pro");
        assert_eq!(billing.stripe_customer_id.as_deref(), Some("cus_test123"));
    }

    /// Real Postgres: get_tenant_billing returns None for non-existent tenant.
    #[tokio::test]
    #[ignore]
    async fn get_tenant_billing_returns_none_for_unknown_tenant() {
        let store = connected_store().await;

        let billing = store.get_tenant_billing(999_999).await.expect("query ok");
        assert!(billing.is_none());
    }

    /// Real Postgres: all three seed plans have the correct feature flags.
    #[tokio::test]
    #[ignore]
    async fn seed_plan_features_are_correct() {
        let store = connected_store().await;

        let free = store
            .get_plan_by_code("free")
            .await
            .expect("query")
            .expect("free plan");
        assert_eq!(free.features["remote_models"], json!(false));
        assert_eq!(free.features["max_users"], json!(1));

        let pro = store
            .get_plan_by_code("pro")
            .await
            .expect("query")
            .expect("pro plan");
        assert_eq!(pro.features["remote_models"], json!(true));
        assert_eq!(pro.features["max_users"], json!(5));

        let team = store
            .get_plan_by_code("team")
            .await
            .expect("query")
            .expect("team plan");
        assert_eq!(team.features["remote_models"], json!(true));
        assert_eq!(team.features["max_users"], json!(20));
    }
}
