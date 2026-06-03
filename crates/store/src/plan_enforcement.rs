//! Plan-driven quota enforcement on [`PgStore`] — storage, tokens, users, and
//! feature gates (plan §29, P11-T5).
//!
//! These methods resolve a tenant's plan (via `plan_id` FK → `plans`),
//! apply admin quota overrides (when set), and enforce:
//! - Storage quota (413 with upsell)
//! - Token budget (429 with upsell)
//! - `max_users` from `plan.features` (403 with upsell)
//! - `remote_models` from `plan.features` (gates remote backends)
//!
//! All methods use the **admin pool** when reading `plans` or `tenants`
//! billing/override columns; usage-sum queries go through the app pool
//! with tenant-scoped transactions.

use anyhow::Context;
use kb_core::quota;

use crate::pg_store::PgStore;
use crate::plans::Plan;

impl PgStore {
    // ── Quota override columns ──────────────────────────────────────────────────

    /// Read a tenant's admin quota overrides from the `tenants` table.
    ///
    /// Returns `(quota_override_bytes, quota_override_tokens)`. Both are `None`
    /// when no override is set (the plan-derived values apply). A non-NULL
    /// override **takes precedence** over the plan value.
    ///
    /// Uses the **admin pool** — the override columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_tenant_quota_overrides(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<(Option<i64>, Option<i64>)> {
        let pool = self.pool()?;
        let (override_bytes, override_tokens): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT quota_override_bytes, quota_override_tokens \
                 FROM tenants WHERE id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&pool)
        .await
        .context("failed to look up tenant quota overrides")?
        .ok_or_else(|| anyhow::anyhow!("tenant {tenant_id} not found"))?;
        Ok((override_bytes, override_tokens))
    }

    /// Set admin quota overrides on a tenant, with an audit trail.
    ///
    /// When both values are `None`, the override columns are cleared to NULL
    /// (reverting to plan-derived limits). A non-NULL value **takes
    /// precedence** over the plan value at enforcement time.
    ///
    /// Records an `admin_override_quota` audit event when at least one value
    /// is provided (including explicit `None` to clear an existing override).
    ///
    /// Uses the **admin pool** — the override columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn set_admin_quota_override(
        &self,
        tenant_id: i64,
        quota_override_bytes: Option<i64>,
        quota_override_tokens: Option<i64>,
        actor_user_id: i64,
    ) -> anyhow::Result<()> {
        let pool = self.pool()?;

        sqlx::query(
            "UPDATE tenants \
             SET quota_override_bytes  = $2, \
                 quota_override_tokens = $3 \
             WHERE id = $1",
        )
        .bind(tenant_id)
        .bind(quota_override_bytes)
        .bind(quota_override_tokens)
        .execute(&pool)
        .await
        .context("failed to set admin quota override")?;

        // Audit: record the override action.
        let details = serde_json::json!({
            "quota_override_bytes": quota_override_bytes,
            "quota_override_tokens": quota_override_tokens,
        });
        self.admin_insert_audit_event(
            actor_user_id,
            tenant_id,
            "admin_override_quota",
            "tenant",
            Some(tenant_id),
            &details,
        )
        .await
        .context("failed to write admin_override_quota audit event")?;

        Ok(())
    }

    // ── Plan resolution for enforcement ─────────────────────────────────────────

    /// Resolve the effective storage and token limits for a tenant, taking the
    /// subscribed plan and admin overrides into account.
    ///
    /// Returns `(effective_quota_bytes, effective_token_budget, plan_code, upsell_plan_code)`:
    /// - `effective_quota_bytes`: the hard storage cap (None = unlimited from
    ///   grandfathered tenant or plan with unlimited quota, though no seed plan
    ///   has unlimited storage).
    /// - `effective_token_budget`: the token cap (None = unlimited).
    /// - `plan_code`: the tenant's current plan code, or None if grandfathered.
    /// - `upsell_plan_code`: the next-higher-tier plan code for upsell prompts,
    ///   or None if no upgrade path exists.
    ///
    /// Resolution order (plan §29, P11-T5):
    /// 1. Admin quota override takes precedence when set.
    /// 2. Otherwise, the subscribed plan's values are used.
    /// 3. A NULL `plan_id` (grandfathered tenant) means no plan → unlimited.
    ///
    /// Uses the **admin pool** for plans + tenant billing columns.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn resolve_effective_quotas(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<(Option<i64>, Option<i64>, Option<String>, Option<String>)> {
        let (override_bytes, override_tokens) = self.get_tenant_quota_overrides(tenant_id).await?;

        let billing = match self.get_tenant_billing(tenant_id).await? {
            Some(b) => b,
            None => {
                // Tenant doesn't exist — effectively no limits.
                return Ok((None, None, None, None));
            }
        };

        let plan = billing.plan;
        let plan_code = plan.as_ref().map(|p| p.code.clone());

        // Effective quotas: override takes precedence over plan.
        let eff_bytes =
            quota::resolve_effective_quota(plan.as_ref().map(|p| p.quota_bytes), override_bytes);
        let eff_tokens = quota::resolve_effective_quota(
            plan.as_ref().and_then(|p| p.token_budget),
            override_tokens,
        );

        // Upsell plan: find the next tier up from the current plan.
        let upsell = if let Some(ref current_plan) = plan {
            self.get_upsell_plan(current_plan).await?
        } else {
            None
        };

        Ok((eff_bytes, eff_tokens, plan_code, upsell))
    }

    /// Find the next-higher-tier plan for upsell suggestions.
    ///
    /// Returns the code of the plan with the smallest `price_cents` that is
    /// greater than the current plan's `price_cents`. Returns `None` if the
    /// current plan has no price (free-tier or grandfathered) or is already
    /// the highest tier.
    async fn get_upsell_plan(&self, current: &Plan) -> anyhow::Result<Option<String>> {
        let current_price = current.price_cents.unwrap_or(0);
        let pool = self.pool()?;
        // Pick the plan with the smallest price_cents above the current.
        let code: Option<String> = sqlx::query_scalar(
            "SELECT code FROM plans \
             WHERE price_cents > $1 \
             ORDER BY price_cents ASC \
             LIMIT 1",
        )
        .bind(current_price)
        .fetch_optional(&pool)
        .await
        .context("failed to look up upsell plan")?;
        Ok(code)
    }

    // ── Storage quota enforcement ───────────────────────────────────────────────

    /// Check whether adding `additional_bytes` would exceed the tenant's
    /// plan-driven storage quota (including admin overrides).
    ///
    /// Resolves the plan + overrides, queries current usage, and delegates
    /// to [`kb_core::quota::check_bytes_quota`] for the pure math. The
    /// returned [`QuotaError`] carries the current plan code and an upsell
    /// suggestion (next tier up) when the tenant has a plan.
    ///
    /// This is a **best-effort** check — it is not transactional, so
    /// concurrent uploads may briefly exceed the cap.
    ///
    /// # Errors
    /// - Returns a [`QuotaError::StorageExceeded`] if the quota would be
    ///   exceeded (with plan + upsell context).
    /// - Returns a generic database error if queries fail.
    pub async fn check_plan_storage_quota(
        &self,
        tenant_id: i64,
        additional_bytes: i64,
    ) -> anyhow::Result<()> {
        let (eff_bytes, _eff_tokens, plan_code, upsell) =
            self.resolve_effective_quotas(tenant_id).await?;

        let current = self.get_storage_usage(tenant_id).await?;

        // Use the pure math function; enrich with plan context on failure.
        quota::check_bytes_quota(current, eff_bytes, additional_bytes)
            .map_err(|e| enrich_quota_error(e, plan_code.as_deref(), upsell.as_deref()))?;

        Ok(())
    }

    // ── Token budget enforcement ────────────────────────────────────────────────

    /// Check whether adding `additional_tokens` would exceed the tenant's
    /// plan-driven token budget (including admin overrides).
    ///
    /// Resolves the plan + overrides, queries current usage across all
    /// `usage_events`, and delegates to [`kb_core::quota::check_token_quota`]
    /// for the pure math. The returned [`QuotaError`] carries the current plan
    /// code and an upsell suggestion when the tenant has a plan.
    ///
    /// # Errors
    /// - Returns a [`QuotaError::TokensExceeded`] if the budget would be
    ///   exceeded (with plan + upsell context).
    /// - Returns a generic database error if queries fail.
    pub async fn check_plan_token_budget(
        &self,
        tenant_id: i64,
        additional_tokens: i64,
    ) -> anyhow::Result<()> {
        let (_eff_bytes, eff_tokens, plan_code, upsell) =
            self.resolve_effective_quotas(tenant_id).await?;

        let current = self.get_token_usage(tenant_id).await?;

        quota::check_token_quota(current, eff_tokens, additional_tokens)
            .map_err(|e| enrich_quota_error(e, plan_code.as_deref(), upsell.as_deref()))?;

        Ok(())
    }

    // ── Max users enforcement ───────────────────────────────────────────────────

    /// Check whether creating `additional_users` would exceed the tenant's
    /// plan `max_users` limit.
    ///
    /// Resolves the plan, counts existing users, and delegates to
    /// [`kb_core::quota::check_user_limit`] for the pure math. The returned
    /// [`QuotaError`] carries the current plan code and an upsell suggestion.
    ///
    /// Uses the **admin pool** for user counting (the users table is not RLS-gated
    /// for cross-tenant admin operations) and for plan resolution.
    ///
    /// # Errors
    /// - Returns a [`QuotaError::UserLimitExceeded`] if the limit would be
    ///   exceeded (with plan + upsell context).
    /// - Returns a generic database error if queries fail.
    pub async fn check_tenant_max_users(
        &self,
        tenant_id: i64,
        additional_users: i64,
    ) -> anyhow::Result<()> {
        let billing = match self.get_tenant_billing(tenant_id).await? {
            Some(b) => b,
            None => return Ok(()), // Tenant doesn't exist yet — no limit.
        };

        let plan = billing.plan;
        let plan_code = plan.as_ref().map(|p| p.code.clone());
        let max_users = plan.as_ref().and_then(|p| p.max_users());

        let current = self.admin_user_count(tenant_id).await?;

        let upsell = match &plan {
            Some(p) => self.get_upsell_plan(p).await?,
            None => None,
        };

        quota::check_user_limit(current, max_users, additional_users)
            .map_err(|e| enrich_quota_error(e, plan_code.as_deref(), upsell.as_deref()))?;

        Ok(())
    }

    // ── Remote models feature gate ──────────────────────────────────────────────

    /// Check whether the tenant's plan allows remote model backends.
    ///
    /// Returns `true` when:
    /// - The tenant has no plan (grandfathered → no restrictions), OR
    /// - The plan's `features.remote_models` is explicitly `true` (pro/team).
    ///
    /// Returns `false` when the plan's `features.remote_models` is `false`
    /// (free plan), meaning all LLM calls must use `local_only = true`.
    ///
    /// Uses the **admin pool** for plan resolution.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn is_remote_models_allowed(&self, tenant_id: i64) -> anyhow::Result<bool> {
        let billing = match self.get_tenant_billing(tenant_id).await? {
            Some(b) => b,
            // Tenant doesn't exist — default to allowed (no restriction).
            None => return Ok(true),
        };

        match billing.plan {
            Some(plan) => Ok(plan.remote_models_allowed()),
            // Grandfathered tenant — no restrictions.
            None => Ok(true),
        }
    }

    // ── Plan per-minute request rate cap ──────────────────────────────────────────────

    /// Resolve the tenant's plan per-minute request rate cap.
    ///
    /// Returns `Some(n)` where `n` is the maximum requests per minute allowed by
    /// the plan's `features.rate_caps.per_minute`. Returns `None` when:
    /// - The tenant has no plan (grandfathered → unlimited), OR
    /// - The plan has no `rate_caps.per_minute` key (treated as unlimited).
    ///
    /// Uses the **admin pool** for plan resolution.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn resolve_rate_cap(&self, tenant_id: i64) -> anyhow::Result<Option<i64>> {
        let billing = match self.get_tenant_billing(tenant_id).await? {
            Some(b) => b,
            None => return Ok(None), // Tenant doesn't exist — unlimited.
        };

        match billing.plan {
            Some(plan) => Ok(plan.per_minute_rate_cap()),
            // Grandfathered tenant — no restrictions.
            None => Ok(None),
        }
    }
}

// ── Plan feature extraction helpers (rate caps) ──────────────────────────────────

impl Plan {
    /// Per-minute request rate cap defined by this plan.
    ///
    /// Reads `features.rate_caps.per_minute` (JSONB integer). Returns `None` when
    /// the key is missing or not an integer (interpreted as unlimited). An explicit
    /// `0` means no requests are allowed (effectively a locked plan).
    pub fn per_minute_rate_cap(&self) -> Option<i64> {
        self.features
            .get("rate_caps")
            .and_then(|rc| rc.get("per_minute"))
            .and_then(|v| v.as_i64())
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Enrich a [`QuotaError`] with plan context fields after the pure math
/// function produced an error with `None` plan fields.
///
/// This is a free function (not a method) so it can be used from any
/// enforcement method without borrowing `self` across an await point.
fn enrich_quota_error(
    err: kb_core::quota::QuotaError,
    plan_code: Option<&str>,
    upsell_plan_code: Option<&str>,
) -> anyhow::Error {
    use kb_core::quota::QuotaError;

    let plan = plan_code.map(String::from);
    let upsell = upsell_plan_code.map(String::from);

    let enriched = match err {
        QuotaError::StorageExceeded {
            current,
            additional,
            total,
            limit,
            ..
        } => QuotaError::StorageExceeded {
            current,
            additional,
            total,
            limit,
            plan_code: plan,
            upsell_plan_code: upsell,
        },
        QuotaError::TokensExceeded {
            current,
            additional,
            total,
            limit,
            ..
        } => QuotaError::TokensExceeded {
            current,
            additional,
            total,
            limit,
            plan_code: plan,
            upsell_plan_code: upsell,
        },
        QuotaError::UserLimitExceeded { current, limit, .. } => QuotaError::UserLimitExceeded {
            current,
            limit,
            plan_code: plan,
            upsell_plan_code: upsell,
        },
    };

    anyhow::Error::from(enriched)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Build a `PgStore` that is not connected — every method must return a
    /// "not connected" error rather than panicking.
    fn unconnected_store() -> PgStore {
        PgStore::new("postgres://localhost/db")
    }

    // ── Plan feature extraction (pure logic) ───────────────────────────────

    #[test]
    fn remote_models_true() {
        let plan = Plan {
            id: 1,
            code: "pro".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: Some(1000),
            features: json!({"remote_models": true, "max_users": 5}),
            price_cents: Some(900),
            currency: Some("usd".into()),
        };
        assert!(plan.remote_models_allowed());
    }

    #[test]
    fn remote_models_false() {
        let plan = Plan {
            id: 2,
            code: "free".into(),
            stripe_price: "p".into(),
            quota_bytes: 50,
            token_budget: Some(100),
            features: json!({"remote_models": false}),
            price_cents: Some(0),
            currency: Some("usd".into()),
        };
        assert!(!plan.remote_models_allowed());
    }

    #[test]
    fn remote_models_missing_key_defaults_false() {
        let plan = Plan {
            id: 3,
            code: "custom".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: None,
            features: json!({}),
            price_cents: None,
            currency: None,
        };
        // No remote_models key → default false (safe by default).
        assert!(!plan.remote_models_allowed());
    }

    #[test]
    fn remote_models_non_bool_defaults_false() {
        let plan = Plan {
            id: 4,
            code: "bad".into(),
            stripe_price: "p".into(),
            quota_bytes: 0,
            token_budget: None,
            features: json!({"remote_models": "yes"}),
            price_cents: None,
            currency: None,
        };
        // "yes" is a string, not a bool → default false.
        assert!(!plan.remote_models_allowed());
    }

    #[test]
    fn max_users_extracted() {
        let plan = Plan {
            id: 1,
            code: "team".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: None,
            features: json!({"max_users": 20, "remote_models": true}),
            price_cents: Some(2900),
            currency: Some("usd".into()),
        };
        assert_eq!(plan.max_users(), Some(20));
    }

    #[test]
    fn max_users_missing_key_returns_none() {
        let plan = Plan {
            id: 2,
            code: "custom".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: None,
            features: json!({"remote_models": true}),
            price_cents: Some(900),
            currency: None,
        };
        assert_eq!(plan.max_users(), None);
    }

    #[test]
    fn max_users_non_int_returns_none() {
        let plan = Plan {
            id: 3,
            code: "bad".into(),
            stripe_price: "p".into(),
            quota_bytes: 0,
            token_budget: None,
            features: json!({"max_users": "twenty"}),
            price_cents: None,
            currency: None,
        };
        assert_eq!(plan.max_users(), None);
    }

    #[test]
    fn max_users_zero_is_valid() {
        let plan = Plan {
            id: 4,
            code: "locked".into(),
            stripe_price: "p".into(),
            quota_bytes: 0,
            token_budget: None,
            features: json!({"max_users": 0, "remote_models": false}),
            price_cents: None,
            currency: None,
        };
        assert_eq!(plan.max_users(), Some(0));
    }

    // ── DB-not-connected tests ────────────────────────────────────────────

    #[tokio::test]
    async fn get_tenant_quota_overrides_errors_before_connect() {
        let store = unconnected_store();
        let err = store.get_tenant_quota_overrides(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn set_admin_quota_override_errors_before_connect() {
        let store = unconnected_store();
        let err = store
            .set_admin_quota_override(1, Some(100), None, 42)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_effective_quotas_errors_before_connect() {
        let store = unconnected_store();
        let err = store.resolve_effective_quotas(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn check_plan_storage_quota_errors_before_connect() {
        let store = unconnected_store();
        let err = store.check_plan_storage_quota(1, 100).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn check_plan_token_budget_errors_before_connect() {
        let store = unconnected_store();
        let err = store.check_plan_token_budget(1, 500).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn check_tenant_max_users_errors_before_connect() {
        let store = unconnected_store();
        let err = store.check_tenant_max_users(1, 1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn is_remote_models_allowed_errors_before_connect() {
        let store = unconnected_store();
        let err = store.is_remote_models_allowed(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    // ── per_minute_rate_cap (Plan) ────────────────────────────────────────

    #[test]
    fn rate_cap_extracted_from_features() {
        let plan = Plan {
            id: 1,
            code: "pro".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: None,
            features: json!({"rate_caps": {"per_minute": 30}, "remote_models": true}),
            price_cents: Some(900),
            currency: Some("usd".into()),
        };
        assert_eq!(plan.per_minute_rate_cap(), Some(30));
    }

    #[test]
    fn rate_cap_missing_key_returns_none() {
        let plan = Plan {
            id: 2,
            code: "custom".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: None,
            features: json!({"remote_models": true}),
            price_cents: None,
            currency: None,
        };
        assert_eq!(plan.per_minute_rate_cap(), None);
    }

    #[test]
    fn rate_cap_non_int_returns_none() {
        let plan = Plan {
            id: 3,
            code: "bad".into(),
            stripe_price: "p".into(),
            quota_bytes: 0,
            token_budget: None,
            features: json!({"rate_caps": {"per_minute": "fast"}}),
            price_cents: None,
            currency: None,
        };
        assert_eq!(plan.per_minute_rate_cap(), None);
    }

    #[test]
    fn rate_cap_zero_is_valid() {
        let plan = Plan {
            id: 4,
            code: "locked".into(),
            stripe_price: "p".into(),
            quota_bytes: 0,
            token_budget: None,
            features: json!({"rate_caps": {"per_minute": 0}}),
            price_cents: None,
            currency: None,
        };
        assert_eq!(plan.per_minute_rate_cap(), Some(0));
    }

    #[test]
    fn rate_cap_missing_per_minute_subkey_returns_none() {
        let plan = Plan {
            id: 5,
            code: "odd".into(),
            stripe_price: "p".into(),
            quota_bytes: 100,
            token_budget: None,
            features: json!({"rate_caps": {"per_hour": 1000}}),
            price_cents: None,
            currency: None,
        };
        assert_eq!(plan.per_minute_rate_cap(), None);
    }

    // ── resolve_rate_cap (PgStore, unconnected) ───────────────────────────

    #[tokio::test]
    async fn resolve_rate_cap_errors_before_connect() {
        let store = unconnected_store();
        let err = store.resolve_rate_cap(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    // ── enrich_quota_error ────────────────────────────────────────────────

    #[test]
    fn enrich_storage_error_adds_plan_context() {
        let original = quota::check_bytes_quota(100, Some(50), 1).unwrap_err();
        let enriched = enrich_quota_error(original, Some("free"), Some("pro"));
        let msg = enriched.to_string();
        assert!(msg.contains("storage quota exceeded"));
        // The QuotaError is inside an anyhow::Error — we can downcast it.
        let quota_err: &quota::QuotaError =
            enriched.downcast_ref().expect("should be a QuotaError");
        let upsell = quota_err.upsell_message().unwrap();
        assert!(upsell.contains("Upgrade from free to pro"));
    }

    #[test]
    fn enrich_token_error_adds_plan_context() {
        let original = quota::check_token_quota(100, Some(50), 1).unwrap_err();
        let enriched = enrich_quota_error(original, Some("free"), Some("pro"));
        let quota_err: &quota::QuotaError =
            enriched.downcast_ref().expect("should be a QuotaError");
        let upsell = quota_err.upsell_message().unwrap();
        assert!(upsell.contains("Upgrade from free to pro"));
    }

    #[test]
    fn enrich_user_limit_error_adds_plan_context() {
        let original = quota::check_user_limit(10, Some(5), 1).unwrap_err();
        let enriched = enrich_quota_error(original, Some("team"), Some("pro"));
        let quota_err: &quota::QuotaError =
            enriched.downcast_ref().expect("should be a QuotaError");
        let upsell = quota_err.upsell_message().unwrap();
        assert!(upsell.contains("Upgrade from team to pro"));
    }

    #[test]
    fn enrich_without_upsell_still_adds_plan_code() {
        let original = quota::check_bytes_quota(100, Some(50), 1).unwrap_err();
        let enriched = enrich_quota_error(original, Some("free"), None);
        let quota_err: &quota::QuotaError =
            enriched.downcast_ref().expect("should be a QuotaError");
        let upsell = quota_err.upsell_message().unwrap();
        assert!(upsell.contains("Consider upgrading your plan"));
    }
}
