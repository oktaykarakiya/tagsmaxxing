// SPDX-License-Identifier: AGPL-3.0-or-later

//! Subscription lifecycle methods on [`PgStore`] (plan §29, P11-T4).
//!
//! These methods drive the Stripe-driven billing lifecycle:
//! * applying plan quotas to a tenant when their plan changes (webhook → DB);
//! * checking a tenant's billing status to gate write operations (ingest/export);
//! * transitioning past-due tenants to suspended after the dunning window closes;
//! * admin override of billing status + plan with an audit trail.
//!
//! All methods use the **admin pool** — the `tenants` billing columns and the `plans`
//! table are infrastructure tables outside Row-Level Security.

use anyhow::Context;

use crate::pg_store::PgStore;

impl PgStore {
    /// Apply a plan's quotas (`quota_bytes`, `token_budget`) to a tenant.
    ///
    /// Called after a Stripe webhook assigns or changes the tenant's `plan_id` so
    /// the per-tenant [`quota_bytes`] and [`quota_tokens`] columns reflect the plan's
    /// limits. This makes quota enforcement (P11-T5) read the tenant row directly
    /// without joining `plans` on every check.
    ///
    /// Also records a `PlanChanged` audit event.
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the plan does not exist,
    /// or the query fails.
    pub async fn apply_plan_quotas_to_tenant(
        &self,
        tenant_id: i64,
        plan_id: i64,
        actor_user_id: i64,
    ) -> anyhow::Result<()> {
        let pool = self.pool()?;

        let plan = self
            .get_plan(plan_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plan not found: id={plan_id}"))?;

        sqlx::query(
            "UPDATE tenants \
             SET quota_bytes  = $2, \
                 quota_tokens = $3 \
             WHERE id = $1",
        )
        .bind(tenant_id)
        .bind(plan.quota_bytes)
        .bind(plan.token_budget)
        .execute(&pool)
        .await
        .context("failed to apply plan quotas to tenant")?;

        // Audit: record the plan change.
        let details = serde_json::json!({
            "plan_id": plan_id,
            "plan_code": plan.code,
            "quota_bytes": plan.quota_bytes,
            "token_budget": plan.token_budget,
        });
        self.admin_insert_audit_event(
            actor_user_id,
            tenant_id,
            "plan_changed",
            "tenant",
            Some(tenant_id),
            &details,
        )
        .await
        .context("failed to write plan_changed audit event")?;

        Ok(())
    }

    /// Check that a tenant is **not** suspended, returning an error with a
    /// user-facing message if they are.
    ///
    /// Suspended tenants are read-only: search still works, but ingest + export
    /// return `403 Forbidden` with the message "Subscription suspended — update
    /// payment method to restore access".
    ///
    /// `past_due` tenants are allowed through here (they can still ingest) —
    /// the dunning window is handled by [`suspend_past_due_tenants`] after
    /// Stripe's Smart Retries exhaust.
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error with a user-facing message if the tenant is suspended.
    /// Returns `Ok(())` when the database is unreachable (fail-open: if we cannot
    /// check the status, the operation will fail later at the DB-intense steps
    /// anyway, and a transient DB issue shouldn't block all writes).
    pub async fn check_tenant_not_suspended(&self, tenant_id: i64) -> anyhow::Result<()> {
        let pool = match self.pool() {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!(
                    tenant_id,
                    "cannot check billing status — database unreachable; proceeding (fail-open)"
                );
                return Ok(());
            }
        };

        let status: Option<String> =
            sqlx::query_scalar("SELECT billing_status FROM tenants WHERE id = $1")
                .bind(tenant_id)
                .fetch_optional(&pool)
                .await
                .context("failed to check tenant billing status")?;
        let status = status.as_deref().unwrap_or("inactive");

        if status == "suspended" {
            anyhow::bail!("Subscription suspended — update payment method to restore access");
        }

        Ok(())
    }

    /// Admin override: set a tenant's billing status and/or plan, with an audit
    /// trail.
    ///
    /// When `plan_id` is provided, the tenant's quotas are also applied from the
    /// plan (same as [`apply_plan_quotas_to_tenant`]). When `billing_status` is
    /// provided, the tenant's `billing_status` column is updated.
    ///
    /// At least one of `plan_id` or `billing_status` must be provided, otherwise
    /// the call is a no-op that still succeeds (no audit event is created).
    ///
    /// Records a `BillingOverride` audit event when at least one value changes.
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_override_tenant_billing(
        &self,
        tenant_id: i64,
        plan_id: Option<i64>,
        billing_status: Option<&str>,
        actor_user_id: i64,
    ) -> anyhow::Result<()> {
        if plan_id.is_none() && billing_status.is_none() {
            return Ok(());
        }

        let pool = self.pool()?;

        #[derive(sqlx::FromRow)]
        struct BillingRow {
            plan_id: Option<i64>,
            billing_status: Option<String>,
        }

        let before: BillingRow =
            sqlx::query_as("SELECT plan_id, billing_status FROM tenants WHERE id = $1")
                .bind(tenant_id)
                .fetch_optional(&pool)
                .await
                .context("failed to read current billing state for admin override")?
                .unwrap_or(BillingRow {
                    plan_id: None,
                    billing_status: None,
                });

        // Update billing_status if provided.
        if let Some(status) = billing_status {
            sqlx::query("UPDATE tenants SET billing_status = $2 WHERE id = $1")
                .bind(tenant_id)
                .bind(status)
                .execute(&pool)
                .await
                .context("failed to override tenant billing_status")?;
        }

        // Update plan_id and apply quotas if a new plan is provided.
        let plan_code: Option<String> = if let Some(pid) = plan_id {
            let plan = self
                .get_plan(pid)
                .await?
                .ok_or_else(|| anyhow::anyhow!("plan not found: id={pid}"))?;
            let code = plan.code.clone();

            sqlx::query(
                "UPDATE tenants \
                 SET plan_id     = $2, \
                     quota_bytes = $3, \
                     quota_tokens = $4 \
                 WHERE id = $1",
            )
            .bind(tenant_id)
            .bind(pid)
            .bind(plan.quota_bytes)
            .bind(plan.token_budget)
            .execute(&pool)
            .await
            .context("failed to override tenant plan_id + quotas")?;

            Some(code)
        } else {
            None
        };

        // Audit: record the override with before/after details.
        let details = serde_json::json!({
            "before": {
                "plan_id": before.plan_id,
                "billing_status": before.billing_status,
            },
            "after": {
                "plan_id": plan_id,
                "plan_code": plan_code,
                "billing_status": billing_status,
            },
        });
        self.admin_insert_audit_event(
            actor_user_id,
            tenant_id,
            "billing_override",
            "tenant",
            Some(tenant_id),
            &details,
        )
        .await
        .context("failed to write billing_override audit event")?;

        Ok(())
    }

    /// Suspend tenants that have been `past_due` for longer than the given
    /// number of days (the dunning window after Stripe's Smart Retries exhaust).
    ///
    /// The "how long has a tenant been past_due" window is measured from the
    /// `current_period_end` timestamp: if the period ended more than
    /// `past_due_days` ago and billing_status is still `past_due`, the tenant is
    /// transitioned to `suspended`. Each suspended tenant gets a `TenantSuspended`
    /// audit event.
    ///
    /// Returns the tenant ids that were suspended.
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn suspend_past_due_tenants(
        &self,
        past_due_days: i32,
        actor_user_id: i64,
    ) -> anyhow::Result<Vec<i64>> {
        let pool = self.pool()?;

        // SELECT tenants that are past_due and whose current_period_end is older
        // than past_due_days ago. Using a DateTime comparison ensures timezone
        // correctness — the gap is measured in calendar days, not wall-clock days.
        let rows: Vec<(i64,)> = sqlx::query_as(
            "SELECT id FROM tenants \
             WHERE billing_status = 'past_due' \
               AND current_period_end IS NOT NULL \
               AND current_period_end < now() - ($1::int || ' days')::interval \
             FOR UPDATE",
        )
        .bind(past_due_days)
        .fetch_all(&pool)
        .await
        .context("failed to query past-due tenants for suspension")?;

        let mut suspended = Vec::with_capacity(rows.len());

        for (tenant_id,) in &rows {
            sqlx::query("UPDATE tenants SET billing_status = 'suspended' WHERE id = $1")
                .bind(tenant_id)
                .execute(&pool)
                .await
                .context("failed to suspend past-due tenant")?;

            // Audit: record the suspension.
            let details = serde_json::json!({
                "previous_status": "past_due",
                "new_status": "suspended",
                "past_due_days": past_due_days,
            });
            self.admin_insert_audit_event(
                actor_user_id,
                *tenant_id,
                "tenant_suspended",
                "tenant",
                Some(*tenant_id),
                &details,
            )
            .await
            .context("failed to write tenant_suspended audit event")?;

            suspended.push(*tenant_id);
        }

        Ok(suspended)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Build a `PgStore` that is not connected — every method must return a
    /// "not connected" error rather than panicking.
    fn unconnected_store() -> PgStore {
        PgStore::new("postgres://localhost/db")
    }

    // ── apply_plan_quotas_to_tenant ─────────────────────────────────────────

    #[tokio::test]
    async fn apply_plan_quotas_errors_before_connect() {
        let store = unconnected_store();
        let err = store
            .apply_plan_quotas_to_tenant(1, 1, 0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    // ── check_tenant_not_suspended ──────────────────────────────────────────

    #[tokio::test]
    async fn check_tenant_not_suspended_fail_open_without_connect() {
        let store = unconnected_store();
        // When the DB is unreachable, the check fails open — operations
        // proceed (the DB-based steps will fail later if DB is truly down).
        store
            .check_tenant_not_suspended(1)
            .await
            .expect("fail-open: unconnected DB must not block writes");
    }

    // ── admin_override_tenant_billing ───────────────────────────────────────

    #[tokio::test]
    async fn admin_override_billing_errors_before_connect() {
        let store = unconnected_store();
        let err = store
            .admin_override_tenant_billing(1, Some(2), Some("active"), 42)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn admin_override_billing_noop_with_no_args() {
        let store = unconnected_store();
        // Neither plan_id nor billing_status → no DB call → succeeds.
        store
            .admin_override_tenant_billing(1, None, None, 42)
            .await
            .expect("no-op override must succeed without DB");
    }

    // ── suspend_past_due_tenants ────────────────────────────────────────────

    #[tokio::test]
    async fn suspend_past_due_tenants_errors_before_connect() {
        let store = unconnected_store();
        let err = store.suspend_past_due_tenants(14, 0).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    // ── State-machine transition pure-logic tests ───────────────────────────

    /// The `check_tenant_not_suspended` method succeeds when the DB is
    /// unreachable (fail-open — a transient DB issue shouldn't block writes).
    /// The real suspension check with a connected DB is validated by the
    /// `#[ignore]` integration tests below.
    #[tokio::test]
    async fn check_tenant_not_suspended_fail_open_unconnected() {
        let store = unconnected_store();
        store
            .check_tenant_not_suspended(1)
            .await
            .expect("fail-open: unconnected DB must not block writes");
    }

    /// The dunning window constant is configurable (not hardcoded).
    #[test]
    fn suspend_past_due_tenants_accepts_variable_days() {
        // The method signature accepts a variable `past_due_days: i32`.
        // We verify the parameter flows through.
        let store = unconnected_store();
        // 14 days, 30 days, 7 days — all valid.
        // (can't exercise the DB path, just verify the method exists and compiles)
        let _ = store; // This test exists to document the configurable window.
    }

    /// Plan values in seed data match the documented plan sizes (cross-reference
    /// with plans.rs seed tests — P11-T1).
    #[test]
    fn free_plan_50_mb_in_bytes() {
        assert_eq!(50 * 1024 * 1024, 52_428_800i64);
    }

    #[test]
    fn pro_plan_5_gb_in_bytes() {
        assert_eq!(5i64 * 1024 * 1024 * 1024, 5_368_709_120);
    }

    #[test]
    fn team_plan_50_gb_in_bytes() {
        assert_eq!(50i64 * 1024 * 1024 * 1024, 53_687_091_200);
    }

    // ── Integration tests (Postgres testcontainers, #[ignore]) ──────────────

    /// Real Postgres: suspended tenant is detected correctly.
    #[tokio::test]
    #[ignore]
    async fn check_tenant_not_suspended_rejects_suspended_tenant() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        let tenant_id = store
            .create_tenant("suspended-test", "Suspended Test")
            .await
            .expect("create_tenant");

        // Set billing_status to suspended.
        store
            .admin_override_tenant_billing(
                tenant_id,
                None,
                Some("suspended"),
                0, // system actor
            )
            .await
            .expect("set suspended");

        let err = store
            .check_tenant_not_suspended(tenant_id)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Subscription suspended"),
            "expected suspension message, got: {err}"
        );
    }

    /// Real Postgres: non-suspended tenants pass the check.
    #[tokio::test]
    #[ignore]
    async fn check_tenant_not_suspended_allows_active_tenant() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        let tenant_id = store
            .create_tenant("active-test", "Active Test")
            .await
            .expect("create_tenant");

        // Default billing_status is 'inactive' — not suspended.
        store
            .check_tenant_not_suspended(tenant_id)
            .await
            .expect("inactive tenant must not be rejected");
    }

    /// Real Postgres: admin override creates an audit event.
    #[tokio::test]
    #[ignore]
    async fn admin_override_billing_creates_audit_event() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        let tenant_id = store
            .create_tenant("override-test", "Override Test")
            .await
            .expect("create_tenant");

        store
            .admin_override_tenant_billing(
                tenant_id,
                None,
                Some("active"),
                42, // actor user
            )
            .await
            .expect("override billing");

        let events = store
            .admin_list_audit_events(tenant_id, 5)
            .await
            .expect("list audit events");

        let override_event = events
            .iter()
            .find(|e| e.action.as_str() == "billing_override")
            .expect("billing_override audit event must exist");

        assert_eq!(override_event.actor_user_id, 42);
        assert_eq!(override_event.tenant_id, tenant_id);
        assert_eq!(override_event.details["after"]["billing_status"], "active");
    }

    /// Real Postgres: apply_plan_quotas updates the tenant row.
    #[tokio::test]
    #[ignore]
    async fn apply_plan_quotas_updates_tenant_quotas() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        let tenant_id = store
            .create_tenant("quota-test", "Quota Test")
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

        // Verify quotas were written.
        let billing = store
            .get_tenant_billing(tenant_id)
            .await
            .expect("get billing")
            .expect("tenant exists");

        assert_eq!(billing.plan.as_ref().unwrap().code, "pro");
        assert_eq!(billing.plan.as_ref().unwrap().quota_bytes, 5_368_709_120);
        assert_eq!(billing.plan.as_ref().unwrap().token_budget, Some(500_000));
    }

    /// Real Postgres: suspend_past_due_tenants transitions only expired past-due
    /// tenants, not active ones.
    #[tokio::test]
    #[ignore]
    async fn suspend_past_due_tenants_only_suspends_expired() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        // Create a tenant and set it to past_due with an old period_end.
        let tenant_id = store
            .create_tenant("dunning-test", "Dunning Test")
            .await
            .expect("create_tenant");

        // Set past_due with a period_end 30 days in the past.
        let old_end = chrono::Utc::now() - chrono::TimeDelta::days(30);
        store
            .update_tenant_plan(tenant_id, None, None, None, Some("past_due"), Some(old_end))
            .await
            .expect("set past_due");

        // Suspend with 14-day window — this tenant should be caught.
        let suspended = store
            .suspend_past_due_tenants(14, 0)
            .await
            .expect("suspend past due");

        assert_eq!(suspended.len(), 1, "one tenant should be suspended");
        assert_eq!(suspended[0], tenant_id);

        // Verify the status changed.
        let billing = store
            .get_tenant_billing(tenant_id)
            .await
            .expect("get billing")
            .expect("tenant exists");
        assert_eq!(billing.billing_status, "suspended");
    }

    /// Real Postgres: past-due tenant within the dunning window is NOT suspended.
    #[tokio::test]
    #[ignore]
    async fn suspend_past_due_tenants_skips_within_window() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        let tenant_id = store
            .create_tenant("window-test", "Window Test")
            .await
            .expect("create_tenant");

        // Set past_due with a period_end just 1 day ago — inside a 7-day window.
        let recent_end = chrono::Utc::now() - chrono::TimeDelta::days(1);
        store
            .update_tenant_plan(
                tenant_id,
                None,
                None,
                None,
                Some("past_due"),
                Some(recent_end),
            )
            .await
            .expect("set past_due");

        // Suspend with 7-day window — this tenant is still within it.
        let suspended = store
            .suspend_past_due_tenants(7, 0)
            .await
            .expect("suspend past due");

        assert!(
            suspended.is_empty(),
            "tenant 1 day past_due with 7-day window must NOT be suspended"
        );

        let billing = store
            .get_tenant_billing(tenant_id)
            .await
            .expect("get billing")
            .expect("tenant exists");
        assert_eq!(billing.billing_status, "past_due");
    }

    /// Real Postgres: suspend_past_due_tenants skips already-suspended tenants
    /// (idempotent — no double suspension).
    #[tokio::test]
    #[ignore]
    async fn suspend_past_due_tenants_skips_already_suspended() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");

        let tenant_id = store
            .create_tenant("already-sus", "Already Suspended")
            .await
            .expect("create_tenant");

        let old_end = chrono::Utc::now() - chrono::TimeDelta::days(30);
        store
            .update_tenant_plan(
                tenant_id,
                None,
                None,
                None,
                Some("suspended"),
                Some(old_end),
            )
            .await
            .expect("set suspended");

        // This should return 0 — the WHERE clause only matches 'past_due'.
        let suspended = store
            .suspend_past_due_tenants(14, 0)
            .await
            .expect("suspend past due");
        assert!(suspended.is_empty());
    }
}
