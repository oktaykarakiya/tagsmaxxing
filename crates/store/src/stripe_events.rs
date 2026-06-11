// SPDX-License-Identifier: AGPL-3.0-or-later

//! Stripe event idempotency and webhook-lookup methods (plan §29, P11-T3).
//!
//! The [`PgStore`](crate::PgStore) methods in this module handle:
//! * recording processed Stripe event IDs for idempotency (ON CONFLICT skip);
//! * looking up a tenant by their Stripe customer id;
//! * looking up a plan by its Stripe price ID.
//!
//! All methods use the **admin pool** — `stripe_events` and `plans` are
//! infrastructure tables, not tenant-scoped.

use anyhow::Context;

use crate::pg_store::PgStore;
use crate::plans::Plan;

impl PgStore {
    /// Record a processed Stripe event for idempotency.
    ///
    /// Returns `true` when the event was **newly inserted** and should be
    /// processed. Returns `false` when the event already exists (idempotent
    /// skip — Stripe redelivered a duplicate).
    ///
    /// Uses the **admin pool** — `stripe_events` is an infrastructure table,
    /// not tenant-scoped.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn record_stripe_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> anyhow::Result<bool> {
        let pool = self.admin_pool()?;
        let result = sqlx::query(
            "INSERT INTO stripe_events (event_id, event_type) \
             VALUES ($1, $2) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event_id)
        .bind(event_type)
        .execute(&pool)
        .await
        .context("failed to record stripe event for idempotency")?;

        // rows_affected == 1 when the INSERT succeeded (new event);
        // == 0 when ON CONFLICT DO NOTHING skipped (duplicate).
        Ok(result.rows_affected() > 0)
    }

    /// Find a tenant id by their Stripe customer ID.
    ///
    /// Returns `None` when no tenant has the given `stripe_customer_id`.
    ///
    /// Uses the **admin pool** — the `tenants` billing columns are outside RLS
    /// because a webhook arrives without a tenant context.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_tenant_by_stripe_customer(
        &self,
        customer_id: &str,
    ) -> anyhow::Result<Option<i64>> {
        let pool = self.admin_pool()?;
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM tenants WHERE stripe_customer_id = $1")
                .bind(customer_id)
                .fetch_optional(&pool)
                .await
                .context("failed to find tenant by stripe customer id")?;
        Ok(row.map(|r| r.0))
    }

    /// Find a plan by its Stripe price ID.
    ///
    /// Returns `None` when no plan has the given `stripe_price`.
    ///
    /// Uses the **admin pool** — `plans` is a cross-tenant reference table.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_plan_by_stripe_price(
        &self,
        stripe_price: &str,
    ) -> anyhow::Result<Option<Plan>> {
        let pool = self.admin_pool()?;
        let row: Option<crate::plans::PlanRow> = sqlx::query_as(
            "SELECT id, code, stripe_price, quota_bytes, token_budget, features, \
             price_cents, currency FROM plans WHERE stripe_price = $1",
        )
        .bind(stripe_price)
        .fetch_optional(&pool)
        .await
        .context("failed to find plan by stripe price")?;
        Ok(row.map(Plan::from))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ═══════════════════════════════════════════════════════════════════════════
    // Async integration tests against a real Postgres (testcontainers, #[ignore]).
    // ═══════════════════════════════════════════════════════════════════════════

    /// Helper: create a connected PgStore against a fresh testcontainers Postgres.
    async fn connected_store() -> PgStore {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");
        store
    }

    /// Real Postgres: a new event inserts and returns `true`; a duplicate returns
    /// `false`.
    #[tokio::test]
    #[ignore]
    async fn record_stripe_event_new_and_duplicate() {
        let store = connected_store().await;

        let is_new = store
            .record_stripe_event("evt_test_001", "checkout.session.completed")
            .await
            .expect("record new event");
        assert!(is_new, "first delivery must be recorded as new");

        let is_dup = store
            .record_stripe_event("evt_test_001", "checkout.session.completed")
            .await
            .expect("record duplicate event");
        assert!(!is_dup, "second delivery must be skipped as duplicate");
    }

    /// Real Postgres: different event IDs are both new.
    #[tokio::test]
    #[ignore]
    async fn record_stripe_event_different_ids_both_new() {
        let store = connected_store().await;

        assert!(
            store
                .record_stripe_event("evt_a", "invoice.paid")
                .await
                .expect("record evt_a")
        );
        assert!(
            store
                .record_stripe_event("evt_b", "invoice.payment_failed")
                .await
                .expect("record evt_b")
        );
    }

    /// Real Postgres: find_tenant_by_stripe_customer returns None when no match.
    #[tokio::test]
    #[ignore]
    async fn find_tenant_by_stripe_customer_none_for_unknown() {
        let store = connected_store().await;

        let tenant = store
            .find_tenant_by_stripe_customer("cus_nonexistent")
            .await
            .expect("query ok");
        assert!(tenant.is_none());
    }

    /// Real Postgres: find_tenant_by_stripe_customer finds the correct tenant
    /// after its stripe_customer_id is set.
    #[tokio::test]
    #[ignore]
    async fn find_tenant_by_stripe_customer_round_trip() {
        let store = connected_store().await;

        // Create a tenant and set its stripe_customer_id.
        let tenant_id = store
            .create_tenant("customer-test", "Customer Test")
            .await
            .expect("create_tenant");
        store
            .update_tenant_plan(
                tenant_id,
                None,
                Some("cus_roundtrip_test"),
                None,
                None,
                None,
            )
            .await
            .expect("set customer id");

        let found = store
            .find_tenant_by_stripe_customer("cus_roundtrip_test")
            .await
            .expect("query ok")
            .expect("should find tenant");
        assert_eq!(found, tenant_id);
    }

    /// Real Postgres: find_plan_by_stripe_price returns the correct plan and
    /// None for an unknown price.
    #[tokio::test]
    #[ignore]
    async fn find_plan_by_stripe_price_round_trip() {
        let store = connected_store().await;

        let pro = store
            .find_plan_by_stripe_price("price_pro_monthly")
            .await
            .expect("query ok")
            .expect("pro plan should exist");
        assert_eq!(pro.code, "pro");
        assert_eq!(pro.stripe_price, "price_pro_monthly");

        let unknown = store
            .find_plan_by_stripe_price("price_enterprise_yearly")
            .await
            .expect("query ok");
        assert!(unknown.is_none(), "unknown price id must return None");
    }
}
