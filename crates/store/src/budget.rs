//! Budget / cost-tracking queries on [`PgStore`] (plan §26.6, P9-T10).
//!
//! These methods query the running monthly spend (`usage_events.cost_micros`)
//! and the per-tenant budget cap (`tenants.budget_monthly_cents`), then delegate
//! to [`kb_core::budget::check_budget`] for the pure logic. They also set the
//! Prometheus gauges for spend and budget-alert visibility.

use anyhow::Context;
use kb_core::budget::{BudgetExceeded, BudgetStatus, check_budget};

use crate::pg_store::PgStore;

impl PgStore {
    /// Sum `cost_micros` for the current calendar month for a tenant.
    ///
    /// Only rows with `cost_micros IS NOT NULL` are summed — free/local
    /// calls that store NULL are excluded. The total is **best-effort**:
    /// concurrent writes in the same month may be invisible to this
    /// snapshot.
    ///
    /// Uses the app pool + tenant transaction (read-only).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_monthly_cost(&self, tenant_id: i64) -> anyhow::Result<u64> {
        let pool = self.app_pool()?;
        let mut tx = crate::pg_store::begin_tenant_tx(&pool, tenant_id).await?;
        // Sum only non-NULL cost_micros; NULL rows are free/local calls.
        // Filter to the current calendar month via date_trunc.
        //
        // `cost_micros` is BIGINT, so Postgres `SUM()` returns NUMERIC; sqlx 0.8
        // (built without the `bigdecimal`/`rust_decimal` feature) decodes NUMERIC
        // as `String`, which would fail an `Option<i64>` decode. Cast the result
        // back to `bigint` so it decodes as `i64` (a monthly micros total fits an
        // i64 with room to spare). Without the cast this query errored every call.
        let total: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(cost_micros), 0)::bigint \
             FROM usage_events \
             WHERE tenant_id = $1 \
               AND cost_micros IS NOT NULL \
               AND date_trunc('month', created_at) = date_trunc('month', now())",
        )
        .bind(tenant_id)
        .fetch_one(&mut *tx)
        .await
        .context("failed to query monthly cost")?;
        tx.commit()
            .await
            .context("failed to commit get_monthly_cost")?;
        Ok(total.unwrap_or(0) as u64)
    }

    /// Fetch a tenant's monthly spend budget from the `tenants` table.
    ///
    /// Returns `None` when the budget is NULL (unlimited) or if the tenant
    /// does not exist.
    ///
    /// Uses the **admin pool** — the `tenants` table is outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_tenant_budget(&self, tenant_id: i64) -> anyhow::Result<Option<i64>> {
        let pool = self.pool()?;
        let budget: Option<i64> =
            sqlx::query_scalar("SELECT budget_monthly_cents FROM tenants WHERE id = $1")
                .bind(tenant_id)
                .fetch_optional(&pool)
                .await
                .context("failed to look up tenant budget")?
                .flatten();
        Ok(budget)
    }

    /// Check whether this tenant's monthly spend exceeds its budget.
    ///
    /// Queries the current monthly cost and budget cap from the database,
    /// then delegates to [`check_budget`]. Also sets Prometheus gauges
    /// for the tenant's spend and budget status.
    ///
    /// If `hard_cap` is true and the budget is exceeded, returns an
    /// [`Err(BudgetExceeded)`](BudgetExceeded). Otherwise (soft cap),
    /// only the Prometheus gauge is updated — the call is never rejected.
    ///
    /// # Errors
    /// - Returns [`BudgetExceeded`] when `hard_cap` is true and the
    ///   monthly spend exceeds the budget.
    /// - Returns a generic error if the database is not connected or
    ///   the queries fail.
    pub async fn check_tenant_budget(&self, tenant_id: i64, hard_cap: bool) -> anyhow::Result<()> {
        let monthly_cost = self.get_monthly_cost(tenant_id).await?;
        let budget_cents = self.get_tenant_budget(tenant_id).await?;

        // Always update the Prometheus spend gauge, even when under budget.
        kb_metrics::record_tenant_spend_monthly(tenant_id, monthly_cost);

        let status = check_budget(monthly_cost, budget_cents);

        match status {
            BudgetStatus::WithinBudget => {
                kb_metrics::record_tenant_budget_exceeded(tenant_id, false);
                Ok(())
            }
            BudgetStatus::OverBudget {
                current_micros,
                budget_micros,
                usage_pct,
            } => {
                tracing::warn!(
                    tenant_id = tenant_id,
                    current_micros,
                    budget_micros,
                    usage_pct,
                    "tenant monthly spend budget exceeded"
                );
                kb_metrics::record_tenant_budget_exceeded(tenant_id, true);

                if hard_cap {
                    // Also record the spend before erroring out.
                    kb_metrics::record_tenant_spend_monthly(tenant_id, monthly_cost);
                    Err(BudgetExceeded {
                        current_micros,
                        budget_micros,
                        usage_pct,
                    }
                    .into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_store() -> PgStore {
        PgStore::new("postgres://localhost/db")
    }

    // ── Error-before-connect tests ─────────────────────────────────────────

    #[tokio::test]
    async fn get_monthly_cost_errors_before_connect() {
        let store = test_store();
        let err = store.get_monthly_cost(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_tenant_budget_errors_before_connect() {
        let store = test_store();
        let err = store.get_tenant_budget(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn check_tenant_budget_errors_before_connect() {
        let store = test_store();
        let err = store.check_tenant_budget(1, false).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }
}
