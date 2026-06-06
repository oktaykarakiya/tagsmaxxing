//! Dashboard query methods on [`PgStore`] (plan §30, ledger P12-T4).
//!
//! These methods supply the `/dashboard` page with per-tenant statistics:
//! document/file/tag counts, recent activity feed, and daily token-usage
//! breakdown. All methods are read-only and scoped to a tenant via
//! [`begin_tenant_tx`] — RLS enforces isolation.

use anyhow::Context;

use crate::pg_store::{PgStore, begin_tenant_tx};

/// A single entry in the dashboard's "recent activity" feed.
#[derive(Debug, Clone)]
pub struct ActivityEvent {
    /// When the event occurred (UTC).
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    /// Human-readable label: `"ingest"`, `"search"`, `"tag"`, `"embed"`, `"rerank"`.
    pub kind: String,
    /// A short description for display in the feed (model name, file count, etc.).
    pub description: String,
}

/// One user's current-month token usage, for the dashboard per-user breakdown
/// (plan §15/§29, P14-T13).
///
/// `user_id` is `None` for the **system bucket** — usage with no attributed user
/// (background jobs, query embeddings; `usage_events.user_id IS NULL`). The
/// `email` is then `None` and the UI labels it "System".
#[derive(Debug, Clone)]
pub struct UserTokenUsage {
    /// The attributed user id, or `None` for the system/no-user bucket.
    pub user_id: Option<i64>,
    /// The user's email (joined from `users`), or `None` for the system bucket
    /// or a user row that no longer exists.
    pub email: Option<String>,
    /// Sum of `prompt_tokens + completion_tokens` for this user this month.
    pub total_tokens: i64,
}

/// A single day's token usage for the daily-breakdown chart.
#[derive(Debug, Clone)]
pub struct DailyTokenUsage {
    /// The date in `YYYY-MM-DD` format.
    pub date: String,
    /// Sum of `prompt_tokens` on that day.
    pub prompt_tokens: i64,
    /// Sum of `completion_tokens` on that day.
    pub completion_tokens: i64,
}

impl PgStore {
    /// Count the number of documents belonging to a tenant.
    ///
    /// Uses the app pool + `begin_tenant_tx` so RLS is enforced.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_document_count(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM documents WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_one(&mut *tx)
                .await
                .context("failed to query document count")?;
        tx.commit()
            .await
            .context("failed to commit get_document_count")?;
        Ok(count)
    }

    /// Count the number of files belonging to a tenant.
    ///
    /// Uses the app pool + `begin_tenant_tx` so RLS is enforced.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_file_count(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM files WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_one(&mut *tx)
                .await
                .context("failed to query file count")?;
        tx.commit()
            .await
            .context("failed to commit get_file_count")?;
        Ok(count)
    }

    /// Count the number of canonical tags belonging to a tenant.
    ///
    /// Uses the app pool + `begin_tenant_tx` so RLS is enforced.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_tag_count(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM tags WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_one(&mut *tx)
                .await
                .context("failed to query tag count")?;
        tx.commit()
            .await
            .context("failed to commit get_tag_count")?;
        Ok(count)
    }

    /// Fetch the most recent activity events for a tenant.
    ///
    /// Queries `usage_events` ordered by `created_at DESC`, mapping each row's
    /// `role` column to a human-readable `kind` label for the activity feed.
    /// Returns at most `limit` entries.
    ///
    /// Uses the app pool + `begin_tenant_tx` so RLS is enforced.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_recent_activity(
        &self,
        tenant_id: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ActivityEvent>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows: Vec<(chrono::DateTime<chrono::Utc>, String, String)> = sqlx::query_as(
            "SELECT created_at, role, model \
             FROM usage_events \
             WHERE tenant_id = $1 \
             ORDER BY created_at DESC \
             LIMIT $2",
        )
        .bind(tenant_id)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .context("failed to query recent activity")?;
        tx.commit()
            .await
            .context("failed to commit get_recent_activity")?;

        Ok(rows
            .into_iter()
            .map(|(occurred_at, role, model)| {
                let kind_label = role_to_activity_kind(&role);
                ActivityEvent {
                    occurred_at,
                    kind: kind_label.to_string(),
                    description: model,
                }
            })
            .collect())
    }

    /// Sum token usage for the **current calendar month**.
    ///
    /// This is the total used against a plan's `token_budget` for the dashboard.
    ///
    /// Uses the app pool + `begin_tenant_tx`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_monthly_token_usage(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(COALESCE(prompt_tokens,0) + COALESCE(completion_tokens,0)), 0)::BIGINT \
             FROM usage_events \
             WHERE tenant_id = $1 \
               AND date_trunc('month', created_at) = date_trunc('month', now())",
        )
        .bind(tenant_id)
        .fetch_one(&mut *tx)
        .await
        .context("failed to query monthly token usage")?;
        tx.commit()
            .await
            .context("failed to commit get_monthly_token_usage")?;
        Ok(total)
    }

    /// Per-user token-usage breakdown for the **current UTC calendar month**.
    ///
    /// Sums `prompt_tokens + completion_tokens` from `usage_events` GROUPED BY
    /// `user_id`, LEFT JOINing `users` for the email. The NULL/system bucket
    /// (background jobs, query embeddings) is **included** as a row with
    /// `user_id = None`. Rows are ordered by usage descending so the heaviest
    /// users appear first.
    ///
    /// Uses the app pool + `begin_tenant_tx` so RLS is enforced — only the
    /// caller's own tenant's events (and users) are visible.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_monthly_token_usage_by_user(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<Vec<UserTokenUsage>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        // GROUP BY the raw user_id (including NULL → system bucket). LEFT JOIN
        // users so a deleted user still shows its usage (email NULL). The total
        // is computed in the aggregate and re-COALESCEd to a non-null BIGINT.
        let rows: Vec<(Option<i64>, Option<String>, i64)> = sqlx::query_as(
            "SELECT ue.user_id, u.email, \
                    COALESCE(SUM(COALESCE(ue.prompt_tokens,0) + COALESCE(ue.completion_tokens,0)), 0)::BIGINT AS total \
             FROM usage_events ue \
             LEFT JOIN users u ON u.id = ue.user_id AND u.tenant_id = ue.tenant_id \
             WHERE ue.tenant_id = $1 \
               AND date_trunc('month', ue.created_at) = date_trunc('month', now()) \
             GROUP BY ue.user_id, u.email \
             ORDER BY total DESC, ue.user_id ASC NULLS LAST",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to query per-user token usage")?;
        tx.commit()
            .await
            .context("failed to commit get_monthly_token_usage_by_user")?;

        Ok(rows
            .into_iter()
            .map(|(user_id, email, total_tokens)| UserTokenUsage {
                user_id,
                email,
                total_tokens,
            })
            .collect())
    }

    /// Fetch a daily token-usage breakdown for the current calendar month.
    ///
    /// Returns one row per day (in `YYYY-MM-DD` format) with sums of prompt
    /// and completion tokens. Only days that have at least one usage event are
    /// included; chart rendering fills in zero-usage days client-side.
    ///
    /// Uses the app pool + `begin_tenant_tx`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_daily_token_usage(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<Vec<DailyTokenUsage>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT to_char(created_at, 'YYYY-MM-DD') AS day, \
                    COALESCE(SUM(prompt_tokens), 0)::BIGINT, \
                    COALESCE(SUM(completion_tokens), 0)::BIGINT \
             FROM usage_events \
             WHERE tenant_id = $1 \
               AND date_trunc('month', created_at) = date_trunc('month', now()) \
             GROUP BY day \
             ORDER BY day",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to query daily token usage")?;
        tx.commit()
            .await
            .context("failed to commit get_daily_token_usage")?;

        Ok(rows
            .into_iter()
            .map(|(date, prompt_tokens, completion_tokens)| DailyTokenUsage {
                date,
                prompt_tokens,
                completion_tokens,
            })
            .collect())
    }
}

/// Map a `usage_events.role` column value to a human-readable activity kind
/// for the dashboard feed.
///
/// Roles that don't match known labels fall through to `"other"`.
fn role_to_activity_kind(role: &str) -> &str {
    match role {
        "tag" => "ingest",
        "embed" => "ingest",
        "chat" => "search",
        "rerank" => "search",
        other => other,
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

    // ── Error-before-connect tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn get_document_count_errors_before_connect() {
        let store = test_store();
        let err = store.get_document_count(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_file_count_errors_before_connect() {
        let store = test_store();
        let err = store.get_file_count(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_tag_count_errors_before_connect() {
        let store = test_store();
        let err = store.get_tag_count(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_recent_activity_errors_before_connect() {
        let store = test_store();
        let err = store.get_recent_activity(1, 20).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_monthly_token_usage_errors_before_connect() {
        let store = test_store();
        let err = store.get_monthly_token_usage(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_daily_token_usage_errors_before_connect() {
        let store = test_store();
        let err = store.get_daily_token_usage(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn get_monthly_token_usage_by_user_errors_before_connect() {
        let store = test_store();
        let err = store.get_monthly_token_usage_by_user(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    // ── Pure-logic tests ──────────────────────────────────────────────────────

    #[test]
    fn role_to_activity_kind_maps_known_roles() {
        assert_eq!(role_to_activity_kind("tag"), "ingest");
        assert_eq!(role_to_activity_kind("embed"), "ingest");
        assert_eq!(role_to_activity_kind("chat"), "search");
        assert_eq!(role_to_activity_kind("rerank"), "search");
    }

    #[test]
    fn role_to_activity_kind_passthrough_unknown() {
        assert_eq!(role_to_activity_kind("transcribe"), "transcribe");
        assert_eq!(role_to_activity_kind("vision"), "vision");
        assert_eq!(role_to_activity_kind(""), "");
    }

    // ── Type construction smoke tests ─────────────────────────────────────────

    #[test]
    fn activity_event_debug_and_clone() {
        let event = ActivityEvent {
            occurred_at: chrono::Utc::now(),
            kind: "ingest".into(),
            description: "bge-m3".into(),
        };
        let cloned = event.clone();
        assert_eq!(event.kind, cloned.kind);
        assert_eq!(event.description, cloned.description);
        let _dbg = format!("{event:?}");
    }

    #[test]
    fn user_token_usage_debug_and_clone() {
        let usage = UserTokenUsage {
            user_id: Some(7),
            email: Some("a@b.com".into()),
            total_tokens: 1234,
        };
        let cloned = usage.clone();
        assert_eq!(usage.user_id, cloned.user_id);
        assert_eq!(usage.email, cloned.email);
        assert_eq!(usage.total_tokens, cloned.total_tokens);
        let _dbg = format!("{usage:?}");

        // System bucket: no user_id, no email.
        let system = UserTokenUsage {
            user_id: None,
            email: None,
            total_tokens: 50,
        };
        assert!(system.user_id.is_none());
        assert!(system.email.is_none());
    }

    #[test]
    fn daily_token_usage_debug_and_clone() {
        let usage = DailyTokenUsage {
            date: "2026-06-01".into(),
            prompt_tokens: 500,
            completion_tokens: 300,
        };
        let cloned = usage.clone();
        assert_eq!(usage.date, cloned.date);
        assert_eq!(usage.prompt_tokens, cloned.prompt_tokens);
        assert_eq!(usage.completion_tokens, cloned.completion_tokens);
        let _dbg = format!("{usage:?}");
    }
}
