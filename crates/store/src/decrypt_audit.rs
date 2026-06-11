// SPDX-License-Identifier: AGPL-3.0-or-later

//! Decrypt-access audit logging on [`PgStore`] (plan §28, P10-T5).
//!
//! Every key-unwrap operation (DEK unwrap, provider API key decrypt) writes a
//! [`DecryptAuditEvent`](kb_core::audit::DecryptAuditEvent) to the `decrypt_audit`
//! table AND emits a `tracing::debug!` event. Failed unwraps also increment the
//! `kb_decrypt_audit_failed` Prometheus counter so a spike in failures triggers an
//! alert for possible attacks or key corruption.
//!
//! The table is **append-only** — rows are INSERTed but never UPDATEd or DELETEd.

use std::str::FromStr;

use anyhow::Context;
use kb_core::audit::DecryptAuditAction;

use crate::pg_store::PgStore;

impl PgStore {
    /// Insert a row into `decrypt_audit` (append-only, no UPDATE / DELETE).
    ///
    /// Every key-unwrap operation must call this — both on success and on failure —
    /// so that the audit trail is complete and failure spikes are detectable via the
    /// `kb_decrypt_audit_failed` Prometheus counter.
    ///
    /// This method:
    /// 1. Emits a `tracing::debug!` event with the audit details (never the key material).
    /// 2. Inserts the row into the `decrypt_audit` table via the admin pool
    ///    (decrypt_audit is outside RLS, like `audit_events`).
    /// 3. On failure, increments `kb_decrypt_audit_failed` for alerting.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn insert_decrypt_audit_event(
        &self,
        tenant_id: i64,
        operation: DecryptAuditAction,
        key_id: &str,
        user_id: Option<i64>,
        success: bool,
    ) -> anyhow::Result<i64> {
        // Emit a debug-level tracing event (never includes key material, only metadata).
        tracing::debug!(
            tenant_id = tenant_id,
            operation = %operation.as_str(),
            key_id = %key_id,
            user_id = user_id,
            success = success,
            "decrypt audit event"
        );

        // Record the failure in the Prometheus counter if the unwrap failed.
        if !success {
            kb_metrics::record_decrypt_audit_failed(operation.as_str());
        }

        let pool = self.admin_pool()?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO decrypt_audit (tenant_id, operation, key_id, user_id, success) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(tenant_id)
        .bind(operation.as_str())
        .bind(key_id)
        .bind(user_id)
        .bind(success)
        .fetch_one(&pool)
        .await
        .context("failed to insert decrypt audit event")?;

        Ok(id)
    }

    /// List recent decrypt-audit events, optionally filtered.
    ///
    /// * `tenant_id` — the tenant to filter by (0 = cross-tenant, super-admin only).
    /// * `operation` — optional operation filter (`None` = all operations).
    /// * `limit` — maximum number of rows to return.
    ///
    /// Uses the admin pool (`decrypt_audit` is outside RLS).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_list_decrypt_audit(
        &self,
        tenant_id: i64,
        operation: Option<DecryptAuditAction>,
        limit: i64,
    ) -> anyhow::Result<Vec<kb_core::audit::DecryptAuditEvent>> {
        use kb_core::audit::DecryptAuditEvent;
        let pool = self.admin_pool()?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            operation: String,
            key_id: String,
            user_id: Option<i64>,
            success: bool,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<Row> = match (tenant_id, &operation) {
            (0, None) => sqlx::query_as(
                "SELECT id, tenant_id, operation, key_id, user_id, success, created_at \
                     FROM decrypt_audit ORDER BY created_at DESC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list cross-tenant decrypt audit events")?,
            (0, Some(op)) => sqlx::query_as(
                "SELECT id, tenant_id, operation, key_id, user_id, success, created_at \
                     FROM decrypt_audit WHERE operation = $1 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(op.as_str())
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list cross-tenant decrypt audit events by operation")?,
            (tid, None) => sqlx::query_as(
                "SELECT id, tenant_id, operation, key_id, user_id, success, created_at \
                     FROM decrypt_audit WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(tid)
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list decrypt audit events")?,
            (tid, Some(op)) => sqlx::query_as(
                "SELECT id, tenant_id, operation, key_id, user_id, success, created_at \
                     FROM decrypt_audit WHERE tenant_id = $1 AND operation = $2 \
                     ORDER BY created_at DESC LIMIT $3",
            )
            .bind(tid)
            .bind(op.as_str())
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list decrypt audit events by operation")?,
        };

        rows.into_iter()
            .map(|r| {
                let operation = DecryptAuditAction::from_str(&r.operation)
                    .with_context(|| format!("invalid decrypt audit operation: {}", r.operation))?;
                Ok(DecryptAuditEvent {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    operation,
                    key_id: r.key_id,
                    user_id: r.user_id,
                    success: r.success,
                    created_at: r.created_at,
                })
            })
            .collect()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::audit::DecryptAuditAction;

    fn test_store() -> PgStore {
        PgStore::new("postgres://localhost/db")
    }

    #[tokio::test]
    async fn insert_decrypt_audit_errors_before_connect() {
        let store = test_store();
        let err = store
            .insert_decrypt_audit_event(1, DecryptAuditAction::UnwrapDek, "dek:test", None, true)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn insert_decrypt_audit_failed_event() {
        let store = test_store();
        let err = store
            .insert_decrypt_audit_event(
                2,
                DecryptAuditAction::UnwrapProviderKey,
                "provider:5",
                Some(42),
                false,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_list_decrypt_audit_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_list_decrypt_audit(1, None, 20)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_list_decrypt_audit_cross_tenant_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_list_decrypt_audit(0, None, 50)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_list_decrypt_audit_with_operation_filter_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_list_decrypt_audit(1, Some(DecryptAuditAction::UnwrapDek), 10)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }
}
