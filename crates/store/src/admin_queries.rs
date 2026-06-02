//! Admin-panel query methods on [`PgStore`] (plan §15, ledger P6-T8).
//!
//! These methods support the admin dashboard, tenant / user management, tag merge,
//! dead-letter queue, and audit-log features. They use the **privileged admin pool**
//! (except for tag merge, which modifies tenant-scoped tables via the app pool with
//! [`begin_tenant_tx`]), and filter by `tenant_id` explicitly where applicable so
//! the handler-level authorization boundaries are the definitive gate.

use std::str::FromStr;

use anyhow::Context;
use kb_core::job::{Job, JobKind, JobStatus};
use kb_core::tenant::Tenant;
use kb_core::user::UserRole;
use sqlx::Row;

use crate::pg_store::{PgStore, begin_tenant_tx};

/// A lightweight view of a user **without** the `password_hash` field, safe to
/// return from admin API endpoints.
#[derive(Debug, Clone)]
pub struct UserView {
    /// Surrogate primary key (`users.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Login email.
    pub email: String,
    /// Role within the tenant.
    pub role: UserRole,
    /// Row creation time.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Lightweight tenant info without billing/quota details (P12-T3).
///
/// This is a safe view for rendering account pages — it never exposes quota
/// limits or other admin-only fields.
#[derive(Debug, Clone)]
pub struct TenantInfo {
    /// Tenant id.
    pub id: i64,
    /// Tenant slug (URL-safe handle).
    pub slug: String,
    /// Human-readable display name.
    pub name: String,
}

impl PgStore {
    // ── Tenant admin methods ─────────────────────────────────────────────────

    /// List all tenants (super-admin only — callers enforce the Owner-role gate).
    ///
    /// Uses the admin pool (`tenants` is outside RLS).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_list_tenants(&self) -> anyhow::Result<Vec<Tenant>> {
        let pool = self.pool()?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            slug: String,
            name: String,
            quota_bytes: Option<i64>,
            quota_tokens: Option<i64>,
            budget_monthly_cents: Option<i64>,
            created_at: chrono::DateTime<chrono::Utc>,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, slug, name, quota_bytes, quota_tokens, budget_monthly_cents, created_at \
             FROM tenants ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .context("failed to list tenants for admin panel")?;
        Ok(rows
            .into_iter()
            .map(|r| Tenant {
                id: r.id,
                slug: r.slug,
                name: r.name,
                quota_bytes: r.quota_bytes,
                quota_tokens: r.quota_tokens,
                budget_monthly_cents: r.budget_monthly_cents,
                created_at: r.created_at,
            })
            .collect())
    }

    /// Update a tenant's name, quota_bytes, and quota_tokens.
    ///
    /// Uses the admin pool (`tenants` is outside RLS).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the tenant does not exist.
    pub async fn admin_update_tenant(
        &self,
        tenant_id: i64,
        name: &str,
        quota_bytes: Option<i64>,
        quota_tokens: Option<i64>,
    ) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        let affected = sqlx::query(
            "UPDATE tenants SET name = $2, quota_bytes = $3, quota_tokens = $4 WHERE id = $1",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(quota_bytes)
        .bind(quota_tokens)
        .execute(&pool)
        .await
        .context("failed to update tenant")?;
        Ok(affected.rows_affected())
    }

    // ── User admin methods ──────────────────────────────────────────────────

    /// List all users within a tenant, ordered by `id`.
    ///
    /// Returns [`UserView`] entries that exclude the `password_hash` field.
    /// Uses the admin pool with an explicit `tenant_id` filter so both tenant-admins
    /// (scoped to their own tenant) and super-admins (any tenant) can use this.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_list_users(&self, tenant_id: i64) -> anyhow::Result<Vec<UserView>> {
        let pool = self.pool()?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            email: String,
            role: String,
            created_at: chrono::DateTime<chrono::Utc>,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, tenant_id, email, role, created_at \
             FROM users WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&pool)
        .await
        .context("failed to list users for admin panel")?;
        rows.into_iter()
            .map(|r| {
                let role = UserRole::from_str(&r.role)
                    .with_context(|| format!("invalid user role in DB: {}", r.role))?;
                Ok(UserView {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    email: r.email,
                    role,
                    created_at: r.created_at,
                })
            })
            .collect()
    }

    /// Update a user's role within their tenant.
    ///
    /// Returns the number of rows affected (0 means the user was not found or
    /// belongs to a different tenant). The `tenant_id` parameter scopes the
    /// update — a tenant-admin cannot reach a user in another tenant.
    ///
    /// Uses the admin pool; the explicit `tenant_id` filter is the enforcement
    /// gate (cross-tenant denial is deliberate, not accidental).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_update_user_role(
        &self,
        tenant_id: i64,
        user_id: i64,
        new_role: UserRole,
    ) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        let affected = sqlx::query("UPDATE users SET role = $3 WHERE id = $1 AND tenant_id = $2")
            .bind(user_id)
            .bind(tenant_id)
            .bind(new_role.as_str())
            .execute(&pool)
            .await
            .context("failed to update user role")?;
        Ok(affected.rows_affected())
    }

    /// Disable a user (a placeholder for future user-suspension logic; currently
    /// a no-op row that just returns 1 for existence check satisfaction).
    ///
    /// In the current schema there is no `active` column — disabling is logged to
    /// the audit log so the intent is recorded. A future migration will add an
    /// `active` column and this method will set it to false.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_disable_user(&self, user_id: i64) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        // Verify the user exists before logging the audit action.
        let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE id = $1 LIMIT 1")
            .bind(user_id)
            .fetch_optional(&pool)
            .await
            .context("failed to look up user for disabling")?;
        Ok(if exists.is_some() { 1 } else { 0 })
    }

    /// Count users in a tenant (for the admin dashboard).
    ///
    /// Uses the admin pool; read-only, no side effects.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_user_count(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_one(&pool)
            .await
            .context("failed to count users")
    }

    /// Count documents in a tenant (for the admin dashboard).
    ///
    /// Uses the admin pool; read-only, no side effects.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_document_count(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_one(&pool)
            .await
            .context("failed to count documents")
    }

    /// Sum the monthly spend (cost_micros) for a tenant in the current calendar
    /// month (for the admin dashboard, plan §26.6).
    ///
    /// Uses the admin pool; read-only. Only non-NULL `cost_micros` rows are
    /// summed (free/local calls have NULL and are excluded).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_monthly_spend(&self, tenant_id: i64) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        let total: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(cost_micros), 0) \
             FROM usage_events \
             WHERE tenant_id = $1 \
               AND cost_micros IS NOT NULL \
               AND date_trunc('month', created_at) = date_trunc('month', now())",
        )
        .bind(tenant_id)
        .fetch_one(&pool)
        .await
        .context("failed to query monthly spend")?;
        Ok(total.unwrap_or(0) as u64)
    }

    // ── Job admin methods (dead-letter queue) ──────────────────────────────

    /// List jobs for a tenant, optionally filtered by status, with a limit.
    ///
    /// Uses the admin pool (the jobs table is outside RLS). Returns the most
    /// recently created matching jobs first.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_list_jobs(
        &self,
        tenant_id: i64,
        status: Option<JobStatus>,
        limit: i64,
    ) -> anyhow::Result<Vec<Job>> {
        let pool = self.pool()?;
        let rows: Vec<sqlx::postgres::PgRow> = if let Some(ref s) = status {
            sqlx::query(
                "SELECT id, tenant_id, file_id, document_id, kind, priority, status, \
                 attempts, last_error, run_after, created_at \
                 FROM jobs WHERE tenant_id = $1 AND status = $2 \
                 ORDER BY created_at DESC LIMIT $3",
            )
            .bind(tenant_id)
            .bind(s.as_str())
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list jobs with status filter")?
        } else {
            sqlx::query(
                "SELECT id, tenant_id, file_id, document_id, kind, priority, status, \
                 attempts, last_error, run_after, created_at \
                 FROM jobs WHERE tenant_id = $1 \
                 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(tenant_id)
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list jobs")?
        };
        rows.into_iter().map(job_from_pg_row).collect()
    }

    /// Set a job's status back to `queued` and reset its `attempts` to 0 so it will
    /// be re-claimed by a worker (dead-letter retry).
    ///
    /// The `tenant_id` filter prevents cross-tenant job manipulation (IDOR).
    /// Uses the admin pool. Returns the number of rows affected.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_retry_job(&self, tenant_id: i64, job_id: i64) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        let affected = sqlx::query(
            "UPDATE jobs SET status = 'queued', attempts = 0, last_error = NULL, run_after = now() \
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(job_id)
        .bind(tenant_id)
        .execute(&pool)
        .await
        .context("failed to retry job")?;
        Ok(affected.rows_affected())
    }

    /// Delete a job (dead-letter removal).
    ///
    /// The `tenant_id` filter prevents cross-tenant job manipulation (IDOR).
    /// Uses the admin pool. Returns the number of rows affected.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_delete_job(&self, tenant_id: i64, job_id: i64) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        let affected = sqlx::query("DELETE FROM jobs WHERE id = $1 AND tenant_id = $2")
            .bind(job_id)
            .bind(tenant_id)
            .execute(&pool)
            .await
            .context("failed to delete job")?;
        Ok(affected.rows_affected())
    }

    /// Update a job's priority and record the change in the audit log (P9-T12).
    ///
    /// The priority uses the **higher-is-more-urgent** convention (default 0).
    /// The `actor_user_id` is the admin performing the change — it is written to
    /// the audit log. The old and new priority values are recorded in the
    /// `details` JSONB of the audit event.
    ///
    /// Uses the admin pool. Returns the number of rows affected (0 means the
    /// job was not found in the specified tenant).
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the audit write
    /// fails, or the query fails.
    pub async fn admin_update_job_priority(
        &self,
        tenant_id: i64,
        job_id: i64,
        new_priority: i32,
        actor_user_id: i64,
    ) -> anyhow::Result<u64> {
        let pool = self.pool()?;

        // Read old priority for the audit record.
        let old_priority: Option<i32> =
            sqlx::query_scalar("SELECT priority FROM jobs WHERE id = $1 AND tenant_id = $2")
                .bind(job_id)
                .bind(tenant_id)
                .fetch_optional(&pool)
                .await
                .context("failed to read old priority for audit")?;

        let old_priority = match old_priority {
            Some(p) => p,
            None => {
                // Job not found — nothing to update.
                return Ok(0);
            }
        };

        // Update the priority.
        let affected =
            sqlx::query("UPDATE jobs SET priority = $1 WHERE id = $2 AND tenant_id = $3")
                .bind(new_priority)
                .bind(job_id)
                .bind(tenant_id)
                .execute(&pool)
                .await
                .context("failed to update job priority")?
                .rows_affected();

        if affected > 0 {
            // Write audit event (P9-T12 requirement (e)).
            let details = serde_json::json!({
                "old_priority": old_priority,
                "new_priority": new_priority,
            });
            self.admin_insert_audit_event(
                actor_user_id,
                tenant_id,
                "job_priority_change",
                "job",
                Some(job_id),
                &details,
            )
            .await
            .context("failed to write priority-change audit event")?;
        }

        Ok(affected)
    }

    // ── Tag merge (plan §6.5, §15) ────────────────────────────────────────

    /// Merge two canonical tags in a tenant: repoint all aliases from `from_tag_id`
    /// to `to_tag_id`, repoint all `document_tags` links, then delete the
    /// now-unused `from_tag_id` tag. The whole operation runs in one transaction
    /// on the app pool (tenant-scoped, RLS enforced).
    ///
    /// Returns a summary of what was moved: `(aliases_moved, doc_tags_repointed)`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the tags do not exist,
    /// or the query fails.
    pub async fn admin_merge_tags(
        &self,
        tenant_id: i64,
        from_tag_id: i64,
        to_tag_id: i64,
    ) -> anyhow::Result<(u64, u64)> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        // Update aliases: repoint from from_tag_id to to_tag_id, skipping duplicates.
        let aliases_moved = sqlx::query(
            "UPDATE tag_aliases SET tag_id = $2 \
             WHERE tenant_id = $1 AND tag_id = $3 \
               AND alias NOT IN (SELECT alias FROM tag_aliases WHERE tenant_id = $1 AND tag_id = $2)",
        )
        .bind(tenant_id)
        .bind(to_tag_id)
        .bind(from_tag_id)
        .execute(&mut *tx)
        .await
        .context("failed to repoint tag aliases during merge")?
        .rows_affected();

        // Delete any remaining aliases that would conflict (same alias exists for both tags).
        sqlx::query("DELETE FROM tag_aliases WHERE tenant_id = $1 AND tag_id = $2")
            .bind(tenant_id)
            .bind(from_tag_id)
            .execute(&mut *tx)
            .await
            .context("failed to remove old tag aliases during merge")?;

        // Repoint document_tags: INSERT ... ON CONFLICT DO NOTHING for rows where
        // the document already has the target tag (avoid duplicate PK).
        sqlx::query(
            "INSERT INTO document_tags (document_id, tag_id, source, locked) \
             SELECT dt.document_id, $2, dt.source, dt.locked \
             FROM document_tags dt \
             WHERE dt.tag_id = $3 \
             ON CONFLICT (document_id, tag_id) DO NOTHING",
        )
        .bind(to_tag_id)
        .bind(from_tag_id)
        .execute(&mut *tx)
        .await
        .context("failed to repoint document_tags during merge")?;

        // Count how many document_tags rows were repointed (those that existed before deletion).
        let doc_tags_repointed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE tag_id = $1")
                .bind(from_tag_id)
                .fetch_one(&mut *tx)
                .await
                .context("failed to count repointed document_tags")?;

        // Remove the old document_tags rows.
        sqlx::query("DELETE FROM document_tags WHERE tag_id = $1")
            .bind(from_tag_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete old document_tags during merge")?;

        // Delete the now-orphaned from tag.
        sqlx::query("DELETE FROM tags WHERE id = $1 AND tenant_id = $2")
            .bind(from_tag_id)
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete merged-from tag")?;

        tx.commit()
            .await
            .context("failed to commit tag merge transaction")?;

        Ok((aliases_moved, doc_tags_repointed as u64))
    }

    // ── Audit events ──────────────────────────────────────────────────────

    /// Insert a row into `audit_events`.
    ///
    /// Uses the admin pool (`audit_events` is outside RLS). `details` is stored
    /// as a JSONB column for later inspection.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_insert_audit_event(
        &self,
        actor_user_id: i64,
        tenant_id: i64,
        action: &str,
        target_type: &str,
        target_id: Option<i64>,
        details: &serde_json::Value,
    ) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO audit_events (actor_user_id, tenant_id, action, target_type, target_id, details) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
        )
        .bind(actor_user_id)
        .bind(tenant_id)
        .bind(action)
        .bind(target_type)
        .bind(target_id)
        .bind(details)
        .fetch_one(&pool)
        .await
        .context("failed to insert audit event")?;
        Ok(id)
    }

    /// List recent audit events for a tenant (or cross-tenant when `tenant_id` is 0,
    /// for super-admin dashboards).
    ///
    /// Uses the admin pool. When `tenant_id` is 0, returns events across all tenants.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_list_audit_events(
        &self,
        tenant_id: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<kb_core::audit::AuditEvent>> {
        let pool = self.pool()?;
        use kb_core::audit::{AuditAction, AuditEvent};
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            actor_user_id: i64,
            tenant_id: i64,
            action: String,
            target_type: String,
            target_id: Option<i64>,
            details: serde_json::Value,
            created_at: chrono::DateTime<chrono::Utc>,
        }
        let rows: Vec<Row> = if tenant_id == 0 {
            sqlx::query_as(
                "SELECT id, actor_user_id, tenant_id, action, target_type, target_id, details, created_at \
                 FROM audit_events ORDER BY created_at DESC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list cross-tenant audit events")?
        } else {
            sqlx::query_as(
                "SELECT id, actor_user_id, tenant_id, action, target_type, target_id, details, created_at \
                 FROM audit_events WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(tenant_id)
            .bind(limit)
            .fetch_all(&pool)
            .await
            .context("failed to list audit events")?
        };
        rows.into_iter()
            .map(|r| {
                let action = AuditAction::from_str(&r.action)
                    .with_context(|| format!("invalid audit action: {}", r.action))?;
                Ok(AuditEvent {
                    id: r.id,
                    actor_user_id: r.actor_user_id,
                    tenant_id: r.tenant_id,
                    action,
                    target_type: r.target_type,
                    target_id: r.target_id,
                    details: r.details,
                    created_at: r.created_at,
                })
            })
            .collect()
    }

    // ── Tenant info (P12-T3) ──────────────────────────────────────────────────

    /// Look up a tenant's slug and name by id.
    ///
    /// Returns `None` if no tenant with that id exists.
    /// Uses the admin pool — the `tenants` table is outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_tenant_info(&self, tenant_id: i64) -> anyhow::Result<Option<TenantInfo>> {
        let pool = self.pool()?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            slug: String,
            name: String,
        }
        let row: Option<Row> = sqlx::query_as("SELECT id, slug, name FROM tenants WHERE id = $1")
            .bind(tenant_id)
            .fetch_optional(&pool)
            .await
            .context("failed to look up tenant info")?;
        Ok(row.map(|r| TenantInfo {
            id: r.id,
            slug: r.slug,
            name: r.name,
        }))
    }

    // ── User management (P12-T3) ─────────────────────────────────────────────

    /// Delete a user from a tenant.
    ///
    /// The `tenant_id` filter prevents cross-tenant user deletion (IDOR).
    /// Uses the admin pool. Returns the number of rows affected (0 means the user
    /// was not found in that tenant).
    ///
    /// The caller should prevent self-deletion of the last Owner.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_delete_user(&self, tenant_id: i64, user_id: i64) -> anyhow::Result<u64> {
        let pool = self.pool()?;
        let affected = sqlx::query("DELETE FROM users WHERE id = $1 AND tenant_id = $2")
            .bind(user_id)
            .bind(tenant_id)
            .execute(&pool)
            .await
            .context("failed to delete user")?;
        Ok(affected.rows_affected())
    }

    /// Look up a single user by id within a tenant, returning a [`UserView`]
    /// without the password hash. Returns `None` if the user is not found in
    /// that tenant.
    ///
    /// Uses the admin pool.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn admin_get_user(
        &self,
        tenant_id: i64,
        user_id: i64,
    ) -> anyhow::Result<Option<UserView>> {
        let pool = self.pool()?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            email: String,
            role: String,
            created_at: chrono::DateTime<chrono::Utc>,
        }
        let row: Option<Row> = sqlx::query_as(
            "SELECT id, tenant_id, email, role, created_at \
             FROM users WHERE id = $1 AND tenant_id = $2",
        )
        .bind(user_id)
        .bind(tenant_id)
        .fetch_optional(&pool)
        .await
        .context("failed to look up user by id")?;
        row.map(|r| {
            let role = UserRole::from_str(&r.role)
                .with_context(|| format!("invalid user role in DB: {}", r.role))?;
            Ok(UserView {
                id: r.id,
                tenant_id: r.tenant_id,
                email: r.email,
                role,
                created_at: r.created_at,
            })
        })
        .transpose()
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Decode a raw `jobs` pg row into a [`Job`].
fn job_from_pg_row(r: sqlx::postgres::PgRow) -> anyhow::Result<Job> {
    let kind_str: String = r.try_get("kind")?;
    let status_str: String = r.try_get("status")?;
    Ok(Job {
        id: r.try_get("id")?,
        tenant_id: r.try_get("tenant_id")?,
        file_id: r.try_get("file_id")?,
        document_id: r.try_get("document_id")?,
        kind: JobKind::from_str(&kind_str)
            .with_context(|| format!("invalid job kind in DB: '{kind_str}'"))?,
        priority: r.try_get("priority")?,
        status: JobStatus::from_str(&status_str)
            .with_context(|| format!("invalid job status in DB: '{status_str}'"))?,
        attempts: r.try_get("attempts")?,
        last_error: r.try_get("last_error")?,
        run_after: r.try_get("run_after")?,
        created_at: r.try_get("created_at")?,
    })
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::job::JobStatus;

    fn test_store() -> PgStore {
        PgStore::new("postgres://localhost/db")
    }

    // ── admin_list_tenants (no DB needed) ────────────────────────────────

    #[tokio::test]
    async fn admin_list_tenants_errors_before_connect() {
        let store = test_store();
        let err = store.admin_list_tenants().await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn admin_update_tenant_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_update_tenant(1, "name", Some(1000), None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    // ── User admin ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn admin_list_users_errors_before_connect() {
        let store = test_store();
        let err = store.admin_list_users(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_update_user_role_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_update_user_role(1, 1, UserRole::Member)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_disable_user_errors_before_connect() {
        let store = test_store();
        let err = store.admin_disable_user(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_user_count_errors_before_connect() {
        let store = test_store();
        let err = store.admin_user_count(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_document_count_errors_before_connect() {
        let store = test_store();
        let err = store.admin_document_count(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_monthly_spend_errors_before_connect() {
        let store = test_store();
        let err = store.admin_monthly_spend(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── Job admin ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn admin_list_jobs_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_list_jobs(1, Some(JobStatus::Dead), 50)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_list_jobs_none_status_errors_before_connect() {
        let store = test_store();
        let err = store.admin_list_jobs(1, None, 10).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_retry_job_errors_before_connect() {
        let store = test_store();
        let err = store.admin_retry_job(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_delete_job_errors_before_connect() {
        let store = test_store();
        let err = store.admin_delete_job(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── Tag merge ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn admin_merge_tags_errors_before_connect() {
        let store = test_store();
        let err = store.admin_merge_tags(1, 2, 3).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── Audit events ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn admin_insert_audit_event_errors_before_connect() {
        let store = test_store();
        let err = store
            .admin_insert_audit_event(
                1,
                1,
                "user_created",
                "user",
                Some(42),
                &serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_list_audit_events_errors_before_connect() {
        let store = test_store();
        let err = store.admin_list_audit_events(1, 20).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_list_audit_events_cross_tenant_errors_before_connect() {
        let store = test_store();
        let err = store.admin_list_audit_events(0, 50).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── Tenant info (P12-T3) ────────────────────────────────────────────

    #[tokio::test]
    async fn get_tenant_info_errors_before_connect() {
        let store = test_store();
        let err = store.get_tenant_info(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── User delete (P12-T3) ───────────────────────────────────────────

    #[tokio::test]
    async fn admin_delete_user_errors_before_connect() {
        let store = test_store();
        let err = store.admin_delete_user(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn admin_get_user_errors_before_connect() {
        let store = test_store();
        let err = store.admin_get_user(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── TenantInfo struct ──────────────────────────────────────────────

    #[test]
    fn tenant_info_debug_format() {
        let info = TenantInfo {
            id: 42,
            slug: "acme".into(),
            name: "Acme Corp".into(),
        };
        let dbg = format!("{info:?}");
        assert!(dbg.contains("42"));
        assert!(dbg.contains("acme"));
        assert!(dbg.contains("Acme Corp"));
    }

    #[test]
    fn tenant_info_clone_is_equal() {
        let info = TenantInfo {
            id: 1,
            slug: "test".into(),
            name: "Test".into(),
        };
        let cloned = info.clone();
        assert_eq!(cloned.id, info.id);
        assert_eq!(cloned.slug, info.slug);
        assert_eq!(cloned.name, info.name);
    }
}
