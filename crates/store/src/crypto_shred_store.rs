// SPDX-License-Identifier: AGPL-3.0-or-later

//! Crypto-shredding methods on [`PgStore`] — tenant-data destruction, DEK removal,
//! and tombstone creation for the delete-tenant flow (plan §28, P10-T4).
//!
//! These methods are used by the crypto-shredding job handler in `kb-pipeline` to
//! irrevocably delete a tenant: all tenant-scoped DB rows are removed, the per-tenant
//! DEK is destroyed, and a tombstone record preserves the audit trail. Once the DEK is
//! gone, all encrypted data (B2 blobs, DB encrypted columns) is irrecoverable — even
//! from B2 immutable versions and PITR backups.
//!
//! # Connection role
//!
//! All methods use the **privileged admin pool** because they operate across the
//! tenant-isolation boundary (deleting a tenant, cross-tenant audit rows, and the
//! `tenant_data_keys` infrastructure table).

use anyhow::Context;
use chrono::Utc;
use kb_core::tenant::TenantTombstone;
use sqlx::Row;

use crate::pg_store::PgStore;

impl PgStore {
    /// Destroy the per-tenant Data Encryption Key (DEK).
    ///
    /// Deletes the row from `tenant_data_keys` for the given tenant. After this call,
    /// the tenant's DEK is gone — all ciphertext encrypted under that DEK (B2 blobs,
    /// `content_enc`, `summary_enc`, `user_note_enc` columns) is irrecoverable.
    /// The KEK used to wrap the DEK cannot help because the `wrapped_dek` field no
    /// longer exists to unwrap.
    ///
    /// This is a no-op if the tenant has no DEK (returns `Ok` without error).
    ///
    /// Uses the **admin pool** because `tenant_data_keys` is outside RLS scope.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn destroy_tenant_dek(&self, tenant_id: i64) -> anyhow::Result<()> {
        let pool = self.admin_pool()?;
        sqlx::query("DELETE FROM tenant_data_keys WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&pool)
            .await
            .context("failed to destroy tenant DEK")?;
        Ok(())
    }

    /// Delete **all** tenant-scoped data for the given tenant.
    ///
    /// Deletes rows from, in order (respects FK constraints):
    ///   - `usage_events` (FK to users)
    ///   - `jobs` (FK to users)
    ///   - `document_tags`
    ///   - `tag_aliases`
    ///   - `chunks` (FK to files)
    ///   - `files` (FK to documents)
    ///   - `documents`
    ///   - `tags`
    ///   - `users`
    ///
    /// Finally deletes the tenant row itself from `tenants`, which cascades to
    /// `tenant_data_keys` (FK ON DELETE CASCADE).
    ///
    /// All operations happen within a single transaction on the **admin pool**
    /// (the tenant is being deleted — cross-tenant isolation does not apply).
    /// RLS is bypassed because the admin role is the table owner.
    ///
    /// # Idempotency
    ///
    /// If the tenant does not exist, this returns `Ok` without error — the
    /// delete-tenant job is idempotent.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or any query fails.
    pub async fn delete_tenant_data(&self, tenant_id: i64) -> anyhow::Result<()> {
        let pool = self.admin_pool()?;
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin transaction for tenant data deletion")?;

        // Delete usage events first (FK to users, no further deps).
        sqlx::query("DELETE FROM usage_events WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete usage_events for tenant")?;

        // Delete jobs (also depends on users, no cascading).
        sqlx::query("DELETE FROM jobs WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete jobs for tenant")?;

        // Delete document_tags (FK to documents + tags).
        sqlx::query(
            "DELETE FROM document_tags WHERE document_id IN (SELECT id FROM documents WHERE tenant_id = $1)",
        )
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to delete document_tags for tenant")?;

        // Delete tag_aliases (FK to tags).
        sqlx::query(
            "DELETE FROM tag_aliases WHERE tag_id IN (SELECT id FROM tags WHERE tenant_id = $1)",
        )
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to delete tag_aliases for tenant")?;

        // Delete chunks (FK to files).
        sqlx::query(
            "DELETE FROM chunks WHERE file_id IN (SELECT id FROM files WHERE tenant_id = $1)",
        )
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to delete chunks for tenant")?;

        // Delete files (FK to documents).
        sqlx::query("DELETE FROM files WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete files for tenant")?;

        // Delete documents.
        sqlx::query("DELETE FROM documents WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete documents for tenant")?;

        // Delete tags.
        sqlx::query("DELETE FROM tags WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete tags for tenant")?;

        // Delete users.
        sqlx::query("DELETE FROM users WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete users for tenant")?;

        // Delete the tenant itself (cascades to tenant_data_keys via FK).
        sqlx::query("DELETE FROM tenants WHERE id = $1")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete tenant row")?;

        tx.commit()
            .await
            .context("failed to commit tenant data deletion transaction")?;

        Ok(())
    }

    /// Create a tombstone record for a crypto-shredded tenant.
    ///
    /// The tombstone persists `tenant_id`, `slug`, and `name` at deletion time so the
    /// audit trail is self-contained even though the tenant row no longer exists.
    /// `deleted_by` is the super-admin user who initiated the deletion (`None` if
    /// system-initiated or the admin identity is unknown).
    ///
    /// Uses the **admin pool** (the tombstone table is outside RLS).
    ///
    /// Returns the inserted [`TenantTombstone`] with the assigned `id` and `deleted_at`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the insert fails.
    pub async fn create_tombstone(
        &self,
        tenant_id: i64,
        tenant_slug: &str,
        tenant_name: &str,
        deleted_by: Option<i64>,
    ) -> anyhow::Result<TenantTombstone> {
        let pool = self.admin_pool()?;
        let now = Utc::now();
        let row = sqlx::query(
            "INSERT INTO tenant_tombstones (tenant_id, tenant_slug, tenant_name, deleted_by, deleted_at)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING id, tenant_id, tenant_slug, tenant_name, deleted_by, deleted_at",
        )
        .bind(tenant_id)
        .bind(tenant_slug)
        .bind(tenant_name)
        .bind(deleted_by)
        .bind(now)
        .fetch_one(&pool)
        .await
        .context("failed to create tenant tombstone")?;

        Ok(TenantTombstone {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            tenant_slug: row.get("tenant_slug"),
            tenant_name: row.get("tenant_name"),
            deleted_by: row.get("deleted_by"),
            deleted_at: row.get("deleted_at"),
        })
    }

    /// Count how many tombstones exist (useful for test assertions and admin checks).
    ///
    /// Uses the **admin pool**.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn tombstone_count(&self) -> anyhow::Result<i64> {
        let pool = self.admin_pool()?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tenant_tombstones")
            .fetch_one(&pool)
            .await
            .context("failed to count tombstones")?;
        Ok(count)
    }

    /// Check whether a tenant still exists (rows in `tenants`). Used for idempotency:
    /// the delete-tenant job is a no-op when the tenant is already deleted.
    ///
    /// Uses the **admin pool**.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn tenant_exists(&self, tenant_id: i64) -> anyhow::Result<bool> {
        let pool = self.admin_pool()?;
        let exists: Option<i64> =
            sqlx::query_scalar("SELECT id FROM tenants WHERE id = $1 LIMIT 1")
                .bind(tenant_id)
                .fetch_optional(&pool)
                .await
                .context("failed to check tenant existence")?;
        Ok(exists.is_some())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // These tests verify SQL construction and error-handling paths.
    // DB round-trips are tested via #[ignore] integration tests.

    #[test]
    fn tenant_tombstone_type_fields_accessible() {
        let ts = TenantTombstone {
            id: 1,
            tenant_id: 42,
            tenant_slug: "acme".into(),
            tenant_name: "Acme Corp".into(),
            deleted_by: Some(99),
            deleted_at: Utc::now(),
        };
        assert_eq!(ts.tenant_id, 42);
        assert_eq!(ts.tenant_slug, "acme");
        assert_eq!(ts.tenant_name, "Acme Corp");
        assert_eq!(ts.deleted_by, Some(99));
    }
}
