// SPDX-License-Identifier: AGPL-3.0-or-later

//! The tenant record — the top-level isolation boundary.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A tenant. Every tenant-scoped row carries this `id`, and Postgres Row-Level Security
/// keys on it so a forgotten `WHERE` cannot leak across tenants (plan §13).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tenant {
    /// Surrogate primary key (`tenants.id`).
    pub id: i64,
    /// URL-safe unique handle (`tenants.slug`).
    pub slug: String,
    /// Human-readable display name.
    pub name: String,
    /// Storage quota in bytes; `None` = unlimited. Sourced from the subscribed plan (§29).
    pub quota_bytes: Option<i64>,
    /// Token budget; `None` = unlimited.
    pub quota_tokens: Option<i64>,
    /// Monthly spend budget in cents (USD); `None` = unlimited (plan §26.6).
    pub budget_monthly_cents: Option<i64>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}

/// A post-deletion audit record for a crypto-shredded tenant (plan §28, P10-T4).
///
/// When a tenant is deleted via crypto-shredding, the tenant row and all tenant-scoped
/// data are removed from the database. The tombstone preserves the `tenant_id`, `slug`,
/// and `name` alongside who deleted it and when — providing proof that crypto-shredding
/// occurred. The tenant's DEK was destroyed with the `tenant_data_keys` row, so all
/// encrypted data (B2 blobs, DB encrypted columns) is irrecoverable.
///
/// Tombstones are purely an audit trail; they carry no encrypted data and never contain
/// plaintext from the deleted tenant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantTombstone {
    /// Surrogate primary key (`tenant_tombstones.id`).
    pub id: i64,
    /// The tenant id that was deleted (no FK — the tenant row is gone).
    pub tenant_id: i64,
    /// Snapshot of the tenant slug at deletion time.
    pub tenant_slug: String,
    /// Snapshot of the tenant name at deletion time.
    pub tenant_name: String,
    /// The super-admin user who initiated the deletion (`None` if system-initiated).
    pub deleted_by: Option<i64>,
    /// When the deletion occurred.
    pub deleted_at: DateTime<Utc>,
}
