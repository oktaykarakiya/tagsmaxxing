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
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}
