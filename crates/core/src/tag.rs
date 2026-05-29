//! Tag records — canonical, multi-label, semantically-deduplicated tags (plan §6.5).

use serde::{Deserialize, Serialize};

/// A canonical tag for a tenant. Free-text categories are deliberately avoided; instead
/// documents carry many canonical tags, deduplicated semantically at write time via the
/// `embedding` (plan §6.5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    /// Surrogate primary key (`tags.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Canonical tag name (unique per tenant).
    pub name: String,
    /// Embedding of the name, for synonym/merge detection; `None` until computed.
    pub embedding: Option<Vec<f32>>,
}

/// A raw form that maps to a canonical [`Tag`] (e.g. "bill" -> "invoice").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagAlias {
    /// Owning tenant.
    pub tenant_id: i64,
    /// The raw/alternate spelling.
    pub alias: String,
    /// The canonical tag this alias resolves to.
    pub tag_id: i64,
}
