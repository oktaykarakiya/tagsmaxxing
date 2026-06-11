// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tag records — canonical, multi-label, semantically-deduplicated tags (plan §6.5).

use serde::{Deserialize, Serialize};

use crate::macros::str_enum;

str_enum! {
    /// Provenance of a document-tag association: whether the tag was assigned by the LLM
    /// (automatic) or by a human user (manual). Stored in the `document_tags.source` column
    /// (plan §6.5, P6-T11).
    ///
    /// User-assigned tags are typically also `locked` (the `document_tags.locked` column)
    /// so they survive re-tag operations.
    pub enum TagSource {
        /// Tag assigned by the LLM during automatic ingestion or re-tag.
        Llm = "llm",
        /// Tag added or confirmed by a human user via the UI.
        User = "user",
    }
}

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn tag_source_wire_strings_are_locked() {
        assert_eq!(TagSource::Llm.as_str(), "llm");
        assert_eq!(TagSource::User.as_str(), "user");
    }

    #[test]
    fn tag_source_round_trips_via_from_str() {
        for v in TagSource::all() {
            assert_eq!(TagSource::from_str(v.as_str()).unwrap(), *v);
        }
    }

    #[test]
    fn tag_source_serde_matches_as_str() {
        for v in TagSource::all() {
            let json = serde_json::to_string(v).unwrap();
            assert_eq!(json, format!("\"{}\"", v.as_str()));
            let back: TagSource = serde_json::from_str(&json).unwrap();
            assert_eq!(back, *v);
        }
    }
}
