//! The document record — the semantic unit a user retrieves (plan §5, §27).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::kind::DocKind;
use crate::status::ProcessingStatus;

/// A document: the unit the user searches for, composed of one or more member
/// [`FileRecord`](crate::file::FileRecord) pages (e.g. front+back of an ID, pages of a PDF).
/// A single-file upload auto-creates a one-page document (plan §27).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    /// Surrogate primary key (`documents.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// LLM-generated title (may be `None` before tagging).
    pub title: Option<String>,
    /// LLM-generated summary synthesized across *all* pages.
    pub summary: Option<String>,
    /// The user's free-text note/description (typed or voice-transcribed); drives tagging.
    pub user_note: Option<String>,
    /// Document kind.
    pub kind: DocKind,
    /// Union of member metadata plus document-level fields (`documents.meta` JSONB).
    pub meta: serde_json::Value,
    /// Number of member pages/files.
    pub page_count: i32,
    /// Processing lifecycle status.
    pub status: ProcessingStatus,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// If `true`, this document's content must never be sent to a remote
    /// provider — even if the tenant allows remote. Set automatically for
    /// `identity_document` kind (plan §17) and optionally by the user.
    /// Enforced at the Pool::acquire level (plan §26.6, P9-T9).
    pub local_only: bool,
}
