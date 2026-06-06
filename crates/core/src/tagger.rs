//! The `Tagger` capability and its I/O types (plan §4, §7, §9).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kind::DocKind;

/// Input to the tagger: everything known about a whole document, tagged once over all pages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagInput {
    /// Owning tenant, used to attribute the tagging model call's token usage to
    /// the right tenant in `usage_events` (BUG-BILL-03).
    pub tenant_id: i64,
    /// Acting user, used to attribute the tagging call's token usage to a
    /// specific user in `usage_events` (P14-T1). `None` for system/background
    /// tagging where no user is attributable.
    pub user_id: Option<i64>,
    /// Combined extracted text across all member pages.
    pub text: String,
    /// The user's note/description (typed or voice-transcribed); strongly drives tags.
    pub user_note: Option<String>,
    /// Detected document kind, supplied to the model as a hint.
    pub kind: DocKind,
    /// Merged deterministic metadata across pages.
    pub meta: serde_json::Value,
}

/// The schema-constrained tagger output (plan §7 step 4).
///
/// There is intentionally **no** `category` field: the design uses multi-label canonical tags
/// rather than one mutually-exclusive bucket (plan §1, §6.5). This corrects the stale §4
/// sketch that listed `category`. (Recorded in `BUILD_LEDGER.toml`, task `P0-T4`.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagOutput {
    /// A concise generated title for the document.
    pub title: String,
    /// A summary synthesized across all pages.
    pub summary: String,
    /// Raw proposed tags, later canonicalized against the tenant's tag set (plan §6.5).
    pub tags: Vec<String>,
}

/// Generates a title/summary/tags for a whole document via a JSON-schema-constrained model.
#[async_trait]
pub trait Tagger: Send + Sync {
    /// Tag a document.
    ///
    /// `local_only` constrains the backend selection to on-premises backends
    /// only (plan §26.6, P9-T9). When `true`, the call must not reach a remote
    /// provider. The scheduler enforces this at acquire time.
    ///
    /// # Errors
    /// Returns an error if the model call fails or the output violates the schema.
    async fn tag(&self, input: &TagInput, local_only: bool) -> anyhow::Result<TagOutput>;
}
