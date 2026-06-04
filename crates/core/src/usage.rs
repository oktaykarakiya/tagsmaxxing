//! Per-call usage accounting record (plan §5, §15).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::role::Role;

/// One model call's usage, written to `usage_events`. Drives capacity planning, per-tenant
/// quotas, and — for priced remote backends — spend (plan §15).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// Surrogate primary key (`usage_events.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Acting user, if attributable.
    pub user_id: Option<i64>,
    /// Provider model id/alias that served the call.
    pub model: String,
    /// The capability role the call used.
    pub role: Role,
    /// Backend id that served the call, if known.
    pub backend_id: Option<String>,
    /// Prompt tokens consumed, if reported.
    pub prompt_tokens: Option<i32>,
    /// Completion tokens produced, if reported.
    pub completion_tokens: Option<i32>,
    /// End-to-end latency in milliseconds, if measured.
    pub latency_ms: Option<i32>,
    /// Micro-dollar cost of this call, computed from tokens × pricing.
    /// `None` means the call was free (local backend) or pricing was
    /// unavailable. Plan §26.6; stored in `usage_events.cost_micros`.
    pub cost_micros: Option<i64>,
    /// When the call occurred.
    pub created_at: DateTime<Utc>,
}

/// Sink that records per-call usage so it can be metered, quota-enforced, and
/// (for priced backends) billed (BUG-BILL-03).
///
/// Implemented by the store (persisting to `usage_events`) and injected into the
/// components that make model calls — the tagger and the chunk embedder — so each
/// call's token usage is attributed to the calling tenant at the point where the
/// model, role, and token counts are known.
///
/// Recording is **best-effort**: implementors surface errors for logging, but
/// callers must not let a metering failure abort the surrounding ingest.
#[async_trait]
pub trait UsageRecorder: Send + Sync {
    /// Persist a single usage event.
    ///
    /// # Errors
    /// Returns an error if the event could not be persisted; the caller logs it
    /// and continues (metering must never fail an ingest).
    async fn record_usage(&self, event: &UsageEvent) -> anyhow::Result<()>;
}
