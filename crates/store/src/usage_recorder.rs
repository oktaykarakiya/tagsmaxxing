//! [`UsageRecorder`] implementation for [`PgStore`] (BUG-BILL-03).
//!
//! Bridges the [`kb_core::usage::UsageRecorder`] capability — injected into the
//! tagger and chunk embedder so each model call's token usage is metered — to
//! the existing tenant-scoped [`PgStore::insert_usage_event`] persistence.

use async_trait::async_trait;
use kb_core::usage::{UsageEvent, UsageRecorder};

use crate::pg_store::PgStore;

#[async_trait]
impl UsageRecorder for PgStore {
    /// Persist a usage event by delegating to
    /// [`PgStore::insert_usage_event`], discarding the returned row id.
    async fn record_usage(&self, event: &UsageEvent) -> anyhow::Result<()> {
        self.insert_usage_event(event).await.map(|_| ())
    }
}
