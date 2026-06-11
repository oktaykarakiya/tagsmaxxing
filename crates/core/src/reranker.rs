// SPDX-License-Identifier: AGPL-3.0-or-later

//! The `Reranker` capability (plan §4, §8).

use async_trait::async_trait;

/// A cross-encoder that rescores retrieval candidates against the query for precision
/// (plan §8 step 5).
#[async_trait]
pub trait Reranker: Send + Sync {
    /// Score each document's relevance to `query`. The returned scores are order-aligned with
    /// `docs` (higher = more relevant).
    ///
    /// `priority` (P9-T12) is the caller's priority for the priority-ordered
    /// scheduler waiter queue (higher = more urgent, default 0).
    ///
    /// `local_only` constrains the backend selection to on-premises backends
    /// only (plan §26.6, P9-T9). When `true`, the call must not reach a remote
    /// provider. The scheduler enforces this at acquire time.
    ///
    /// `tenant_id` and `user_id` attribute the rerank call's token usage on the
    /// search read path (P14-T2). Implementations that meter (e.g. the
    /// llama.cpp-backed reranker) record a [`Role::Rerank`](crate::role::Role)
    /// usage event for the owning tenant, stamped with the acting user when
    /// known (`None` for system/CLI searches). Metering is best-effort and must
    /// never fail the rerank call.
    ///
    /// # Errors
    /// Returns an error if the backend call fails. On success the output length equals
    /// `docs.len()`.
    async fn rerank(
        &self,
        query: &str,
        docs: &[String],
        local_only: bool,
        priority: i32,
        tenant_id: i64,
        user_id: Option<i64>,
    ) -> anyhow::Result<Vec<f32>>;
}
