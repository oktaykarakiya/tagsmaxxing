// SPDX-License-Identifier: AGPL-3.0-or-later

//! Reciprocal Rank Fusion (RRF) and document rollup — re-exported from `kb_core::query`
//! where the pure functions live (plan §8). They reside in core to avoid a future circular
//! dependency: `kb-store` will need them for `PgStore::hybrid_search` (P4-T3), and
//! `kb-store` already depends on `kb-core` but cannot depend on `kb-pipeline`.

pub use kb_core::query::{ChunkHit, RRF_K, document_rollup, rrf_fuse};
