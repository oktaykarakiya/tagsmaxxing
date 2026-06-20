// SPDX-License-Identifier: AGPL-3.0-or-later

//! Hybrid search SQL implementation — vector (HNSW cosine) + keyword (ts_rank on tsv),
//! fused with Reciprocal Rank Fusion and rolled up to document-level [`Hit`]s with
//! deep-link provenance (plan §8, P4-T3).
//!
//! The module runs two queries inside a single `PgStore::hybrid_search` call:
//! 1. Vector top-N via `chunks.embedding <=> CAST($vec AS vector) ORDER BY distance LIMIT N`
//! 2. Keyword top-N via `ts_rank(tsv, websearch_to_tsquery('simple', $q)) ORDER BY rank DESC`
//!
//! Both arms **overscan** the index (bounded by [`overscan_limit`]) and then keep
//! only the best chunk per document ([`dedup_best_chunk_per_document`]) so each
//! arm contributes up to `top_k` *distinct documents* — a single document with
//! many near-identical matching chunks must not monopolize the candidate set
//! (BUG-SEARCH-04).
//!
//! Filters (kinds, tags via document_tags, created_after/before) are pushed into both
//! queries identically. The two ranked lists feed [`rrf_fuse`] → [`document_rollup`]
//! → top-K [`Hit`]s. Tenant isolation is handled by Postgres RLS (§13); no explicit
//! `tenant_id` filter is added to the SQL.

use std::collections::HashMap;

use anyhow::Context;
use kb_core::query::{ChunkHit, Hit, Query, RRF_K, document_rollup, rrf_fuse};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use crate::pg_store::{PgStore, begin_tenant_tx};

/// One row returned by the vector or keyword chunk query. Field names match the
/// column aliases in the SELECT list so `sqlx::FromRow` works without `#[sqlx(rename)]`.
#[derive(Debug, sqlx::FromRow)]
struct ChunkRow {
    chunk_id: i64,
    document_id: i64,
    file_id: i64,
    page_no: Option<i32>,
    ts_offset: Option<f64>,
    content: String,
    content_enc: Option<Vec<u8>>,
    title: Option<String>,
    #[sqlx(default)]
    kind: Option<String>,
}

/// Execute the full hybrid search pipeline (plan §8 steps 3–4).
///
/// Sets `app.current_tenant` before querying so Postgres RLS (§13) enforces
/// tenant isolation.
///
/// # Errors
/// Returns an error if the database is not connected, tag resolution fails, or any
/// query fails.
pub(crate) async fn run_hybrid_search(
    store: &PgStore,
    tenant_id: i64,
    query: &Query,
    query_embedding: &[f32],
) -> anyhow::Result<Vec<Hit>> {
    // Edge case: top_k of 0.
    if query.top_k == 0 {
        return Ok(Vec::new());
    }

    // All three reads run inside ONE tenant transaction on the kb_app pool, so the
    // `app.current_tenant` GUC set by begin_tenant_tx is live for every query and RLS scopes
    // them to this tenant (P6-T14). Running them on separate pooled connections would lose the
    // transaction-local GUC and deny every row under the non-BYPASSRLS role.
    let pool = store.app_pool()?;
    let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

    // Resolve tag names to IDs if a tag filter is set. We resolve once and reuse for
    // both queries.
    let tag_ids = if !query.filters.tags.is_empty() {
        resolve_tag_ids_for_filter(&mut tx, &query.filters.tags).await?
    } else {
        Vec::new()
    };

    // If a tag filter was requested but no tags resolved (e.g. the named tags don't
    // exist), no document can match — return empty.
    if !query.filters.tags.is_empty() && tag_ids.is_empty() {
        return Ok(Vec::new());
    }

    let vec_chunks = execute_vector_query(&mut tx, query, query_embedding, &tag_ids).await?;
    let kw_chunks = execute_keyword_query(&mut tx, query, &tag_ids).await?;
    tx.commit()
        .await
        .context("failed to commit hybrid_search transaction")?;

    // Extract ranked IDs for RRF. The DB query returns rows in relevance order, so
    // position in the Vec is the rank.
    let vec_ids: Vec<i64> = vec_chunks.iter().map(|c| c.chunk_id).collect();
    let kw_ids: Vec<i64> = kw_chunks.iter().map(|c| c.chunk_id).collect();

    // RRF fusion on chunk IDs.
    let fused = rrf_fuse(&vec_ids, &kw_ids, RRF_K);

    // Map fused (chunk_id, score) pairs back to full ChunkHit values. We collect all
    // chunk rows into a map by chunk_id for O(1) lookup.
    let chunk_map: HashMap<i64, &ChunkRow> = vec_chunks
        .iter()
        .chain(kw_chunks.iter())
        .map(|c| (c.chunk_id, c))
        .collect();

    let chunk_hits = ranked_rows_to_chunk_hits(store, tenant_id, &fused, &chunk_map).await;

    // Roll up to one Hit per document, keeping best chunk's provenance.
    let mut hits = document_rollup(&chunk_hits);

    // Truncate to requested top_k.
    hits.truncate(query.top_k);

    Ok(hits)
}

/// Execute the **keyword-only** (lexical) search path — the RLS-safe degrade used when a
/// tenant is over budget or the embed/rerank backend is unavailable (plan §8, §29, P14-T5).
///
/// This is structurally a slice of [`run_hybrid_search`]: it opens the **same**
/// `begin_tenant_tx` on the kb_app pool (so the `app.current_tenant` GUC drives RLS),
/// runs **only** [`execute_keyword_query`] with the identical [`push_filters`]
/// (kinds/tags/dates), then rolls the matching chunks up to documents and truncates to
/// `query.top_k`, decrypting snippets exactly as [`run_hybrid_search`] does. There is no
/// vector query, no RRF fusion, and no rerank.
///
/// Because lexical relevance order from `ORDER BY ts_rank(...) DESC` already ranks the
/// rows, position in the returned `Vec` is the rank — we synthesise a descending score
/// from that rank so `document_rollup` keeps each document's best (earliest) chunk.
///
/// # Errors
/// Returns an error if the database is not connected, tag resolution fails, or the
/// keyword query fails.
pub(crate) async fn run_keyword_search(
    store: &PgStore,
    tenant_id: i64,
    query: &Query,
) -> anyhow::Result<Vec<Hit>> {
    // Edge case: top_k of 0.
    if query.top_k == 0 {
        return Ok(Vec::new());
    }

    // IDENTICAL RLS setup to run_hybrid_search: one tenant transaction on the kb_app pool
    // so the `app.current_tenant` GUC set by begin_tenant_tx scopes the keyword query to
    // this tenant under the non-BYPASSRLS role (P6-T14, plan §13).
    let pool = store.app_pool()?;
    let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

    // Same tag-filter resolution + short-circuit as hybrid search.
    let tag_ids = if !query.filters.tags.is_empty() {
        resolve_tag_ids_for_filter(&mut tx, &query.filters.tags).await?
    } else {
        Vec::new()
    };
    if !query.filters.tags.is_empty() && tag_ids.is_empty() {
        return Ok(Vec::new());
    }

    let kw_chunks = execute_keyword_query(&mut tx, query, &tag_ids).await?;
    tx.commit()
        .await
        .context("failed to commit keyword_search transaction")?;

    // The DB returns rows in descending ts_rank order, so position == rank. Synthesise a
    // strictly-decreasing score from the rank so document_rollup keeps the best chunk per
    // document (mirrors how RRF scores feed rollup in hybrid search).
    let ranked: Vec<(i64, f32)> = kw_chunks
        .iter()
        .enumerate()
        .map(|(rank, c)| (c.chunk_id, 1.0 / (rank as f32 + 1.0)))
        .collect();

    let chunk_map: HashMap<i64, &ChunkRow> = kw_chunks.iter().map(|c| (c.chunk_id, c)).collect();

    let chunk_hits = ranked_rows_to_chunk_hits(store, tenant_id, &ranked, &chunk_map).await;

    let mut hits = document_rollup(&chunk_hits);
    hits.truncate(query.top_k);

    Ok(hits)
}

/// Map ranked `(chunk_id, score)` pairs back to [`ChunkHit`]s, decrypting each winning
/// chunk's snippet under the tenant DEK. Rows whose `chunk_id` is absent from `chunk_map`
/// are silently dropped (they may have been filtered by another tenant's RLS). Shared by
/// the hybrid and keyword-only paths so snippet decryption is identical (P14-T5).
async fn ranked_rows_to_chunk_hits(
    store: &PgStore,
    tenant_id: i64,
    ranked: &[(i64, f32)],
    chunk_map: &HashMap<i64, &ChunkRow>,
) -> Vec<ChunkHit> {
    let mut hits = Vec::with_capacity(ranked.len());
    for (chunk_id, score) in ranked {
        if let Some(row) = chunk_map.get(chunk_id) {
            let snippet = store
                .decrypt_chunk_content(tenant_id, row.content_enc.as_deref(), &row.content)
                .await;
            hits.push(ChunkHit {
                chunk_id: row.chunk_id,
                document_id: row.document_id,
                score: *score,
                file_id: row.file_id,
                page_no: row.page_no,
                ts_offset: row.ts_offset,
                snippet,
                title: row.title.clone(),
                kind: row.kind.clone(),
            });
        }
    }
    hits
}

// ── Tag resolution ────────────────────────────────────────────────────────────

/// Resolve a list of canonical tag *names* to their primary-key ids.
///
/// Tags that don't exist are silently dropped. If the caller requested a tag filter
/// and *no* tags resolve, the search returns zero results (handled by the caller).
async fn resolve_tag_ids_for_filter(
    conn: &mut PgConnection,
    tag_names: &[String],
) -> anyhow::Result<Vec<i64>> {
    let rows: Vec<(i64,)> = sqlx::query_as("SELECT id FROM tags WHERE name = ANY($1)")
        .bind(tag_names)
        .fetch_all(&mut *conn)
        .await
        .context("failed to resolve tag names for filter")?;

    Ok(rows.into_iter().map(|(id,)| id).collect())
}

// ── Query execution ──────────────────────────────────────────────────────────

/// Overscan multiplier for per-document candidate diversification (BUG-SEARCH-04).
const OVERSCAN_FACTOR: usize = 8;
/// Overscan floor — must exceed the chunk count of any realistic single
/// document "attractor" so one document cannot fill the whole scan window
/// (a 200 KB uniform file chunks to ~100 near-identical embeddings).
const OVERSCAN_FLOOR: usize = 256;
/// Overscan ceiling — pgvector's `hnsw.ef_search` upper bound is 1000, and the
/// scan window must stay within it for recall to cover the window.
const OVERSCAN_CEILING: usize = 1000;

/// How many chunk rows to fetch from one arm before per-document dedup.
fn overscan_limit(top_k: usize) -> usize {
    top_k
        .saturating_mul(OVERSCAN_FACTOR)
        .clamp(OVERSCAN_FLOOR, OVERSCAN_CEILING)
}

/// Keep only the best-ranked (first) chunk of each document, preserving rank
/// order, until `top_k` distinct documents are collected (BUG-SEARCH-04).
///
/// Without this, a single document whose many near-identical chunks all sit
/// close to the query monopolizes a plain `LIMIT top_k` arm — e.g. one large
/// uniform-content file became a semantic "attractor" whose ~100 identical
/// chunks consumed every vector slot, starving all other documents out of the
/// candidate set (the downstream rollup then yields a single hit). The
/// document rollup keeps only each document's best chunk anyway, so dropping
/// the duplicates here loses nothing.
fn dedup_best_chunk_per_document(rows: Vec<ChunkRow>, top_k: usize) -> Vec<ChunkRow> {
    if top_k == 0 {
        return Vec::new();
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(top_k.min(rows.len()));
    for row in rows {
        if seen.insert(row.document_id) {
            out.push(row);
            if out.len() == top_k {
                break;
            }
        }
    }
    out
}

/// Run the vector (HNSW cosine) top-N query, returning the best chunk of each
/// of up to `top_k` distinct documents, ordered by increasing cosine distance
/// (most similar first).
async fn execute_vector_query(
    conn: &mut PgConnection,
    query: &Query,
    query_embedding: &[f32],
    tag_ids: &[i64],
) -> anyhow::Result<Vec<ChunkRow>> {
    let fetch = overscan_limit(query.top_k);

    // HNSW returns at most ef_search candidates regardless of LIMIT; the scan
    // window must cover the overscan or the widened LIMIT silently degrades.
    // `SET LOCAL` is transaction-scoped (we are inside begin_tenant_tx) and the
    // value is a computed integer, so inlining it is safe (GUCs cannot be
    // bound as parameters).
    sqlx::query(&format!("SET LOCAL hnsw.ef_search = {fetch}"))
        .execute(&mut *conn)
        .await
        .context("failed to set hnsw.ef_search for vector overscan")?;

    let mut builder = build_base_select();
    push_filters(&mut builder, &query.filters, tag_ids);
    // pgvector caps `vector` ANN indexes at 2000 dims, so the chunks HNSW index is built on a
    // `halfvec(2560)` cast. The ORDER BY must use the SAME cast on both sides
    // to hit that index instead of a sequential scan.
    builder.push(" ORDER BY c.embedding::halfvec(2560) <=> CAST(");
    builder.push_bind(crate::pg_store::format_vector(query_embedding));
    builder.push(" AS halfvec(2560)) LIMIT ");
    builder.push_bind(fetch as i64);

    let rows = builder
        .build_query_as::<ChunkRow>()
        .fetch_all(&mut *conn)
        .await
        .context("vector search query failed")?;

    Ok(dedup_best_chunk_per_document(rows, query.top_k))
}

/// Run the keyword (ts_rank on tsv) top-N query, returning the best chunk of
/// each of up to `top_k` distinct documents (same diversification as the
/// vector arm — a many-matching-chunks document must not starve the rest).
/// When `query.text` is empty or whitespace-only the keyword signal
/// contributes nothing — return an empty list.
async fn execute_keyword_query(
    conn: &mut PgConnection,
    query: &Query,
    tag_ids: &[i64],
) -> anyhow::Result<Vec<ChunkRow>> {
    let trimmed = query.text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    // Owned copy for binding into QueryBuilder<'static, _>.
    let query_text = trimmed.to_string();

    let mut builder = build_base_select();
    // Keyword match condition — must come before optional filters.
    builder.push(" AND c.tsv @@ plainto_tsquery('simple', ");
    builder.push_bind(query_text.clone());
    builder.push(")");
    push_filters(&mut builder, &query.filters, tag_ids);
    builder.push(" ORDER BY ts_rank(c.tsv, plainto_tsquery('simple', ");
    builder.push_bind(query_text);
    builder.push(")) DESC LIMIT ");
    builder.push_bind(overscan_limit(query.top_k) as i64);

    let rows = builder
        .build_query_as::<ChunkRow>()
        .fetch_all(&mut *conn)
        .await
        .context("keyword search query failed")?;

    Ok(dedup_best_chunk_per_document(rows, query.top_k))
}

// ── SQL construction helpers ─────────────────────────────────────────────────

/// Build the common SELECT + JOIN prefix used by both vector and keyword queries.
fn build_base_select() -> QueryBuilder<'static, Postgres> {
    QueryBuilder::new(
        "SELECT c.id AS chunk_id, c.document_id, c.file_id, \
         c.page_no, c.ts_offset, c.content, c.content_enc, d.title, d.kind \
         FROM chunks c \
         JOIN documents d ON c.document_id = d.id \
         WHERE TRUE",
    )
}

/// Push optional filter clauses (kinds, tags, date range) into `builder`.
///
/// Each active filter is appended as ` AND <clause>`. `WHERE TRUE` must already
/// be present in the builder before calling this function.
fn push_filters(
    builder: &mut QueryBuilder<'static, Postgres>,
    filters: &kb_core::query::QueryFilters,
    tag_ids: &[i64],
) {
    // Kind filter.
    if !filters.kinds.is_empty() {
        let kind_strs: Vec<String> = filters
            .kinds
            .iter()
            .map(|k| k.as_str().to_string())
            .collect();
        builder.push(" AND d.kind = ANY(");
        builder.push_bind(kind_strs);
        builder.push(")");
    }

    // Tag filter — documents must carry ALL specified tags.
    if !tag_ids.is_empty() {
        builder.push(
            " AND (SELECT COUNT(DISTINCT dt2.tag_id) FROM document_tags dt2 \
             WHERE dt2.document_id = d.id AND dt2.tag_id = ANY(",
        );
        builder.push_bind(tag_ids.to_vec());
        builder.push(")) = ");
        builder.push_bind(tag_ids.len() as i64);
    }

    // Date range filters.
    if let Some(after) = filters.created_after {
        builder.push(" AND d.created_at >= ");
        builder.push_bind(after);
    }
    if let Some(before) = filters.created_before {
        builder.push(" AND d.created_at <= ");
        builder.push_bind(before);
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use kb_core::kind::DocKind;
    use kb_core::query::QueryFilters;
    use kb_core::store::Store;

    // ── resolve_tag_ids_for_filter (unit — requires connected DB; tested via integration below) ──

    // ── Candidate diversification (BUG-SEARCH-04) ──────────────────────────

    fn row(chunk_id: i64, document_id: i64) -> ChunkRow {
        ChunkRow {
            chunk_id,
            document_id,
            file_id: 1,
            page_no: None,
            ts_offset: None,
            content: format!("chunk {chunk_id}"),
            content_enc: None,
            title: None,
            kind: None,
        }
    }

    #[test]
    fn overscan_limit_clamps_to_floor_and_ceiling() {
        // Small top_k hits the floor (must cover a ~100-chunk attractor doc).
        assert_eq!(overscan_limit(0), OVERSCAN_FLOOR);
        assert_eq!(overscan_limit(8), OVERSCAN_FLOOR);
        // Mid-range scales by the factor.
        assert_eq!(overscan_limit(64), 64 * OVERSCAN_FACTOR);
        // Large top_k is capped at pgvector's ef_search bound.
        assert_eq!(overscan_limit(500), OVERSCAN_CEILING);
        assert_eq!(overscan_limit(usize::MAX), OVERSCAN_CEILING);
    }

    #[test]
    fn dedup_keeps_best_chunk_per_document_in_rank_order() {
        // Doc 7's three chunks lead the ranking (the attractor pattern); docs
        // 8 and 9 follow. Dedup must keep 7's FIRST chunk only, then 8, then 9.
        let rows = vec![row(1, 7), row(2, 7), row(3, 7), row(4, 8), row(5, 9)];
        let out = dedup_best_chunk_per_document(rows, 10);
        let kept: Vec<(i64, i64)> = out.iter().map(|r| (r.chunk_id, r.document_id)).collect();
        assert_eq!(kept, vec![(1, 7), (4, 8), (5, 9)]);
    }

    #[test]
    fn dedup_stops_at_top_k_distinct_documents() {
        let rows = vec![row(1, 1), row(2, 2), row(3, 3), row(4, 4)];
        let out = dedup_best_chunk_per_document(rows, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].document_id, 1);
        assert_eq!(out[1].document_id, 2);
    }

    #[test]
    fn dedup_attractor_cannot_monopolize_the_arm() {
        // 100 identical-embedding chunks of doc 42 ahead of two real matches —
        // exactly the observed corpus poisoning. The arm must still surface
        // the other documents.
        let mut rows: Vec<ChunkRow> = (0..100).map(|i| row(i, 42)).collect();
        rows.push(row(200, 1));
        rows.push(row(201, 2));
        let out = dedup_best_chunk_per_document(rows, 8);
        let docs: Vec<i64> = out.iter().map(|r| r.document_id).collect();
        assert_eq!(docs, vec![42, 1, 2]);
    }

    #[test]
    fn dedup_empty_and_zero_top_k() {
        assert!(dedup_best_chunk_per_document(Vec::new(), 5).is_empty());
        assert!(dedup_best_chunk_per_document(vec![row(1, 1)], 0).is_empty());
    }

    // ── SQL construction tests ─────────────────────────────────────────────

    /// Build a vector query with no filters and check the SQL structure.
    #[test]
    fn vector_query_no_filters_sql_structure() {
        let filters = QueryFilters::default();
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &[]);
        builder.push(" ORDER BY c.embedding <=> CAST(");
        builder.push_bind("[0.1,0.2]".to_string());
        builder.push(" AS vector) LIMIT ");
        builder.push_bind(10i64);

        let sql = builder.sql();
        assert!(
            sql.contains("SELECT c.id AS chunk_id"),
            "missing SELECT: {sql}"
        );
        assert!(sql.contains("JOIN documents d"), "missing JOIN: {sql}");
        assert!(sql.contains("WHERE TRUE"), "missing WHERE TRUE: {sql}");
        assert!(
            sql.contains("ORDER BY c.embedding <=>"),
            "missing ORDER BY: {sql}"
        );
        assert!(sql.contains("LIMIT"), "missing LIMIT: {sql}");
        // No filter clauses when filters are empty.
        assert!(
            !sql.contains("d.kind = ANY"),
            "unexpected kind filter: {sql}"
        );
        assert!(
            !sql.contains("document_tags"),
            "unexpected tag filter: {sql}"
        );
        assert!(
            !sql.contains("d.created_at"),
            "unexpected date filter: {sql}"
        );
    }

    /// Build a query with kind filter and verify the SQL contains the expected clauses.
    #[test]
    fn vector_query_with_kind_filter() {
        let filters = QueryFilters {
            kinds: vec![DocKind::Image, DocKind::Document],
            ..Default::default()
        };
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &[]);
        builder.push(" ORDER BY c.embedding <=> CAST(");
        builder.push_bind("[0.0]".to_string());
        builder.push(" AS vector) LIMIT ");
        builder.push_bind(5i64);

        let sql = builder.sql();
        assert!(sql.contains("d.kind = ANY"), "missing kind filter: {sql}");
        assert!(
            !sql.contains("document_tags"),
            "unexpected tag filter: {sql}"
        );
    }

    /// Build a query with tag filter and verify the document_tags subquery is present.
    #[test]
    fn vector_query_with_tag_filter() {
        let filters = QueryFilters::default();
        let tag_ids = vec![1i64, 2i64];
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &tag_ids);
        builder.push(" ORDER BY c.embedding <=> CAST(");
        builder.push_bind("[0.0]".to_string());
        builder.push(" AS vector) LIMIT ");
        builder.push_bind(5i64);

        let sql = builder.sql();
        assert!(sql.contains("document_tags"), "missing tag subquery: {sql}");
        assert!(
            sql.contains("COUNT(DISTINCT dt2.tag_id)"),
            "missing COUNT in tag filter: {sql}"
        );
    }

    /// Build a query with created_after filter.
    #[test]
    fn vector_query_with_date_after_filter() {
        let filters = QueryFilters {
            created_after: Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
            ..Default::default()
        };
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &[]);
        builder.push(" ORDER BY c.embedding <=> CAST(");
        builder.push_bind("[0.0]".to_string());
        builder.push(" AS vector) LIMIT ");
        builder.push_bind(5i64);

        let sql = builder.sql();
        assert!(
            sql.contains("d.created_at >="),
            "missing created_after filter: {sql}"
        );
        assert!(
            !sql.contains("d.created_at <="),
            "unexpected created_before: {sql}"
        );
    }

    /// Build a query with created_before filter.
    #[test]
    fn vector_query_with_date_before_filter() {
        let filters = QueryFilters {
            created_before: Some(Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap()),
            ..Default::default()
        };
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &[]);
        builder.push(" ORDER BY c.embedding <=> CAST(");
        builder.push_bind("[0.0]".to_string());
        builder.push(" AS vector) LIMIT ");
        builder.push_bind(5i64);

        let sql = builder.sql();
        assert!(
            sql.contains("d.created_at <="),
            "missing created_before filter: {sql}"
        );
    }

    /// All filters combined — verify each clause appears and AND is used.
    #[test]
    fn vector_query_all_filters() {
        let filters = QueryFilters {
            kinds: vec![DocKind::Document],
            tags: vec!["invoice".to_string()], // tags field on QueryFilters (unused in push_filters directly)
            created_after: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
            created_before: Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
        };
        let tag_ids = vec![42i64];
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &tag_ids);
        builder.push(" ORDER BY c.embedding <=> CAST(");
        builder.push_bind("[0.0]".to_string());
        builder.push(" AS vector) LIMIT ");
        builder.push_bind(10i64);

        let sql = builder.sql();
        assert!(sql.contains("d.kind = ANY"), "missing kind: {sql}");
        assert!(sql.contains("document_tags"), "missing tag subquery: {sql}");
        assert!(
            sql.contains("d.created_at >="),
            "missing created_after: {sql}"
        );
        assert!(
            sql.contains("d.created_at <="),
            "missing created_before: {sql}"
        );
    }

    /// Keyword query basic structure (query text present).
    #[test]
    fn keyword_query_structure() {
        let filters = QueryFilters::default();
        let mut builder = build_base_select();
        builder.push(" AND c.tsv @@ websearch_to_tsquery('simple', ");
        builder.push_bind("test query");
        builder.push(")");
        push_filters(&mut builder, &filters, &[]);
        builder.push(" ORDER BY ts_rank(c.tsv, websearch_to_tsquery('simple', ");
        builder.push_bind("test query");
        builder.push(")) DESC LIMIT ");
        builder.push_bind(10i64);

        let sql = builder.sql();
        assert!(
            sql.contains("tsv @@ websearch_to_tsquery"),
            "missing tsv match: {sql}"
        );
        assert!(sql.contains("ts_rank"), "missing ts_rank: {sql}");
        assert!(sql.contains("DESC"), "missing DESC: {sql}");
    }

    /// Empty tag_ids passed to push_filters should not add any tag clause.
    #[test]
    fn push_filters_empty_tags_noop() {
        let filters = QueryFilters::default();
        let mut builder = build_base_select();
        push_filters(&mut builder, &filters, &[]);
        let sql = builder.sql();
        assert!(
            !sql.contains("document_tags"),
            "unexpected tag filter: {sql}"
        );
    }

    /// Empty kinds passed to push_filters should not add any kind clause.
    #[test]
    fn push_filters_empty_kinds_noop() {
        let mut builder = build_base_select();
        push_filters(&mut builder, &QueryFilters::default(), &[]);
        let sql = builder.sql();
        assert!(
            !sql.contains("d.kind = ANY"),
            "unexpected kind filter: {sql}"
        );
    }

    // ── ChunkRow → ChunkHit → Hit rollup (pure logic) ────────────────────

    /// Verify that fused (chunk_id, score) pairs are correctly mapped to ChunkHit
    /// via the chunk_map and that document_rollup produces the expected Hits.
    #[test]
    fn fused_to_hit_through_rollup() {
        // Simulate what run_hybrid_search does after RRF fusion.
        let rows = [
            ChunkRow {
                chunk_id: 1,
                document_id: 10,
                file_id: 100,
                page_no: Some(3),
                ts_offset: None,
                content: "snippet A".into(),
                content_enc: None,
                title: Some("Doc Ten".into()),
                kind: Some("document".into()),
            },
            ChunkRow {
                chunk_id: 2,
                document_id: 10,
                file_id: 101,
                page_no: Some(5),
                ts_offset: None,
                content: "snippet B better".into(),
                content_enc: None,
                title: Some("Doc Ten".into()),
                kind: Some("document".into()),
            },
            ChunkRow {
                chunk_id: 3,
                document_id: 20,
                file_id: 200,
                page_no: None,
                ts_offset: Some(30.0),
                content: "audio snippet".into(),
                content_enc: None,
                title: None,
                kind: Some("audio".into()),
            },
        ];

        let chunk_map: HashMap<i64, &ChunkRow> = rows.iter().map(|r| (r.chunk_id, r)).collect();

        // Simulate RRF fused scores: chunk 2 scored highest, then 3, then 1.
        let fused = [(2i64, 0.05f32), (3i64, 0.03f32), (1i64, 0.02f32)];

        let chunk_hits: Vec<ChunkHit> = fused
            .iter()
            .filter_map(|(chunk_id, score)| {
                chunk_map.get(chunk_id).map(|row| ChunkHit {
                    chunk_id: row.chunk_id,
                    document_id: row.document_id,
                    score: *score,
                    file_id: row.file_id,
                    page_no: row.page_no,
                    ts_offset: row.ts_offset,
                    snippet: row.content.clone(),
                    title: row.title.clone(),
                    kind: row.kind.clone(),
                })
            })
            .collect();

        assert_eq!(chunk_hits.len(), 3);
        assert_eq!(chunk_hits[0].chunk_id, 2);
        assert_eq!(chunk_hits[0].score, 0.05);
        assert_eq!(chunk_hits[1].chunk_id, 3);
        assert_eq!(chunk_hits[2].chunk_id, 1);

        // Roll-up: doc 10 has two chunks (2 and 1), best is chunk 2.
        let hits = document_rollup(&chunk_hits);
        assert_eq!(hits.len(), 2, "expected 2 documents, got {}", hits.len());

        // Doc 10 (best chunk 2, score 0.05).
        let hit10 = hits
            .iter()
            .find(|h| h.document_id == 10)
            .expect("doc 10 missing");
        assert_eq!(hit10.score, 0.05);
        assert_eq!(hit10.file_id, 101);
        assert_eq!(hit10.page_no, Some(5));
        assert_eq!(hit10.snippet, "snippet B better");
        assert_eq!(hit10.title.as_deref(), Some("Doc Ten"));

        // Doc 20 (only chunk 3, ts_offset preserved).
        let hit20 = hits
            .iter()
            .find(|h| h.document_id == 20)
            .expect("doc 20 missing");
        assert_eq!(hit20.score, 0.03);
        assert_eq!(hit20.file_id, 200);
        assert_eq!(hit20.ts_offset, Some(30.0));
        assert_eq!(hit20.title, None);
    }

    /// A chunk listed in the fused scores that is missing from chunk_map is silently
    /// dropped (it might have been filtered by a different tenant's RLS).
    #[test]
    fn fused_missing_from_chunk_map_dropped() {
        let rows = [ChunkRow {
            chunk_id: 1,
            document_id: 10,
            file_id: 100,
            page_no: Some(1),
            ts_offset: None,
            content: "present".into(),
            content_enc: None,
            title: None,
            kind: None,
        }];
        let chunk_map: HashMap<i64, &ChunkRow> = rows.iter().map(|r| (r.chunk_id, r)).collect();

        // Fused references chunk 2 which is not in the map.
        let fused = [(1i64, 0.05f32), (2i64, 0.04f32)];
        let chunk_hits: Vec<ChunkHit> = fused
            .iter()
            .filter_map(|(chunk_id, score)| {
                chunk_map.get(chunk_id).map(|row| ChunkHit {
                    chunk_id: row.chunk_id,
                    document_id: row.document_id,
                    score: *score,
                    file_id: row.file_id,
                    page_no: row.page_no,
                    ts_offset: row.ts_offset,
                    snippet: row.content.clone(),
                    title: row.title.clone(),
                    kind: row.kind.clone(),
                })
            })
            .collect();

        assert_eq!(chunk_hits.len(), 1);
        assert_eq!(chunk_hits[0].chunk_id, 1);
    }

    /// Empty fused scores → empty hits.
    #[test]
    fn empty_fused_yields_empty_hits() {
        let chunk_map: HashMap<i64, &ChunkRow> = HashMap::new();
        let fused: Vec<(i64, f32)> = vec![];
        let chunk_hits: Vec<ChunkHit> = fused
            .iter()
            .filter_map(|(chunk_id, score)| {
                chunk_map.get(chunk_id).map(|row| ChunkHit {
                    chunk_id: row.chunk_id,
                    document_id: row.document_id,
                    score: *score,
                    file_id: row.file_id,
                    page_no: row.page_no,
                    ts_offset: row.ts_offset,
                    snippet: row.content.clone(),
                    title: row.title.clone(),
                    kind: row.kind.clone(),
                })
            })
            .collect();

        assert!(chunk_hits.is_empty());
        let hits = document_rollup(&chunk_hits);
        assert!(hits.is_empty());
    }

    /// Top-k truncation after rollup works correctly.
    #[test]
    fn top_k_truncation_after_rollup() {
        let rows: Vec<ChunkRow> = (0..10)
            .map(|i| ChunkRow {
                chunk_id: i,
                document_id: i * 10,
                file_id: i * 100,
                page_no: Some(1),
                ts_offset: None,
                content: format!("chunk {i}"),
                content_enc: None,
                title: Some(format!("Doc {}", i * 10)),
                kind: Some("document".into()),
            })
            .collect();

        let chunk_map: HashMap<i64, &ChunkRow> = rows.iter().map(|r| (r.chunk_id, r)).collect();

        let fused: Vec<(i64, f32)> = (0..10).map(|i| (i, 1.0 - i as f32 * 0.1)).collect();

        let chunk_hits: Vec<ChunkHit> = fused
            .iter()
            .filter_map(|(chunk_id, score)| {
                chunk_map.get(chunk_id).map(|row| ChunkHit {
                    chunk_id: row.chunk_id,
                    document_id: row.document_id,
                    score: *score,
                    file_id: row.file_id,
                    page_no: row.page_no,
                    ts_offset: row.ts_offset,
                    snippet: row.content.clone(),
                    title: row.title.clone(),
                    kind: row.kind.clone(),
                })
            })
            .collect();

        let mut hits = document_rollup(&chunk_hits);
        // Truncate to top 3.
        hits.truncate(3);
        assert_eq!(hits.len(), 3);
        // Should be the highest-scored documents.
        assert_eq!(hits[0].document_id, 0); // score 1.0
        assert_eq!(hits[1].document_id, 10); // score 0.9
        assert_eq!(hits[2].document_id, 20); // score 0.8
    }

    // ── Integration tests (require Postgres + pgvector, #[ignore] in just ci) ──

    /// Helper: connect a PgStore to a pgvector testcontainers Postgres, insert a
    /// tenant row, and call `f` with the store. Sets `app.current_tenant` before
    /// each test so RLS allows queries.
    async fn with_connected_store<F, Fut>(f: F) -> anyhow::Result<()>
    where
        F: FnOnce(PgStore) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        // Shared per-binary container + fresh DB; search runs as kb_app under RLS (P6-T14).
        let db = kb_testsupport::fresh_db().await?;
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await?;

        // Insert a tenant (needed for FK constraints) via the privileged pool.
        let pool = store.pool()?;
        sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
            .execute(&pool)
            .await?;

        f(store).await
    }

    /// Process-wide counter so every seeded file gets a unique `sha256` — two files with the
    /// same `content` (e.g. both `""`) would otherwise collide on `files_tenant_id_sha256_key`
    /// (the bug-5 class, docs/analysis/07-rls-enforcement-blocker.md).
    static FILE_SHA_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// Create a test document and file, returning their ids. The `content` argument is kept for
    /// call-site readability but no longer used as the sha (a unique counter guarantees no
    /// collision regardless of content).
    async fn create_test_document(
        pool: &sqlx::postgres::PgPool,
        kind: &str,
        _content: &str,
    ) -> anyhow::Result<(i64, i64)> {
        let doc_id: i64 = sqlx::query_scalar(
            "INSERT INTO documents (tenant_id, title, kind, status) VALUES (1, 'Test Doc', $1, 'ready') RETURNING id",
        )
        .bind(kind)
        .fetch_one(pool)
        .await?;

        let n = FILE_SHA_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut sha = vec![0u8; 32];
        sha[..8].copy_from_slice(&n.to_be_bytes());

        let file_id: i64 = sqlx::query_scalar(
            "INSERT INTO files (tenant_id, document_id, page_no, sha256, blob_key, status) \
             VALUES (1, $1, 1, $2, $3, 'ready') RETURNING id",
        )
        .bind(doc_id)
        .bind(sha)
        .bind(format!("t1/{doc_id}"))
        .fetch_one(pool)
        .await?;

        Ok((doc_id, file_id))
    }

    /// Create a chunk with the given embedding, content, and provenance.
    #[allow(clippy::too_many_arguments)]
    async fn create_chunk(
        pool: &sqlx::postgres::PgPool,
        doc_id: i64,
        file_id: i64,
        idx: i32,
        content: &str,
        embedding: &[f32],
        page_no: Option<i32>,
        ts_offset: Option<f64>,
    ) -> anyhow::Result<i64> {
        let vec_str = crate::pg_store::format_vector(embedding);
        let chunk_id: i64 = sqlx::query_scalar(
            "INSERT INTO chunks (tenant_id, document_id, file_id, page_no, idx, content, ts_offset, embedding) \
             VALUES (1, $1, $2, $3, $4, $5, $6, CAST($7 AS vector)) RETURNING id",
        )
        .bind(doc_id)
        .bind(file_id)
        .bind(page_no)
        .bind(idx)
        .bind(content)
        .bind(ts_offset)
        .bind(&vec_str)
        .fetch_one(pool)
        .await?;

        Ok(chunk_id)
    }

    /// Helper: an EMBED_DIM-dimensional vector with a single spike at `pos`.
    fn spike_vector(pos: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; crate::EMBED_DIM];
        if pos < crate::EMBED_DIM {
            v[pos] = 1.0;
        }
        v
    }

    #[ignore]
    #[tokio::test]
    async fn vector_search_returns_results() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
            // Chunk with embedding spike at position 0.
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                "Rust programming guide",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            let q = Query {
                text: String::new(),
                filters: Default::default(),
                top_k: 5,
            };
            // Query embedding also spikes at position 0 → high cosine similarity.
            let hits = store.hybrid_search(1, &q, &spike_vector(0)).await?;
            assert!(!hits.is_empty(), "expected at least one hit");
            assert_eq!(hits[0].document_id, doc_id);
            assert_eq!(hits[0].file_id, file_id);
            assert_eq!(hits[0].page_no, Some(1));
            assert_eq!(hits[0].snippet, "Rust programming guide");
            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn keyword_search_returns_results_for_matching_text() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
            // Content with a distinctive word.
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                "The quick brown fox jumps over the lazy dog",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            let q = Query {
                text: "brown fox".to_string(),
                filters: Default::default(),
                top_k: 5,
            };
            let hits = store.hybrid_search(1, &q, &spike_vector(0)).await?;
            assert!(!hits.is_empty(), "keyword search should find 'brown fox'");
            let found = hits.iter().any(|h| h.document_id == doc_id);
            assert!(found, "document {doc_id} not found in keyword results");
            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn empty_query_text_vector_only_search() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                "irrelevant text not matching anything",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            // Empty text → keyword skipped, vector only.
            let q = Query {
                text: String::new(),
                filters: Default::default(),
                top_k: 5,
            };
            let hits = store.hybrid_search(1, &q, &spike_vector(0)).await?;
            assert!(!hits.is_empty(), "vector-only search should return results");
            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn kind_filter_excludes_non_matching_documents() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            // Doc 1: image kind
            let (img_id, img_file) = create_test_document(&pool, "image", "").await?;
            create_chunk(
                &pool,
                img_id,
                img_file,
                0,
                "sunset photo",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;
            // Doc 2: document kind
            let (doc_id, doc_file) = create_test_document(&pool, "document", "").await?;
            create_chunk(
                &pool,
                doc_id,
                doc_file,
                0,
                "meeting notes",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            // Filter for image only.
            let q = Query {
                text: String::new(),
                filters: QueryFilters {
                    kinds: vec![DocKind::Image],
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits = store.hybrid_search(1, &q, &spike_vector(0)).await?;
            // Only the image document should appear.
            for hit in &hits {
                assert_eq!(
                    hit.document_id, img_id,
                    "non-image doc should be filtered out"
                );
            }
            assert!(!hits.is_empty());
            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn tag_filter_returns_only_tagged_documents() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
            create_chunk(&pool, doc_id, file_id, 0, "tagged content", &spike_vector(0), Some(1), None).await?;

            // Create a tag and link it to the document.
            let vec_str = crate::pg_store::format_vector(&spike_vector(0));
            let tag_id: i64 = sqlx::query_scalar(
                "INSERT INTO tags (tenant_id, name, embedding) VALUES (1, 'rust', CAST($1 AS vector)) ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
            )
            .bind(&vec_str)
            .fetch_one(&pool)
            .await?;
            sqlx::query("INSERT INTO document_tags (document_id, tag_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(doc_id)
                .bind(tag_id)
                .execute(&pool)
                .await?;

            let q = Query {
                text: String::new(),
                filters: QueryFilters {
                    tags: vec!["rust".to_string()],
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits = store.hybrid_search(1, &q, &spike_vector(0)).await?;
            assert!(!hits.is_empty(), "tag filter should match the tagged doc");
            assert_eq!(hits[0].document_id, doc_id);

            // Filter for a non-existent tag → empty results.
            let q2 = Query {
                text: String::new(),
                filters: QueryFilters {
                    tags: vec!["nonexistent".to_string()],
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits2 = store.hybrid_search(1, &q2, &spike_vector(0)).await?;
            assert!(hits2.is_empty(), "non-existent tag should yield no results");

            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn date_filter_bounds_results() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            // Need to insert with explicit created_at for date filtering.
            let doc_id: i64 = sqlx::query_scalar(
                "INSERT INTO documents (tenant_id, title, kind, status, created_at) \
                 VALUES (1, 'Old Doc', 'document', 'ready', '2020-01-01T00:00:00Z') RETURNING id",
            )
            .fetch_one(&pool)
            .await?;
            let file_id: i64 = sqlx::query_scalar(
                "INSERT INTO files (tenant_id, document_id, page_no, sha256, blob_key, status) \
                 VALUES (1, $1, 1, 'oldfile', 't1/old', 'ready') RETURNING id",
            )
            .bind(doc_id)
            .fetch_one(&pool)
            .await?;
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                "old content",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            // Search with created_after far in the future → no results.
            let q = Query {
                text: String::new(),
                filters: QueryFilters {
                    created_after: Some(Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap()),
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits = store.hybrid_search(1, &q, &spike_vector(0)).await?;
            assert!(
                hits.is_empty(),
                "old document should be filtered out by date"
            );

            // Search with created_before far in the past → still no results.
            let q2 = Query {
                text: String::new(),
                filters: QueryFilters {
                    created_before: Some(Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap()),
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits2 = store.hybrid_search(1, &q2, &spike_vector(0)).await?;
            assert!(
                hits2.is_empty(),
                "old document should be filtered out by date"
            );

            // Search with wide date range → document found.
            let q3 = Query {
                text: String::new(),
                filters: QueryFilters {
                    created_after: Some(Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap()),
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits3 = store.hybrid_search(1, &q3, &spike_vector(0)).await?;
            assert!(
                !hits3.is_empty(),
                "document within date range should appear"
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn multiple_chunks_same_document_roll_up_to_one_hit() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
            // Two chunks in the same document.
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                "first chunk about Rust",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;
            create_chunk(
                &pool,
                doc_id,
                file_id,
                1,
                "second chunk about programming",
                &spike_vector(1),
                Some(1),
                None,
            )
            .await?;

            let q = Query {
                text: String::new(),
                filters: Default::default(),
                top_k: 10,
            };
            // Query embedding similar to both (spike at both positions).
            let mut query_vec = vec![0.0f32; crate::EMBED_DIM];
            query_vec[0] = 0.7;
            query_vec[1] = 0.7;

            let hits = store.hybrid_search(1, &q, &query_vec).await?;
            assert_eq!(
                hits.len(),
                1,
                "two chunks from same doc should roll up to one hit"
            );
            assert_eq!(hits[0].document_id, doc_id);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn hybrid_search_respects_top_k_limit() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;
            for i in 0..5 {
                let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
                // Each doc gets a slightly different embedding.
                let emb = spike_vector(i);
                create_chunk(
                    &pool,
                    doc_id,
                    file_id,
                    0,
                    &format!("document {i}"),
                    &emb,
                    Some(1),
                    None,
                )
                .await?;
            }

            let q = Query {
                text: String::new(),
                filters: Default::default(),
                top_k: 3,
            };
            let query_vec = spike_vector(0); // best match for doc 0
            let hits = store.hybrid_search(1, &q, &query_vec).await?;
            assert!(hits.len() <= 3, "should respect top_k limit");
            Ok(())
        })
        .await
        .unwrap();
    }

    // ── keyword_search (lexical degrade path, P14-T5) ───────────────────────

    /// `keyword_search` returns lexical matches and respects kind/tag/date filters,
    /// using NO embedding (it is the keyword-only degrade path).
    #[ignore]
    #[tokio::test]
    async fn keyword_search_returns_lexical_matches_and_respects_filters() {
        with_connected_store(|store| async move {
            let pool = store.pool()?;

            // Doc 1: a "document" containing the distinctive word "marmot".
            let (doc_id, file_id) = create_test_document(&pool, "document", "").await?;
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                "a curious marmot on the ridge",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            // Doc 2: an "image" also containing "marmot" — used to prove the kind filter bites.
            let (img_id, img_file) = create_test_document(&pool, "image", "").await?;
            create_chunk(
                &pool,
                img_id,
                img_file,
                0,
                "marmot photo at dawn",
                &spike_vector(0),
                Some(1),
                None,
            )
            .await?;

            // Plain keyword search finds both documents (lexical match, no vector).
            let q = Query {
                text: "marmot".to_string(),
                filters: QueryFilters::default(),
                top_k: 10,
            };
            let hits = store.keyword_search(1, &q).await?;
            let ids: Vec<i64> = hits.iter().map(|h| h.document_id).collect();
            assert!(
                ids.contains(&doc_id),
                "keyword_search should match the document"
            );
            assert!(
                ids.contains(&img_id),
                "keyword_search should match the image"
            );

            // Kind filter (image only) restricts results to the image document.
            let q_kind = Query {
                text: "marmot".to_string(),
                filters: QueryFilters {
                    kinds: vec![DocKind::Image],
                    ..Default::default()
                },
                top_k: 10,
            };
            let hits_kind = store.keyword_search(1, &q_kind).await?;
            assert!(
                !hits_kind.is_empty(),
                "kind-filtered keyword search should match"
            );
            for h in &hits_kind {
                assert_eq!(
                    h.document_id, img_id,
                    "kind filter must exclude the non-image doc"
                );
            }

            // A non-matching query returns nothing.
            let q_none = Query {
                text: "wolverine".to_string(),
                filters: QueryFilters::default(),
                top_k: 10,
            };
            let hits_none = store.keyword_search(1, &q_none).await?;
            assert!(hits_none.is_empty(), "no lexical match → empty results");

            // Empty query text → no keyword signal → empty (mirrors hybrid's keyword arm).
            let q_empty = Query {
                text: String::new(),
                filters: QueryFilters::default(),
                top_k: 10,
            };
            let hits_empty = store.keyword_search(1, &q_empty).await?;
            assert!(
                hits_empty.is_empty(),
                "empty query → keyword_search returns nothing"
            );

            Ok(())
        })
        .await
        .unwrap();
    }

    /// **Cross-tenant negative test for the lexical degrade path (§31.5 RLS proof, P14-T5).**
    ///
    /// Two tenants each own a document containing the SAME distinctive keyword. Tenant A's
    /// `keyword_search` must return ONLY tenant A's document and NEVER tenant B's, and
    /// vice-versa. The chunks are seeded via the privileged (BYPASSRLS) pool; the assertions
    /// query through `keyword_search`, which runs inside `begin_tenant_tx` on the kb_app pool
    /// so Postgres RLS scopes each search to its own tenant.
    #[ignore]
    #[tokio::test]
    async fn keyword_search_is_tenant_isolated_cross_tenant() {
        // Self-contained two-tenant setup (the shared helper seeds only tenant 1).
        let db = kb_testsupport::fresh_db().await.unwrap();
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.unwrap();
        let pool = store.pool().unwrap(); // privileged pool — RLS bypassed for seeding.

        // Two tenants.
        let tenant_a: i64 = sqlx::query_scalar(
            "INSERT INTO tenants (slug, name) VALUES ('kw-a', 'KW A') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let tenant_b: i64 = sqlx::query_scalar(
            "INSERT INTO tenants (slug, name) VALUES ('kw-b', 'KW B') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Seed one document + file + chunk per tenant, both containing "platypus".
        let seed = |tenant: i64, marker: &'static str| {
            let pool = pool.clone();
            async move {
                let doc_id: i64 = sqlx::query_scalar(
                    "INSERT INTO documents (tenant_id, title, kind, status) \
                     VALUES ($1, 'Doc', 'document', 'ready') RETURNING id",
                )
                .bind(tenant)
                .fetch_one(&pool)
                .await
                .unwrap();

                let n = FILE_SHA_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut sha = vec![0u8; 32];
                sha[..8].copy_from_slice(&n.to_be_bytes());
                let file_id: i64 = sqlx::query_scalar(
                    "INSERT INTO files (tenant_id, document_id, page_no, sha256, blob_key, status) \
                     VALUES ($1, $2, 1, $3, $4, 'ready') RETURNING id",
                )
                .bind(tenant)
                .bind(doc_id)
                .bind(sha)
                .bind(format!("t{tenant}/{marker}"))
                .fetch_one(&pool)
                .await
                .unwrap();

                let vec_str = crate::pg_store::format_vector(&vec![0.0f32; crate::EMBED_DIM]);
                sqlx::query(
                    "INSERT INTO chunks (tenant_id, document_id, file_id, page_no, idx, content, embedding) \
                     VALUES ($1, $2, $3, 1, 0, $4, CAST($5 AS vector))",
                )
                .bind(tenant)
                .bind(doc_id)
                .bind(file_id)
                .bind(format!("the platypus of {marker}"))
                .bind(&vec_str)
                .execute(&pool)
                .await
                .unwrap();
                doc_id
            }
        };
        let doc_a = seed(tenant_a, "tenant-a").await;
        let doc_b = seed(tenant_b, "tenant-b").await;

        let q = Query {
            text: "platypus".to_string(),
            filters: QueryFilters::default(),
            top_k: 10,
        };

        // Tenant A sees ONLY its own document.
        let hits_a = store.keyword_search(tenant_a, &q).await.unwrap();
        let ids_a: Vec<i64> = hits_a.iter().map(|h| h.document_id).collect();
        assert!(
            ids_a.contains(&doc_a),
            "tenant A must find its own document"
        );
        assert!(
            !ids_a.contains(&doc_b),
            "RLS violated: tenant A's keyword_search returned tenant B's document"
        );

        // Tenant B sees ONLY its own document.
        let hits_b = store.keyword_search(tenant_b, &q).await.unwrap();
        let ids_b: Vec<i64> = hits_b.iter().map(|h| h.document_id).collect();
        assert!(
            ids_b.contains(&doc_b),
            "tenant B must find its own document"
        );
        assert!(
            !ids_b.contains(&doc_a),
            "RLS violated: tenant B's keyword_search returned tenant A's document"
        );
    }
}
