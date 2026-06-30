-- ═══════════════════════════════════════════════════════════════════
-- 0002_paradedb_bm25.sql — ParadeDB pg_search BM25 keyword search
-- ═══════════════════════════════════════════════════════════════════
--
-- Replaces the plain Postgres tsvector keyword search on `chunks`
-- with ParadeDB's BM25 index (pg_search extension).  The vector
-- (pgvector HNSW) arm is unchanged — ParadeDB bundles pgvector.
--
-- Reason (hybrid search, plan §6.5):
--   • BM25 relevance scoring (TF-IDF with doc-length normalisation)
--     consistently outperforms ts_rank for information retrieval.
--   • ParadeDB's default tokenizer handles stemming and stop-word
--     removal, improving recall over the 'simple' config.
--   • Single-index BM25 eliminates the generated tsv column and GIN
--     index — slightly smaller on-disk footprint.

-- ── Phase 1: Extension ──

CREATE EXTENSION IF NOT EXISTS pg_search;

-- ── Phase 2: Drop old tsvector keyword search ──

DROP INDEX IF EXISTS chunks_tsv_gin;
ALTER TABLE chunks DROP COLUMN IF EXISTS tsv;

-- ── Phase 3: Create BM25 index ──

CALL paradedb.create_bm25(
    index_name => 'chunks_bm25',
    table_name => 'chunks',
    key_field => 'id',
    text_fields => paradedb.field('content')
);
