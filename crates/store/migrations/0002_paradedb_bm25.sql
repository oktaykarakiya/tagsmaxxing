-- ═══════════════════════════════════════════════════════════════════
-- 0002_paradedb_bm25.sql — ParadeDB pg_search BM25 keyword search
-- ═══════════════════════════════════════════════════════════════════
--
-- Replaces the plain Postgres tsvector keyword search on `chunks`
-- with ParadeDB's BM25 index (pg_search extension).  The vector
-- (pgvector HNSW) arm is unchanged — ParadeDB bundles pgvector.
--
-- Compatible with pg_search 0.23+ (Postgres 18).  The pre-0.23
-- `CALL paradedb.create_bm25()` procedure was replaced with
-- `CREATE INDEX ... USING bm25` DDL syntax.

-- ── Phase 1: Extension ──

CREATE EXTENSION IF NOT EXISTS pg_search;

-- ── Phase 2: Drop old tsvector keyword search ──

DROP INDEX IF EXISTS chunks_tsv_gin;
ALTER TABLE chunks DROP COLUMN IF EXISTS tsv;

-- ── Phase 3: Create BM25 index (pg_search 0.23+ DDL API) ──

CREATE INDEX chunks_bm25 ON chunks USING bm25 (id, content)
    WITH (key_field = 'id');
