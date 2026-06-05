-- Switch the embedder to Qwen3-Embedding-4B (native 2560-dim).
--
-- pgvector HNSW/IVFFlat indexes on the `vector` type are capped at 2000 dimensions, so the
-- chunks ANN index is built on a `halfvec(2560)` cast (storage stays full-precision
-- `vector(2560)`; only the approximate index is half-precision, which is lossless enough for
-- cosine retrieval). Search must ORDER BY the same `::halfvec(2560)` cast to use the index.
--
-- The old 1024-dim embeddings are meaningless under the new model and must be recomputed
-- (re-embed = wipe + re-ingest), so they are cleared here before the column is widened — a
-- `vector` column cannot be re-typed from 1024 to 2560 while it holds 1024-dim data. On a fresh
-- database (the e2e path: `down -v`) the tables are empty and this is just a widen + index swap.

-- ── chunks: clear stale embeddings, widen, rebuild the HNSW index via a halfvec cast ──
DROP INDEX IF EXISTS chunks_embedding_hnsw;
TRUNCATE TABLE chunks;
ALTER TABLE chunks ALTER COLUMN embedding TYPE vector(2560);
CREATE INDEX chunks_embedding_hnsw ON chunks USING hnsw ((embedding::halfvec(2560)) halfvec_cosine_ops);

-- ── tags: clear stale synonym embeddings (recomputed lazily by the canonicalizer), widen ──
UPDATE tags SET embedding = NULL;
ALTER TABLE tags ALTER COLUMN embedding TYPE vector(2560);

-- ── record the new embedder lock-in (id + dim) so the reembed-check detects future changes ──
UPDATE settings
SET value = '{"id": "qwen3-embed-4b", "dim": 2560}'::jsonb, updated_at = now()
WHERE key = 'embedder';
