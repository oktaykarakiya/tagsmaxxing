-- 0001 — required extensions (plan §5).
-- pgvector supplies the VECTOR type used by tags.embedding and chunks.embedding.
-- Idempotent so the migration is safe on an image that already ships the extension.
CREATE EXTENSION IF NOT EXISTS vector;
