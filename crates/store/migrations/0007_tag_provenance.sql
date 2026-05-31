-- 0007 — tag provenance + locking (plan §6.5, ledger P6-T11).
--
-- WHY THIS EXISTS. Currently every document-tag link is undifferentiated: both the original
-- LLM-assigned tags and tags a user manually added or confirmed later look the same. When the
-- tagger is re-run (re-tag) there is no way to distinguish "keep this, the user chose it" from
-- "safe to replace with new LLM output". This migration adds two columns:
--
--   * `source` — `'llm'` (auto-assigned) or `'user'` (manually added/confirmed);
--   * `locked` — when true, the tag must survive re-tag (canonicalizer and re-tag job handlers
--     honour this flag; P6-T11 / P6-T7).
--
-- It also extends the `jobs.kind` CHECK to allow `'retag'` (re-tag a document with lock
-- preservation), and adds an optional `document_id` column to `jobs` so retag jobs can reference
-- their target document directly.

-- 1. document_tags: source column.
ALTER TABLE document_tags
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'llm'
    CHECK (source IN ('llm', 'user'));

-- 2. document_tags: locked column.
ALTER TABLE document_tags
    ADD COLUMN IF NOT EXISTS locked BOOLEAN NOT NULL DEFAULT false;

-- 3. jobs: document_id column (optional; used by retag jobs).
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS document_id BIGINT;

-- 4. jobs.kind: add 'retag' to the CHECK constraint.
--    The original inline constraint has an auto-generated name; find it dynamically so
--    the migration is portable across naming conventions.
DO $$
DECLARE
    con_name text;
BEGIN
    SELECT con.conname INTO con_name
    FROM pg_constraint con
    JOIN pg_class cls ON con.conrelid = cls.oid
    WHERE cls.relname = 'jobs'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%kind%';

    IF con_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE jobs DROP CONSTRAINT ' || quote_ident(con_name);
    END IF;

    EXECUTE 'ALTER TABLE jobs ADD CONSTRAINT jobs_kind_check CHECK (kind IN (''ingest'', ''reembed'', ''export'', ''retag''))';
END
$$;
