-- Migration 0010: add orphan_gc and integrity_scan to jobs.kind CHECK constraint.
-- Since Postgres does not support ALTER CONSTRAINT for CHECK, we drop and
-- recreate the constraint with the expanded enum.
ALTER TABLE jobs DROP CONSTRAINT jobs_kind_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_kind_check CHECK (
    kind IN ('ingest', 'reembed', 'export', 'retag', 'restore_test', 'orphan_gc', 'integrity_scan')
);
