-- 0009 — add 'restore_test' job kind (plan §21, ledger P8-T7).
--
-- The restore-test job is a scheduled maintenance task that restores the latest pgBackRest
-- backup to a scratch Postgres instance, runs integrity checks, and alerts on failure or
-- stale backups via Prometheus gauges + ERROR-level logs. This migration extends the
-- jobs.kind CHECK constraint to allow the new kind.

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

    EXECUTE 'ALTER TABLE jobs ADD CONSTRAINT jobs_kind_check CHECK (kind IN (''ingest'', ''reembed'', ''export'', ''retag'', ''restore_test''))';
END
$$;
