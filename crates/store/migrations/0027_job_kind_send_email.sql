-- 0027: admit 'send_email' to the jobs.kind CHECK (P15-T2, latent P12-T7 gap).
--
-- JobKind::SendEmail ("send_email") has existed since P12-T7 and
-- email_jobs.rs enqueues it, but no migration ever added it to the
-- jobs_kind_check constraint — every transactional-email enqueue would have
-- violated the CHECK at runtime (latent until now because e2e runs without
-- SMTP configured). Surfaced by the P15-T2 kind-filter queue tests.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_kind_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_kind_check CHECK (
    kind IN (
        'ingest', 'reembed', 'export', 'retag', 'restore_test',
        'orphan_gc', 'integrity_scan', 'delete_tenant', 'send_email'
    )
);
