-- ═══════════════════════════════════════════════════════════════════
-- 0004_jobs_tenant_created_index.sql — tenant-scoped job listing index
-- ═══════════════════════════════════════════════════════════════════
--
-- admin_queries::list_jobs orders by created_at DESC for a given
-- tenant.  The existing partial index jobs_tenant_pending covers only
-- queued/running/failed statuses and lacks the created_at column.
-- This full (non-partial) index serves both the unfiltered and
-- status-filtered listing queries.

CREATE INDEX IF NOT EXISTS jobs_tenant_created
    ON jobs (tenant_id, created_at DESC);
