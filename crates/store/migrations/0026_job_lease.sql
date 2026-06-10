-- 0026: job lease / visibility timeout + per-tenant pending-count index (P15-T1, plan §16).
--
-- Lease columns let a reaper distinguish a job that is genuinely being worked
-- (its holder keeps extending `lease_expires_at`) from one orphaned by a
-- crashed worker (lease in the past → requeue via the normal retry/backoff
-- accounting). `locked_by` identifies the claiming worker (`hostname:pid`) for
-- observability and targeted lease extension.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS locked_by TEXT;

-- The reaper scans only running jobs by lease expiry.
CREATE INDEX IF NOT EXISTS jobs_running_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running';

-- Bounded-queue admission counts a tenant's not-yet-done ingest work per upload
-- request; this partial index keeps that count O(pending) instead of O(table).
CREATE INDEX IF NOT EXISTS jobs_tenant_pending
    ON jobs (tenant_id)
    WHERE status IN ('queued', 'running', 'failed');
