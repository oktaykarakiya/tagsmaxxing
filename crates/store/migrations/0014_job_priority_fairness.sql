-- Migration 0014: flip job priority semantics from "lower = preferred" to
-- "higher = served first" (P9-T12, plan §16). Change the default from 100 to 0
-- and rebuild the status/priority index to support the new ordering direction.
--
-- AGING is applied at claim time in the application layer via the SQL query,
-- so no schema change is needed for that.

-- 1. Change the default so newly enqueued jobs without an explicit priority
--    get the neutral (lowest) priority 0 instead of the old 100.
ALTER TABLE jobs ALTER COLUMN priority SET DEFAULT 0;

-- 2. Drop the old index that was built for "lower runs first" (priority ASC).
DROP INDEX IF EXISTS jobs_status_priority_run_after;

-- 3. Create a new index supporting "higher runs first" (priority DESC) so the
--    planner can use an index-only scan for the claim query.
CREATE INDEX jobs_status_priority_run_after ON jobs (status, priority DESC, run_after);
