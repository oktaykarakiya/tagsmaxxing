-- 0024 — per-user job attribution (plan §5, §15; ledger P14-T1).
--
-- Records WHICH user enqueued an ingest job so the resulting model-call usage can be
-- attributed to that user (jobs are otherwise tenant-scoped only). Nullable on purpose:
-- system/maintenance jobs (reembed, export, orphan_gc, …) carry no acting user, and
-- existing rows stay NULL (no retroactive attribution — usage before P14 was tenant-only).
-- ON DELETE SET NULL keeps the audit row alive after the user is deleted/crypto-shredded.
ALTER TABLE jobs ADD COLUMN created_by BIGINT REFERENCES users(id) ON DELETE SET NULL;
