-- 0025 — per-tenant monthly token rollup (plan §15, §29; ledger P14-T1).
--
-- A pre-aggregated counter of tokens consumed per tenant per calendar month (UTC), upserted in
-- the SAME transaction as each `usage_events` insert (see `PgStore::insert_usage_event`). It
-- exists so quota enforcement is an O(1) point-select on the current month instead of a
-- `SUM(prompt_tokens + completion_tokens)` scan of every event in the period.
--
-- TENANT-SCOPED exactly like `usage_events`: RLS ENABLE/FORCE + a `tenant_isolation` policy
-- keyed on `current_setting('app.current_tenant', true)::bigint`, and the same `kb_app` DML
-- grant. `period_month` is the first day of the month (a DATE), so `(tenant_id, period_month)`
-- is the natural upsert key.
CREATE TABLE tenant_monthly_usage (
    tenant_id    BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    period_month DATE NOT NULL,
    tokens_used  BIGINT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, period_month)
);

-- Tenant isolation (identical predicate to usage_events in 0003_rls.sql). FORCE makes it apply
-- to the table owner too, so the policy holds even for privileged-pool reads.
ALTER TABLE tenant_monthly_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_monthly_usage FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_monthly_usage
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

-- The application connects as kb_app for tenant-scoped data (the same grant style as the 0006
-- list); the rollup is upserted in the tenant transaction alongside the usage_events insert.
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_monthly_usage TO kb_app;
