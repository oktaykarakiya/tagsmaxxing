-- 0003 — Row-Level Security (plan §5, §13). Tenant isolation that holds even if a query
-- forgets the `tenant_id` filter. The app runs, per transaction:
--     SELECT set_config('app.current_tenant', $tenant, true);   -- (local = transaction-scoped)
-- or the convenience wrapper below. `current_setting(..., true)` is missing_ok: when the GUC
-- is unset it yields NULL, the predicate is NULL, and NO rows match — deny-by-default.
--
-- RLS is INTRODUCED here; the cross-tenant negative-test suite + sign-off is the P5 checkpoint
-- (§31.5). FORCE makes the policy apply to the table owner too, so isolation is real for the
-- app's own role (migrations are pure DDL, so FORCE does not block this migration).

-- Set the current tenant for the surrounding transaction. `is_local => true` scopes it to the
-- transaction, so a pooled connection never leaks one tenant's setting into the next checkout.
CREATE FUNCTION app_set_current_tenant(p_tenant BIGINT) RETURNS VOID
    LANGUAGE sql
    AS $$ SELECT set_config('app.current_tenant', p_tenant::text, true) $$;

-- Tables keyed directly by tenant_id share the identical predicate.
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE users FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON users
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON documents
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE files ENABLE ROW LEVEL SECURITY;
ALTER TABLE files FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON files
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE tags FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tags
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE tag_aliases ENABLE ROW LEVEL SECURITY;
ALTER TABLE tag_aliases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tag_aliases
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE chunks FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON chunks
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON jobs
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE usage_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON usage_events
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

-- document_tags carries no tenant_id (PK is document_id + tag_id), so it is isolated through
-- its parent document. The subquery itself runs under the documents policy above.
ALTER TABLE document_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_tags FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON document_tags
    USING (EXISTS (SELECT 1 FROM documents d
                   WHERE d.id = document_tags.document_id
                     AND d.tenant_id = current_setting('app.current_tenant', true)::bigint))
    WITH CHECK (EXISTS (SELECT 1 FROM documents d
                        WHERE d.id = document_tags.document_id
                          AND d.tenant_id = current_setting('app.current_tenant', true)::bigint));
