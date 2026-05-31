-- 0006 — the non-superuser application role `kb_app` (plan §5, §13; ledger P6-T14).
--
-- WHY THIS EXISTS. Migrations `0003_rls.sql` declare `FORCE ROW LEVEL SECURITY`, but RLS is
-- bypassed unconditionally by superusers and by any role with `BYPASSRLS`. The bootstrap
-- connection role (the cluster superuser created by the container / operator) is exactly such
-- a role, so until now the per-tenant policies were a no-op for the app's own queries (see
-- docs/analysis/07-rls-enforcement-blocker.md). The fix is the standard two-role model:
--
--   * the PRIVILEGED role (the existing owner/superuser) keeps running migrations, the
--     cross-tenant job queue, and admin/usage roll-ups — paths that legitimately span tenants;
--   * `kb_app` (created here) is `NOSUPERUSER NOBYPASSRLS`, owns nothing, and is granted only
--     DML on the tenant-scoped tables. Because RLS is *enforced* for it, a query that forgets
--     its `WHERE tenant_id = …` still cannot read another tenant's rows — defense in depth.
--
-- The application connects as `kb_app` for all tenant-scoped data operations (kb-store routes
-- them through its `app_pool`), pairing this role with a per-transaction `app.current_tenant`
-- GUC so isolation actually holds.
--
-- PASSWORD / ROTATION. The default password below is a *development* credential, matching the
-- project's existing `kb:kb` convention (compose, .env.example, config.example.toml). An
-- operator MUST rotate it for any non-throwaway deployment:
--     ALTER ROLE kb_app PASSWORD '<strong-secret>';
-- and point `storage.app_postgres_url` / `APP_POSTGRES_URL` at the new credential. That URL is
-- hot-swappable (CLAUDE.md), so rotation needs no code change and no restart.
--
-- IDEMPOTENCY. Roles are cluster-global, so `CREATE ROLE` is guarded by an existence check
-- (the same cluster may host many databases, each running this migration). GRANTs are
-- per-database and re-running them is harmless, so each database that applies this migration
-- (re)asserts `kb_app`'s privileges on *its* tables.

-- 1. The role itself (cluster-global; create once).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kb_app') THEN
        CREATE ROLE kb_app LOGIN PASSWORD 'kb_app'
            NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END
$$;

-- 2. Schema + per-table DML (per-database). kb_app is granted ONLY the tenant-scoped tables —
--    NOT `tenants`, `sessions`, or `settings`, which are reached exclusively through the
--    privileged pool (provisioning / pre-auth session validation / global config). This keeps
--    kb_app unable to read or write any cross-tenant or auth-bootstrap state.
GRANT USAGE ON SCHEMA public TO kb_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON
    users, documents, files, tags, tag_aliases, document_tags, chunks, jobs, usage_events
    TO kb_app;

-- 3. Sequences backing the BIGSERIAL primary keys — required so INSERTs can draw `nextval`.
--    Granting on ALL sequences is harmless (a sequence is just a counter, and kb_app cannot
--    INSERT into the tables whose grants it lacks, e.g. tenants) and is robust to future tables.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO kb_app;

-- 4. The tenant-GUC setter used at the start of every tenant transaction.
GRANT EXECUTE ON FUNCTION app_set_current_tenant(BIGINT) TO kb_app;
