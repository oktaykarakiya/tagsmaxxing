-- ═══════════════════════════════════════════════════════════════════
-- 0001_base_schema.sql — Squashed base schema (replaces migrations 1–27)
-- ═══════════════════════════════════════════════════════════════════

-- ── Phase 1: Extensions ──

CREATE EXTENSION IF NOT EXISTS vector;

-- ── Phase 2: Helper Function ──

CREATE FUNCTION app_set_current_tenant(p_tenant BIGINT) RETURNS VOID
    LANGUAGE sql
    AS $$ SELECT set_config('app.current_tenant', p_tenant::text, true) $$;

-- ── Phase 3: Root Tables (no FKs to other user tables) ──

CREATE TABLE plans (
    id BIGSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    stripe_price TEXT NOT NULL,
    quota_bytes BIGINT NOT NULL,
    token_budget BIGINT,
    features JSONB NOT NULL DEFAULT '{}',
    price_cents INT,
    currency TEXT
);

CREATE TABLE tenants (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    quota_bytes BIGINT,
    quota_tokens BIGINT,
    residency TEXT NOT NULL DEFAULT 'allow_remote' CHECK (residency IN ('local_only', 'allow_remote')),
    budget_monthly_cents BIGINT,
    plan_id BIGINT REFERENCES plans(id),
    stripe_customer_id TEXT,
    subscription_id TEXT,
    billing_status TEXT DEFAULT 'inactive',
    current_period_end TIMESTAMPTZ,
    quota_override_bytes BIGINT,
    quota_override_tokens BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE providers (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('openai_compat', 'anthropic', 'gemini_native', 'local')),
    endpoint TEXT,
    api_key_enc BYTEA,
    headers JSONB NOT NULL DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stripe_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_events (
    id BIGSERIAL PRIMARY KEY,
    actor_user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id BIGINT,
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_events_tenant ON audit_events (tenant_id, created_at DESC);
CREATE INDEX idx_audit_events_actor ON audit_events (actor_user_id);

CREATE TABLE decrypt_audit (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    operation TEXT NOT NULL,
    key_id TEXT NOT NULL,
    user_id BIGINT,
    success BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_decrypt_audit_tenant ON decrypt_audit (tenant_id, created_at DESC);
CREATE INDEX idx_decrypt_audit_key ON decrypt_audit (key_id);
CREATE INDEX idx_decrypt_audit_operation ON decrypt_audit (operation);

CREATE TABLE tenant_tombstones (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    tenant_slug TEXT NOT NULL,
    tenant_name TEXT NOT NULL,
    deleted_by BIGINT,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tenant_tombstones IS 'Audit trail for crypto-shredded tenants. Each row records a completed tenant deletion.';

-- ── Phase 4: Tables with Foreign Keys ──

CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner', 'admin', 'member')),
    email_verified BOOLEAN NOT NULL DEFAULT true,
    email_verify_token TEXT,
    password_reset_token TEXT,
    password_reset_expires TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, email)
);

CREATE TABLE documents (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    title TEXT,
    summary TEXT,
    user_note TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('document', 'image', 'audio', 'video', 'identity_document', 'code', 'archive', 'binary')),
    meta JSONB NOT NULL DEFAULT '{}',
    page_count INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'ready', 'failed')),
    local_only BOOLEAN NOT NULL DEFAULT false,
    summary_enc BYTEA,
    user_note_enc BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX documents_meta_gin ON documents USING GIN (meta jsonb_path_ops);

CREATE TABLE files (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    page_no INT NOT NULL DEFAULT 1,
    page_label TEXT,
    sha256 BYTEA NOT NULL,
    blob_key TEXT NOT NULL,
    path TEXT,
    mime TEXT,
    size_bytes BIGINT,
    meta JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'ready', 'failed')),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, sha256)
);

CREATE INDEX files_document_id ON files (document_id);
CREATE INDEX files_meta_gin ON files USING GIN (meta jsonb_path_ops);

CREATE TABLE tags (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    embedding VECTOR(2560),
    UNIQUE (tenant_id, name)
);

CREATE TABLE tag_aliases (
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    tag_id BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (tenant_id, alias)
);

CREATE TABLE document_tags (
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    tag_id BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    source TEXT NOT NULL DEFAULT 'llm' CHECK (source IN ('llm', 'user')),
    locked BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (document_id, tag_id)
);

CREATE TABLE chunks (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    page_no INT,
    idx INT NOT NULL,
    content TEXT NOT NULL,
    ts_offset DOUBLE PRECISION,
    embedding VECTOR(2560) NOT NULL,
    content_enc BYTEA,
    tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
);

CREATE INDEX chunks_embedding_hnsw ON chunks USING hnsw ((embedding::halfvec(2560)) halfvec_cosine_ops);
CREATE INDEX chunks_tsv_gin ON chunks USING GIN (tsv);
CREATE INDEX chunks_document_id ON chunks (document_id);
CREATE INDEX chunks_file_id ON chunks (file_id);

CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    file_id BIGINT,
    document_id BIGINT,
    kind TEXT NOT NULL CHECK (kind IN ('ingest', 'reembed', 'export', 'retag', 'restore_test', 'orphan_gc', 'integrity_scan', 'delete_tenant', 'send_email')),
    priority INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'failed', 'dead', 'done')),
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
    lease_expires_at TIMESTAMPTZ,
    locked_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_status_priority_run_after ON jobs (status, priority DESC, run_after);
CREATE INDEX jobs_running_lease ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX jobs_tenant_pending ON jobs (tenant_id) WHERE status IN ('queued', 'running', 'failed');

CREATE TABLE usage_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    user_id BIGINT,
    model TEXT NOT NULL,
    role TEXT NOT NULL,
    backend_id TEXT,
    prompt_tokens INT,
    completion_tokens INT,
    latency_ms INT,
    cost_micros BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX usage_events_tenant_created ON usage_events (tenant_id, created_at);

CREATE TABLE models (
    id BIGSERIAL PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL,
    caps TEXT[] NOT NULL,
    ctx_tokens INT,
    max_conc INT,
    rpm INT,
    tpm INT,
    price_in NUMERIC,
    price_out NUMERIC,
    data_class TEXT NOT NULL DEFAULT 'remote' CHECK (data_class IN ('local', 'remote')),
    enabled BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE routes (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT,
    role TEXT NOT NULL CHECK (role IN ('text', 'vision', 'code', 'embed', 'rerank')),
    tier INT NOT NULL,
    strategy TEXT NOT NULL DEFAULT 'least_loaded' CHECK (strategy IN ('least_loaded', 'round_robin', 'cost_asc', 'priority')),
    spill_ms INT NOT NULL DEFAULT 800,
    model_id BIGINT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    UNIQUE (tenant_id, role, tier, model_id)
);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user_role TEXT NOT NULL DEFAULT 'member',
    email_verified BOOLEAN NOT NULL DEFAULT true,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX sessions_expires_at ON sessions (expires_at);

CREATE TABLE tenant_data_keys (
    tenant_id BIGINT PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    data_key_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_tokens (
    id SERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_api_tokens_token_hash ON api_tokens (token_hash);
CREATE INDEX idx_api_tokens_tenant_id ON api_tokens (tenant_id);

CREATE TABLE tenant_monthly_usage (
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    period_month DATE NOT NULL,
    tokens_used BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, period_month)
);

-- ── Phase 5: Row-Level Security ──

ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE users FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON users
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON documents
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE files ENABLE ROW LEVEL SECURITY;
ALTER TABLE files FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON files
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE tags FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tags
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE tag_aliases ENABLE ROW LEVEL SECURITY;
ALTER TABLE tag_aliases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tag_aliases
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE document_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_tags FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON document_tags
    FOR ALL
    USING (EXISTS (SELECT 1 FROM documents d WHERE d.id = document_tags.document_id AND d.tenant_id = current_setting('app.current_tenant', true)::bigint))
    WITH CHECK (EXISTS (SELECT 1 FROM documents d WHERE d.id = document_tags.document_id AND d.tenant_id = current_setting('app.current_tenant', true)::bigint));

ALTER TABLE chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE chunks FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON chunks
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON jobs
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE usage_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON usage_events
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

ALTER TABLE tenant_monthly_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_monthly_usage FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_monthly_usage
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

-- ── Phase 6: kb_app Role and Grants ──

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kb_app') THEN
        CREATE ROLE kb_app LOGIN PASSWORD 'kb_app' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO kb_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON users TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON documents TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON files TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tags TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tag_aliases TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON document_tags TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON chunks TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON jobs TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON usage_events TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_monthly_usage TO kb_app;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO kb_app;

GRANT EXECUTE ON FUNCTION app_set_current_tenant(BIGINT) TO kb_app;

-- ── Phase 7: Seed Data ──

INSERT INTO settings (key, value) VALUES ('embedder', '{"id": "qwen3-embed-4b", "dim": 2560}'::jsonb);

INSERT INTO plans (code, stripe_price, quota_bytes, token_budget, features, price_cents, currency) VALUES
    ('free', 'price_free', 52428800, 10000, '{"remote_models": false, "max_users": 1, "rate_caps": {"per_minute": 5}}'::jsonb, 0, 'usd')
ON CONFLICT (code) DO NOTHING;

INSERT INTO plans (code, stripe_price, quota_bytes, token_budget, features, price_cents, currency) VALUES
    ('pro', 'price_pro_monthly', 5368709120, 500000, '{"remote_models": true, "max_users": 5, "rate_caps": {"per_minute": 30}}'::jsonb, 900, 'usd')
ON CONFLICT (code) DO NOTHING;

INSERT INTO plans (code, stripe_price, quota_bytes, token_budget, features, price_cents, currency) VALUES
    ('team', 'price_team_monthly', 53687091200, 2000000, '{"remote_models": true, "max_users": 20, "rate_caps": {"per_minute": 100}}'::jsonb, 2900, 'usd')
ON CONFLICT (code) DO NOTHING;
