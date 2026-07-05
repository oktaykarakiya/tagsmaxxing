-- 0006: Document Source Sync with Version History
--
-- documents gain source_url + fetch scheduling columns;
-- files + chunks gain superseded_at for soft-delete;
-- new document_versions table for immutable snapshots;
-- jobs.kind gains 'refetch'.

-- ── documents: source-sync columns ────────────────────────────────────────

ALTER TABLE documents
    ADD COLUMN source_url TEXT,
    ADD COLUMN fetch_interval_secs BIGINT,
    ADD COLUMN next_fetch_at TIMESTAMPTZ,
    ADD COLUMN last_fetched_at TIMESTAMPTZ,
    ADD COLUMN last_fetch_sha256 BYTEA,
    ADD COLUMN current_version INT NOT NULL DEFAULT 1,
    ADD COLUMN fetch_failure_count INT NOT NULL DEFAULT 0;

-- Cover the scanner query: WHERE source_url IS NOT NULL AND next_fetch_at IS NOT NULL AND next_fetch_at <= now()
CREATE INDEX documents_next_fetch_due ON documents (next_fetch_at)
    WHERE source_url IS NOT NULL AND next_fetch_at IS NOT NULL;

-- ── files + chunks: soft-delete (NULL = live) ─────────────────────────────

ALTER TABLE files  ADD COLUMN superseded_at TIMESTAMPTZ;
ALTER TABLE chunks ADD COLUMN superseded_at TIMESTAMPTZ;

-- ── document_versions: immutable version history ──────────────────────────

CREATE TABLE document_versions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    version_number INT NOT NULL,
    sha256 BYTEA,
    title TEXT,
    summary TEXT,
    summary_enc BYTEA,
    tags_snapshot JSONB NOT NULL DEFAULT '[]',
    chunk_count INT NOT NULL DEFAULT 0,
    content_diff_summary TEXT,
    content_diff_summary_enc BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (document_id, version_number)
);

CREATE INDEX document_versions_doc_desc ON document_versions (document_id, version_number DESC);

-- RLS: direct tenant_isolation (table has tenant_id column).
ALTER TABLE document_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON document_versions
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

-- Immutability at role level: INSERT only, no UPDATE/DELETE.
GRANT SELECT, INSERT ON document_versions TO kb_app;
GRANT USAGE, SELECT ON SEQUENCE document_versions_id_seq TO kb_app;

-- ── jobs.kind: add 'refetch' ──────────────────────────────────────────────

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_kind_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_kind_check CHECK (kind IN (
    'ingest',
    'reembed',
    'export',
    'retag',
    'restore_test',
    'orphan_gc',
    'integrity_scan',
    'delete_tenant',
    'send_email',
    'refetch'
));
