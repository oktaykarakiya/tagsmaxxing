-- 0002 — core schema (plan §5). Tables, the embedder lock-in settings row, and the
-- HNSW + GIN + btree indexes. CHECK constraints pin the closed-enum columns to the exact
-- wire strings locked in kb-core (DocKind / ProcessingStatus / UserRole / JobKind /
-- JobStatus); changing one is a breaking schema change, mirrored by the kb-core tests.

CREATE TABLE tenants (
    id           BIGSERIAL PRIMARY KEY,
    slug         TEXT UNIQUE NOT NULL,
    name         TEXT NOT NULL,
    quota_bytes  BIGINT,                          -- NULL = unlimited
    quota_tokens BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,                  -- argon2id
    role          TEXT NOT NULL DEFAULT 'member'
                  CHECK (role IN ('owner', 'admin', 'member')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, email)
);

-- A DOCUMENT is the semantic unit the user retrieves; it has one or more member FILES
-- (pages / sides / parts), each its own blob. See §27.
CREATE TABLE documents (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    title       TEXT,
    summary     TEXT,                             -- synthesized across ALL pages
    user_note   TEXT,
    kind        TEXT NOT NULL
                CHECK (kind IN ('document', 'image', 'audio', 'video',
                                'identity_document', 'code', 'archive', 'binary')),
    meta        JSONB NOT NULL DEFAULT '{}',      -- union of member metadata + doc-level fields
    page_count  INT NOT NULL DEFAULT 1,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'ready', 'failed')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX documents_meta_gin ON documents USING GIN (meta jsonb_path_ops);

CREATE TABLE files (                              -- physical blob = one page/member of a document
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    page_no     INT NOT NULL DEFAULT 1,           -- order within the document
    page_label  TEXT,                             -- 'front' | 'back' | 'p3' | original filename
    sha256      BYTEA NOT NULL,
    blob_key    TEXT NOT NULL,                    -- content-addressed (= sha256), tenant-prefixed
    path        TEXT,                             -- original filename
    mime        TEXT,
    size_bytes  BIGINT,
    meta        JSONB NOT NULL DEFAULT '{}',      -- per-page exif / ffprobe / doc props (raw)
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'ready', 'failed')),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, sha256)                    -- blob-level dedup per tenant
);
CREATE INDEX files_document_id ON files (document_id);
CREATE INDEX files_meta_gin ON files USING GIN (meta jsonb_path_ops);

-- Canonical, multi-label, semantically-deduplicated tags (replaces a hard category; §6.5).
CREATE TABLE tags (
    id        BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name      TEXT NOT NULL,
    embedding VECTOR(1024),                       -- for synonym detection
    UNIQUE (tenant_id, name)
);
CREATE TABLE tag_aliases (                        -- "bill" -> canonical "invoice"
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    alias     TEXT NOT NULL,
    tag_id    BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (tenant_id, alias)
);
CREATE TABLE document_tags (                      -- tags attach to the DOCUMENT, not a page
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    tag_id      BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (document_id, tag_id)
);

CREATE TABLE chunks (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,  -- rolls up to here
    file_id     BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,      -- provenance: page
    page_no     INT,                              -- deep-link target (page within the document)
    idx         INT NOT NULL,
    content     TEXT NOT NULL,
    ts_offset   DOUBLE PRECISION,                 -- seconds into audio/video → jump-to-moment
    embedding   VECTOR(1024) NOT NULL,            -- MUST match embedder dim (BGE-M3 = 1024)
    tsv         TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
);
CREATE INDEX chunks_embedding_hnsw ON chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX chunks_tsv_gin ON chunks USING GIN (tsv);
CREATE INDEX chunks_document_id ON chunks (document_id);
CREATE INDEX chunks_file_id ON chunks (file_id);

-- Durable async ingestion jobs (retryable, with dead-letter); see §16.
CREATE TABLE jobs (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    file_id    BIGINT,
    kind       TEXT NOT NULL
               CHECK (kind IN ('ingest', 'reembed', 'export')),
    priority   INT NOT NULL DEFAULT 100,          -- lower runs first; queries bypass jobs
    status     TEXT NOT NULL DEFAULT 'queued'
               CHECK (status IN ('queued', 'running', 'failed', 'dead', 'done')),
    attempts   INT NOT NULL DEFAULT 0,
    last_error TEXT,
    run_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX jobs_status_priority_run_after ON jobs (status, priority, run_after);

-- Per-call usage for admin + per-tenant quotas; see §15.
CREATE TABLE usage_events (
    id                BIGSERIAL PRIMARY KEY,
    tenant_id         BIGINT NOT NULL,
    user_id           BIGINT,
    model             TEXT NOT NULL,
    role              TEXT NOT NULL,
    backend_id        TEXT,
    prompt_tokens     INT,
    completion_tokens INT,
    latency_ms        INT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX usage_events_tenant_created ON usage_events (tenant_id, created_at);

-- Global key/value settings. Records the embedder lock-in (§5 note): the VECTOR(1024) dim is
-- baked into the schema, so a change requires a `reembed` job over every chunk — not a manual
-- reindex. This is NOT tenant-scoped, so no RLS.
CREATE TABLE settings (
    key        TEXT PRIMARY KEY,
    value      JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO settings (key, value)
VALUES ('embedder', '{"id": "bge-m3", "dim": 1024}'::jsonb);
