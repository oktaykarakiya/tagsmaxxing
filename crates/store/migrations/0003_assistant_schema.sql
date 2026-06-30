-- ═══════════════════════════════════════════════════════════════════
-- 0003_assistant_schema.sql — Assistant tables (kb-assistant crate)
-- ═══════════════════════════════════════════════════════════════════

-- ── Assistant Sessions ──────────────────────────────────────────────

CREATE TABLE assistant_sessions (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id),
    user_id         BIGINT NOT NULL REFERENCES users(id),
    session_key     TEXT NOT NULL UNIQUE,
    model_ref       TEXT NOT NULL,
    department      TEXT,
    status          TEXT NOT NULL DEFAULT 'idle'
                        CHECK (status IN ('idle', 'running', 'done', 'killed')),
    sandbox_path    TEXT,
    prompt_count    INT NOT NULL DEFAULT 0,
    total_tokens    BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
ALTER TABLE assistant_sessions ENABLE ROW LEVEL SECURITY;
CREATE INDEX idx_asst_sessions_tenant ON assistant_sessions(tenant_id, created_at DESC);
CREATE INDEX idx_asst_sessions_user   ON assistant_sessions(user_id, created_at DESC);

-- ── Action Items (tasks, reminders, decisions) ──────────────────────

CREATE TABLE assistant_action_items (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id),
    user_id         BIGINT NOT NULL REFERENCES users(id),
    text            TEXT NOT NULL,
    due_date        DATE,
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'done', 'cancelled')),
    source          TEXT NOT NULL DEFAULT 'manual'
                        CHECK (source IN ('manual', 'keyword', 'stale_watch', 'regulatory')),
    source_session  BIGINT REFERENCES assistant_sessions(id),
    source_turn     INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE assistant_action_items ENABLE ROW LEVEL SECURITY;
CREATE INDEX idx_asst_actions_tenant  ON assistant_action_items(tenant_id, status, due_date);
CREATE INDEX idx_asst_actions_user    ON assistant_action_items(user_id, created_at DESC);

-- ── Decisions (logged from agent interactions) ─────────────────────

CREATE TABLE assistant_decisions (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id),
    user_id         BIGINT NOT NULL REFERENCES users(id),
    text            TEXT NOT NULL,
    context         TEXT,
    source_session  BIGINT REFERENCES assistant_sessions(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE assistant_decisions ENABLE ROW LEVEL SECURITY;
CREATE INDEX idx_asst_decisions_tenant ON assistant_decisions(tenant_id, created_at DESC);

-- ── Stale Watch (document freshness tracking) ───────────────────────

CREATE TABLE assistant_stale_watches (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id),
    relpath         TEXT NOT NULL,
    department      TEXT,
    schedule        TEXT NOT NULL CHECK (schedule IN ('daily', 'weekly', 'monthly')),
    last_checked_at TIMESTAMPTZ,
    next_check_at   TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE assistant_stale_watches ENABLE ROW LEVEL SECURITY;
CREATE UNIQUE INDEX idx_asst_stale_watch_unique
    ON assistant_stale_watches(tenant_id, relpath);
CREATE INDEX idx_asst_stale_watch_due
    ON assistant_stale_watches(tenant_id, status, next_check_at);

-- ── Session Transcripts (prompt/response archive) ───────────────────

CREATE TABLE assistant_transcripts (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id),
    session_id      BIGINT NOT NULL REFERENCES assistant_sessions(id),
    turn_number     INT NOT NULL,
    prompt          TEXT NOT NULL,
    response_text   TEXT NOT NULL,
    tool_calls_json JSONB,
    tokens_in       INT,
    tokens_out      INT,
    violations_json JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(session_id, turn_number)
);
ALTER TABLE assistant_transcripts ENABLE ROW LEVEL SECURITY;
CREATE INDEX idx_asst_transcripts_session ON assistant_transcripts(session_id, turn_number);
