-- 0007_chat.sql — Multi-turn Chat / Conversations (P18)
--
-- Dedicated chat tables: conversations (scoped to tenant + user) and
-- conversation_messages (individual turns with RAG context tracking).
-- The existing assistant_sessions/assistant_transcripts are designed for
-- the opencode subprocess agent and are not reused here.

CREATE TABLE conversations (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title TEXT,
    model_ref TEXT NOT NULL,
    message_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_conv_tenant_user ON conversations (tenant_id, user_id, updated_at DESC);

ALTER TABLE conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON conversations
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

CREATE TABLE conversation_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL,
    tokens_in INT,
    tokens_out INT,
    search_results_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_convmsg_conv ON conversation_messages (conversation_id, created_at);

ALTER TABLE conversation_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversation_messages FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON conversation_messages
    FOR ALL
    USING (tenant_id = current_setting('app.current_tenant', true)::bigint)
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::bigint);

GRANT SELECT, INSERT, UPDATE, DELETE ON conversations TO kb_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON conversation_messages TO kb_app;
GRANT USAGE, SELECT ON SEQUENCE conversations_id_seq TO kb_app;
GRANT USAGE, SELECT ON SEQUENCE conversation_messages_id_seq TO kb_app;
