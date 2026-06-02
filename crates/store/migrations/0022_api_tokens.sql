-- API tokens for programmatic Bearer-auth access (plan §30, P12-T5).
--
-- Tokens are hashed (SHA-256) before storage so the plaintext is never persisted.
-- Validation: hash the incoming Bearer token → look up by hash → check revoked/expired.
-- The api_tokens table is NOT RLS-scoped (like sessions) because tokens must be validated
-- before the tenant context is known.

CREATE TABLE api_tokens (
    id              SERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    user_id         BIGINT NOT NULL,
    name            TEXT NOT NULL,
    token_hash      TEXT NOT NULL,
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked         BOOLEAN NOT NULL DEFAULT false
);

-- Lookup by token hash for Bearer-auth validation (the hot path).
CREATE INDEX IF NOT EXISTS idx_api_tokens_token_hash ON api_tokens (token_hash);

-- List tokens for a tenant (user-facing listing).
CREATE INDEX IF NOT EXISTS idx_api_tokens_tenant_id ON api_tokens (tenant_id);
