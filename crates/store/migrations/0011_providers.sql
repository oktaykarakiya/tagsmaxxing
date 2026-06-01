-- Migration 0011: providers, models, and routes tables for DB-driven routing (plan §26.5, P9-T5).
-- These tables are the runtime source of truth for the scheduler's routing table; the config.toml
-- [[backend]] entries serve only as a bootstrap seed path.

CREATE TABLE providers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('openai_compat', 'anthropic', 'gemini_native', 'local')),
    endpoint    TEXT,                   -- base_url (null for native SDK endpoints)
    api_key_enc BYTEA,                  -- envelope-encrypted (§24); null for local
    headers     JSONB NOT NULL DEFAULT '{}',
    enabled     BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE models (
    id          BIGSERIAL PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    model_id    TEXT NOT NULL,          -- provider's model name / local alias
    caps        TEXT[] NOT NULL,        -- {text,vision,code,embed,rerank}
    ctx_tokens  INT,
    max_conc    INT,                    -- slots (local) or concurrency cap (remote)
    rpm         INT, tpm INT,           -- remote rate limits (null = none)
    price_in    NUMERIC, price_out NUMERIC,  -- $/Mtok (null = free/local)
    data_class  TEXT NOT NULL DEFAULT 'remote' CHECK (data_class IN ('local', 'remote')),
    enabled     BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE routes (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT,                 -- null = global default
    role        TEXT NOT NULL CHECK (role IN ('text', 'vision', 'code', 'embed', 'rerank')),
    tier        INT NOT NULL,           -- 0 = primary, 1 = first fallback, …
    strategy    TEXT NOT NULL DEFAULT 'least_loaded'
                    CHECK (strategy IN ('least_loaded', 'round_robin', 'cost_asc', 'priority')),
    spill_ms    INT NOT NULL DEFAULT 800,
    model_id    BIGINT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    UNIQUE (tenant_id, role, tier, model_id)
);
