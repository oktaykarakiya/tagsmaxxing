-- 0018 — billing plans table + seed data + tenant billing columns (plan §29, ledger P11-T1).
--
-- Creates the `plans` table defining tiers with storage/token quotas, feature gates (JSONB),
-- and optional Stripe price tracking. Adds billing columns to `tenants` so each tenant can
-- be associated with a plan, Stripe customer, subscription, and billing lifecycle state.
-- Seeds the three default plans (free/pro/team) idempotently.
--
-- All new `tenants` columns are nullable: existing tenants with NULL plan_id are grandfathered
-- with unlimited quotas (their existing `tenants.quota_bytes/quota_tokens` still apply). Once
-- a tenant subscribes, `plan_id` is set and plan-driven quota enforcement takes over (P11-T4).

CREATE TABLE plans (
    id            BIGSERIAL PRIMARY KEY,
    code          TEXT UNIQUE NOT NULL,        -- free|pro|team|…
    stripe_price  TEXT NOT NULL,               -- Stripe price ID (placeholder for seed rows)
    quota_bytes   BIGINT NOT NULL,             -- storage hard cap (bytes)
    token_budget  BIGINT,                      -- token budget per billing period
    features      JSONB NOT NULL DEFAULT '{}', -- remote-models allowed, max users, rate caps…
    price_cents   INT,                         -- price in cents (NULL = free)
    currency      TEXT                         -- ISO 4217 (NULL = free)
);

-- Billing columns on tenants: NULL = not yet subscribed / grandfathered unlimited.
-- billing_status: 'active' | 'past_due' | 'canceled' | 'suspended' | 'inactive'
ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS plan_id            BIGINT REFERENCES plans(id);

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS subscription_id    TEXT;

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS billing_status     TEXT DEFAULT 'inactive';

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS current_period_end TIMESTAMPTZ;

-- Seed the three default plans. ON CONFLICT DO NOTHING makes this idempotent:
-- re-running the migration (or re-seeding via connect()) does not duplicate rows.
INSERT INTO plans (code, stripe_price, quota_bytes, token_budget, features, price_cents, currency)
VALUES
    ('free',
     'price_free',
     52428800,          -- 50 MB
     10000,             -- 10K tokens / period
     '{"remote_models": false, "max_users": 1, "rate_caps": {"per_minute": 5}}'::jsonb,
     0, 'usd')
ON CONFLICT (code) DO NOTHING;

INSERT INTO plans (code, stripe_price, quota_bytes, token_budget, features, price_cents, currency)
VALUES
    ('pro',
     'price_pro_monthly',
     5368709120,        -- 5 GB
     500000,            -- 500K tokens / period
     '{"remote_models": true, "max_users": 5, "rate_caps": {"per_minute": 30}}'::jsonb,
     900, 'usd')        -- $9/mo
ON CONFLICT (code) DO NOTHING;

INSERT INTO plans (code, stripe_price, quota_bytes, token_budget, features, price_cents, currency)
VALUES
    ('team',
     'price_team_monthly',
     53687091200,       -- 50 GB
     2000000,           -- 2M tokens / period
     '{"remote_models": true, "max_users": 20, "rate_caps": {"per_minute": 100}}'::jsonb,
     2900, 'usd')       -- $29/mo
ON CONFLICT (code) DO NOTHING;
