-- 0008 — admin audit log (plan §15, ledger P6-T8).
--
-- Records every mutating admin action for accountability. This table lives outside
-- RLS (queried via the admin pool) so super-admins can see cross-tenant audit trails.

CREATE TABLE IF NOT EXISTS audit_events (
    id            BIGSERIAL PRIMARY KEY,
    actor_user_id BIGINT NOT NULL,
    tenant_id     BIGINT NOT NULL,
    action        TEXT NOT NULL,
    target_type   TEXT NOT NULL,
    target_id     BIGINT,
    details       JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_tenant
    ON audit_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_actor
    ON audit_events (actor_user_id);
