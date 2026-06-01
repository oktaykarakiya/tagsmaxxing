-- 0017 — decrypt-access audit log (plan §28, ledger P10-T5).
--
-- Records every key-unwrap operation (DEK unwrap, provider key decrypt) for
-- confidentiality auditing — answers "who accessed which keys when" for compliance.
-- Rows are INSERT-only (no UPDATE / DELETE) — this table is an immutable append-only
-- log. Failed unwraps are also recorded; a spike in failures triggers a Prometheus
-- alert for possible attacks or key corruption.
--
-- This table lives outside RLS (queried via the admin pool) so super-admins can
-- see cross-tenant audit trails.

CREATE TABLE IF NOT EXISTS decrypt_audit (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    operation   TEXT NOT NULL,                    -- 'unwrap_dek' | 'unwrap_provider_key'
    key_id      TEXT NOT NULL,                    -- opaque key identifier (e.g. "dek:{uuid}", "provider:{id}")
    user_id     BIGINT,                          -- the user who initiated the operation (NULL for background jobs)
    success     BOOLEAN NOT NULL,                 -- true = unwrap succeeded, false = failed
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_decrypt_audit_tenant
    ON decrypt_audit (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_decrypt_audit_key
    ON decrypt_audit (key_id);

CREATE INDEX IF NOT EXISTS idx_decrypt_audit_operation
    ON decrypt_audit (operation);
