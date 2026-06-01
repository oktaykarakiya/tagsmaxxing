-- 0016 — Tenant tombstone records for crypto-shredding audit trail (plan §28, P10-T4).
--
-- When a tenant is deleted (crypto-shredding), we keep a tombstone row recording
-- who deleted the tenant and when. The tombstone provides proof that crypto-shredding
-- occurred — the tenant data is irrecoverable because the DEK was destroyed with the
-- tenant_data_keys row.
--
-- The tombstone does NOT reference tenants(id) via a foreign key because the tenant
-- row is deleted as part of the shredding job. The slug + name are snapshotted at
-- deletion time so the audit trail is self-contained.

CREATE TABLE tenant_tombstones (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    BIGINT       NOT NULL,
    tenant_slug  TEXT         NOT NULL,
    tenant_name  TEXT         NOT NULL,
    deleted_by   BIGINT,                    -- super-admin user who initiated deletion (NULL if system)
    deleted_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE tenant_tombstones IS
  'Audit trail for crypto-shredded tenants. Each row records a completed tenant deletion.';

-- The jobs.kind CHECK must also include the new 'delete_tenant' kind so the job
-- queue can accept crypto-shredding jobs. We drop and re-add the constraint.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_kind_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_kind_check CHECK (
    kind IN (
        'ingest', 'reembed', 'export', 'retag', 'restore_test',
        'orphan_gc', 'integrity_scan', 'delete_tenant'
    )
);
