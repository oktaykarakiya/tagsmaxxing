-- 0012_tenant_residency.sql: data-residency governance (plan §26.6, P9-T9).
--
-- Adds per-tenant residency policy and per-document local_only flag so
-- the scheduler (Pool::acquire) can exclude DataClass::Remote backends
-- for local-only tenants and sensitive documents (e.g. identity_document,
-- plan §17).

ALTER TABLE tenants
    ADD COLUMN residency TEXT NOT NULL DEFAULT 'allow_remote'
        CHECK (residency IN ('local_only', 'allow_remote'));

ALTER TABLE documents
    ADD COLUMN local_only BOOLEAN NOT NULL DEFAULT false;
