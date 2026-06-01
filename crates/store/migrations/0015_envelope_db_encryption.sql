-- 0015 — Envelope encryption for sensitive DB columns (plan §28, P10-T2).
--
-- Adds per-tenant Data Encryption Key (DEK) storage and encrypted shadow columns
-- alongside their plaintext originals. The plaintext columns are kept for transition
-- (they feed the tsv search index and allow existing rows to remain readable until
-- a background encryption job runs).

-- Each tenant gets one DEK, wrapped by the master KEK and stored as JSON (DataKey,
-- §28 key hierarchy). The app unwraps the DEK at query time to encrypt/decrypt
-- column values. Crypto-shredding: deleting a tenant cascades to this row, destroying
-- the key and making ciphertext irrecoverable.
CREATE TABLE tenant_data_keys (
    tenant_id     BIGINT PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    data_key_json JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Encrypted chunk content. The existing `content` column feeds the tsv GENERATED
-- ALWAYS column — it is kept for transition and must remain populated until the
-- background encryption job has marked all rows encrypted, at which point it can
-- be dropped and tsv rebuilt from a different source.
ALTER TABLE chunks ADD COLUMN content_enc BYTEA;

-- Encrypted document summary and user note. These have no index dependency, so
-- the transition is simpler — once all rows are encrypted, the plaintext columns
-- can be dropped.
ALTER TABLE documents ADD COLUMN summary_enc BYTEA;
ALTER TABLE documents ADD COLUMN user_note_enc BYTEA;
