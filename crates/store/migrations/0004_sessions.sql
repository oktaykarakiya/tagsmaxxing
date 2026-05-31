-- 0004 — Sessions table for cookie-based authentication (plan §13, P5-T4).
--
-- Each row represents one active login session. The `id` is a 64-char hex-encoded
-- 32-byte cryptographically random token — the opaque bearer value placed in the
-- `__Host-session` cookie. Tokens are unguessable (SysRng), so possession IS
-- authentication.
--
-- Sessions are NOT covered by RLS: the token is validated BEFORE the tenant context
-- is established (chicken-and-egg — you need tenant_id from the session to set the
-- GUC, but a GUC-based policy would block reading the session). Security rests on
-- the 256-bit random token being unguessable + HttpOnly cookies.

CREATE TABLE sessions (
    id          TEXT PRIMARY KEY,                     -- 64-char hex token
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fast expiry-based cleanup/lookup. No covering index needed — the PK covers
-- id lookups; this index solely exists so a janitor can `DELETE FROM sessions
-- WHERE expires_at < now()` without a seq scan.
CREATE INDEX sessions_expires_at ON sessions (expires_at);
