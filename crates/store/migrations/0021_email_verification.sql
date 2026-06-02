-- Migration 0021: Email verification + password reset columns for signup/auth flow (P12-T2).
-- Adds email_verified flag, verification tokens, and password reset tokens to users.
-- Also adds email_verified to sessions so the auth middleware can surface it without a DB query.

-- Users table: email verification and password reset columns.
-- DEFAULT true: existing users are grandfathered as verified.
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified    BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_token TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_token TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_expires TIMESTAMPTZ;

-- Sessions table: surface email_verified to the middleware without a per-request DB lookup.
-- DEFAULT true: sessions created before this migration are treated as verified.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT true;
