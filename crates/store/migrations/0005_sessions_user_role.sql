-- 0005 — Add user_role column to sessions (P5-T5).
--
-- The auth middleware needs the user's role to populate request extensions.
-- Storing it in the session avoids a JOIN to `users` on every validation call.
-- `DEFAULT 'member'` is safe: the column is always written on INSERT with the
-- correct value; the default only covers any pre-existing rows (none in practice,
-- since the app has not yet shipped).
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS user_role TEXT NOT NULL DEFAULT 'member';
