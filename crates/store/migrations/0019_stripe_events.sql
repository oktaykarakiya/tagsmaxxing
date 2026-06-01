-- 0019 — Stripe event idempotency table (plan §29, ledger P11-T3).
--
-- Stores processed Stripe webhook event IDs so duplicate deliveries
-- (Stripe retries) are skipped idempotently. The PRIMARY KEY on event_id
-- ensures that an INSERT … ON CONFLICT DO NOTHING reliably detects
-- already-processed events.
--
-- Uses the admin role: this is infrastructure data, not tenant-scoped.

CREATE TABLE stripe_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
