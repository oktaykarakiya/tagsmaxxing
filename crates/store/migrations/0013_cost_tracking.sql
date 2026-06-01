-- Migration 0013: cost tracking columns (plan §26.6, P9-T10).
--
-- Adds cost_micros to usage_events so priced remote calls accrue spend,
-- and budget_monthly_cents to tenants so per-tenant budget caps work.

-- Micro-dollar cost for the call (NULL = free / local-only).
-- Stored separately so token quotas and cost budgets are independent.
ALTER TABLE usage_events
    ADD COLUMN IF NOT EXISTS cost_micros BIGINT;

-- Per-tenant monthly spend budget in cents (USD).
-- NULL = unlimited (default).
ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS budget_monthly_cents BIGINT;
