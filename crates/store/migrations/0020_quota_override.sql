-- 0020 — Admin quota override columns on tenants (plan §29, ledger P11-T5).
--
-- Admins can set per-tenant quota overrides that take precedence over the
-- plan-derived limits. When set, quota enforcement reads these columns
-- first; when NULL, the plan's quota_bytes / token_budget (synced by
-- P11-T4's apply_plan_quotas_to_tenant) are used.
--
-- Uses the admin role — these are infrastructure columns, not tenant-scoped.

ALTER TABLE tenants
    ADD COLUMN quota_override_bytes BIGINT,
    ADD COLUMN quota_override_tokens BIGINT;
