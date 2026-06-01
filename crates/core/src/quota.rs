//! Per-tenant quota enforcement: pure counting logic (plan §13, P5-T6, P11-T5).
//!
//! These are the I/O-free math helpers that compare current usage against a tenant's
//! quota limits. The DB-backed queries (summing files.size_bytes and usage_events tokens)
//! live in `kb-store` on [`PgStore`]; they call into these functions after fetching
//! the totals from Postgres.
//!
//! All checks are **best-effort** — they are not transactional, so a concurrent upload
//! may briefly exceed the cap. This is documented at every call site.
//!
//! ## Plan-driven quotas (P11-T5)
//!
//! Since P11-T5, every `QuotaError` variant carries optional plan context
//! (`plan_code`, `upsell_plan_code`) so the API layer can render upgrade prompts.
//! The pure check functions (`check_bytes_quota`, `check_token_quota`) set these
//! to `None`; the store layer enriches the error with plan info after resolution.

/// Errors returned when a tenant's quota is exceeded.
///
/// Each variant carries enough detail that the API layer can map it to the
/// appropriate HTTP status code: [`StorageExceeded`] → 413 Payload Too Large,
/// [`TokensExceeded`] → 429 Too Many Requests with a `Retry-After` header,
/// [`UserLimitExceeded`] → 403 Forbidden.
///
/// The optional `plan_code` and `upsell_plan_code` fields (added in P11-T5)
/// enable user-facing upgrade suggestions when a plan-based limit is hit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QuotaError {
    /// Storage quota exceeded.
    ///
    /// `current` bytes are already stored; adding `additional` would bring the total to
    /// `total`, which exceeds the `limit`.
    #[error(
        "storage quota exceeded: {current} bytes stored + {additional} new = {total}, limit is {limit} bytes"
    )]
    StorageExceeded {
        /// Bytes already stored by this tenant.
        current: i64,
        /// Additional bytes the caller is trying to store.
        additional: i64,
        /// What the total would be after this operation.
        total: i64,
        /// The tenant's hard limit.
        limit: i64,
        /// The tenant's current plan code (e.g. `"free"`), if any.
        plan_code: Option<String>,
        /// A suggested plan code to upgrade to, if one exists at a higher tier.
        upsell_plan_code: Option<String>,
    },

    /// Token quota exceeded.
    ///
    /// `current` tokens have been consumed in the current period; adding `additional`
    /// would bring the total to `total`, which exceeds the `limit`.
    #[error(
        "token quota exceeded: {current} tokens used + {additional} new = {total}, limit is {limit} tokens"
    )]
    TokensExceeded {
        /// Tokens already consumed in the current period.
        current: i64,
        /// Additional tokens the caller is trying to use.
        additional: i64,
        /// What the total would be after this operation.
        total: i64,
        /// The tenant's hard limit.
        limit: i64,
        /// The tenant's current plan code (e.g. `"free"`), if any.
        plan_code: Option<String>,
        /// A suggested plan code to upgrade to, if one exists at a higher tier.
        upsell_plan_code: Option<String>,
    },

    /// User limit for this tenant's plan exceeded.
    ///
    /// `current` users already exist in the tenant; the plan allows at most `limit`.
    #[error("user limit exceeded: {current} users exist, plan allows at most {limit}")]
    UserLimitExceeded {
        /// Current number of users in the tenant.
        current: i64,
        /// Maximum users allowed by the plan.
        limit: i32,
        /// The tenant's current plan code (e.g. `"free"`), if any.
        plan_code: Option<String>,
        /// A suggested plan code to upgrade to, if one exists at a higher tier.
        upsell_plan_code: Option<String>,
    },
}

impl QuotaError {
    /// Return a user-facing upsell message when the plan info is populated,
    /// or `None` when there is no upgrade suggestion.
    ///
    /// The message names the current plan and the suggested upgrade tier.
    /// Callers should prepend the quota-limit detail from the `Display` impl
    /// for a complete error response.
    pub fn upsell_message(&self) -> Option<String> {
        let (plan_code, upsell_plan_code) = match self {
            Self::StorageExceeded {
                plan_code,
                upsell_plan_code,
                ..
            } => (plan_code, upsell_plan_code),
            Self::TokensExceeded {
                plan_code,
                upsell_plan_code,
                ..
            } => (plan_code, upsell_plan_code),
            Self::UserLimitExceeded {
                plan_code,
                upsell_plan_code,
                ..
            } => (plan_code, upsell_plan_code),
        };

        match (plan_code.as_deref(), upsell_plan_code.as_deref()) {
            (Some(current), Some(upgrade)) => Some(format!(
                "Upgrade from {current} to {upgrade} for more capacity"
            )),
            (Some(_current), None) => Some("Consider upgrading your plan for more capacity".into()),
            (None, Some(upgrade)) => Some(format!("Upgrade to {upgrade} for more capacity")),
            (None, None) => None,
        }
    }
}

/// Resolve the effective storage quota for a tenant, applying admin override precedence.
///
/// When `override_quota` is `Some`, it takes precedence over `plan_quota`.
/// When both are `None`, the quota is unlimited.
///
/// This is a pure, I/O-free resolution — callers must look up the plan and
/// override values from the database separately.
#[inline]
pub fn resolve_effective_quota(
    plan_quota: Option<i64>,
    override_quota: Option<i64>,
) -> Option<i64> {
    override_quota.or(plan_quota)
}

/// Check whether adding `additional_bytes` to `current_bytes` would exceed an
/// optional storage-quota limit.
///
/// - `None` limit → always returns `Ok(())` (unlimited).
/// - `Some(0)` limit → any non-zero addition is rejected (exhausted free tier).
/// - Otherwise returns `Ok(())` if `current + additional <= limit`, or a
///   [`QuotaError::StorageExceeded`] with structured details.
///
/// The error's `plan_code` and `upsell_plan_code` are set to `None`; the caller
/// (typically the store layer) should enrich the error with plan context via
/// [`QuotaError::with_plan_context`] after resolving the tenant's plan.
///
/// # Panics
/// Panics on overflow if `current + additional` exceeds `i64::MAX` — this is a
/// best-effort advisory check; real usage of that magnitude is implausible.
#[inline]
pub fn check_bytes_quota(
    current: i64,
    limit: Option<i64>,
    additional: i64,
) -> Result<(), QuotaError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let total = current
        .checked_add(additional)
        .ok_or_else(|| QuotaError::StorageExceeded {
            current,
            additional,
            total: current.saturating_add(additional),
            limit,
            plan_code: None,
            upsell_plan_code: None,
        })?;
    if total > limit {
        return Err(QuotaError::StorageExceeded {
            current,
            additional,
            total,
            limit,
            plan_code: None,
            upsell_plan_code: None,
        });
    }
    Ok(())
}

/// Check whether adding `additional_tokens` to `current_tokens` would exceed an
/// optional token-quota limit.
///
/// - `None` limit → always returns `Ok(())` (unlimited).
/// - `Some(0)` limit → any non-zero addition is rejected (exhausted budget).
/// - Otherwise returns `Ok(())` if `current + additional <= limit`, or a
///   [`QuotaError::TokensExceeded`] with structured details.
///
/// The error's `plan_code` and `upsell_plan_code` are set to `None`; the caller
/// should enrich the error with plan context via [`QuotaError::with_plan_context`]
/// after resolving the tenant's plan.
///
/// # Panics
/// Panics on overflow if `current + additional` exceeds `i64::MAX`.
#[inline]
pub fn check_token_quota(
    current: i64,
    limit: Option<i64>,
    additional: i64,
) -> Result<(), QuotaError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let total = current
        .checked_add(additional)
        .ok_or_else(|| QuotaError::TokensExceeded {
            current,
            additional,
            total: current.saturating_add(additional),
            limit,
            plan_code: None,
            upsell_plan_code: None,
        })?;
    if total > limit {
        return Err(QuotaError::TokensExceeded {
            current,
            additional,
            total,
            limit,
            plan_code: None,
            upsell_plan_code: None,
        });
    }
    Ok(())
}

/// Check whether adding `additional_users` to `current_users` would exceed
/// a `max_users` limit from the plan features.
///
/// - `None` limit → always returns `Ok(())` (no user limit).
/// - `Some(0)` limit → any non-zero addition is rejected.
/// - Otherwise returns `Ok(())` if `current + additional <= limit`, or
///   a [`QuotaError::UserLimitExceeded`] with structured details.
///
/// The error's `plan_code` and `upsell_plan_code` are set to `None`; the
/// caller should enrich the error after resolving the tenant's plan.
#[inline]
pub fn check_user_limit(
    current: i64,
    limit: Option<i32>,
    additional: i64,
) -> Result<(), QuotaError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let total = current
        .checked_add(additional)
        .ok_or(QuotaError::UserLimitExceeded {
            current,
            limit,
            plan_code: None,
            upsell_plan_code: None,
        })?;
    if total > limit as i64 {
        return Err(QuotaError::UserLimitExceeded {
            current,
            limit,
            plan_code: None,
            upsell_plan_code: None,
        });
    }
    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── check_bytes_quota ─────────────────────────────────────────────────

    #[test]
    fn bytes_under_limit_succeeds() {
        check_bytes_quota(500, Some(1000), 200).unwrap();
    }

    #[test]
    fn bytes_at_exact_limit_succeeds() {
        // 500 + 500 == 1000, within the limit.
        check_bytes_quota(500, Some(1000), 500).unwrap();
    }

    #[test]
    fn bytes_over_limit_returns_error() {
        let err = check_bytes_quota(900, Some(1000), 200).unwrap_err();
        let QuotaError::StorageExceeded {
            current,
            additional,
            total,
            limit,
            ..
        } = err
        else {
            panic!("expected StorageExceeded, got {err:?}");
        };
        assert_eq!(current, 900);
        assert_eq!(additional, 200);
        assert_eq!(total, 1100);
        assert_eq!(limit, 1000);
    }

    #[test]
    fn bytes_unlimited_none_succeeds() {
        // None = no quota, always succeeds.
        check_bytes_quota(1_000_000_000, None, 1_000_000).unwrap();
    }

    #[test]
    fn bytes_zero_limit_rejects_any_addition() {
        let err = check_bytes_quota(0, Some(0), 1).unwrap_err();
        let QuotaError::StorageExceeded { limit, .. } = err else {
            panic!("expected StorageExceeded");
        };
        assert_eq!(limit, 0);
    }

    #[test]
    fn bytes_zero_limit_zero_addition_succeeds() {
        // Adding 0 below a 0 limit is a no-op.
        check_bytes_quota(0, Some(0), 0).unwrap();
    }

    #[test]
    fn bytes_current_already_over_limit() {
        // Current usage already exceeds the limit (could happen after a quota downgrade).
        // Any additional should still fail.
        let err = check_bytes_quota(1100, Some(1000), 1).unwrap_err();
        assert!(matches!(err, QuotaError::StorageExceeded { .. }));
    }

    #[test]
    fn bytes_error_message_is_descriptive() {
        let err = check_bytes_quota(500, Some(1000), 600).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("500 bytes stored"));
        assert!(msg.contains("600 new"));
        assert!(msg.contains("1100"));
        assert!(msg.contains("limit is 1000 bytes"));
    }

    // ── check_token_quota ─────────────────────────────────────────────────

    #[test]
    fn tokens_under_limit_succeeds() {
        check_token_quota(5000, Some(10_000), 2000).unwrap();
    }

    #[test]
    fn tokens_at_exact_limit_succeeds() {
        check_token_quota(5000, Some(10_000), 5000).unwrap();
    }

    #[test]
    fn tokens_over_limit_returns_error() {
        let err = check_token_quota(9000, Some(10_000), 2000).unwrap_err();
        let QuotaError::TokensExceeded {
            current,
            additional,
            total,
            limit,
            ..
        } = err
        else {
            panic!("expected TokensExceeded, got {err:?}");
        };
        assert_eq!(current, 9000);
        assert_eq!(additional, 2000);
        assert_eq!(total, 11_000);
        assert_eq!(limit, 10_000);
    }

    #[test]
    fn tokens_unlimited_none_succeeds() {
        check_token_quota(100_000, None, 50_000).unwrap();
    }

    #[test]
    fn tokens_zero_limit_rejects_any_usage() {
        let err = check_token_quota(0, Some(0), 1).unwrap_err();
        let QuotaError::TokensExceeded { limit, .. } = err else {
            panic!("expected TokensExceeded");
        };
        assert_eq!(limit, 0);
    }

    #[test]
    fn tokens_zero_limit_zero_addition_succeeds() {
        check_token_quota(0, Some(0), 0).unwrap();
    }

    #[test]
    fn tokens_error_message_is_descriptive() {
        let err = check_token_quota(5000, Some(10_000), 7000).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("5000 tokens used"));
        assert!(msg.contains("7000 new"));
        assert!(msg.contains("12000"));
        assert!(msg.contains("limit is 10000 tokens"));
    }

    // ── QuotaError discriminability ──────────────────────────────────────

    #[test]
    fn storage_and_token_errors_are_distinct() {
        let storage = check_bytes_quota(100, Some(50), 1).unwrap_err();
        let token = check_token_quota(100, Some(50), 1).unwrap_err();
        assert!(matches!(storage, QuotaError::StorageExceeded { .. }));
        assert!(matches!(token, QuotaError::TokensExceeded { .. }));
        assert_ne!(storage.to_string(), token.to_string());
    }

    #[test]
    fn quota_error_is_cloneable() {
        let err = check_bytes_quota(10, Some(5), 1).unwrap_err();
        let _clone = err.clone();
        assert_eq!(err, _clone);
    }

    // ── Edge: large-but-still-valid values ───────────────────────────────

    #[test]
    fn bytes_large_under_limit() {
        // 1 TB in bytes (approximate).
        let tb = 1_000_000_000_000_i64;
        check_bytes_quota(500_000_000_000, Some(tb), 100_000_000).unwrap();
    }

    #[test]
    fn tokens_large_under_limit() {
        // 1 billion tokens, plausible for heavy usage.
        let billion = 1_000_000_000_i64;
        check_token_quota(500_000_000, Some(billion), 100_000).unwrap();
    }

    // ── Plan-aware fields (P11-T5) ───────────────────────────────────────

    #[test]
    fn storage_exceeded_has_none_plan_fields_by_default() {
        let err = check_bytes_quota(100, Some(50), 1).unwrap_err();
        let QuotaError::StorageExceeded {
            plan_code,
            upsell_plan_code,
            ..
        } = &err
        else {
            panic!("expected StorageExceeded");
        };
        assert!(plan_code.is_none());
        assert!(upsell_plan_code.is_none());
    }

    #[test]
    fn tokens_exceeded_has_none_plan_fields_by_default() {
        let err = check_token_quota(100, Some(50), 1).unwrap_err();
        let QuotaError::TokensExceeded {
            plan_code,
            upsell_plan_code,
            ..
        } = &err
        else {
            panic!("expected TokensExceeded");
        };
        assert!(plan_code.is_none());
        assert!(upsell_plan_code.is_none());
    }

    #[test]
    fn upsell_message_when_both_codes_present() {
        let err = QuotaError::StorageExceeded {
            current: 100,
            additional: 50,
            total: 150,
            limit: 100,
            plan_code: Some("free".into()),
            upsell_plan_code: Some("pro".into()),
        };
        let msg = err.upsell_message().unwrap();
        assert!(msg.contains("Upgrade from free to pro"));
        assert!(msg.contains("more capacity"));
    }

    #[test]
    fn upsell_message_when_only_current_plan() {
        let err = QuotaError::TokensExceeded {
            current: 100,
            additional: 50,
            total: 150,
            limit: 100,
            plan_code: Some("free".into()),
            upsell_plan_code: None,
        };
        let msg = err.upsell_message().unwrap();
        assert!(msg.contains("Consider upgrading your plan"));
    }

    #[test]
    fn upsell_message_when_only_upsell_plan() {
        let err = QuotaError::TokensExceeded {
            current: 100,
            additional: 50,
            total: 150,
            limit: 100,
            plan_code: None,
            upsell_plan_code: Some("pro".into()),
        };
        let msg = err.upsell_message().unwrap();
        assert!(msg.contains("Upgrade to pro"));
    }

    #[test]
    fn upsell_message_none_when_no_plan_info() {
        let err = check_bytes_quota(100, Some(50), 1).unwrap_err();
        assert!(err.upsell_message().is_none());
    }

    // ── resolve_effective_quota ──────────────────────────────────────────

    #[test]
    fn resolve_override_takes_precedence() {
        // Plan says 50 MB, admin override says 100 MB.
        assert_eq!(
            resolve_effective_quota(Some(50_000_000), Some(100_000_000)),
            Some(100_000_000)
        );
    }

    #[test]
    fn resolve_no_override_uses_plan() {
        assert_eq!(
            resolve_effective_quota(Some(50_000_000), None),
            Some(50_000_000)
        );
    }

    #[test]
    fn resolve_no_plan_uses_override() {
        // Grandfathered tenant with admin override.
        assert_eq!(
            resolve_effective_quota(None, Some(50_000_000)),
            Some(50_000_000)
        );
    }

    #[test]
    fn resolve_both_none_is_unlimited() {
        assert_eq!(resolve_effective_quota(None, None), None);
    }

    #[test]
    fn resolve_override_can_be_more_restrictive() {
        // Admin reduces quota below plan value.
        assert_eq!(
            resolve_effective_quota(Some(100_000_000), Some(10_000_000)),
            Some(10_000_000)
        );
    }

    #[test]
    fn resolve_override_zero_is_valid_override() {
        // Admin can set override to 0 (effectively blocking uploads).
        assert_eq!(resolve_effective_quota(Some(100_000_000), Some(0)), Some(0));
    }

    // ── check_user_limit ─────────────────────────────────────────────────

    #[test]
    fn users_under_limit_succeeds() {
        check_user_limit(5, Some(20), 1).unwrap();
    }

    #[test]
    fn users_at_exact_limit_succeeds() {
        check_user_limit(20, Some(20), 0).unwrap();
    }

    #[test]
    fn users_over_limit_returns_error() {
        let err = check_user_limit(20, Some(20), 1).unwrap_err();
        let QuotaError::UserLimitExceeded { current, limit, .. } = err else {
            panic!("expected UserLimitExceeded, got {err:?}");
        };
        assert_eq!(current, 20);
        assert_eq!(limit, 20);
    }

    #[test]
    fn users_unlimited_none_succeeds() {
        check_user_limit(1000, None, 50).unwrap();
    }

    #[test]
    fn users_zero_limit_rejects_any_addition() {
        let err = check_user_limit(0, Some(0), 1).unwrap_err();
        let QuotaError::UserLimitExceeded { limit, .. } = err else {
            panic!("expected UserLimitExceeded");
        };
        assert_eq!(limit, 0);
    }

    #[test]
    fn users_zero_limit_zero_addition_succeeds() {
        check_user_limit(0, Some(0), 0).unwrap();
    }

    #[test]
    fn users_error_message_is_descriptive() {
        let err = check_user_limit(20, Some(20), 1).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("20 users exist"));
        assert!(msg.contains("allows at most 20"));
    }

    #[test]
    fn user_limit_exceeded_has_none_plan_fields_by_default() {
        let err = check_user_limit(10, Some(5), 1).unwrap_err();
        let QuotaError::UserLimitExceeded {
            plan_code,
            upsell_plan_code,
            ..
        } = &err
        else {
            panic!("expected UserLimitExceeded");
        };
        assert!(plan_code.is_none());
        assert!(upsell_plan_code.is_none());
    }

    #[test]
    fn user_limit_upsell_message() {
        let err = QuotaError::UserLimitExceeded {
            current: 1,
            limit: 1,
            plan_code: Some("free".into()),
            upsell_plan_code: Some("pro".into()),
        };
        let msg = err.upsell_message().unwrap();
        assert!(msg.contains("Upgrade from free to pro"));
    }

    #[test]
    fn user_limit_and_storage_errors_are_distinct() {
        let storage = check_bytes_quota(100, Some(50), 1).unwrap_err();
        let user = check_user_limit(10, Some(5), 1).unwrap_err();
        assert!(matches!(storage, QuotaError::StorageExceeded { .. }));
        assert!(matches!(user, QuotaError::UserLimitExceeded { .. }));
    }
}
