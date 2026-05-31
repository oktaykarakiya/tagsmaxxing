//! Per-tenant quota enforcement: pure counting logic (plan §13, P5-T6).
//!
//! These are the I/O-free math helpers that compare current usage against a tenant's
//! quota limits. The DB-backed queries (summing files.size_bytes and usage_events tokens)
//! live in `kb-store` on [`PgStore`]; they call into these functions after fetching
//! the totals from Postgres.
//!
//! All checks are **best-effort** — they are not transactional, so a concurrent upload
//! may briefly exceed the cap. This is documented at every call site.

/// Errors returned when a tenant's quota is exceeded.
///
/// Each variant carries enough detail that the API layer (P6) can map it to the
/// appropriate HTTP status code: `StorageExceeded` → 413 Payload Too Large,
/// `TokensExceeded` → 429 Too Many Requests with a `Retry-After` header.
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
    },
}

/// Check whether adding `additional_bytes` to `current_bytes` would exceed an
/// optional storage-quota limit.
///
/// - `None` limit → always returns `Ok(())` (unlimited).
/// - `Some(0)` limit → any non-zero addition is rejected (exhausted free tier).
/// - Otherwise returns `Ok(())` if `current + additional <= limit`, or a
///   [`QuotaError::StorageExceeded`] with structured details.
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
        })?;
    if total > limit {
        return Err(QuotaError::StorageExceeded {
            current,
            additional,
            total,
            limit,
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
        })?;
    if total > limit {
        return Err(QuotaError::TokensExceeded {
            current,
            additional,
            total,
            limit,
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
}
