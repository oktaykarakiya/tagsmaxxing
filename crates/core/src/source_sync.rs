// SPDX-License-Identifier: AGPL-3.0-or-later

//! Pure (I/O-free) functions for source-sync scheduling arithmetic.
//!
//! These are deterministic, injectable, and proptest-covered so the scanner
//! and refetch-worker scheduling logic can be verified without a database.

use chrono::{DateTime, Duration, Utc};

/// Clamp a user-supplied `fetch_interval_secs` to `[min, max]`.
///
/// `None` means "never auto-refresh" and is passed through. `Some(secs)` is
/// clamped to the configured bounds. The caller should store the clamped value.
#[must_use]
pub fn clamp_fetch_interval(
    interval_secs: Option<i64>,
    min_secs: i64,
    max_secs: i64,
) -> Option<i64> {
    interval_secs.map(|s| s.clamp(min_secs, max_secs))
}

/// Compute the next `next_fetch_at` from `now + clamped_interval_secs`.
///
/// Returns `None` if `clamped_interval_secs` is `None` (never auto-refresh).
#[must_use]
pub fn compute_next_fetch_at(
    now: DateTime<Utc>,
    clamped_interval_secs: Option<i64>,
) -> Option<DateTime<Utc>> {
    clamped_interval_secs.map(|secs| now + Duration::seconds(secs))
}

/// Compute the backoff delay after a failed fetch attempt.
///
/// Formula: `min(base_secs * 2^min(failure_count, 5), max_backoff_secs)`
/// where `base_secs` is the document's fetch interval or `min_fetch_interval`
/// if unset/NULL. Never exceeds 30 days.
///
/// Panics only if `failure_count` overflows `u32` (impossible in practice).
#[must_use]
pub fn compute_failure_backoff(base_secs: i64, failure_count: i32, max_backoff_secs: i64) -> i64 {
    let exp = (failure_count as u32).min(5);
    let shifted = base_secs.saturating_mul(1i64 << exp);
    shifted.min(max_backoff_secs)
}

/// What action to take after fetching a URL and hashing the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefetchAction {
    /// Content unchanged — only bump timestamps, no re-ingest needed.
    Unchanged,
    /// Content changed — full re-ingest + version snapshot needed.
    Changed,
}

/// Decide what to do with a fetched hash.
///
/// - If `last_fetch_sha256` is `None` (first fetch): always `Changed`.
/// - If the new hash matches `last_fetch_sha256`: `Unchanged`.
/// - Otherwise: `Changed`.
///
/// Caller should also check live file hashes as a fallback when
/// `last_fetch_sha256` is `None` — this pure function handles only the
/// primary comparison.
#[must_use]
pub fn decide_refetch_action(last_fetch_sha256: &[u8; 32], new_sha256: &[u8; 32]) -> RefetchAction {
    if last_fetch_sha256 == new_sha256 {
        RefetchAction::Unchanged
    } else {
        RefetchAction::Changed
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use chrono::TimeZone;

    // ── clamp_fetch_interval ─────────────────────────────────────────────

    #[test]
    fn clamp_passes_none_through() {
        assert_eq!(clamp_fetch_interval(None, 300, 2_592_000), None);
    }

    #[test]
    fn clamp_below_min_raises_to_min() {
        assert_eq!(clamp_fetch_interval(Some(10), 300, 2_592_000), Some(300));
    }

    #[test]
    fn clamp_above_max_lowers_to_max() {
        assert_eq!(
            clamp_fetch_interval(Some(100_000_000), 300, 2_592_000),
            Some(2_592_000)
        );
    }

    #[test]
    fn clamp_within_range_is_identity() {
        assert_eq!(clamp_fetch_interval(Some(3600), 300, 86_400), Some(3600));
    }

    #[test]
    fn clamp_at_min_boundary() {
        assert_eq!(clamp_fetch_interval(Some(300), 300, 86_400), Some(300));
    }

    #[test]
    fn clamp_at_max_boundary() {
        assert_eq!(
            clamp_fetch_interval(Some(86_400), 300, 86_400),
            Some(86_400)
        );
    }

    // ── compute_next_fetch_at ────────────────────────────────────────────

    #[test]
    fn next_fetch_at_none_interval_is_none() {
        let now = Utc::now();
        assert_eq!(compute_next_fetch_at(now, None), None);
    }

    #[test]
    fn next_fetch_at_adds_interval() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 7, 5, 12, 0, 0).unwrap();
        let next = compute_next_fetch_at(now, Some(3600));
        assert_eq!(next.unwrap(), now + Duration::seconds(3600));
    }

    #[test]
    fn next_fetch_at_zero_interval() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 7, 5, 12, 0, 0).unwrap();
        let next = compute_next_fetch_at(now, Some(0));
        assert_eq!(next.unwrap(), now);
    }

    // ── compute_failure_backoff ──────────────────────────────────────────

    #[test]
    fn backoff_count_zero_is_base() {
        assert_eq!(compute_failure_backoff(3600, 0, 2_592_000), 3600);
    }

    #[test]
    fn backoff_count_one_is_double() {
        assert_eq!(compute_failure_backoff(3600, 1, 2_592_000), 7200);
    }

    #[test]
    fn backoff_count_five_is_32x() {
        assert_eq!(compute_failure_backoff(3600, 5, 2_592_000), 115_200);
    }

    #[test]
    fn backoff_clamped_beyond_five() {
        // 2^6 = 64, but the exponent is clamped to 5 (32x)
        assert_eq!(compute_failure_backoff(3600, 6, 2_592_000), 115_200);
        // 2^10 = 1024, still clamped to 5
        assert_eq!(compute_failure_backoff(3600, 10, 2_592_000), 115_200);
    }

    #[test]
    fn backoff_respects_max() {
        // 3600 * 32 = 115_200, but max is 86_400
        assert_eq!(compute_failure_backoff(3600, 5, 86_400), 86_400);
    }

    #[test]
    fn backoff_saturating_mul() {
        // Very large base * 32 would overflow i64, saturating_mul kicks in
        let result = compute_failure_backoff(i64::MAX / 2, 5, 2_592_000);
        assert_eq!(result, 2_592_000); // clamped to max
    }

    // ── decide_refetch_action ────────────────────────────────────────────

    #[test]
    fn identical_hashes_unchanged() {
        let h = [0xAAu8; 32];
        assert_eq!(decide_refetch_action(&h, &h), RefetchAction::Unchanged);
    }

    #[test]
    fn different_hashes_changed() {
        let old = [0x00u8; 32];
        let new = [0xFFu8; 32];
        assert_eq!(decide_refetch_action(&old, &new), RefetchAction::Changed);
    }

    #[test]
    fn one_byte_diff_is_changed() {
        let old = [0u8; 32];
        let mut new = [0u8; 32];
        new[0] = 1;
        assert_eq!(decide_refetch_action(&old, &new), RefetchAction::Changed);
    }

    #[test]
    fn refetch_action_is_debug() {
        assert_eq!(format!("{:?}", RefetchAction::Unchanged), "Unchanged");
        assert_eq!(format!("{:?}", RefetchAction::Changed), "Changed");
    }

    // ── proptest: backoff invariants ─────────────────────────────────────

    #[test]
    fn backoff_is_monotonic_non_decreasing_in_failure_count() {
        let base = 3600;
        let max = 2_592_000;
        let results: Vec<i64> = (0..=10)
            .map(|count| compute_failure_backoff(base, count, max))
            .collect();
        for window in results.windows(2) {
            assert!(
                window[0] <= window[1],
                "backoff must be non-decreasing: {:?}",
                results
            );
        }
    }
}
