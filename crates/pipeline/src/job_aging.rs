// SPDX-License-Identifier: AGPL-3.0-or-later

//! Job priority aging to prevent starvation (plan §16, P9-T12).
//!
//! Low-priority jobs should not starve forever behind a stream of high-priority
//! work. Aging gradually lifts a job's effective priority as it ages in the
//! queue: `effective_priority = priority + floor(age_seconds / AGING_STEP_SECS)`.
//!
//! With the default [`AGING_STEP_SECS`] of 3600 (one hour), a priority-0 job
//! becomes as attractive as a freshly-enqueued priority-1 job after one hour,
//! as a priority-2 job after two hours, and so on.

/// How many seconds of age add 1 to effective priority.
///
/// With this value (3600 = 1 hour), a job waiting for `n` hours gets its
/// effective priority boosted by `n`. So an old low-priority job will
/// eventually overtake newer high-priority work.
pub const AGING_STEP_SECS: i64 = 3600;

/// Compute the effective priority of a job, incorporating age-based boost.
///
/// `age_seconds` is the number of seconds since the job was created (i.e.
/// `now - created_at`). The result is `priority + floor(age_seconds / AGING_STEP_SECS)`.
///
/// Negative `age_seconds` (clock skew) is treated the same as zero — age
/// never *reduces* effective priority.
///
/// # Examples
///
/// ```
/// use kb_pipeline::job_aging::effective_priority;
/// assert_eq!(effective_priority(5, 0), 5);
/// assert_eq!(effective_priority(5, 3600), 6);   // +1 after 1 hour
/// assert_eq!(effective_priority(0, 7200), 2);   // +2 after 2 hours
/// assert_eq!(effective_priority(10, -100), 10); // clock skew: no penalty
/// ```
#[must_use]
pub fn effective_priority(priority: i32, age_seconds: i64) -> i64 {
    let effective_age = age_seconds.max(0);
    let boost = effective_age / AGING_STEP_SECS;
    i64::from(priority) + boost
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── effective_priority ──────────────────────────────────────────────

    #[test]
    fn zero_age_gives_base_priority() {
        assert_eq!(effective_priority(5, 0), 5);
        assert_eq!(effective_priority(0, 0), 0);
        assert_eq!(effective_priority(100, 0), 100);
    }

    #[test]
    fn one_hour_adds_one() {
        assert_eq!(effective_priority(5, 3600), 6);
        assert_eq!(effective_priority(0, 3600), 1);
    }

    #[test]
    fn two_hours_adds_two() {
        assert_eq!(effective_priority(0, 7200), 2);
        assert_eq!(effective_priority(5, 7200), 7);
    }

    #[test]
    fn fractional_hour_truncated() {
        // 3599 seconds = just under 1 hour → floor = 0 boost.
        assert_eq!(effective_priority(5, 3599), 5);
        // 3601 seconds = just over → floor = 1 boost.
        assert_eq!(effective_priority(5, 3601), 6);
    }

    #[test]
    fn twenty_four_hours() {
        assert_eq!(effective_priority(0, 86400), 24);
        assert_eq!(effective_priority(5, 86400), 29);
    }

    #[test]
    fn negative_age_is_no_penalty() {
        // Clock skew: negative age should not reduce priority.
        assert_eq!(effective_priority(5, -100), 5);
        assert_eq!(effective_priority(0, -3600), 0);
    }

    #[test]
    fn large_priority_with_large_age() {
        // No overflow: large values stay in i64 range.
        let result = effective_priority(i32::MAX, i64::MAX);
        // Should be positive (saturating add would also be OK, but our formula
        // should not panic). The key property is it is >= i32::MAX.
        assert!(result >= i64::from(i32::MAX));
    }

    #[test]
    fn aging_step_const_is_reasonable() {
        // Guard against accidental changes: the constant should be 3600.
        assert_eq!(AGING_STEP_SECS, 3600);
    }

    #[test]
    fn monotonic_in_age() {
        // Older job (same base priority) always has >= effective priority.
        let p = 3;
        let mut prev = effective_priority(p, 0);
        for age in 1..=10000 {
            let cur = effective_priority(p, age);
            assert!(cur >= prev, "not monotonic at age {age}: {prev} -> {cur}");
            prev = cur;
        }
    }
}
