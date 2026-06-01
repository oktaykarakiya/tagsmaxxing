//! Time-of-day and day-of-week access windows for backends (plan §26.2, P9-T13).
//!
//! A [`TimeWindow`] gates a backend on wall-clock time: only during certain hours
//! of the day and/or certain days of the week is the backend eligible for
//! capacity acquisition.  An empty window means "always allowed".

use chrono::{DateTime, Datelike, Local, Timelike};

/// A time-based access window for a backend (plan §26.2, P9-T13).
///
/// When attached to a [`Backend`](crate::backend::Backend) via
/// [`Backend::set_time_window`](crate::backend::Backend::set_time_window),
/// the backend is excluded from candidate lists when [`is_active`](Self::is_active)
/// returns `false`.
///
/// Both `hour_ranges` and `days_of_week` are independent gates: the window is
/// **active** only when **both** the current hour falls within a configured range
/// (or no hour restriction) **and** the current day is in the allow-list
/// (or no day restriction).
///
/// # Examples
///
/// ```rust
/// # use kb_scheduler::time_window::TimeWindow;
/// # use chrono::Local;
/// let tw = TimeWindow {
///     hour_ranges: vec![(2, 6), (22, 24)],   // 2am-6am or 10pm-midnight
///     days_of_week: vec![1, 2, 3, 4, 5],      // Mon–Fri (0=Mon)
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct TimeWindow {
    /// Allowed hour-of-day ranges, each `(inclusive_start, exclusive_end)` in
    /// local 24-hour time (`0..=23`).  E.g., `[(2, 6), (22, 24)]` means "only
    /// between 02:00–05:59 and 22:00–23:59".  Empty = no hour restriction.
    pub hour_ranges: Vec<(u8, u8)>,
    /// Allowed ISO weekdays, `0 = Monday` through `6 = Sunday`.
    /// Empty = no day-of-week restriction.
    pub days_of_week: Vec<u8>,
}

impl TimeWindow {
    /// Whether `now` (local time) falls within this time window.
    ///
    /// In production, pass `chrono::Local::now()`.  In tests, pass a fixed
    /// [`DateTime`](chrono::DateTime) to keep the predicate deterministic.
    pub fn is_active(&self, now: &DateTime<Local>) -> bool {
        // ── Day-of-week check ────────────────────────────────────────────
        if !self.days_of_week.is_empty() {
            let weekday = now.weekday().number_from_monday();
            // chrono returns 1=Mon..7=Sun; convert to 0=Mon..6=Sun.
            let dow: u8 = (weekday - 1) as u8;
            if !self.days_of_week.contains(&dow) {
                return false;
            }
        }

        // ── Hour-of-day check ────────────────────────────────────────────
        if !self.hour_ranges.is_empty() {
            let hour = now.hour() as u8; // chrono: hour() returns u32, 0..=23
            if !self
                .hour_ranges
                .iter()
                .any(|(start, end)| hour >= *start && hour < *end)
            {
                return false;
            }
        }

        true
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use chrono::TimeZone;

    use super::*;

    /// Create a `DateTime<Local>` with a known hour and weekday.
    fn dt_local(hour: u32, weekday: u32) -> DateTime<Local> {
        // weekday = 1=Mon..7=Sun (chrono ISO).
        // Use 2026-06-01 as the base: it's a Monday (chrono weekday=1).
        // 2026-06-01 = Monday = day 1.
        let day: u32 = 1 + (weekday - 1);
        Local.with_ymd_and_hms(2026, 6, day, hour, 0, 0).unwrap()
    }

    // ── Default ───────────────────────────────────────────────────────────

    #[test]
    fn default_window_always_active() {
        let tw = TimeWindow::default();
        for hour in 0..24 {
            for wd in 1..=7 {
                let dt = dt_local(hour, wd);
                assert!(
                    tw.is_active(&dt),
                    "default window must be active at hour={hour}, weekday={wd}"
                );
            }
        }
    }

    // ── Hour ranges ───────────────────────────────────────────────────────

    #[test]
    fn within_hour_range_is_active() {
        let tw = TimeWindow {
            hour_ranges: vec![(8, 18)], // 8am–5:59pm
            ..Default::default()
        };
        assert!(tw.is_active(&dt_local(10, 1))); // Mon 10am
        assert!(tw.is_active(&dt_local(8, 1))); // Mon 8am (inclusive)
        assert!(tw.is_active(&dt_local(17, 1))); // Mon 5pm (exclusive end)
    }

    #[test]
    fn outside_hour_range_not_active() {
        let tw = TimeWindow {
            hour_ranges: vec![(8, 18)],
            ..Default::default()
        };
        assert!(!tw.is_active(&dt_local(7, 1))); // Mon 7am — before range
        assert!(!tw.is_active(&dt_local(18, 1))); // Mon 6pm — end is exclusive
        assert!(!tw.is_active(&dt_local(0, 1))); // Mon midnight
    }

    #[test]
    fn multiple_hour_ranges() {
        let tw = TimeWindow {
            hour_ranges: vec![(2, 6), (22, 24)], // 2am–6am and 10pm–midnight
            ..Default::default()
        };
        assert!(tw.is_active(&dt_local(3, 1))); // 3am — in first range
        assert!(tw.is_active(&dt_local(23, 1))); // 11pm — in second range
        assert!(!tw.is_active(&dt_local(12, 1))); // noon — between ranges
        assert!(!tw.is_active(&dt_local(1, 1))); // 1am — between ranges
    }

    #[test]
    fn hour_range_crossing_midnight() {
        // 10pm to 6am spans midnight.
        let tw = TimeWindow {
            hour_ranges: vec![(22, 24), (0, 6)],
            ..Default::default()
        };
        assert!(tw.is_active(&dt_local(23, 1))); // 11pm
        assert!(tw.is_active(&dt_local(3, 1))); // 3am
        assert!(!tw.is_active(&dt_local(12, 1))); // noon
    }

    // ── Day-of-week ───────────────────────────────────────────────────────

    #[test]
    fn within_days_of_week_is_active() {
        // Mon–Fri only (0=Mon..4=Fri).
        let tw = TimeWindow {
            hour_ranges: vec![],
            days_of_week: vec![0, 1, 2, 3, 4],
        };
        assert!(tw.is_active(&dt_local(10, 1))); // Monday
        assert!(tw.is_active(&dt_local(10, 3))); // Wednesday
        assert!(tw.is_active(&dt_local(10, 5))); // Friday
    }

    #[test]
    fn outside_days_of_week_not_active() {
        let tw = TimeWindow {
            hour_ranges: vec![],
            days_of_week: vec![0, 1, 2, 3, 4], // Mon–Fri
        };
        assert!(!tw.is_active(&dt_local(10, 6))); // Saturday
        assert!(!tw.is_active(&dt_local(10, 7))); // Sunday
    }

    #[test]
    fn weekend_only_window() {
        let tw = TimeWindow {
            hour_ranges: vec![],
            days_of_week: vec![5, 6], // Sat, Sun
        };
        assert!(!tw.is_active(&dt_local(10, 1))); // Monday
        assert!(!tw.is_active(&dt_local(10, 5))); // Friday
        assert!(tw.is_active(&dt_local(10, 6))); // Saturday
        assert!(tw.is_active(&dt_local(10, 7))); // Sunday
    }

    #[test]
    fn single_day_window() {
        let tw = TimeWindow {
            hour_ranges: vec![],
            days_of_week: vec![2], // Wednesday only
        };
        assert!(!tw.is_active(&dt_local(10, 2))); // Tuesday
        assert!(tw.is_active(&dt_local(10, 3))); // Wednesday
        assert!(!tw.is_active(&dt_local(10, 4))); // Thursday
    }

    // ── Combined hour + day ───────────────────────────────────────────────

    #[test]
    fn combined_hour_and_day() {
        let tw = TimeWindow {
            hour_ranges: vec![(8, 18)],
            days_of_week: vec![0, 1, 2, 3, 4], // Mon–Fri, 8am–6pm
        };
        assert!(tw.is_active(&dt_local(10, 1))); // Mon 10am — both pass
        assert!(!tw.is_active(&dt_local(10, 6))); // Sat 10am — day fails
        assert!(!tw.is_active(&dt_local(7, 1))); // Mon 7am — hour fails
        assert!(!tw.is_active(&dt_local(7, 6))); // Sat 7am — both fail
    }

    // ── Edge cases ────────────────────────────────────────────────────────

    #[test]
    fn zero_hour_inclusive() {
        let tw = TimeWindow {
            hour_ranges: vec![(0, 1)], // midnight to 1am
            ..Default::default()
        };
        assert!(tw.is_active(&dt_local(0, 1))); // midnight
        assert!(!tw.is_active(&dt_local(1, 1))); // 1am (exclusive)
        assert!(!tw.is_active(&dt_local(23, 1))); // 11pm
    }

    #[test]
    fn full_day_range() {
        let tw = TimeWindow {
            hour_ranges: vec![(0, 24)], // all hours
            ..Default::default()
        };
        assert!(tw.is_active(&dt_local(0, 1)));
        assert!(tw.is_active(&dt_local(12, 1)));
        assert!(tw.is_active(&dt_local(23, 1)));
    }

    #[test]
    fn all_days() {
        // Empty days_of_week = all days, explicitly.
        let tw = TimeWindow {
            hour_ranges: vec![(8, 18)],
            days_of_week: vec![], // all days
        };
        assert!(tw.is_active(&dt_local(10, 6))); // Saturday
        assert!(tw.is_active(&dt_local(10, 7))); // Sunday
    }

    // ── Proptest-like edge cases ──────────────────────────────────────────

    #[test]
    fn range_boundaries() {
        let tw = TimeWindow {
            hour_ranges: vec![(0, 24)],
            ..Default::default()
        };
        // Every boundary hour is within [0, 24).
        assert!(tw.is_active(&dt_local(0, 1)));
        assert!(tw.is_active(&dt_local(23, 1)));
    }

    #[test]
    fn empty_ranges_are_always_active() {
        let tw = TimeWindow::default();
        assert!(tw.is_active(&dt_local(0, 1)));
        assert!(tw.is_active(&dt_local(23, 7)));
    }
}
