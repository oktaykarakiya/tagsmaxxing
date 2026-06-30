// SPDX-License-Identifier: AGPL-3.0-or-later

//! Document freshness watch.
//!
//! Allows users to register documents for periodic freshness checks. Each watch
//! has a schedule (daily/weekly/monthly). When a document is past its check
//! date, an action item is created reminding the user to review it.

use chrono::{DateTime, Duration, Utc};

/// Schedule intervals for document freshness checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchSchedule {
    /// Check daily.
    Daily,
    /// Check weekly.
    Weekly,
    /// Check monthly.
    Monthly,
}

impl WatchSchedule {
    /// Parse from a config/DB string.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "daily" => Some(Self::Daily),
            "weekly" => Some(Self::Weekly),
            "monthly" => Some(Self::Monthly),
            _ => None,
        }
    }

    /// Wire string for DB storage.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }

    /// Duration between checks.
    #[must_use]
    pub fn interval(self) -> Duration {
        match self {
            Self::Daily => Duration::days(1),
            Self::Weekly => Duration::weeks(1),
            Self::Monthly => Duration::days(30),
        }
    }
}

/// Default schedule for a department.
#[must_use]
pub fn default_schedule_for_department(dept: &str) -> WatchSchedule {
    match dept {
        "legal" | "compliance-risk" => WatchSchedule::Weekly,
        "accounting" | "finance" | "tax" => WatchSchedule::Monthly,
        _ => WatchSchedule::Weekly,
    }
}

/// A document freshness watch record.
#[derive(Debug, Clone)]
pub struct StaleWatch {
    /// Primary key.
    pub id: i64,
    /// Tenant.
    pub tenant_id: i64,
    /// File path relative to workspace root.
    pub relpath: String,
    /// Department this file belongs to.
    pub department: Option<String>,
    /// How often to check.
    pub schedule: WatchSchedule,
    /// Last time the document was checked.
    pub last_checked_at: Option<DateTime<Utc>>,
    /// Next scheduled check.
    pub next_check_at: DateTime<Utc>,
    /// Whether the watch is active.
    pub active: bool,
}

impl StaleWatch {
    /// Compute the next check date from last_checked_at or now.
    pub fn next_check(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let base = self.last_checked_at.unwrap_or(now);
        base + self.schedule.interval()
    }

    /// Returns true if the watch is due for a check.
    #[must_use]
    pub fn is_due(&self, now: DateTime<Utc>) -> bool {
        now >= self.next_check_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_parsing() {
        assert_eq!(WatchSchedule::from_str("daily"), Some(WatchSchedule::Daily));
        assert_eq!(WatchSchedule::from_str("weekly"), Some(WatchSchedule::Weekly));
        assert_eq!(WatchSchedule::from_str("bogus"), None);
    }

    #[test]
    fn default_dept_schedules() {
        assert_eq!(default_schedule_for_department("legal"), WatchSchedule::Weekly);
        assert_eq!(default_schedule_for_department("accounting"), WatchSchedule::Monthly);
    }

    #[test]
    fn watch_is_due_after_interval() {
        let watch = StaleWatch {
            id: 1,
            tenant_id: 1,
            relpath: "test.txt".into(),
            department: None,
            schedule: WatchSchedule::Daily,
            last_checked_at: None,
            next_check_at: Utc::now() - Duration::hours(1),
            active: true,
        };
        assert!(watch.is_due(Utc::now()));
    }
}
