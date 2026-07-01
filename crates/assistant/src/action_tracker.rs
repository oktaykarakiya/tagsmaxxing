// SPDX-License-Identifier: AGPL-3.0-or-later

//! Action item and decision detection via regex keyword scanning.
//!
//! Ported from `ai-assistant/plugins/business/action_tracker.py`. Phase 1:
//! regex-based instant detection. Phase 2 (LLM-based transcript extraction)
//! can be added later.

use chrono::{Datelike, Duration, NaiveDate, Utc, Weekday};
use regex::Regex;

/// A detected action item from a prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedAction {
    /// The extracted action text.
    pub text: String,
    /// Optional due date in ISO format (YYYY-MM-DD).
    pub due_date: Option<String>,
    /// Source keyword: "remind", "task", "decision".
    pub kind: ActionKind,
}

/// The type of detected item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// "Remind me to X by Y"
    Reminder,
    /// "Task: X due Y"
    Task,
    /// "Decision: X"
    Decision,
}

/// Pre-compiled regex patterns for action detection.
pub struct ActionTracker {
    remind: Regex,
    task: Regex,
    task_no_date: Regex,
    decision: Regex,
}

impl ActionTracker {
    /// Create a new tracker with compiled regexes.
    ///
    /// All regex patterns are hardcoded — they will never fail to compile.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            remind: Regex::new(r"(?i)remind\s+me\s+to\s+(.+?)\s+(?:by|on|before)\s+(.+)")
                .expect("hardcoded regex"),
            task: Regex::new(r"(?i)(?:task|todo):\s*(.+?)\s+(?:due|by)\s+(.+)")
                .expect("hardcoded regex"),
            task_no_date: Regex::new(r"(?i)(?:task|todo):\s*(.+)").expect("hardcoded regex"),
            decision: Regex::new(r"(?i)(?:decision|decided):\s*(.+)").expect("hardcoded regex"),
        }
    }

    /// Scan a prompt for explicit action items and decisions.
    #[must_use]
    pub fn detect(&self, prompt: &str) -> Vec<DetectedAction> {
        let mut items = Vec::new();

        // "Remind me to X by Y"
        for caps in self.remind.captures_iter(prompt) {
            let text = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
            let date_str = caps.get(2).map(|m| m.as_str().trim()).unwrap_or("");
            let due = parse_due_date(date_str);
            items.push(DetectedAction {
                text: text.to_string(),
                due_date: due,
                kind: ActionKind::Reminder,
            });
        }

        // "Task: X due Y" — check before no-date variant to avoid double-matching
        for caps in self.task.captures_iter(prompt) {
            let text = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
            let date_str = caps.get(2).map(|m| m.as_str().trim()).unwrap_or("");
            let due = parse_due_date(date_str);
            items.push(DetectedAction {
                text: text.to_string(),
                due_date: due,
                kind: ActionKind::Task,
            });
        }

        // "Task: X" (no due date) — only if not already matched
        for caps in self.task_no_date.captures_iter(prompt) {
            let text = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
            // Skip if this was already captured by the dated variant
            if items
                .iter()
                .any(|i| i.text == text && i.kind == ActionKind::Task)
            {
                continue;
            }
            items.push(DetectedAction {
                text: text.to_string(),
                due_date: None,
                kind: ActionKind::Task,
            });
        }

        // "Decision: X"
        for caps in self.decision.captures_iter(prompt) {
            let text = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
            items.push(DetectedAction {
                text: text.to_string(),
                due_date: None,
                kind: ActionKind::Decision,
            });
        }

        items
    }

    /// Format pending items as a context block for prompt injection.
    #[must_use]
    pub fn format_pending(items: &[DetectedAction]) -> String {
        if items.is_empty() {
            return String::new();
        }
        let mut buf = String::new();
        for item in items {
            match item.kind {
                ActionKind::Reminder | ActionKind::Task => {
                    buf.push_str(&format!("- Task: {}", item.text));
                    if let Some(ref due) = item.due_date {
                        buf.push_str(&format!(" (due: {due})"));
                    }
                    buf.push('\n');
                }
                ActionKind::Decision => {
                    buf.push_str(&format!("- Decision: {}\n", item.text));
                }
            }
        }
        buf
    }
}

impl Default for ActionTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ── Due date parsing ──

/// Parse a natural-language date string to ISO 8601 (YYYY-MM-DD).
fn parse_due_date(text: &str) -> Option<String> {
    let text = text.trim().trim_matches(&[',', '.', ';', '!', '?'][..]);
    if text.is_empty() {
        return None;
    }

    // Already ISO format?
    let iso_re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").ok()?;
    if iso_re.is_match(text) {
        return Some(text.to_string());
    }

    let today = Utc::now().date_naive();
    let text_lower = text.to_lowercase();

    match text_lower.as_str() {
        "today" | "tod" => return Some(today.format("%Y-%m-%d").to_string()),
        "tomorrow" | "tmrw" => {
            return Some((today + Duration::days(1)).format("%Y-%m-%d").to_string());
        }
        "day after tomorrow" => {
            return Some((today + Duration::days(2)).format("%Y-%m-%d").to_string());
        }
        "next week" => {
            return Some((today + Duration::days(7)).format("%Y-%m-%d").to_string());
        }
        _ => {}
    }

    // "in N days"
    if let Some(n) = parse_in_days(&text_lower) {
        return Some((today + Duration::days(n)).format("%Y-%m-%d").to_string());
    }

    // Day of week
    if let Some(days) = parse_weekday_offset(&text_lower) {
        return Some(
            (today + Duration::days(days))
                .format("%Y-%m-%d")
                .to_string(),
        );
    }

    // Month + day (e.g., "March 15")
    if let Some(parsed) = parse_month_day(&text_lower, today) {
        return Some(parsed.format("%Y-%m-%d").to_string());
    }

    None
}

fn parse_in_days(text: &str) -> Option<i64> {
    let re = Regex::new(r"in\s+(\d+)\s+days?").ok()?;
    re.captures(text)?.get(1)?.as_str().parse::<i64>().ok()
}

fn parse_weekday_offset(text: &str) -> Option<i64> {
    let days: &[(&str, Weekday)] = &[
        ("monday", Weekday::Mon),
        ("tuesday", Weekday::Tue),
        ("wednesday", Weekday::Wed),
        ("thursday", Weekday::Thu),
        ("friday", Weekday::Fri),
        ("saturday", Weekday::Sat),
        ("sunday", Weekday::Sun),
    ];

    for (name, weekday) in days {
        if text.contains(name) {
            let today = Utc::now().date_naive();
            let today_wd = today.weekday();
            let target = *weekday;
            let days_until =
                (target.num_days_from_monday() as i64 - today_wd.num_days_from_monday() as i64 + 7)
                    % 7;
            return Some(if days_until == 0 { 7 } else { days_until });
        }
    }
    None
}

fn parse_month_day(text: &str, today: NaiveDate) -> Option<NaiveDate> {
    let months: &[(&str, u32)] = &[
        ("january", 1),
        ("february", 2),
        ("march", 3),
        ("april", 4),
        ("may", 5),
        ("june", 6),
        ("july", 7),
        ("august", 8),
        ("september", 9),
        ("october", 10),
        ("november", 11),
        ("december", 12),
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ];

    for (name, month) in months {
        if let Some(rest) = text.strip_prefix(&format!("{name} "))
            && let Ok(day) = rest.trim().parse::<u32>()
        {
            let year = if *month < today.month() || (*month == today.month() && day < today.day()) {
                today.year() + 1
            } else {
                today.year()
            };
            return NaiveDate::from_ymd_opt(year, *month, day);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_remind_me() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("Remind me to pay the invoice by Friday");
        assert!(!items.is_empty());
        assert_eq!(items[0].kind, ActionKind::Reminder);
        assert_eq!(items[0].text, "pay the invoice");
    }

    #[test]
    fn detect_task_with_date() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("Task: review Q2 report due next week");
        assert!(!items.is_empty());
        assert_eq!(items[0].kind, ActionKind::Task);
        assert!(items[0].due_date.is_some());
    }

    #[test]
    fn detect_task_no_date() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("TODO: update the readme file");
        assert!(!items.is_empty());
        assert_eq!(items[0].kind, ActionKind::Task);
    }

    #[test]
    fn detect_decision() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("Decision: use Rust for the backend");
        assert!(!items.is_empty());
        assert_eq!(items[0].kind, ActionKind::Decision);
        assert_eq!(items[0].text, "use Rust for the backend");
    }

    #[test]
    fn parse_today_tomorrow() {
        assert!(parse_due_date("today").is_some());
        assert!(parse_due_date("tomorrow").is_some());
    }

    #[test]
    fn parse_iso_date() {
        assert_eq!(parse_due_date("2026-06-15").as_deref(), Some("2026-06-15"));
    }

    #[test]
    fn format_pending_items() {
        let items = vec![
            DetectedAction {
                text: "pay invoice".into(),
                due_date: Some("2026-07-01".into()),
                kind: ActionKind::Reminder,
            },
            DetectedAction {
                text: "use Rust".into(),
                due_date: None,
                kind: ActionKind::Decision,
            },
        ];
        let formatted = ActionTracker::format_pending(&items);
        assert!(formatted.contains("pay invoice"));
        assert!(formatted.contains("2026-07-01"));
        assert!(formatted.contains("use Rust"));
    }

    #[test]
    fn detect_multiple_in_one_prompt() {
        let tracker = ActionTracker::new();
        let items = tracker.detect(
            "Remind me to call client by Friday. TODO: update docs. Decision: use Postgres.",
        );
        assert_eq!(items.len(), 3);
        let kinds: Vec<ActionKind> = items.iter().map(|i| i.kind).collect();
        assert!(kinds.contains(&ActionKind::Reminder));
        assert!(kinds.contains(&ActionKind::Task));
        assert!(kinds.contains(&ActionKind::Decision));
    }

    #[test]
    fn detect_remind_with_on() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("Remind me to pay rent on next week");
        assert!(!items.is_empty());
        assert_eq!(items[0].kind, ActionKind::Reminder);
        assert!(items[0].due_date.is_some());
    }

    #[test]
    fn detect_remind_with_before() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("Remind me to submit report before tomorrow");
        assert!(!items.is_empty());
        assert_eq!(items[0].kind, ActionKind::Reminder);
    }

    #[test]
    fn parse_in_n_days() {
        let result = parse_due_date("in 5 days");
        assert!(result.is_some());
        let date_str = result.unwrap();
        assert!(date_str.starts_with("202"));
        assert_eq!(date_str.len(), 10);
        assert_eq!(&date_str[4..5], "-");
        assert_eq!(&date_str[7..8], "-");
    }

    #[test]
    fn parse_next_week() {
        let result = parse_due_date("next week");
        assert!(result.is_some());
        let date_str = result.unwrap();
        assert!(date_str.starts_with("202"));
        assert_eq!(date_str.len(), 10);
    }

    #[test]
    fn parse_weekday_friday() {
        let result = parse_due_date("friday");
        assert!(result.is_some());
        assert!(result.unwrap().starts_with("202"));
    }

    #[test]
    fn detect_empty_prompt() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("");
        assert!(items.is_empty());
    }

    #[test]
    fn detect_no_matches() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("hello world how are you");
        assert!(items.is_empty());
    }

    #[test]
    fn detect_overlapping_no_duplicate() {
        let tracker = ActionTracker::new();
        let items = tracker.detect("Task: review code due Friday");
        assert!(
            items.len() <= 2,
            "expected at most 2 items, got {}",
            items.len()
        );
        assert!(
            items.iter().any(|i| i.due_date.is_some()),
            "should have dated task"
        );
    }

    #[test]
    fn format_pending_mixed_types() {
        let items = vec![
            DetectedAction {
                text: "review code".into(),
                due_date: Some("2026-07-01".into()),
                kind: ActionKind::Task,
            },
            DetectedAction {
                text: "use Rust".into(),
                due_date: None,
                kind: ActionKind::Decision,
            },
        ];
        let output = ActionTracker::format_pending(&items);
        assert!(output.contains("review code"));
        assert!(output.contains("use Rust"));
    }
}
