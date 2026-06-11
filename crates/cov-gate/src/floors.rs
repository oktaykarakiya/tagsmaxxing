// SPDX-License-Identifier: AGPL-3.0-or-later

//! Coverage groups: parse `--group` specs, aggregate per-group line coverage, and evaluate
//! each group against its floor.

use std::fmt::Write as _;

use crate::error::CovGateError;
use crate::lcov::FileCov;

/// A named coverage floor over a set of files selected by path substrings.
#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    /// Human-readable group name (e.g. `overall`, `kb-store`, `auth`).
    pub name: String,
    /// Minimum line-coverage percentage required (`0.0..=100.0`).
    pub floor_pct: f64,
    /// Path substrings selecting the group's files. Empty means *all* files in the report.
    pub prefixes: Vec<String>,
}

/// Parse a `NAME=FLOOR[:PREFIX[,PREFIX...]]` group spec.
///
/// `overall=85` selects every file (a whole-report floor); `kb-store=85:crates/store/`
/// selects files whose path contains `crates/store/`; multiple comma-separated prefixes are
/// OR-ed together.
///
/// # Errors
///
/// Returns [`CovGateError::BadGroupSpec`] if the name is empty, the `=FLOOR` is missing, or the
/// floor is not a number in `0..=100`.
pub fn parse_group_spec(spec: &str) -> Result<Group, CovGateError> {
    let bad = |reason: &str| CovGateError::BadGroupSpec {
        spec: spec.to_owned(),
        reason: reason.to_owned(),
    };
    let (name, rest) = spec
        .split_once('=')
        .ok_or_else(|| bad("expected NAME=FLOOR[:PREFIX,...]"))?;
    if name.is_empty() {
        return Err(bad("empty group name"));
    }
    let (floor_str, prefixes) = match rest.split_once(':') {
        Some((floor, prefix_list)) => (
            floor,
            prefix_list
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        ),
        None => (rest, Vec::new()),
    };
    let floor_pct: f64 = floor_str
        .trim()
        .parse()
        .map_err(|_| bad("floor must be a number"))?;
    if !(0.0..=100.0).contains(&floor_pct) {
        return Err(bad("floor must be between 0 and 100"));
    }
    Ok(Group {
        name: name.to_owned(),
        floor_pct,
        prefixes,
    })
}

/// The computed coverage result for one [`Group`].
#[derive(Debug, Clone, PartialEq)]
pub struct GroupResult {
    /// The group name.
    pub name: String,
    /// The required floor percentage.
    pub floor_pct: f64,
    /// The achieved line-coverage percentage.
    pub actual_pct: f64,
    /// Total instrumented lines across the group's files.
    pub lines_found: u64,
    /// Total covered lines across the group's files.
    pub lines_hit: u64,
    /// How many report files matched the group's prefixes.
    pub matched_files: usize,
}

impl GroupResult {
    /// Whether the group meets its floor. The achieved percentage is rounded to two decimals
    /// before comparison, matching how llvm-cov's own `--fail-under-lines` reports coverage.
    #[must_use]
    pub fn passed(&self) -> bool {
        round2(self.actual_pct) >= self.floor_pct
    }
}

/// Round to two decimal places (llvm-cov reports coverage at 2-decimal precision).
fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Whether `path` belongs to `group` — it matches any prefix, or the group selects all files.
fn matches(group: &Group, path: &str) -> bool {
    group.prefixes.is_empty() || group.prefixes.iter().any(|p| path.contains(p.as_str()))
}

/// Aggregate per-group line coverage and compare each group to its floor.
///
/// # Errors
///
/// Returns [`CovGateError::NoFilesMatched`] if a group that specifies prefixes matches no files
/// — so a renamed or mistyped path can never silently turn a floor into a no-op.
pub fn evaluate(files: &[FileCov], groups: &[Group]) -> Result<Vec<GroupResult>, CovGateError> {
    let mut results = Vec::with_capacity(groups.len());
    for g in groups {
        let mut lines_found = 0u64;
        let mut lines_hit = 0u64;
        let mut matched_files = 0usize;
        for f in files {
            if matches(g, &f.path) {
                lines_found += f.lines_found;
                lines_hit += f.lines_hit;
                matched_files += 1;
            }
        }
        if matched_files == 0 && !g.prefixes.is_empty() {
            return Err(CovGateError::NoFilesMatched {
                name: g.name.clone(),
                prefixes: g.prefixes.clone(),
            });
        }
        let actual_pct = if lines_found == 0 {
            100.0
        } else {
            (lines_hit as f64 / lines_found as f64) * 100.0
        };
        results.push(GroupResult {
            name: g.name.clone(),
            floor_pct: g.floor_pct,
            actual_pct,
            lines_found,
            lines_hit,
            matched_files,
        });
    }
    Ok(results)
}

/// Render a human-readable table of [`GroupResult`]s, returning the table and whether every
/// group passed its floor.
#[must_use]
pub fn render_report(results: &[GroupResult]) -> (String, bool) {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{:<12} {:>7} {:>7} {:>8} {:>8} {:>6}  status",
        "group", "lines", "hit", "cover", "floor", "files"
    );
    let mut all_passed = true;
    for r in results {
        let ok = r.passed();
        all_passed &= ok;
        let _ = writeln!(
            s,
            "{:<12} {:>7} {:>7} {:>7.2}% {:>7.2}% {:>6}  {}",
            r.name,
            r.lines_found,
            r.lines_hit,
            r.actual_pct,
            r.floor_pct,
            r.matched_files,
            if ok { "PASS" } else { "FAIL" }
        );
    }
    (s, all_passed)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn file(path: &str, found: u64, hit: u64) -> FileCov {
        FileCov {
            path: path.to_owned(),
            lines_found: found,
            lines_hit: hit,
        }
    }

    // ── parse_group_spec ──────────────────────────────────────────────────────

    #[test]
    fn parses_overall_spec_without_prefixes() {
        let g = parse_group_spec("overall=85").unwrap();
        assert_eq!(g.name, "overall");
        assert!((g.floor_pct - 85.0).abs() < 1e-9);
        assert!(g.prefixes.is_empty());
    }

    #[test]
    fn parses_spec_with_multiple_prefixes() {
        let g =
            parse_group_spec("auth=90:crates/core/src/auth.rs,crates/store/src/session_store.rs")
                .unwrap();
        assert_eq!(g.name, "auth");
        assert!((g.floor_pct - 90.0).abs() < 1e-9);
        assert_eq!(
            g.prefixes,
            vec![
                "crates/core/src/auth.rs".to_owned(),
                "crates/store/src/session_store.rs".to_owned()
            ]
        );
    }

    #[test]
    fn empty_prefix_list_after_colon_means_all_files() {
        let g = parse_group_spec("overall=85:").unwrap();
        assert!(g.prefixes.is_empty());
    }

    #[test]
    fn rejects_missing_equals() {
        assert!(matches!(
            parse_group_spec("overall85"),
            Err(CovGateError::BadGroupSpec { .. })
        ));
    }

    #[test]
    fn rejects_empty_name() {
        assert!(matches!(
            parse_group_spec("=85"),
            Err(CovGateError::BadGroupSpec { .. })
        ));
    }

    #[test]
    fn rejects_non_numeric_floor() {
        assert!(matches!(
            parse_group_spec("a=high"),
            Err(CovGateError::BadGroupSpec { .. })
        ));
    }

    #[test]
    fn rejects_out_of_range_floor() {
        assert!(matches!(
            parse_group_spec("a=150"),
            Err(CovGateError::BadGroupSpec { .. })
        ));
        assert!(matches!(
            parse_group_spec("a=-1"),
            Err(CovGateError::BadGroupSpec { .. })
        ));
    }

    // ── evaluate ──────────────────────────────────────────────────────────────

    #[test]
    fn overall_group_aggregates_all_files() {
        let files = vec![file("/a/x.rs", 100, 77), file("/b/y.rs", 100, 77)];
        let groups = vec![parse_group_spec("overall=76").unwrap()];
        let res = evaluate(&files, &groups).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].lines_found, 200);
        assert_eq!(res[0].lines_hit, 154);
        assert!((res[0].actual_pct - 77.0).abs() < 1e-9);
        assert_eq!(res[0].matched_files, 2);
        assert!(res[0].passed());
    }

    #[test]
    fn prefix_group_selects_only_matching_files() {
        let files = vec![
            file("/repo/crates/store/src/pg_store.rs", 1000, 900),
            file("/repo/crates/core/src/query.rs", 100, 10),
        ];
        let groups = vec![parse_group_spec("kb-store=85:crates/store/").unwrap()];
        let res = evaluate(&files, &groups).unwrap();
        assert_eq!(res[0].matched_files, 1);
        assert_eq!(res[0].lines_found, 1000);
        assert!((res[0].actual_pct - 90.0).abs() < 1e-9);
        assert!(res[0].passed());
    }

    #[test]
    fn group_below_floor_fails() {
        let files = vec![file("/repo/crates/store/src/pg_store.rs", 1000, 300)];
        let groups = vec![parse_group_spec("kb-store=85:crates/store/").unwrap()];
        let res = evaluate(&files, &groups).unwrap();
        assert!((res[0].actual_pct - 30.0).abs() < 1e-9);
        assert!(!res[0].passed(), "30% must not pass an 85% floor");
    }

    #[test]
    fn prefix_matching_nothing_is_an_error_not_a_pass() {
        let files = vec![file("/repo/crates/core/src/query.rs", 100, 100)];
        let groups = vec![parse_group_spec("kb-store=85:crates/store/").unwrap()];
        let err = evaluate(&files, &groups).unwrap_err();
        assert!(matches!(err, CovGateError::NoFilesMatched { .. }));
    }

    #[test]
    fn empty_group_with_no_files_treated_as_full_coverage() {
        // An overall group (no prefixes) over zero instrumented lines yields 100% — but the
        // binary guards against a genuinely empty report separately (see lib::run tests).
        let res = evaluate(&[], &[parse_group_spec("overall=85").unwrap()]).unwrap();
        assert!((res[0].actual_pct - 100.0).abs() < 1e-9);
        assert!(res[0].passed());
    }

    #[test]
    fn rounding_matches_llvm_cov_two_decimals() {
        // 84.999% rounds to 85.00 and should pass an 85 floor (parity with llvm-cov).
        let files = vec![file("/x.rs", 100_000, 84_999)];
        let groups = vec![parse_group_spec("overall=85").unwrap()];
        let res = evaluate(&files, &groups).unwrap();
        assert!(res[0].passed());
    }

    // ── render_report ─────────────────────────────────────────────────────────

    #[test]
    fn report_flags_all_passed_true_when_every_group_meets_floor() {
        let files = vec![file("/x.rs", 100, 90)];
        let groups = vec![parse_group_spec("overall=85").unwrap()];
        let res = evaluate(&files, &groups).unwrap();
        let (table, all_passed) = render_report(&res);
        assert!(all_passed);
        assert!(table.contains("PASS"));
        assert!(table.contains("overall"));
    }

    #[test]
    fn report_flags_all_passed_false_when_a_group_fails() {
        let files = vec![file("/x.rs", 100, 50)];
        let groups = vec![parse_group_spec("overall=85").unwrap()];
        let res = evaluate(&files, &groups).unwrap();
        let (table, all_passed) = render_report(&res);
        assert!(!all_passed);
        assert!(table.contains("FAIL"));
    }
}
