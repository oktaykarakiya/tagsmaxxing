// SPDX-License-Identifier: AGPL-3.0-or-later

//! `kb-cov-gate` — enforce per-group line-coverage floors from an LCOV report.
//!
//! Build tooling for the Podman-backed integration lane (`just ci-integration`, ledger task
//! P6-T0). After `cargo llvm-cov` collects coverage from the full test suite — *including* the
//! `#[ignore]` testcontainers suites (cross-tenant RLS isolation, auth/session store, pg_store,
//! job queue) — this tool parses the emitted LCOV and fails the build if overall coverage, the
//! `kb-store` crate, or the auth paths fall below their floors (plan §31.2: ">=85% lines,
//! higher on crypto/auth/store").
//!
//! # CLI
//!
//! ```text
//! kb-cov-gate --lcov <PATH> --group NAME=FLOOR[:PREFIX,...] [--group ...]
//! ```
//!
//! Exit status: `0` if every group meets its floor, `1` if a floor is missed, `2` on misuse
//! (bad arguments, an unreadable/empty report, or a group whose prefixes match no files).

mod error;
mod floors;
mod lcov;

pub use error::CovGateError;
pub use floors::{Group, GroupResult, evaluate, parse_group_spec, render_report};
pub use lcov::{FileCov, parse_lcov};

use std::path::Path;

/// A parsed command line: the LCOV report path and the groups to check.
#[derive(Debug)]
struct Cli {
    lcov_path: String,
    groups: Vec<Group>,
}

/// Parse `argv[1..]` into a [`Cli`].
fn parse_args(args: &[String]) -> Result<Cli, CovGateError> {
    let mut lcov_path: Option<String> = None;
    let mut groups = Vec::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--lcov" => {
                let v = it
                    .next()
                    .ok_or_else(|| CovGateError::BadArgs("--lcov needs a <PATH>".to_owned()))?;
                lcov_path = Some(v.clone());
            }
            "--group" => {
                let v = it
                    .next()
                    .ok_or_else(|| CovGateError::BadArgs("--group needs a <SPEC>".to_owned()))?;
                groups.push(parse_group_spec(v)?);
            }
            other => {
                return Err(CovGateError::BadArgs(format!(
                    "unexpected argument {other:?}"
                )));
            }
        }
    }
    let lcov_path =
        lcov_path.ok_or_else(|| CovGateError::BadArgs("missing --lcov <PATH>".to_owned()))?;
    if groups.is_empty() {
        return Err(CovGateError::BadArgs(
            "at least one --group <SPEC> is required".to_owned(),
        ));
    }
    Ok(Cli { lcov_path, groups })
}

/// Run the gate: parse `args` (excluding the binary name), read and parse the LCOV report,
/// evaluate every group against its floor, print a report, and return a process exit code
/// (`0` = all floors met, `1` = at least one floor missed).
///
/// # Errors
///
/// Returns an error for invalid arguments, an unreadable or empty report, or a group whose
/// prefixes match no files (a misconfiguration that must not silently pass).
pub fn run(args: &[String]) -> Result<i32, CovGateError> {
    let cli = parse_args(args)?;
    let path = Path::new(&cli.lcov_path);
    let text = std::fs::read_to_string(path).map_err(|source| CovGateError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let files = parse_lcov(&text);
    if files.is_empty() {
        return Err(CovGateError::BadArgs(format!(
            "LCOV report {:?} contained no file records — was coverage collected?",
            cli.lcov_path
        )));
    }
    let results = evaluate(&files, &cli.groups)?;
    let (table, all_passed) = render_report(&results);
    print!("{table}");
    if all_passed {
        println!("✓ all coverage floors met");
        Ok(0)
    } else {
        eprintln!("✗ coverage floor(s) not met — see the table above");
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::io::Write as _;

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_owned()).collect()
    }

    fn write_lcov(body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    const REPORT: &str = "\
SF:/repo/crates/store/src/pg_store.rs
LF:1000
LH:900
end_of_record
SF:/repo/crates/core/src/auth.rs
LF:100
LH:95
end_of_record
SF:/repo/crates/core/src/query.rs
LF:100
LH:50
end_of_record
";

    // ── parse_args ────────────────────────────────────────────────────────────

    #[test]
    fn parses_lcov_and_groups() {
        let cli = parse_args(&args(&[
            "--lcov",
            "cov.info",
            "--group",
            "overall=85",
            "--group",
            "kb-store=85:crates/store/",
        ]))
        .unwrap();
        assert_eq!(cli.lcov_path, "cov.info");
        assert_eq!(cli.groups.len(), 2);
    }

    #[test]
    fn missing_lcov_is_an_error() {
        assert!(matches!(
            parse_args(&args(&["--group", "overall=85"])),
            Err(CovGateError::BadArgs(_))
        ));
    }

    #[test]
    fn no_groups_is_an_error() {
        assert!(matches!(
            parse_args(&args(&["--lcov", "cov.info"])),
            Err(CovGateError::BadArgs(_))
        ));
    }

    #[test]
    fn dangling_flag_value_is_an_error() {
        assert!(matches!(
            parse_args(&args(&["--lcov"])),
            Err(CovGateError::BadArgs(_))
        ));
        assert!(matches!(
            parse_args(&args(&["--group"])),
            Err(CovGateError::BadArgs(_))
        ));
    }

    #[test]
    fn unknown_flag_is_an_error() {
        assert!(matches!(
            parse_args(&args(&["--nope"])),
            Err(CovGateError::BadArgs(_))
        ));
    }

    // ── run ───────────────────────────────────────────────────────────────────

    #[test]
    fn run_returns_zero_when_all_floors_met() {
        let f = write_lcov(REPORT);
        let path = f.path().to_string_lossy().into_owned();
        // overall = (900+95+50)/(1200) = 87.08%; kb-store = 90%; auth = 95%.
        let code = run(&args(&[
            "--lcov",
            &path,
            "--group",
            "overall=85",
            "--group",
            "kb-store=85:crates/store/",
            "--group",
            "auth=85:crates/core/src/auth.rs",
        ]))
        .unwrap();
        assert_eq!(code, 0);
    }

    #[test]
    fn run_returns_one_when_a_floor_is_missed() {
        let f = write_lcov(REPORT);
        let path = f.path().to_string_lossy().into_owned();
        // query.rs is 50% — an overall floor of 95 cannot be met.
        let code = run(&args(&["--lcov", &path, "--group", "overall=95"])).unwrap();
        assert_eq!(code, 1);
    }

    #[test]
    fn run_errors_on_missing_file() {
        let err = run(&args(&[
            "--lcov",
            "/definitely/not/here.info",
            "--group",
            "overall=85",
        ]))
        .unwrap_err();
        assert!(matches!(err, CovGateError::Io { .. }));
    }

    #[test]
    fn run_errors_on_empty_report() {
        let f = write_lcov("TN:\n# no SF records here\n");
        let path = f.path().to_string_lossy().into_owned();
        let err = run(&args(&["--lcov", &path, "--group", "overall=85"])).unwrap_err();
        assert!(matches!(err, CovGateError::BadArgs(_)));
    }

    #[test]
    fn run_errors_when_a_group_matches_no_files() {
        let f = write_lcov(REPORT);
        let path = f.path().to_string_lossy().into_owned();
        let err = run(&args(&[
            "--lcov",
            &path,
            "--group",
            "ghost=85:crates/does-not-exist/",
        ]))
        .unwrap_err();
        assert!(matches!(err, CovGateError::NoFilesMatched { .. }));
    }
}
