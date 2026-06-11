// SPDX-License-Identifier: AGPL-3.0-or-later

//! Regression guard (ledger P6-T0, part d): the `cov` coverage gate in the repo `justfile`
//! must stay a REAL, unmasked floor. This test is what prevents the gate from being silently
//! neutered again — the review/p6-readiness finding was that a trailing `|| true` plus a
//! lowered `--fail-under-lines` had made the coverage gate unable to fail (plan §31.2: "never
//! weaken a gate"). It runs in the fast `just ci` (no container runtime needed).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

/// Read the repo-root `justfile` (two levels up from this crate).
fn justfile() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("justfile");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn coverage_gate_is_not_masked() {
    // Inspect only *command* lines, not comments: a comment may legitimately mention the
    // forbidden patterns as cautionary documentation (this justfile does exactly that).
    for line in code_lines(&justfile()) {
        if line.contains("llvm-cov") || line.contains("fail-under") {
            assert!(
                !line.contains("|| true"),
                "coverage gate command masks its own failure with `|| true`: {line:?}"
            );
            assert!(
                !line.contains("--fail-under-lines 0"),
                "coverage gate command disables the floor with `--fail-under-lines 0`: {line:?}"
            );
        }
    }
}

/// Lines of `text` that are not whole-line `#` comments (leading/trailing whitespace trimmed).
fn code_lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
}

#[test]
fn cov_recipe_enforces_the_configured_floor() {
    let jf = justfile();
    // The enforcing recipe must reference the configured floor variable, not a hardcoded value
    // that could quietly be set to something meaningless.
    assert!(
        jf.contains("--fail-under-lines {{cov_min}}"),
        "the `cov` recipe must enforce `--fail-under-lines {{{{cov_min}}}}`"
    );
}

#[test]
fn fast_ci_floor_is_a_meaningful_value() {
    let jf = justfile();
    let line = jf
        .lines()
        .find(|l| l.trim_start().starts_with("cov_min :="))
        .expect("`cov_min :=` must be defined in the justfile");
    let value = quoted_int(line).expect("cov_min must be a quoted integer");
    // The honest fast-lane line level is ~77% (see the justfile comment). The floor must stay
    // close to that — high enough that it cannot be quietly gutted, while leaving the spec's
    // >=85 to the Podman-backed `just ci-integration` lane (which exercises the DB/auth code).
    assert!(
        value >= 75,
        "fast `just ci` floor cov_min={value} is too low to be a meaningful gate; cover the \
         code or run `just ci-integration` — do not gut the floor (§31.2)"
    );
}

#[test]
fn integration_lane_enforces_the_spec_floors() {
    let jf = justfile();
    // The integration lane must enforce the spec's >=85 overall and >=85 on the security
    // crate/auth paths (§31.2 "higher on store/auth"); these are defined, not zeroed.
    let overall = quoted_int(
        jf.lines()
            .find(|l| l.trim_start().starts_with("cov_min_integration :="))
            .expect("cov_min_integration must be defined"),
    )
    .expect("cov_min_integration must be a quoted integer");
    let secure = quoted_int(
        jf.lines()
            .find(|l| l.trim_start().starts_with("cov_min_secure :="))
            .expect("cov_min_secure must be defined"),
    )
    .expect("cov_min_secure must be a quoted integer");
    assert!(
        overall >= 85,
        "integration overall floor must be >=85 (§31.2), got {overall}"
    );
    assert!(
        secure >= 85,
        "store/auth floor must be >=85 (§31.2 higher on store/auth), got {secure}"
    );
}

/// Extract the integer inside the first pair of double quotes on a line, e.g. `x := "76"`.
fn quoted_int(line: &str) -> Option<u32> {
    let start = line.find('"')? + 1;
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse().ok()
}
