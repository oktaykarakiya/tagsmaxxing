//! Minimal LCOV parser — extracts per-file line counts from a coverage report.
//!
//! `cargo llvm-cov report --lcov` emits standard LCOV: one record per source file, with an
//! `SF:<path>` header, per-line `DA:` entries, `LF:`/`LH:` line summaries, and a terminating
//! `end_of_record`. We read the authoritative `LF` (lines found) and `LH` (lines hit)
//! summaries; summing them across files reproduces llvm-cov's own TOTAL line coverage exactly
//! (verified against the workspace report during P6-T0).

/// Per-file line-coverage counts parsed from one LCOV record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCov {
    /// Source path from the record's `SF:` line (absolute, as llvm-cov emits it).
    pub path: String,
    /// Instrumented (found) lines — the LCOV `LF` value.
    pub lines_found: u64,
    /// Covered (hit) lines — the LCOV `LH` value.
    pub lines_hit: u64,
}

/// Parse LCOV text into one [`FileCov`] per `SF:` record.
///
/// A record is opened by `SF:` and closed by `end_of_record` (or by the next `SF:`, or by
/// end of input — so a truncated final record is still captured). Records without an explicit
/// `LF`/`LH` default those counts to zero; an unparseable count is treated as zero rather than
/// aborting the whole report. Content before the first `SF:` is ignored.
pub fn parse_lcov(text: &str) -> Vec<FileCov> {
    let mut out = Vec::new();
    let mut cur: Option<FileCov> = None;
    for raw in text.lines() {
        let line = raw.trim();
        if let Some(path) = line.strip_prefix("SF:") {
            // A new record begins; flush a previous one that lacked an end_of_record.
            if let Some(prev) = cur.take() {
                out.push(prev);
            }
            cur = Some(FileCov {
                path: path.trim().to_owned(),
                lines_found: 0,
                lines_hit: 0,
            });
        } else if let Some(v) = line.strip_prefix("LF:") {
            if let Some(f) = cur.as_mut() {
                f.lines_found = v.trim().parse().unwrap_or(0);
            }
        } else if let Some(v) = line.strip_prefix("LH:") {
            if let Some(f) = cur.as_mut() {
                f.lines_hit = v.trim().parse().unwrap_or(0);
            }
        } else if line == "end_of_record"
            && let Some(f) = cur.take()
        {
            out.push(f);
        }
    }
    if let Some(f) = cur.take() {
        out.push(f);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    const SAMPLE: &str = "\
TN:
SF:/repo/crates/store/src/pg_store.rs
FN:10,_some_fn
DA:10,5
DA:11,0
LF:2543
LH:769
end_of_record
SF:/repo/crates/core/src/auth.rs
DA:1,1
LF:133
LH:128
end_of_record
";

    #[test]
    fn parses_two_records_with_lf_lh() {
        let files = parse_lcov(SAMPLE);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "/repo/crates/store/src/pg_store.rs");
        assert_eq!(files[0].lines_found, 2543);
        assert_eq!(files[0].lines_hit, 769);
        assert_eq!(files[1].path, "/repo/crates/core/src/auth.rs");
        assert_eq!(files[1].lines_found, 133);
        assert_eq!(files[1].lines_hit, 128);
    }

    #[test]
    fn empty_input_yields_no_records() {
        assert!(parse_lcov("").is_empty());
        assert!(
            parse_lcov("TN:\nDA:1,1\n").is_empty(),
            "no SF: => no records"
        );
    }

    #[test]
    fn truncated_final_record_is_captured() {
        // No trailing end_of_record — the parser still flushes the open record.
        let files = parse_lcov("SF:/x/y.rs\nLF:10\nLH:7\n");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].lines_found, 10);
        assert_eq!(files[0].lines_hit, 7);
    }

    #[test]
    fn new_sf_flushes_previous_unterminated_record() {
        let files = parse_lcov("SF:/a.rs\nLF:4\nLH:4\nSF:/b.rs\nLF:2\nLH:1\nend_of_record\n");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "/a.rs");
        assert_eq!(files[0].lines_found, 4);
        assert_eq!(files[1].path, "/b.rs");
        assert_eq!(files[1].lines_hit, 1);
    }

    #[test]
    fn missing_or_unparseable_counts_default_to_zero() {
        let files = parse_lcov("SF:/a.rs\nend_of_record\nSF:/b.rs\nLF:notanumber\nend_of_record\n");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].lines_found, 0);
        assert_eq!(files[0].lines_hit, 0);
        assert_eq!(files[1].lines_found, 0);
    }
}
