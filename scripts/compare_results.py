#!/usr/bin/env python3
"""Compare two E2E test result CSVs (history.csv) and produce a delta report.

Usage:
    compare_results.py --baseline <path> --current <path> [--output <csv>] [--summary]

Reads the ``history.csv`` files produced by conftest.py (columns: run_id,
timestamp_utc, nodeid, outcome, duration_s, error) and produces a per-test
delta CSV with columns:

    nodeid, baseline_outcome, current_outcome, delta, baseline_error, current_error

Delta values:
    FIXED          — was failed/error, now passed
    REGRESSED      — was passed, now failed/error
    STILL_FAILING  — was failed/error, still failed/error
    STILL_PASSING  — was passed, still passed
    NOW_SKIPPED    — was not skipped, now skipped
    WAS_SKIPPED    — was skipped, now not skipped
    UNCHANGED      — same non-pass/fail outcome
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path


def load_latest_per_test(path: Path) -> dict[str, dict]:
    """Load a history.csv, keeping only the latest run_id's row per nodeid."""
    rows: dict[str, dict] = {}
    if not path.exists():
        print(f"compare_results: WARNING — {path} does not exist; treating as empty.", file=sys.stderr)
        return rows
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            nodeid = row.get("nodeid", "").strip()
            if not nodeid:
                continue
            # Keep the LAST row for each nodeid (latest run)
            rows[nodeid] = {
                "outcome": row.get("outcome", "unknown").strip(),
                "error": row.get("error", "").strip(),
            }
    return rows


def classify(baseline_outcome: str, current_outcome: str) -> str:
    """Classify the delta between two outcomes."""
    b, c = baseline_outcome, current_outcome
    if b == c:
        if b == "passed":
            return "STILL_PASSING"
        if b in ("failed", "error"):
            return "STILL_FAILING"
        return "UNCHANGED"
    # Outcome changed
    if b in ("failed", "error") and c == "passed":
        return "FIXED"
    if b == "passed" and c in ("failed", "error"):
        return "REGRESSED"
    if c == "skipped" and b != "skipped":
        return "NOW_SKIPPED"
    if b == "skipped" and c != "skipped":
        return "WAS_SKIPPED"
    return "UNCHANGED"


def compare(baseline: dict[str, dict], current: dict[str, dict]) -> list[dict]:
    """Produce per-test delta rows."""
    all_nodes = sorted(set(baseline.keys()) | set(current.keys()))
    rows = []
    for nodeid in all_nodes:
        b = baseline.get(nodeid, {})
        c = current.get(nodeid, {})
        b_out = b.get("outcome", "unknown")
        c_out = c.get("outcome", "unknown")
        delta = classify(b_out, c_out)
        rows.append({
            "nodeid": nodeid,
            "baseline_outcome": b_out,
            "current_outcome": c_out,
            "delta": delta,
            "baseline_error": b.get("error", ""),
            "current_error": c.get("error", ""),
        })
    return rows


def print_summary(rows: list[dict]) -> None:
    """Print a human-readable summary."""
    counts = Counter(r["delta"] for r in rows)
    total = len(rows)
    print(f"\n{'='*60}")
    print(f"  Delta Report  ({total} tests)")
    print(f"{'='*60}")
    for label, emoji in [
        ("FIXED", "✅"),
        ("REGRESSED", "🔴"),
        ("STILL_FAILING", "❌"),
        ("STILL_PASSING", "✅"),
        ("NOW_SKIPPED", "⏭️"),
        ("WAS_SKIPPED", "▶️"),
        ("UNCHANGED", "➖"),
    ]:
        n = counts.get(label, 0)
        if n:
            bar = "█" * min(40, n * 40 // max(total, 1))
            print(f"  {emoji} {label:<18} {n:>4}  {bar}")

    # List regressions prominently
    regressed = [r for r in rows if r["delta"] == "REGRESSED"]
    if regressed:
        print(f"\n  🔴 REGRESSIONS ({len(regressed)}):")
        for r in regressed:
            print(f"     {r['nodeid']}")
            if r["current_error"]:
                print(f"       → {r['current_error'][:120]}")

    # List fixes
    fixed = [r for r in rows if r["delta"] == "FIXED"]
    if fixed:
        print(f"\n  ✅ FIXED ({len(fixed)}):")
        for r in fixed:
            print(f"     {r['nodeid']}")

    still = [r for r in rows if r["delta"] == "STILL_FAILING"]
    if still:
        print(f"\n  ❌ STILL FAILING ({len(still)}):")
        for r in still:
            print(f"     {r['nodeid']}")

    print()


def write_csv(rows: list[dict], path: Path) -> None:
    """Write delta rows to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=[
            "nodeid", "baseline_outcome", "current_outcome", "delta",
            "baseline_error", "current_error",
        ])
        w.writeheader()
        w.writerows(rows)
    print(f"  Delta CSV written → {path}")


def main(argv):
    p = argparse.ArgumentParser(description="Compare two E2E test result CSVs")
    p.add_argument("--baseline", required=True, type=Path, help="Baseline history.csv")
    p.add_argument("--current", required=True, type=Path, help="Current history.csv")
    p.add_argument("--output", type=Path, help="Write delta CSV to this path")
    p.add_argument("--summary", action="store_true", default=True,
                   help="Print summary (default)")
    args = p.parse_args(argv[1:])

    baseline = load_latest_per_test(args.baseline)
    current = load_latest_per_test(args.current)
    rows = compare(baseline, current)

    if args.output:
        write_csv(rows, args.output)

    if args.summary:
        print_summary(rows)

    # Exit non-zero if there are regressions (for CI / scripting)
    regressed = sum(1 for r in rows if r["delta"] == "REGRESSED")
    if regressed:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
