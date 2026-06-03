#!/usr/bin/env python3
"""Track which catalog tests are implemented, pending, or blocked.

Pure AST inspection — no imports of the tests, no running pytest — so it is fast
and safe to call from many processes at once. A catalog test function is:

* DONE     — no ``@pytest.mark.skip`` decorator.
* PENDING  — skipped with a reason starting ``step2`` (the step-2 stub marker).
* BLOCKED  — skipped with any other reason (an agent marked it genuinely blocked).

Only PENDING tests are dispatched for implementation, so re-runs converge and
BLOCKED tests are not retried forever.

Usage:
  catalog_status.py summary                 # counts per file + totals (default)
  catalog_status.py list                    # STATUS file::test, one per line
  catalog_status.py pending-files           # absolute paths of files with >=1 pending
  catalog_status.py pending-in <file.py>    # pending test names in one file
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "tests" / "catalog"


def _skip_reason(dec: ast.expr) -> str | None:
    """Return the skip reason if ``dec`` is a pytest.mark.skip decorator, else None.

    Returns ``""`` for a skip decorator with no reason.
    """
    node = dec.func if isinstance(dec, ast.Call) else dec
    if not (isinstance(node, ast.Attribute) and node.attr == "skip"):
        return None
    parent = node.value
    if not (isinstance(parent, ast.Attribute) and parent.attr == "mark"):
        return None
    if not isinstance(dec, ast.Call):
        return ""
    for kw in dec.keywords:
        if kw.arg == "reason" and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    if dec.args and isinstance(dec.args[0], ast.Constant):
        return str(dec.args[0].value)
    return ""


def _status_of(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    for dec in fn.decorator_list:
        reason = _skip_reason(dec)
        if reason is None:
            continue
        return "PENDING" if reason.lower().startswith("step2") else "BLOCKED"
    return "DONE"


def tests_in(path: Path) -> list[tuple[str, str]]:
    """Return ``[(test_name, STATUS), ...]`` for a catalog file."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    out: list[tuple[str, str]] = []

    def visit(body: list[ast.stmt]) -> None:
        for n in body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test"):
                out.append((n.name, _status_of(n)))
            elif isinstance(n, ast.ClassDef):
                visit(n.body)

    visit(tree.body)
    return out


def collect(root: Path) -> dict[Path, list[tuple[str, str]]]:
    return {f: tests_in(f) for f in sorted(root.glob("test_*.py"))}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("mode", nargs="?", default="summary",
                    choices=["summary", "list", "pending-files", "pending-in"])
    ap.add_argument("file", nargs="?", help="catalog file (for pending-in)")
    ap.add_argument("--root", default=str(CATALOG), help="catalog directory")
    args = ap.parse_args()

    if args.mode == "pending-in":
        if not args.file:
            ap.error("pending-in requires a file argument")
        for name, status in tests_in(Path(args.file)):
            if status == "PENDING":
                print(name)
        return 0

    data = collect(Path(args.root))

    if args.mode == "pending-files":
        for f, tests in data.items():
            if any(s == "PENDING" for _, s in tests):
                print(f.resolve())
        return 0

    if args.mode == "list":
        for f, tests in data.items():
            for name, status in tests:
                print(f"{status:8} {f.name}::{name}")
        return 0

    # summary
    tot = done = pend = blocked = 0
    print(f"{'file':42} done pend blkd / total")
    print("-" * 64)
    for f, tests in data.items():
        d = sum(1 for _, s in tests if s == "DONE")
        p = sum(1 for _, s in tests if s == "PENDING")
        b = sum(1 for _, s in tests if s == "BLOCKED")
        tot += len(tests); done += d; pend += p; blocked += b
        print(f"{f.name:42} {d:4} {p:4} {b:4} / {len(tests)}")
    print("-" * 64)
    print(f"{'TOTAL':42} {done:4} {pend:4} {blocked:4} / {tot}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
