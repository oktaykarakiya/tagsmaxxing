#!/usr/bin/env python3
"""Read-only helper for BUG_LEDGER.toml — the autonomous bug-fix pipeline.

Same CLI contract as ``scripts/ledger.py`` so the fix-loop can be modelled on
build-loop.sh:

    bug_ledger.py next                 → next eligible bug id, or ALL_DONE / BLOCKED
    bug_ledger.py next-batch [N]       → up to N bug ids from DIFFERENT conflict groups
    bug_ledger.py field <id> <field>   → a field value (lists are comma-joined)
    bug_ledger.py status               → human-readable summary
    bug_ledger.py group <id>           → conflict group name
    bug_ledger.py tests <id>           → space-separated pytest -k expression

"Next eligible" = the first bug (in declaration order) whose status is "todo" and
all of whose ``depends_on`` are "done".

"Next batch" = up to N eligible bugs, each from a different conflict group, so
they can be fixed in parallel without file conflicts.
"""

import pathlib
import sys
import tomllib

LEDGER = pathlib.Path(__file__).resolve().parent.parent / "BUG_LEDGER.toml"


def load_bugs():
    with LEDGER.open("rb") as fh:
        data = tomllib.load(fh)
    return data.get("bug", []), data.get("conflict_group", [])


def index(bugs):
    return {b["id"]: b for b in bugs}


def next_eligible(bugs):
    """Return (bug_or_None, label)."""
    done = {b["id"] for b in bugs if b.get("status") == "done"}
    for b in bugs:  # declaration order
        if b.get("status") != "todo":
            continue
        if all(dep in done for dep in b.get("depends_on", [])):
            return b, b["id"]
    todos = [b for b in bugs if b.get("status") == "todo"]
    if not todos:
        return None, "ALL_DONE"
    return None, "BLOCKED"


def next_batch(bugs, n=4):
    """Return up to N eligible bug ids, each from a different conflict group."""
    done = {b["id"] for b in bugs if b.get("status") == "done"}
    chosen_groups: set[str] = set()
    batch: list[str] = []
    for b in bugs:
        if b.get("status") != "todo":
            continue
        if not all(dep in done for dep in b.get("depends_on", [])):
            continue
        grp = b.get("conflict_group", "")
        if grp in chosen_groups:
            continue
        batch.append(b["id"])
        chosen_groups.add(grp)
        if len(batch) >= n:
            break
    return batch


def cmd_next(bugs, _groups):
    _, label = next_eligible(bugs)
    print(label)


def cmd_next_batch(bugs, _groups, n=4):
    batch = next_batch(bugs, n)
    if batch:
        print(" ".join(batch))
    else:
        _, label = next_eligible(bugs)
        print(label)


def cmd_field(bugs, _groups, tid, field):
    b = index(bugs).get(tid)
    if b is None:
        sys.exit(f"bug_ledger.py: unknown bug id: {tid}")
    value = b.get(field, "")
    print(",".join(map(str, value)) if isinstance(value, list) else value)


def cmd_tests(bugs, _groups, tid):
    """Print a pytest -k expression for this bug's tests."""
    b = index(bugs).get(tid)
    if b is None:
        sys.exit(f"bug_ledger.py: unknown bug id: {tid}")
    names = b.get("test_names", [])
    test_file = b.get("test_file", "")
    if not names:
        print("")
        return
    # Build: test_file::test_name1 or test_file::test_name2 ...
    or_expr = " or ".join(f"{test_file}::{n}" for n in names)
    print(or_expr)


def cmd_group(bugs, _groups, tid):
    b = index(bugs).get(tid)
    if b is None:
        sys.exit(f"bug_ledger.py: unknown bug id: {tid}")
    print(b.get("conflict_group", ""))


def cmd_status(bugs, groups):
    counts: dict[str, int] = {}
    for b in bugs:
        s = b.get("status", "?")
        counts[s] = counts.get(s, 0) + 1
    print(f"Bug Ledger: {LEDGER.name}  ({len(bugs)} bugs)")
    print("  totals: " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print()
    if groups:
        print("Conflict groups:")
        for g in groups:
            print(f"  {g.get('name', '?'):20s}  files: {', '.join(g.get('files', []))}")
        print()
    for b in bugs:
        deps = ",".join(b.get("depends_on", [])) or "-"
        desc = b.get("description", "")
        desc = desc[:72] + "…" if len(desc) > 73 else desc
        grp = b.get("conflict_group", "?")
        tests = ",".join(b.get("test_names", [])) or "-"
        print(f"  [{b.get('status', '?'):>11}] {b['id']:<14} grp={grp:<14} "
              f"deps={deps:<10} tests={tests[:40]}")
        print(f"                      {desc}")
    _, label = next_eligible(bugs)
    print(f"\n  next: {label}")


def main(argv):
    if len(argv) < 2:
        sys.exit("usage: bug_ledger.py {next|next-batch [N]|field <id> <field>|tests <id>|group <id>|status}")
    bugs, groups = load_bugs()
    cmd = argv[1]
    if cmd == "next":
        cmd_next(bugs, groups)
    elif cmd == "next-batch":
        n = int(argv[2]) if len(argv) > 2 else 4
        cmd_next_batch(bugs, groups, n)
    elif cmd == "field":
        cmd_field(bugs, groups, argv[2], argv[3])
    elif cmd == "tests":
        cmd_tests(bugs, groups, argv[2])
    elif cmd == "group":
        cmd_group(bugs, groups, argv[2])
    elif cmd == "status":
        cmd_status(bugs, groups)
    else:
        sys.exit(f"bug_ledger.py: unknown command: {cmd}")


if __name__ == "__main__":
    main(sys.argv)
