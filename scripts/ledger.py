#!/usr/bin/env python3
"""Read-only helper for BUILD_LEDGER.toml (plan §31.1).

The implementing agent updates task `status` by editing the ledger directly, in the same
commit as the work. This script only *reads* the ledger to drive scripts/build-loop.sh:

    ledger.py next                 -> next eligible task id, or PHASE_COMPLETE / BLOCKED
    ledger.py is-checkpoint <id>   -> "true" | "false"
    ledger.py field <id> <field>   -> a field value (lists are comma-joined)
    ledger.py status               -> human-readable summary

"Next eligible" = the first task (in declaration order, which is dependency order) whose
status is "todo" and all of whose `depends_on` are "done".
"""

import pathlib
import sys
import tomllib

LEDGER = pathlib.Path(__file__).resolve().parent.parent / "BUILD_LEDGER.toml"


def load_tasks():
    with LEDGER.open("rb") as fh:
        return tomllib.load(fh).get("task", [])


def index(tasks):
    return {t["id"]: t for t in tasks}


def next_eligible(tasks):
    """Return (task_or_None, label)."""
    done = {t["id"] for t in tasks if t.get("status") == "done"}
    todos = [t for t in tasks if t.get("status") == "todo"]
    if not todos:
        return None, "PHASE_COMPLETE"
    for t in tasks:  # declaration order == intended dependency order
        if t.get("status") != "todo":
            continue
        if all(dep in done for dep in t.get("depends_on", [])):
            return t, t["id"]
    # todos remain but none are unblocked (deadlock, or upstream blocked/in_progress)
    return None, "BLOCKED"


def cmd_next(tasks):
    _, label = next_eligible(tasks)
    print(label)


def cmd_is_checkpoint(tasks, tid):
    t = index(tasks).get(tid)
    print("true" if (t and t.get("checkpoint")) else "false")


def cmd_field(tasks, tid, field):
    t = index(tasks).get(tid)
    if t is None:
        sys.exit(f"ledger.py: unknown task id: {tid}")
    value = t.get(field, "")
    print(",".join(map(str, value)) if isinstance(value, list) else value)


def cmd_status(tasks):
    counts = {}
    for t in tasks:
        s = t.get("status", "?")
        counts[s] = counts.get(s, 0) + 1
    print(f"Ledger: {LEDGER.name}  ({len(tasks)} tasks)")
    print("  totals: " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    for t in tasks:
        cp = "  [CHECKPOINT]" if t.get("checkpoint") else ""
        deps = ",".join(t.get("depends_on", [])) or "-"
        desc = t.get("description", "")
        desc = desc[:68] + "…" if len(desc) > 69 else desc
        print(f"  [{t.get('status', '?'):>11}] {t['id']:<7} deps={deps:<14}{cp}  {desc}")
    _, label = next_eligible(tasks)
    print(f"  next: {label}")


def main(argv):
    if len(argv) < 2:
        sys.exit("usage: ledger.py {next | is-checkpoint <id> | field <id> <field> | status}")
    tasks = load_tasks()
    cmd = argv[1]
    if cmd == "next":
        cmd_next(tasks)
    elif cmd == "is-checkpoint":
        cmd_is_checkpoint(tasks, argv[2])
    elif cmd == "field":
        cmd_field(tasks, argv[2], argv[3])
    elif cmd == "status":
        cmd_status(tasks)
    else:
        sys.exit(f"ledger.py: unknown command: {cmd}")


if __name__ == "__main__":
    main(sys.argv)
