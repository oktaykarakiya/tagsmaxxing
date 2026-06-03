#!/usr/bin/env bash
# fix-bug.sh <BUG_ID>
#
# Spawns ONE headless Claude Code instance to fix a single bug from BUG_LEDGER.toml.
# The agent may edit ONLY the source files listed in the bug's `touches` field.
# Invoked by fix-loop.sh (one process per bug in a batch → parallel across
# different conflict groups).
#
# Env: FIX_MODEL (optional --model), BUG_TIMEOUT (default 2400s).
#      PERM_MODE (default acceptEdits, or bypassPermissions for --yolo).
# Always exits 0 so the pool keeps going — fix-loop.sh independently verifies.
set -euo pipefail

BUG_ID="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

LEDGER="$SCRIPT_DIR/bug_ledger.py"
STATE_DIR="$REPO_ROOT/.fix-loop"
LOG_DIR="$STATE_DIR/logs"
mkdir -p "$LOG_DIR"

stem="${BUG_ID}"
log="$LOG_DIR/${stem}.log"

# ── Read bug details from the ledger ────────────────────────────────────────
description="$("$LEDGER" field "$BUG_ID" description)"
test_names="$("$LEDGER" field "$BUG_ID" test_names)"
test_file="$("$LEDGER" field "$BUG_ID" test_file)"
touches="$("$LEDGER" field "$BUG_ID" touches)"
conflict_group="$("$LEDGER" field "$BUG_ID" conflict_group)"
severity="$("$LEDGER" field "$BUG_ID" severity)"
acceptance="$("$LEDGER" field "$BUG_ID" acceptance)"
notes="$("$LEDGER" field "$BUG_ID" notes)"
tests_expr="$("$LEDGER" tests "$BUG_ID")"

if [[ -z "$description" || "$description" == "__field_not_found__" ]]; then
  echo "ERROR  $BUG_ID: not found in BUG_LEDGER.toml" | tee -a "$log"
  exit 0
fi

echo "START  $BUG_ID  [$severity]  group=$conflict_group"
echo "        $description"
echo "        tests: $test_names"
echo "        touches: $touches"

# ── Build the agent prompt ──────────────────────────────────────────────────
read -r -d '' PROMPT <<EOF || true
You are fixing a SINGLE bug in the Local File Knowledge Base (a Rust web app).
Work autonomously until the bug is fixed and all gates pass.

BUG ID:     ${BUG_ID}
SEVERITY:   ${severity}
SUMMARY:    ${description}
NOTES:      ${notes}

FAILING TEST(S): ${test_names}
TEST FILE:       ${test_file}
PYTEST EXPR:     ${tests_expr}

FILES YOU MAY EDIT (ONLY these files — nothing else):
${touches}

READ FIRST (read-only — do not edit):
  - CLAUDE.md (build rules, testing mandate, modularity, hot-swappable config)
  - local-kb-plan.md (architecture specification)
  - The failing test(s) in test/${test_file} (understand what the test asserts)
  - The source files listed above (understand the current implementation)

THE FIX:
  1. Read the failing test(s) to understand the CORRECT, intended contract.
     The test encodes the truth — do NOT weaken, relax, or skip it.
  2. Read the source files you may edit to understand the current (buggy) code.
  3. Make the MINIMAL change that fixes the bug. Follow existing patterns.
  4. If you need a new function, add unit tests for it (§31.3).
  5. Run \`just ci\` — ALL gates must be green (fmt, build, clippy, test, deny,
     audit, cov >= 76%). Do NOT weaken or skip any gate.
  6. Run the specific failing test(s) to confirm the fix:
        ./test/run.sh pytest ${test_file} -k "${tests_expr}"
     (requires the E2E stack to be up — skip if not running, but say so)
  7. In BUG_LEDGER.toml, set bug ${BUG_ID} status to "done" and append a one-line
     note describing what you changed.
  8. Commit everything in ONE commit with message:
        ${BUG_ID}: ${description}

HARD CONSTRAINTS:
  - Edit ONLY the files listed in "FILES YOU MAY EDIT". Do NOT touch any other
    source file, test file, or config file.
  - Do NOT modify the test file(s) — the test is the contract.
  - Do NOT use todo!(), unimplemented!(), or unreachable!() in reachable paths.
  - Every public item must have a doc comment.
  - Follow the hot-swappable configuration rule in CLAUDE.md where applicable.

If you genuinely cannot fix this bug (e.g., it requires Stripe/SMTP/B2 that is
not configured), set ${BUG_ID} status to "blocked" with a reason, commit that, and stop.
EOF

# ── Invoke Claude ───────────────────────────────────────────────────────────
model_arg=()
[[ -n "${FIX_MODEL:-}" ]] && model_arg=(--model "$FIX_MODEL")
perm_mode="${PERM_MODE:-bypassPermissions}"

echo "→ invoking Claude for $BUG_ID (model=${FIX_MODEL:-default}, perm=$perm_mode, log: ${log#"$REPO_ROOT"/})"

if timeout "${BUG_TIMEOUT:-2400}" claude -p "$PROMPT" \
      "${model_arg[@]}" \
      --permission-mode "$perm_mode" \
      >"$log" 2>&1; then
  claude_rc=0
else
  claude_rc=$?
  printf '\033[1;33m⚠ claude exited %s for %s\033[0m\n' "$claude_rc" "$BUG_ID"
fi

status="$("$LEDGER" field "$BUG_ID" status 2>/dev/null || echo "unknown")"
echo "DONE   $BUG_ID (claude exit $claude_rc, status=$status) — log: ${log#"$REPO_ROOT"/}"
exit 0
