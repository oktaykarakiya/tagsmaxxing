#!/usr/bin/env bash
#
# build-loop.sh — the autonomous, ledger-driven build loop (plan §31.1).
#
# It does NOT write code. Each iteration it: (1) asks the ledger for the next eligible task,
# (2) invokes Claude Code headless to implement THAT ONE task with tests + commit, then
# (3) INDEPENDENTLY re-runs `just ci` and confirms the task flipped to `done` and a commit was
# made. It stops immediately on a red gate, a blocked task, an unapproved checkpoint, or phase
# completion — so partial progress is always safe and resumable.
#
# Usage:
#   just loop                          # run until checkpoint / blocked / complete
#   just loop --approve P1-T1          # pre-approve specific checkpoint(s) (comma-separated)
#   just loop --max 3                  # stop after 3 successful tasks
#   just loop --dry-run                # show what it would do; invoke nothing
#   just loop --yolo                   # bypass permission prompts (UNATTENDED; see warning)
#
# Env knobs:
#   CLAUDE_MODEL   (default: opus)         model for each task
#   PERM_MODE      (default: acceptEdits)  claude --permission-mode (overridden by --yolo)
#   TASK_TIMEOUT   (default: 1800)         seconds per task before abort
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
STATE_DIR="$ROOT/.build-loop"
LOG_DIR="$STATE_DIR/logs"
mkdir -p "$LOG_DIR"

# ── args / config ─────────────────────────────────────────────────────────────
APPROVED=""
MAX_TASKS=0          # 0 = unlimited
DRY_RUN=0
CLAUDE_MODEL="${CLAUDE_MODEL:-opus}"
PERM_MODE="${PERM_MODE:-acceptEdits}"
TASK_TIMEOUT="${TASK_TIMEOUT:-1800}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --approve) APPROVED="$2"; shift 2 ;;
    --max)     MAX_TASKS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --yolo)    PERM_MODE="bypassPermissions"; shift ;;
    --model)   CLAUDE_MODEL="$2"; shift 2 ;;
    *) echo "build-loop: unknown arg: $1" >&2; exit 2 ;;
  esac
done

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓ %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

is_approved() { [[ ",$APPROVED," == *",$1,"* ]]; }
ledger() { python3 scripts/ledger.py "$@"; }

# ── preconditions ───────────────────────────────────────────────────────────
for tool in cargo just python3 git claude; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not on PATH: $tool"
done
[[ -f BUILD_LEDGER.toml ]] || die "BUILD_LEDGER.toml not found (run from repo root)"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git repository"
if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is dirty — commit or stash first (the loop verifies clean per-task commits)"
fi

say "Autonomous build loop  (model=$CLAUDE_MODEL  perm=$PERM_MODE  approved=[${APPROVED:-none}])"
[[ "$PERM_MODE" == "bypassPermissions" ]] && \
  printf '\033[1;33m⚠ --yolo: permission prompts are bypassed; Claude can run commands unattended.\033[0m\n'

# ── loop ──────────────────────────────────────────────────────────────────────
completed=0
while :; do
  next="$(ledger next)"
  case "$next" in
    PHASE_COMPLETE) ok "No eligible tasks remain — phase complete. Generate the next phase's ledger, review, then re-run."; exit 0 ;;
    BLOCKED)        ledger status; die "Tasks remain but none are unblocked (blocked/in_progress or dependency deadlock)." ;;
  esac
  TASK_ID="$next"

  if [[ "$(ledger is-checkpoint "$TASK_ID")" == "true" ]] && ! is_approved "$TASK_ID"; then
    say "CHECKPOINT — human review required before $TASK_ID"
    echo "  $(ledger field "$TASK_ID" description)"
    echo "  Reason: $(ledger field "$TASK_ID" notes)"
    echo
    echo "  Review the work so far, then resume with:  just loop --approve $TASK_ID"
    exit 0
  fi

  say "TASK $TASK_ID"
  echo "  $(ledger field "$TASK_ID" description)"
  echo "  plan sections: $(ledger field "$TASK_ID" plan_sections)"

  if [[ "$DRY_RUN" == "1" ]]; then ok "(dry-run) would implement $TASK_ID"; completed=$((completed+1));
    [[ "$MAX_TASKS" -gt 0 && "$completed" -ge "$MAX_TASKS" ]] && { ok "reached --max $MAX_TASKS"; exit 0; }
    # dry-run can't actually advance the ledger; stop to avoid an infinite loop.
    ok "(dry-run) stopping after showing the next task"; exit 0
  fi

  before_head="$(git rev-parse HEAD)"
  log="$LOG_DIR/$TASK_ID.$(date +%Y%m%d-%H%M%S).log"

  read -r -d '' PROMPT <<EOF || true
Continue the autonomous build per BUILD_LEDGER.toml, §31 of local-kb-plan.md, and CLAUDE.md.
Implement EXACTLY ONE task: ${TASK_ID}.
Steps:
 1. Read that task's plan_sections in local-kb-plan.md and the existing code it touches.
 2. Implement it WITH unit tests for every function carrying logic (§31.3), obeying the
    modularity / file-size / no-unwrap / no-stub rules (§31.4, §31.2). Verify any new crate
    versions/APIs against current sources before using them (§31.6).
 3. Run \`just ci\` and fix until ALL gates are green. Do not weaken or skip a gate.
 4. In BUILD_LEDGER.toml set task ${TASK_ID} status to "done" and append a one-line note.
 5. Commit everything in ONE commit whose message begins with "${TASK_ID}: " (§31.6).
Do NOT implement any other task. If you genuinely cannot finish, set ${TASK_ID} status to
"blocked" with a reason, commit that, and stop.
EOF

  say "→ invoking Claude (log: ${log#$ROOT/})"
  if ! timeout "$TASK_TIMEOUT" claude -p "$PROMPT" \
        --model "$CLAUDE_MODEL" \
        --permission-mode "$PERM_MODE" \
        2>&1 | tee "$log"; then
    die "Claude invocation for $TASK_ID failed or timed out (see $log)."
  fi

  # ── independent verification (don't trust "should pass") ────────────────────
  say "verifying gates for $TASK_ID"
  just ci 2>&1 | tee "$log.ci" | tail -5 || die "GATES RED after $TASK_ID — stopping (see $log.ci)."

  status="$(ledger field "$TASK_ID" status)"
  [[ "$status" == "done" ]] || die "$TASK_ID is '$status', not 'done' — agent did not complete it. Stopping."

  after_head="$(git rev-parse HEAD)"
  [[ "$after_head" != "$before_head" ]] || die "no new commit was created for $TASK_ID. Stopping."
  git log --oneline -1 | grep -q "$TASK_ID" || printf '\033[1;33m⚠ latest commit subject does not mention %s\033[0m\n' "$TASK_ID"

  ok "$TASK_ID done, gates green, committed ($(git rev-parse --short "$after_head"))"
  completed=$((completed+1))
  if [[ "$MAX_TASKS" -gt 0 && "$completed" -ge "$MAX_TASKS" ]]; then
    ok "reached --max $MAX_TASKS — stopping."; exit 0
  fi
done
