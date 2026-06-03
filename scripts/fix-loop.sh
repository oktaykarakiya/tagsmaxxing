#!/usr/bin/env bash
#
# fix-loop.sh — autonomous bug-fix pipeline for the Local File Knowledge Base.
#
# Drives BUG_LEDGER.toml: picks eligible bugs, forms conflict-free batches,
# spawns parallel Claude Code instances (one per bug in a batch), independently
# verifies each fix, re-runs the full E2E suite after each batch, and compares
# against a pre-fix baseline to catch regressions.
#
# Modeled on build-loop.sh (§31.1) but adapted for bug fixing:
#   - Parallel within a batch (different conflict groups = disjoint files)
#   - Serial across batches (full E2E re-run between batches)
#   - Baseline → interim → final comparison via compare_results.py
#
# Usage:
#   ./scripts/fix-loop.sh                          # run until done / blocked
#   ./scripts/fix-loop.sh --max-batches 3          # stop after 3 batches
#   ./scripts/fix-loop.sh --batch-size 4           # max 4 parallel fixes
#   ./scripts/fix-loop.sh --dry-run                # show what it would do
#   ./scripts/fix-loop.sh --yolo                   # bypass permission prompts
#   ./scripts/fix-loop.sh --skip-e2e               # skip full E2E re-run (faster)
#   ./scripts/fix-loop.sh --model opus --effort xhigh
#
# Env knobs:
#   FIX_MODEL     (default: claude-opus-4-8)  model for each bug fix
#   EFFORT         (default: xhigh)            reasoning effort
#   PERM_MODE      (default: acceptEdits)      claude --permission-mode
#   BUG_TIMEOUT    (default: 2400)             seconds per bug before abort
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
STATE_DIR="$ROOT/.fix-loop"
BASELINE_DIR="$STATE_DIR/baseline"
INTERIM_DIR="$STATE_DIR/interim"
FINAL_DIR="$STATE_DIR/final"
LOG_DIR="$STATE_DIR/logs"
mkdir -p "$BASELINE_DIR" "$INTERIM_DIR" "$LOG_DIR"

# ── args / config ───────────────────────────────────────────────────────────
MAX_BATCHES=0        # 0 = unlimited
BATCH_SIZE=4         # max parallel fixes per batch
DRY_RUN=0
SKIP_E2E=0
FIX_MODEL="${FIX_MODEL:-claude-opus-4-8}"
EFFORT="${EFFORT:-xhigh}"
PERM_MODE="${PERM_MODE:-acceptEdits}"
BUG_TIMEOUT="${BUG_TIMEOUT:-2400}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-batches) MAX_BATCHES="$2"; shift 2 ;;
    --batch-size)  BATCH_SIZE="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=1; shift ;;
    --yolo)        export PERM_MODE="bypassPermissions"; shift ;;
    --skip-e2e)    SKIP_E2E=1; shift ;;
    --model)       FIX_MODEL="$2"; shift 2 ;;
    --effort)      EFFORT="$2"; shift 2 ;;
    *) echo "fix-loop: unknown arg: $1" >&2; exit 2 ;;
  esac
done

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m⚠ %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

ledger() { python3 scripts/bug_ledger.py "$@"; }

# ── preconditions ───────────────────────────────────────────────────────────
for tool in cargo just python3 git claude; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not on PATH: $tool"
done
[[ -f BUG_LEDGER.toml ]] || die "BUG_LEDGER.toml not found (run from repo root)"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git repository"
if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is dirty — commit or stash first"
fi

say "Autonomous bug-fix loop  (model=$FIX_MODEL  effort=$EFFORT  perm=$PERM_MODE  batch-size=$BATCH_SIZE)"
[[ "$PERM_MODE" == "bypassPermissions" ]] && \
  printf '\033[1;33m⚠ --yolo: permission prompts are bypassed.\033[0m\n'

# ── Phase 0: capture baseline ───────────────────────────────────────────────
BASELINE_CSV="$BASELINE_DIR/history.csv"
if [[ ! -f "$BASELINE_CSV" ]]; then
  say "Phase 0 — capturing baseline E2E results"
  if [[ "$DRY_RUN" == "1" ]]; then
    ok "(dry-run) would capture baseline"
  else
    # Run non-judge, non-perf tests to establish the baseline
    E2E_RECORD=1 E2E_RESULTS_DIR="$BASELINE_DIR" \
      ./test/run.sh pytest tests/ -m "not judge and not perf" --timeout=120 2>&1 | tail -20 || true
    if [[ -f "$BASELINE_CSV" ]]; then
      n_tests=$(tail -n +2 "$BASELINE_CSV" | wc -l | tr -d ' ')
      ok "baseline captured: $n_tests test results → $BASELINE_CSV"
    else
      warn "baseline capture may have failed — no history.csv produced; continuing anyway"
    fi
  fi
fi

# Also snapshot catalog status
python3 test/scripts/catalog_status.py list > "$BASELINE_DIR/catalog_status.txt" 2>/dev/null || true

# ── Main loop ───────────────────────────────────────────────────────────────
batch_num=0
total_fixed=0

while :; do
  # Get the next batch of eligible bugs (different conflict groups)
  batch="$(ledger next-batch "$BATCH_SIZE")"
  case "$batch" in
    ALL_DONE)
      ok "All bugs processed — no todo bugs remain."
      break
      ;;
    BLOCKED)
      warn "Bugs remain but none are unblocked. Check dependency chains:"
      ledger status
      break
      ;;
  esac

  # Parse space-separated bug IDs
  read -ra BUG_IDS <<< "$batch"
  batch_num=$((batch_num + 1))
  say "BATCH $batch_num  (${#BUG_IDS[@]} bugs: ${BUG_IDS[*]})"

  if [[ "$DRY_RUN" == "1" ]]; then
    for bid in "${BUG_IDS[@]}"; do
      echo "  would fix: $bid — $(ledger field "$bid" description)"
    done
    ok "(dry-run) batch $batch_num shown"; continue
  fi

  # ── Fix each bug in this batch (parallel via background jobs) ───────────
  pids=()
  bug_logs=()
  for bid in "${BUG_IDS[@]}"; do
    log="$LOG_DIR/${bid}.$(date +%Y%m%d-%H%M%S).log"
    bug_logs+=("$log")
    say "  → launching fix for $bid"
    timeout "$BUG_TIMEOUT" bash scripts/fix-bug.sh "$bid" >"$log" 2>&1 &
    pids+=($!)
  done

  # Wait for all fixes in this batch
  say "  waiting for ${#pids[@]} fix agents to complete…"
  failed_pids=()
  for i in "${!pids[@]}"; do
    pid="${pids[$i]}"
    bid="${BUG_IDS[$i]}"
    if wait "$pid"; then
      ok "  $bid agent completed successfully"
    else
      warn "  $bid agent exited non-zero (see ${bug_logs[$i]})"
      failed_pids+=("$bid")
    fi
  done

  # ── Independent verification per bug ────────────────────────────────────
  say "verifying batch $batch_num fixes"
  batch_ok=0
  for bid in "${BUG_IDS[@]}"; do
    status="$(ledger field "$bid" status 2>/dev/null || echo "unknown")"
    if [[ "$status" == "done" ]]; then
      ok "  $bid → done"
      batch_ok=$((batch_ok + 1))
      total_fixed=$((total_fixed + 1))
    elif [[ "$status" == "blocked" ]]; then
      warn "  $bid → blocked (agent could not fix)"
    else
      warn "  $bid → status=$status (agent may not have completed)"
    fi
  done

  # ── Run just ci once for the whole batch ────────────────────────────────
  say "running just ci for batch $batch_num"
  if just ci 2>&1 | tail -8; then
    ok "just ci green after batch $batch_num"
  else
    die "GATES RED after batch $batch_num — stopping for investigation."
  fi

  # ── E2E re-run after each batch ─────────────────────────────────────────
  if [[ "$SKIP_E2E" == "0" ]]; then
    say "re-running E2E suite after batch $batch_num"
    batch_dir="$INTERIM_DIR/batch-$(printf '%02d' "$batch_num")"
    mkdir -p "$batch_dir"
    E2E_RECORD=1 E2E_RESULTS_DIR="$batch_dir" \
      ./test/run.sh pytest tests/ -m "not judge and not perf" --timeout=120 2>&1 | tail -20 || true

    # Compare against baseline if we have one
    if [[ -f "$BASELINE_CSV" && -f "$batch_dir/history.csv" ]]; then
      say "comparing batch $batch_num against baseline"
      python3 scripts/compare_results.py \
        --baseline "$BASELINE_CSV" \
        --current "$batch_dir/history.csv" \
        --output "$batch_dir/delta.csv" || {
        warn "REGRESSION DETECTED in batch $batch_num — see $batch_dir/delta.csv"
        python3 scripts/compare_results.py \
          --baseline "$BASELINE_CSV" \
          --current "$batch_dir/history.csv"
        die "Regressions found — stopping. Review the delta report above."
      }
      ok "no regressions detected in batch $batch_num"
    fi
  fi

  # ── Stop conditions ─────────────────────────────────────────────────────
  if [[ "$MAX_BATCHES" -gt 0 && "$batch_num" -ge "$MAX_BATCHES" ]]; then
    ok "reached --max-batches $MAX_BATCHES — stopping."
    break
  fi

  # Check if any more work remains
  next_check="$(ledger next)"
  if [[ "$next_check" == "ALL_DONE" ]]; then
    ok "All bugs done!"
    break
  fi
done

# ── Phase 3: Final comparison ───────────────────────────────────────────────
say "Phase 3 — final comparison"
mkdir -p "$FINAL_DIR"

# Run one final E2E suite
E2E_RECORD=1 E2E_RESULTS_DIR="$FINAL_DIR" \
  ./test/run.sh pytest tests/ -m "not judge and not perf" --timeout=120 2>&1 | tail -20 || true

if [[ -f "$BASELINE_CSV" && -f "$FINAL_DIR/history.csv" ]]; then
  python3 scripts/compare_results.py \
    --baseline "$BASELINE_CSV" \
    --current "$FINAL_DIR/history.csv" \
    --output "$FINAL_DIR/delta.csv" || true
fi

say "Bug-fix loop complete"
echo "  Batches run:    $batch_num"
echo "  Bugs fixed:     $total_fixed"
echo "  Baseline:       $BASELINE_DIR"
echo "  Final results:  $FINAL_DIR"
ledger status
