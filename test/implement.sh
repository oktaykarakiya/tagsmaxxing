#!/usr/bin/env bash
# test/implement.sh — drive the test catalog to completion with a swarm of
# headless Claude Code agents, up to N in parallel (default 10).
#
#   ./test/implement.sh status            show implemented / pending / blocked per file
#   ./test/implement.sh list              list every test with its status
#   ./test/implement.sh run [--dry-run]   spawn one agent per file with pending tests,
#                                         <=MAX_AGENTS in parallel; already-implemented
#                                         tests/files are skipped automatically
#   ./test/implement.sh                   alias for `run`
#
# Each agent owns exactly ONE catalog file (no concurrent edits to the same file)
# and implements all of that file's pending tests. Status is derived live from the
# code (AST), so the swarm is idempotent: re-run it until everything is DONE.
#
# Env: MAX_AGENTS (default 10), IMPLEMENT_MODEL (optional, e.g. sonnet for cheaper
#      bulk runs), AGENT_TIMEOUT (per agent, default 2400s), BASE_URL.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # test/
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

STATUS="$SCRIPT_DIR/scripts/catalog_status.py"
ONE="$SCRIPT_DIR/scripts/implement_one.sh"
MAX_AGENTS="${MAX_AGENTS:-10}"
ENV_FILE="$SCRIPT_DIR/.env"; [ -f "$ENV_FILE" ] || ENV_FILE="$SCRIPT_DIR/.env.example"
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
BASE_URL="${BASE_URL:-https://localhost:9443}"

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m⚠ %s\033[0m\n' "$*"; }

MODE="${1:-run}"
case "$MODE" in
  status) exec python3 "$STATUS" summary ;;
  list)   exec python3 "$STATUS" list ;;
  run)    shift || true ;;
  *) echo "usage: ./test/implement.sh [status|list|run] [--dry-run]" >&2; exit 2 ;;
esac

DRY=0; [ "${1:-}" = "--dry-run" ] && DRY=1

log "Catalog status (before)"
python3 "$STATUS" summary

mapfile -t FILES < <(python3 "$STATUS" pending-files)
if [ "${#FILES[@]}" -eq 0 ]; then
  log "Nothing pending — every catalog test is implemented or blocked. 🎉"
  exit 0
fi

log "${#FILES[@]} file(s) have pending tests; dispatching up to $MAX_AGENTS agents in parallel:"
for f in "${FILES[@]}"; do printf '  %s\n' "${f#"$REPO_ROOT"/}"; done

if [ "$DRY" -eq 1 ]; then
  warn "--dry-run: not spawning any agents."
  exit 0
fi

# Preflight (non-fatal): verifying tests needs the venv + a healthy stack.
[ -x "$SCRIPT_DIR/.venv/bin/python" ] || warn "venv missing — agents can't run tests. Run ./test/run.sh setup first."
if curl -ksf "$BASE_URL/health" >/dev/null 2>&1; then
  log "Stack healthy at $BASE_URL — agents will verify by running their tests."
else
  warn "Stack not healthy at $BASE_URL — agents will implement WITHOUT running. Bring it up: ./test/run.sh up"
fi

command -v claude >/dev/null 2>&1 || { warn "'claude' CLI not on PATH — cannot spawn agents."; exit 1; }

log "Working… (per-file logs in test/.implement-logs/)"
printf '%s\0' "${FILES[@]}" | xargs -0 -P "$MAX_AGENTS" -n1 "$ONE"

log "Catalog status (after)"
python3 "$STATUS" summary
log "Done. Re-run ./test/implement.sh to pick up anything still pending."
