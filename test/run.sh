#!/usr/bin/env bash
# test/run.sh — single-command, isolated, full-stack E2E harness for the
# Local File Knowledge Base. Builds the app fresh (no cache), stands up the real
# model stack from ./models, waits for health, then runs the black-box suite.
#
#   ./test/run.sh             full run: cargo build -> image -> stack -> pytest -> teardown
#   ./test/run.sh setup       one-time: create venv, install deps + Chromium
#   ./test/run.sh up          build + start the stack and wait for health (no tests)
#   ./test/run.sh down        tear the stack down (-v)
#   ./test/run.sh quality     full run INCLUDING the DeepSeek-judge lane (excludes perf)
#   ./test/run.sh perf        full run of ONLY the performance/load lane
#   ./test/run.sh pytest ...  run pytest against an already-up stack (args passed through)
#
# Env knobs (test/.env): E2E_HW=gpu|cpu, E2E_KEEP=1 (leave stack up on exit),
# STACK_HEALTH_TIMEOUT, BASE_URL, model-file names, DEEPSEEK_*.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Load env (the script reads a few vars directly; compose gets the same via --env-file).
ENV_FILE="$SCRIPT_DIR/.env"
[ -f "$ENV_FILE" ] || ENV_FILE="$SCRIPT_DIR/.env.example"
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

E2E_HW="${E2E_HW:-gpu}"
# Inference source. "host" reuses the operator's host-native llama servers via
# socat relays (scripts/host_relays.sh) — no inference containers. "container"
# runs the containerized llama-* stack (compose.e2e.yaml [+ .cpu]).
E2E_LLAMA="${E2E_LLAMA:-container}"
STACK_HEALTH_TIMEOUT="${STACK_HEALTH_TIMEOUT:-900}"
BASE_URL="${BASE_URL:-https://localhost:9443}"
APP_IMAGE="${APP_IMAGE:-kb:latest}"
VENV="$SCRIPT_DIR/.venv"
PY="$VENV/bin/python"
# App config + compose overlay are chosen by the inference mode. run.sh OWNS
# CONFIG_FILE (forces the matching file) so it can't drift from E2E_LLAMA.
if [ "$E2E_LLAMA" = "host" ]; then
  export CONFIG_FILE="./test/config.e2e.host.toml"
  COMPOSE=(podman compose --env-file "$ENV_FILE" -f compose.yaml -f test/compose.e2e.host.yaml)
else
  export CONFIG_FILE="./test/config.e2e.toml"
  COMPOSE=(podman compose --env-file "$ENV_FILE" -f compose.yaml -f test/compose.e2e.yaml)
  [ "$E2E_HW" = "cpu" ] && COMPOSE+=(-f test/compose.e2e.cpu.yaml)
fi

# Model files that must exist under ./models.
EXPECTED_MODELS=(
  "${LLAMA_TEXT_MODEL_FILE:-Qwen3VL-30B-A3B-Instruct-Q4_K_M.gguf}"
  "${LLAMA_MMPROJ_FILE:-mmproj-Qwen3VL-30B-A3B-Instruct-F16.gguf}"
  "${LLAMA_EMBED_MODEL_FILE:-bge-m3-q8_0.gguf}"
  "${LLAMA_RERANK_MODEL_FILE:-bge-reranker-v2-m3-q8_0.gguf}"
  "${WHISPER_MODEL_FILE:-ggml-large-v3-turbo.bin}"
)

log() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
die() { printf '\n\033[1;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

preflight_models() {
  log "Preflight: checking ./models"
  [ -d models ] || die "No ./models directory at $REPO_ROOT/models"
  local missing=0
  for m in "${EXPECTED_MODELS[@]}"; do
    if [ -f "models/$m" ]; then printf '  ✓ %s\n' "$m"; else printf '  ✗ MISSING models/%s\n' "$m"; missing=1; fi
  done
  [ "$missing" -eq 0 ] || die "Missing model file(s). Fix the *_MODEL_FILE names in test/.env or add the files."
}

ensure_venv() { [ -x "$PY" ] || die "venv missing — run: ./test/run.sh setup"; }

build_image() {
  log "Building app image fresh (--no-cache): $APP_IMAGE"
  # Use an e2e-specific ignore file so the ~21GB models/ dir (+ other large,
  # build-irrelevant paths) never enter the build context — pulling them in makes
  # podman assemble a >20GB blob on the RAM-backed /tmp tmpfs and OOMs the host.
  # Falls back to the repo default .containerignore if the file is absent.
  local ignore_args=()
  [ -f "$SCRIPT_DIR/.containerignore.e2e" ] && ignore_args=(--ignorefile "$SCRIPT_DIR/.containerignore.e2e")
  podman build --no-cache "${ignore_args[@]}" -t "$APP_IMAGE" -f Containerfile .
}

stack_up() {
  if [ "$E2E_LLAMA" = "host" ]; then
    log "Ensuring host-llama relays (reuse host-native servers, no inference containers)"
    bash "$SCRIPT_DIR/scripts/host_relays.sh" start \
      || die "host-llama relays not up — is socat installed and are the host servers listening on 127.0.0.1:8080/8081/8082?"
    log "Starting stack (host-llama): postgres, tika, app, caddy"
  else
    preflight_models
    log "Starting stack ($E2E_HW): postgres, tika, app, caddy + llama-text/embed/rerank + whisper"
  fi
  "${COMPOSE[@]}" up -d
}

stack_down() {
  if [ "${E2E_KEEP:-0}" = "1" ]; then
    log "E2E_KEEP=1 — leaving the stack up. Tear down later with: ./test/run.sh down"
    return 0
  fi
  log "Tearing down the stack"
  "${COMPOSE[@]}" down -v || true
}

wait_healthy() {
  log "Waiting for app /health at $BASE_URL (timeout ${STACK_HEALTH_TIMEOUT}s — large models load slowly)"
  local deadline=$(( $(date +%s) + STACK_HEALTH_TIMEOUT ))
  until curl -ksf "$BASE_URL/health" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      "${COMPOSE[@]}" ps || true
      "${COMPOSE[@]}" logs --tail=40 app llama-text llama-embed llama-rerank || true
      die "Stack did not become healthy within ${STACK_HEALTH_TIMEOUT}s"
    fi
    sleep 5
  done
  log "Stack healthy ✓"
}

run_pytest() {
  ensure_venv
  log "pytest $*"
  ( cd "$SCRIPT_DIR" && BASE_URL="$BASE_URL" "$PY" -m pytest "$@" )
}

cmd_setup() {
  log "Creating venv at $VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --upgrade pip >/dev/null
  "$VENV/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
  log "Installing Playwright Chromium"
  "$VENV/bin/playwright" install chromium
  log "Setup complete → run: ./test/run.sh"
}

SUBCMD="${1:-run}"
case "$SUBCMD" in
  setup)   cmd_setup ;;
  up)      build_image; stack_up; wait_healthy ;;
  down)    "${COMPOSE[@]}" down -v ;;
  pytest)  shift; run_pytest "$@" ;;
  quality)
    trap stack_down EXIT
    log "cargo build (compile gate)"; cargo build --workspace
    build_image; stack_up; wait_healthy
    export E2E_RECORD=1
    run_pytest tests -m "not perf"
    ;;
  run)
    trap stack_down EXIT
    log "cargo build (compile gate)"; cargo build --workspace
    build_image; stack_up; wait_healthy
    export E2E_RECORD=1
    run_pytest tests -m "not judge and not perf"
    ;;
  perf)
    trap stack_down EXIT
    log "cargo build (compile gate)"; cargo build --workspace
    build_image; stack_up; wait_healthy
    export E2E_RECORD=1
    run_pytest tests -m perf
    ;;
  *) die "Unknown subcommand '$SUBCMD' (use: run | setup | up | down | quality | perf | pytest)" ;;
esac
