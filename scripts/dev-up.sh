#!/usr/bin/env bash
# scripts/dev-up.sh — single entrypoint for the whole Local File Knowledge Base.
#
# Starts everything needed for a local dev session in one command:
#   host model servers → socat relays → model watchdog → Podman app stack → health
#
# Usage:
#   bash scripts/dev-up.sh              # start everything (skip image rebuild)
#   bash scripts/dev-up.sh --rebuild    # rebuild the app image first, then start
#   bash scripts/dev-up.sh down         # tear everything down (-v wipe)
#   bash scripts/dev-up.sh status       # show what's running
#
# Also wired as `just up` / `just down` for discoverability.
set -euo pipefail

cd "$(dirname "$0")/.."

# ── config (same defaults as start-models.sh) ───────────────────────────────
LLAMA_BIN="${LLAMA_BIN:-$HOME/.local/bin/llama-server}"
LLAMA_BIN_TEXT="${LLAMA_BIN_TEXT:-$HOME/.local/lib/llama-b9592/llama-server}"
[ -x "$LLAMA_BIN_TEXT" ] || LLAMA_BIN_TEXT="$LLAMA_BIN"
MODEL_DIR="${MODEL_DIR:-./models}"
WHISPER_BIN="${WHISPER_BIN:-${MODEL_DIR}/whisper-server}"
ETTIN_VENV="${ETTIN_VENV:-$HOME/.venvs/ettin-rerank}"

MODEL_TEXT="${MODEL_TEXT:-Qwen3.6-35B-A3B-Uncensored-HauhauCS-Aggressive-Q6_K_P.gguf}"
MODEL_MMPROJ="${MODEL_MMPROJ:-mmproj-Qwen3.6-35B-A3B.gguf}"
MODEL_EMBED="${MODEL_EMBED:-Qwen3-Embedding-4B-Q4_K_M.gguf}"
MODEL_WHISPER="${MODEL_WHISPER:-ggml-large-v3-turbo.bin}"

APP_IMAGE="${APP_IMAGE:-kb:latest}"
ENV_FILE="./test/.env"
# fall back to .env.example if the real .env doesn't exist yet
[ -f "$ENV_FILE" ] || ENV_FILE="./test/.env.example"
CONFIG_FILE="./test/config.e2e.host.toml"
HEALTH_URL="${HEALTH_URL:-https://localhost:9443}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-600}"

COMPOSE=(podman compose --env-file "$ENV_FILE" -f compose.yaml -f test/compose.e2e.host.yaml)

# ── helpers ─────────────────────────────────────────────────────────────────
_listening() { ss -tlnH "( sport = :$1 )" 2>/dev/null | grep -q LISTEN; }
_proc_running() { pgrep -f "$1" >/dev/null 2>&1; }
_red()    { echo -e "\033[31m$*\033[0m"; }
_green()  { echo -e "\033[32m$*\033[0m"; }
_yellow() { echo -e "\033[33m$*\033[0m"; }
_bold()   { echo -e "\033[1m$*\033[0m"; }

# ── auto-bootstrap missing prerequisites ────────────────────────────────────
bootstrap() {
  echo "$(_bold "── Checking prerequisites ──")"
  local needed=0

  # socat — needed for container→host relays
  if ! command -v socat >/dev/null 2>&1; then
    _yellow "  ! socat not found — installing…"
    sudo dnf install -y socat || { _red "  ✗ failed to install socat"; ((needed++)); }
  fi

  # llama-server binary (embeddings server uses this)
  if [ ! -x "$LLAMA_BIN" ]; then
    _red "  ✗ llama-server not found at $LLAMA_BIN"
    _red "    Install llama.cpp and set LLAMA_BIN, or run scripts/bootstrap-dev.sh"
    ((needed++))
  fi
  if [ ! -x "$LLAMA_BIN_TEXT" ]; then
    _red "  ✗ text llama-server (b9592) not found at $LLAMA_BIN_TEXT"
    _red "    The Qwen3.6-35B model needs a newer llama.cpp build (b9592+)."
    _red "    Set LLAMA_BIN_TEXT to the path, or install it."
    ((needed++))
  fi

  # model files
  for m in "$MODEL_TEXT" "$MODEL_MMPROJ" "$MODEL_EMBED"; do
    if [ ! -f "$MODEL_DIR/$m" ]; then
      _red "  ✗ model file missing: $MODEL_DIR/$m"
      ((needed++))
    fi
  done

  # ettin reranker venv
  if [ ! -x "$ETTIN_VENV/bin/python" ]; then
    _yellow "  ! ettin reranker venv not found — creating at $ETTIN_VENV …"
    python3 -m venv "$ETTIN_VENV" || { _red "  ✗ venv creation failed"; ((needed++)); }
    if [ -x "$ETTIN_VENV/bin/python" ]; then
      "$ETTIN_VENV/bin/pip" install -qU pip
      "$ETTIN_VENV/bin/pip" install -q torch --index-url https://download.pytorch.org/whl/cpu
      "$ETTIN_VENV/bin/pip" install -q sentence-transformers fastapi uvicorn
      _green "  ✓ ettin reranker venv ready"
    fi
  fi

  # whisper.cpp server (optional — only needed for audio/video ingestion)
  if [ ! -x "$WHISPER_BIN" ]; then
    _yellow "  ! whisper-server binary not found — building whisper.cpp into /tmp …"
    if [ ! -f /tmp/whisper.cpp/CMakeLists.txt ]; then
      git clone https://github.com/ggml-org/whisper.cpp /tmp/whisper.cpp
    fi
    cmake -S /tmp/whisper.cpp -B /tmp/whisper.cpp/build \
      -DGGML_NATIVE=ON -DGGML_VULKAN=ON -DWHISPER_BUILD_TESTS=OFF
    cmake --build /tmp/whisper.cpp/build -j"$(nproc)" --target whisper-server
    mkdir -p "$MODEL_DIR"
    cp /tmp/whisper.cpp/build/bin/whisper-server "$WHISPER_BIN"
    _green "  ✓ whisper-server cached to $WHISPER_BIN"
  fi

  if [ ! -f "$MODEL_DIR/$MODEL_WHISPER" ]; then
    _yellow "  ! whisper model not found: $MODEL_DIR/$MODEL_WHISPER (audio/video ingestion won't work)"
  fi

  if [ "$needed" -gt 0 ]; then
    _red "  ✗ $needed prerequisite(s) missing — fix before starting"
    return 1
  fi
  _green "  ✓ all prerequisites present"
  echo
}

# ── start ───────────────────────────────────────────────────────────────────
do_start() {
  local rebuild=false
  for arg in "$@"; do
    case "$arg" in
      --rebuild) rebuild=true ;;
    esac
  done

  echo
  _bold "=== Local KB — startup ==="
  echo

  # ── 1. prerequisites + auto-bootstrap ─────────────────────────────────────
  bootstrap || return 1

  # ── 2. host model servers ─────────────────────────────────────────────────
  echo "$(_bold "── Starting host model servers ──")"
  bash scripts/start-models.sh start --no-fan --no-whisper
  echo

  # ── 3. model watchdog ─────────────────────────────────────────────────────
  echo "$(_bold "── Starting model watchdog ──")"
  if _proc_running "scripts/model-watchdog.sh"; then
    _green "  ✓ watchdog already running"
  else
    nohup bash scripts/model-watchdog.sh >>/tmp/model-watchdog.log 2>&1 &
    disown "$!" 2>/dev/null || true
    _green "  ▶ watchdog started (pid $!)"
  fi
  echo

  # ── 4. optional image rebuild ─────────────────────────────────────────────
  if [ "$rebuild" = true ]; then
    echo "$(_bold "── Rebuilding app image ──")"
    local ignore_args=()
    [ -f test/.containerignore.e2e ] && ignore_args=(--ignorefile test/.containerignore.e2e)
    podman build "${ignore_args[@]}" -t "$APP_IMAGE" -f Containerfile .
    echo
  fi

  # ── 5. compose stack ──────────────────────────────────────────────────────
  echo "$(_bold "── Starting Podman stack (postgres, tika, app, caddy) ──")"
  "${COMPOSE[@]}" up -d --force-recreate
  echo

  # ── 6. wait for health ────────────────────────────────────────────────────
  echo "$(_bold "── Waiting for app health (timeout ${HEALTH_TIMEOUT}s) ──")"
  local deadline
  deadline=$(($(date +%s) + HEALTH_TIMEOUT))
  until curl -ksf "$HEALTH_URL/health" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      _red "  ✗ app did not become healthy within ${HEALTH_TIMEOUT}s"
      "${COMPOSE[@]}" ps || true
      "${COMPOSE[@]}" logs --tail=30 app || true
      echo
      do_status
      return 1
    fi
    sleep 5
  done
  _green "  ✓ app healthy at $HEALTH_URL"
  echo

  # ── 7. summary ────────────────────────────────────────────────────────────
  _bold "=== All systems ready ==="
  do_status
  echo
  echo "  Open: $HEALTH_URL"
  echo "  Login: admin@local.kb / admin"
}

# ── down ────────────────────────────────────────────────────────────────────
do_down() {
  echo "$(_bold "=== Local KB — teardown ===")"
  echo

  echo "$(_bold "── Stopping Podman stack ──")"
  "${COMPOSE[@]}" down -v 2>/dev/null || true
  echo

  echo "$(_bold "── Stopping model watchdog ──")"
  pkill -f "scripts/model-watchdog.sh" 2>/dev/null && echo "  ✗ stopped" || echo "  · not running"

  echo "$(_bold "── Stopping host model servers + relays ──")"
  bash scripts/start-models.sh stop

  echo
  _green "=== Teardown complete ==="
}

# ── status ──────────────────────────────────────────────────────────────────
do_status() {
  echo "── Model servers ──"
  bash scripts/start-models.sh status 2>/dev/null || {
    for p in 8080 8081 8082 8083; do
      if _listening "$p"; then
        _green "  ✓ :$p listening"
      else
        echo "  · :$p not listening"
      fi
    done
  }
  echo "── Watchdog ──"
  if _proc_running "scripts/model-watchdog.sh"; then
    _green "  ✓ watchdog running"
  else
    echo "  · watchdog not running"
  fi
  echo "── Podman containers ──"
  podman ps --format '{{.Names}}\t{{.Status}}' 2>/dev/null | grep -E 'local-kb|kb-' || echo "  · no kb containers running"
  echo "── App health ──"
  if curl -ksf "$HEALTH_URL/health" >/dev/null 2>&1; then
    _green "  ✓ $HEALTH_URL/health → 200"
  else
    echo "  · $HEALTH_URL not reachable"
  fi
}

# ── dispatch ────────────────────────────────────────────────────────────────
case "${1:-start}" in
  start|up)   [ $# -gt 0 ] && shift; do_start "$@" ;;
  # Bare-flag form documented in the header and wired as `just up --rebuild`:
  # the flag goes straight to do_start without a leading start/up word.
  --rebuild)  do_start "$@" ;;
  down|stop)  do_down ;;
  status)     do_status ;; 
  --help|-h|help)
    echo "Usage: bash scripts/dev-up.sh {start|up|down|stop|status} [--rebuild]"
    ;;
  *)
    echo "Usage: bash scripts/dev-up.sh {start|up|down|stop|status} [--rebuild]"
    exit 1
    ;;
esac
