#!/usr/bin/env bash
# scripts/start-models.sh — start all LLM/inference servers the app requires
#
# Launches (host-native, background):
#   1. llama-server :8080  — Qwen3.6-35B-A3B-VL (text/vision/code)
#   2. llama-server :8081  — Qwen3-Embedding-4B (embeddings, 2560-dim)
#   3. ettin reranker :8082 — torch + sentence-transformers sidecar
#   4. whisper :8083        — whisper.cpp large-v3-turbo (audio/video, optional)
#
# Also:
#   - Sets the CPU/GPU fan to 60% via `sudo ectool fanduty 64`
#   - Starts socat relays (9080→8080, 9081→8081, 9082→8082) so the Podman
#     containers can reach the host-loopback servers
#   - Waits for every server to report healthy before returning
#
# Usage:
#   bash scripts/start-models.sh            # start everything
#   bash scripts/start-models.sh --no-fan   # skip fan control
#   bash scripts/start-models.sh --no-whisper  # skip whisper (faster start)
#   bash scripts/start-models.sh stop       # kill all servers + relays
#   bash scripts/start-models.sh status     # check which servers are listening
set -euo pipefail

cd "$(dirname "$0")/.."

# ── config ──────────────────────────────────────────────────────────────────
LLAMA_BIN="${LLAMA_BIN:-$HOME/.local/bin/llama-server}"
# Text server runs a NEWER llama.cpp than embeddings: the old build (24d2ee0,
# 2026-03-04) predates Qwen3.6 — its BPE pretokenizer lacks this model's
# fast-path and falls back to libstdc++ std::regex, whose backtracking
# recursion overflows the worker-thread stack (SIGSEGV) on long unbroken text
# segments (CSV one-liners, large filler docs). That was "F5". The embed
# server intentionally STAYS on the old binary so Qwen3-Embedding-4B numerics
# remain byte-stable with the 2560-dim vectors already stored in Postgres.
LLAMA_BIN_TEXT="${LLAMA_BIN_TEXT:-$HOME/.local/lib/llama-b9592/llama-server}"
[ -x "$LLAMA_BIN_TEXT" ] || LLAMA_BIN_TEXT="$LLAMA_BIN"
WHISPER_BIN="${WHISPER_BIN:-/tmp/whisper.cpp/build/bin/whisper-server}"
ETTIN_VENV="${ETTIN_VENV:-$HOME/.venvs/ettin-rerank}"
ECTL="${ECTL:-/usr/local/bin/ectool}"
FAN_PCT="${FAN_PCT:-64}"
MODEL_DIR="${MODEL_DIR:-./models}"

MODEL_TEXT="${MODEL_TEXT:-Qwen3.6-35B-A3B-APEX-I-Compact.gguf}"
MODEL_MMPROJ="${MODEL_MMPROJ:-mmproj-Qwen3.6-35B-A3B.gguf}"
MODEL_EMBED="${MODEL_EMBED:-Qwen3-Embedding-4B-Q4_K_M.gguf}"
MODEL_WHISPER="${MODEL_WHISPER:-ggml-large-v3-turbo.bin}"

# ── helpers ─────────────────────────────────────────────────────────────────
_listening() { ss -tlnH "( sport = :$1 )" 2>/dev/null | grep -q LISTEN; }
_red()    { echo -e "\033[31m$*\033[0m"; }
_green()  { echo -e "\033[32m$*\033[0m"; }
_yellow() { echo -e "\033[33m$*\033[0m"; }

# Resolve the host IP on the Podman bridge so socat relays bind only to that
# interface, not 0.0.0.0 (all interfaces). Falls back to 127.0.0.1 if Podman
# isn't running (relays will only work with host-network containers then).
_resolve_bind_ip() {
  local gw
  gw=$(podman network inspect podman --format '{{range .Subnets}}{{.Gateway}}{{end}}' 2>/dev/null || true)
  if [ -n "${gw:-}" ]; then
    echo "$gw"
  else
    # Also check compose networks that might exist
    gw=$(podman network inspect local-kb_default --format '{{range .Subnets}}{{.Gateway}}{{end}}' 2>/dev/null || true)
    echo "${gw:-127.0.0.1}"
  fi
}

_fan_set() {
  if [ -x "$ECTL" ]; then
    echo "  ▶ fan → ${FAN_PCT}% (sudo ectool fanduty $FAN_PCT)"
    sudo "$ECTL" fanduty "$FAN_PCT" 2>/dev/null || _yellow "  ⚠ ectool fanduty failed (non-Framework machine?)"
  else
    _yellow "  ⚠ ectool not found at $ECTL — skip fan control"
  fi
}

# ── stop ────────────────────────────────────────────────────────────────────
stop_all() {
  echo "Stopping all model servers + relays…"
  for p in 8080 8081 8082 8083 9080 9081 9082 9093; do
    local pid
    pid=$(ss -tlnpH "sport = :$p" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2) || true
    if [ -n "${pid:-}" ]; then
      kill "$pid" 2>/dev/null && echo "  ✗ killed :$p (pid $pid)" || true
    else
      echo "  · :$p not listening"
    fi
  done
  echo "Done."
}

# ── status ──────────────────────────────────────────────────────────────────
show_status() {
  echo "Model server status:"
  for p in 8080 8081 8082 8083; do
    if _listening "$p"; then
      _green "  ✓ :$p listening"
    else
      _red  "  ✗ :$p not listening"
    fi
  done
  echo "Relay status:"
  for p in 9080 9081 9082 9093; do
    if _listening "$p"; then
      _green "  ✓ :$p listening"
    else
      echo "  · :$p not listening"
    fi
  done
}

# ── start ───────────────────────────────────────────────────────────────────
start_all() {
  local skip_fan=false skip_whisper=false
  for arg in "$@"; do
    case "$arg" in
      --no-fan) skip_fan=true ;;
      --no-whisper) skip_whisper=true ;;
    esac
  done

  echo "=== Local KB model startup ==="
  echo

  # ── pre-flight ──────────────────────────────────────────────────────────
  echo "── Prerequisites ──"
  errors=0
  [ -x "$LLAMA_BIN" ]   || { _red "  ✗ llama-server not found: $LLAMA_BIN"; ((errors++)); }
  [ -f "$MODEL_DIR/$MODEL_TEXT" ]   || { _red "  ✗ text model missing: $MODEL_DIR/$MODEL_TEXT"; ((errors++)); }
  [ -f "$MODEL_DIR/$MODEL_EMBED" ]  || { _red "  ✗ embed model missing: $MODEL_DIR/$MODEL_EMBED"; ((errors++)); }
  [ -f "$MODEL_DIR/$MODEL_MMPROJ" ] || { _red "  ✗ mmproj missing: $MODEL_DIR/$MODEL_MMPROJ"; ((errors++)); }
  command -v socat >/dev/null       || { _red "  ✗ socat not installed"; ((errors++)); }
  [ -x "$ETTIN_VENV/bin/python" ]   || { _red "  ✗ ettin venv not found: $ETTIN_VENV"; ((errors++)); }

  if [ "$errors" -gt 0 ]; then
    _red "  ✗ $errors prerequisite(s) missing — abort"
    return 1
  fi
  _green "  ✓ all prerequisites present"
  echo

  # ── fan ─────────────────────────────────────────────────────────────────
  if [ "$skip_fan" = false ]; then
    echo "── Fan ──"
    _fan_set
    echo
  fi

  # ── text / vision / code — Qwen3.6-35B-A3B-VL :8080 ─────────────────────
  echo "── Starting :8080 text/vision/code (Qwen3.6-35B-A3B-VL) ──"
  if _listening 8080; then
    _green "  ✓ :8080 already listening (reusing)"
  else
    # $LLAMA_BIN_TEXT (b9592): fixes the F5 SIGSEGV — see the note at the top.
    # Keep the DEFAULT --reasoning-format (extraction): on b9592, leftover think
    # tags would stay in message.content with `--reasoning-format none`,
    # corrupting tagger/caption JSON; the default parser strips them into
    # reasoning_content (which the app ignores). On b9592 `--reasoning-budget 0`
    # alone no longer prevents thinking (the model still burns its max_tokens in
    # the think block) — Qwen3-family templates need enable_thinking=false,
    # which skips the <think> block at the template level like the old build did.
    nohup "$LLAMA_BIN_TEXT" -ngl 99 --temp 0 -c 8192 --host 127.0.0.1 --port 8080 \
      -m "$MODEL_DIR/$MODEL_TEXT" --mmproj "$MODEL_DIR/$MODEL_MMPROJ" \
      --reasoning-budget 0 --chat-template-kwargs '{"enable_thinking":false}' \
      >/tmp/llama-text-8080.log 2>&1 &
    disown "$!" 2>/dev/null || true
    echo "  ▶ started :8080 (pid $!)"
  fi
  echo

  # ── embeddings — Qwen3-Embedding-4B :8081 ────────────────────────────────
  echo "── Starting :8081 embeddings (Qwen3-Embedding-4B) ──"
  if _listening 8081; then
    _green "  ✓ :8081 already listening (reusing)"
  else
    nohup "$LLAMA_BIN" -ngl 99 -c 8192 --host 127.0.0.1 --port 8081 \
      -m "$MODEL_DIR/$MODEL_EMBED" --embedding --pooling last \
      >/tmp/llama-embed-8081.log 2>&1 &
    disown "$!" 2>/dev/null || true
    echo "  ▶ started :8081 (pid $!)"
  fi
  echo

  # ── reranker — ettin sidecar :8082 ───────────────────────────────────────
  echo "── Starting :8082 reranker (ettin-reranker-400m-v1) ──"
  if _listening 8082; then
    _green "  ✓ :8082 already listening (reusing)"
  else
    ETTIN_LOG=/tmp/ettin-rerank-8082.log \
      PORT=8082 nohup bash tools/ettin-rerank/run.sh >/dev/null 2>&1 &
    disown "$!" 2>/dev/null || true
    echo "  ▶ started :8082 (pid $!)"
  fi
  echo

  # ── (optional) transcription — whisper :8083 ─────────────────────────────
  if [ "$skip_whisper" = false ]; then
    echo "── Starting :8083 transcription (whisper large-v3-turbo) ──"
    if [ ! -x "$WHISPER_BIN" ]; then
      if [ -f /tmp/whisper.cpp/CMakeLists.txt ]; then
        _yellow "  ! whisper binary not built — building…"
        cmake -S /tmp/whisper.cpp -B /tmp/whisper.cpp/build -DGGML_NATIVE=ON -DGGML_VULKAN=ON -DWHISPER_BUILD_TESTS=OFF
        cmake --build /tmp/whisper.cpp/build -j"$(nproc)" --target whisper-server
      else
        _yellow "  ! whisper.cpp source not found — clone + build…"
        git clone https://github.com/ggml-org/whisper.cpp /tmp/whisper.cpp
        cmake -S /tmp/whisper.cpp -B /tmp/whisper.cpp/build -DGGML_NATIVE=ON -DGGML_VULKAN=ON -DWHISPER_BUILD_TESTS=OFF
        cmake --build /tmp/whisper.cpp/build -j"$(nproc)" --target whisper-server
      fi
    fi
    if _listening 8083; then
      _green "  ✓ :8083 already listening (reusing)"
    else
      nohup "$WHISPER_BIN" -m "$MODEL_DIR/$MODEL_WHISPER" \
        --host 127.0.0.1 --port 8083 --inference-path /v1/audio/transcriptions -t 6 \
        >/tmp/whisper-8083.log 2>&1 &
      disown "$!" 2>/dev/null || true
      echo "  ▶ started :8083 (pid $!)"
    fi
    echo
  fi

  # ── socat relays ─────────────────────────────────────────────────────────
  echo "── Socat relays (container → host) ──"
  BIND_IP=$(_resolve_bind_ip)
  echo "  (binding relays to Podman bridge: ${BIND_IP})"
  if [ -x test/scripts/host_relays.sh ]; then
    BIND_IP="$BIND_IP" bash test/scripts/host_relays.sh start
  else
    _yellow "  ⚠ host_relays.sh not found — starting relays inline"
    for map in "9080:8080" "9081:8081" "9082:8082"; do
      local lp="${map%%:*}" tp="${map##*:}"
      if _listening "$lp"; then
        _green "  ✓ relay :$lp already listening"
      else
        nohup socat "TCP-LISTEN:${lp},fork,reuseaddr,bind=${BIND_IP}" "TCP:127.0.0.1:${tp}" \
          >/dev/null 2>&1 &
        disown "$!" 2>/dev/null || true
        echo "  ▶ started relay :$lp → 127.0.0.1:$tp (pid $!)"
      fi
    done
  fi
  if [ "$skip_whisper" = false ]; then
    if _listening 9093; then
      _green "  ✓ whisper relay :9093 already listening"
    else
      nohup socat "TCP-LISTEN:9093,fork,reuseaddr,bind=${BIND_IP}" "TCP:127.0.0.1:8083" \
        >/dev/null 2>&1 &
      disown "$!" 2>/dev/null || true
      echo "  ▶ started whisper relay :9093 → 127.0.0.1:8083 (pid $!)"
    fi
  fi
  echo

  # ── wait for readiness ───────────────────────────────────────────────────
  echo "── Waiting for models to load into memory ──"
  MAX_WAIT=300  # up to 5 min for the 35B model
  STARTED=$SECONDS
  for p in 8080 8081 8082; do
    echo -n "  :$p … "
    until curl -sf --max-time 3 "http://127.0.0.1:$p/health" >/dev/null 2>&1; do
      elapsed=$((SECONDS - STARTED))
      if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        _red "TIMEOUT (${elapsed}s)"
        echo "  ✗ :$p did not become healthy — check /tmp/llama-*-$p.log"
        exit 1
      fi
      sleep 3
    done
    elapsed=$((SECONDS - STARTED))
    _green "ready (${elapsed}s)"
  done

  if [ "$skip_whisper" = false ] && _listening 8083; then
    echo -n "  :8083 … "
    whisper_ok=true
    until curl -sf --max-time 3 "http://127.0.0.1:8083/health" >/dev/null 2>&1; do
      elapsed=$((SECONDS - STARTED))
      if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        _red "TIMEOUT"
        echo "  ⚠ :8083 did not become healthy — check /tmp/whisper-8083.log"
        whisper_ok=false
        break
      fi
      sleep 2
    done
    if [ "$whisper_ok" = true ]; then
      elapsed=$((SECONDS - STARTED))
      _green "ready (${elapsed}s)"
    fi
  fi

  echo
  echo "=== All models ready ==="
  show_status
}

# ── dispatch ────────────────────────────────────────────────────────────────
case "${1:-start}" in
  start)   shift; start_all "$@" ;;
  stop)    stop_all ;;
  status)  show_status ;;
  *)
    echo "Usage: $0 {start|stop|status} [--no-fan] [--no-whisper]"
    exit 1
    ;;
esac
