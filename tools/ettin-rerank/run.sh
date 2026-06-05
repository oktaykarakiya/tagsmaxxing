#!/usr/bin/env bash
# Persistent, self-restarting launcher for the ettin rerank sidecar (server.py).
#
#   PORT=8082 nohup bash tools/ettin-rerank/run.sh >/dev/null 2>&1 & disown
#
# Restarts the server if it ever exits, so the rerank backend stays up across the
# unattended window. Logs to ${ETTIN_LOG:-/tmp/ettin-rerank-$PORT.log}.
set -u
PORT="${PORT:-8082}"
VENV="${ETTIN_VENV:-$HOME/.venvs/ettin-rerank}"
LOG="${ETTIN_LOG:-/tmp/ettin-rerank-$PORT.log}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PORT
echo "[wrapper] starting ettin sidecar on :$PORT (venv=$VENV) $(date -Is)" >>"$LOG"
while true; do
  "$VENV/bin/python" "$HERE/server.py" >>"$LOG" 2>&1
  echo "[wrapper] server exited (code $?) at $(date -Is); restart in 2s" >>"$LOG"
  sleep 2
done
