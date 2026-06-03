#!/usr/bin/env bash
# test/scripts/host_relays.sh — manage host-side socat relays for REUSE-HOST-LLAMA
# mode (E2E_LLAMA=host). The operator's llama.cpp servers listen on the host
# LOOPBACK (127.0.0.1:8080/8081/8082), which a containerized app on a podman
# bridge network cannot reach. These relays expose them on 0.0.0.0:9080/9081/9082
# so the app can reach them via host.containers.internal — WITHOUT restarting or
# touching the operator's servers.
#
#   host_relays.sh start    # idempotent: start any relay not already listening
#   host_relays.sh stop     # stop relays we started (by pidfile)
#   host_relays.sh status   # report which relay ports are listening
#
# Relay map: <listen_port>:<target_loopback_port>. Override targets via env
# (LLAMA_TEXT_HOST_PORT etc.) if the host servers move.
set -uo pipefail

TEXT_SRC="${LLAMA_TEXT_HOST_PORT:-8080}"
EMBED_SRC="${LLAMA_EMBED_HOST_PORT:-8081}"
RERANK_SRC="${LLAMA_RERANK_HOST_PORT:-8082}"
RELAYS=("9080:${TEXT_SRC}" "9081:${EMBED_SRC}" "9082:${RERANK_SRC}")

PIDFILE="${E2E_RELAY_PIDFILE:-${TMPDIR:-/tmp}/kb-e2e-host-relays.pids}"

_listening() { ss -tlnH "( sport = :$1 )" 2>/dev/null | grep -q LISTEN; }

start() {
  command -v socat >/dev/null 2>&1 || { echo "✗ socat not found — install socat" >&2; return 1; }
  : >"$PIDFILE.tmp"
  for map in "${RELAYS[@]}"; do
    local lp="${map%%:*}" tp="${map##*:}"
    if _listening "$lp"; then
      echo "  ✓ relay :$lp already listening (reusing)"
    else
      nohup socat "TCP-LISTEN:${lp},fork,reuseaddr,bind=0.0.0.0" "TCP:127.0.0.1:${tp}" \
        >/dev/null 2>&1 &
      local pid=$!
      disown "$pid" 2>/dev/null || true
      echo "$pid" >>"$PIDFILE.tmp"
      echo "  ▶ started relay :$lp -> 127.0.0.1:$tp (pid $pid)"
    fi
  done
  # Merge any newly-started pids into the pidfile.
  [ -f "$PIDFILE" ] && cat "$PIDFILE" >>"$PIDFILE.tmp" 2>/dev/null || true
  sort -u "$PIDFILE.tmp" >"$PIDFILE" 2>/dev/null || true
  rm -f "$PIDFILE.tmp"
  sleep 1
  status
}

stop() {
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do
      [ -n "$pid" ] && kill "$pid" 2>/dev/null && echo "  ✗ stopped relay pid $pid"
    done <"$PIDFILE"
    rm -f "$PIDFILE"
  else
    echo "  (no relay pidfile — nothing started by this script)"
  fi
}

status() {
  local ok=0
  for map in "${RELAYS[@]}"; do
    local lp="${map%%:*}"
    if _listening "$lp"; then echo "  ✓ :$lp listening"; ok=$((ok+1)); else echo "  ✗ :$lp NOT listening"; fi
  done
  [ "$ok" -eq "${#RELAYS[@]}" ]
}

case "${1:-status}" in
  start)  start ;;
  stop)   stop ;;
  status) status ;;
  *) echo "usage: host_relays.sh {start|stop|status}" >&2; exit 2 ;;
esac
