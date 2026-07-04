#!/usr/bin/env bash
#
# model-watchdog.sh — auto-restart dead model servers (campaign finding F5).
#
# The Qwen3.6-35B text server on the 2 GB-iGPU host dies silently under
# sustained inference load (its log just ends; no error). This loop probes the
# model health endpoints and re-runs the idempotent start-models.sh when one is
# down, bounding the outage to roughly one probe interval + model load time
# (~60 s for the 35B). Queued ingest jobs ride it out via their retry/backoff;
# the lease reaper requeues anything a crash orphaned.
#
# Usage:
#   nohup bash scripts/model-watchdog.sh >/tmp/model-watchdog.log 2>&1 &
#   ... ; kill %1   (or: pkill -f model-watchdog)
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTERVAL="${WATCHDOG_INTERVAL_S:-10}"

probe() { curl -sf --max-time 5 "http://127.0.0.1:$1/health" >/dev/null 2>&1; }

# Preserve the dying server's log before start-models.sh truncates it — the
# crash signature (Vulkan device-lost, abort, OOM, …) is the F5 evidence.
rotate_log() {
  local log="/tmp/llama-$1-$2.log"
  if [ -s "$log" ]; then
    cp -p "$log" "${log%.log}.died-$(date +%Y%m%dT%H%M%S).log" 2>/dev/null || true
    # keep only the 5 most recent death logs per server
    ls -t "${log%.log}".died-*.log 2>/dev/null | tail -n +6 | xargs -r rm -f
  fi
}

echo "$(date -Is) model watchdog started (interval ${INTERVAL}s)"
while true; do
  for port in 8080 8081 8082; do
    if ! probe "$port"; then
      echo "$(date -Is) :$port dead — restarting single model via start-models.sh"
      case "$port" in
        8080) rotate_log text 8080 ;;
        8081) rotate_log embed 8081 ;;
        8082) rotate_log rerank 8082 ;;
      esac
      bash "$SCRIPT_DIR/start-models.sh" restart "$port" || true
      break
    fi
  done
  sleep "$INTERVAL"
done
