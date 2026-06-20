#!/usr/bin/env bash
# Resilient, no-auth download of the DeepSeek-V4-Flash Q8_0 GGUF from HF.
#
# Why this shape: the file (~302 GB) exceeds HF's regular-download size limit, so
# the `hf` client refuses it and only the Xet path is offered -- but unauthenticated
# Xet stalls. The contiguous file is still reachable via the resolve/cas-bridge URL,
# with two quirks discovered empirically:
#   * open-ended ranges ("bytes=N-") are rejected 400 by the presigned S3 URL;
#   * a single span larger than ~2 GiB is also rejected 400.
# So we pull fixed 1 GiB *bounded* ranges and append. -f keeps HTTP error bodies
# out of the file; we do NOT use curl --retry (it would re-append a duplicate
# partial) -- the outer loop re-derives the offset from the real file size instead.
# Unauthenticated bandwidth is per-IP capped at ~3 MB/s, so expect ~a day total.
set -u

REPO="cyberneurova/CyberNeurova-DeepSeek-V4-Flash-abliterated-GGUF"
FILE="cyberneurova-DeepSeek-V4-Flash-abliterated-Q8_0.gguf"
URL="https://huggingface.co/${REPO}/resolve/main/${FILE}"
OUT="$(cd "$(dirname "$0")/.." && pwd)/models/${FILE}"
SIZE=302251447616               # bytes (x-linked-size, authoritative)
CHUNK=$(( 1024 * 1024 * 1024 )) # 1 GiB bounded range per request
MAX_NOPROGRESS=30               # give up after this many consecutive empty attempts

echo "$(date -Is) start target=$OUT expected=$SIZE"
fails=0
while :; do
  cur=$(stat -c%s "$OUT" 2>/dev/null || echo 0)
  if [ "$cur" -ge "$SIZE" ]; then echo "$(date -Is) COMPLETE $cur"; break; fi
  end=$(( cur + CHUNK - 1 ))
  [ "$end" -gt $(( SIZE - 1 )) ] && end=$(( SIZE - 1 ))
  # --speed-limit/--speed-time aborts a stalled mid-transfer in ~60s (cas-bridge
  # occasionally wedges near a chunk's end); the outer loop then resumes the
  # remaining bytes. --max-time is just a hard ceiling.
  curl -sS -f -L -r "${cur}-${end}" --connect-timeout 30 --max-time 2400 \
       --speed-limit 102400 --speed-time 60 "$URL" >> "$OUT"
  rc=$?
  new=$(stat -c%s "$OUT" 2>/dev/null || echo 0)
  if [ "$new" -le "$cur" ]; then
    fails=$(( fails + 1 ))
    echo "$(date -Is) rc=$rc NO PROGRESS @ $cur (fail $fails/$MAX_NOPROGRESS)"
    [ "$fails" -ge "$MAX_NOPROGRESS" ] && { echo "$(date -Is) ABORT: stuck at $cur"; exit 1; }
    sleep 15
  else
    fails=0
    pct=$(awk "BEGIN{printf \"%.2f\",$new/$SIZE*100}")
    echo "$(date -Is) rc=$rc +$(( (new-cur)/1024/1024 ))MB total=$new (${pct}%)"
  fi
done

magic=$(head -c4 "$OUT" | tr -d '\0')
echo "$(date -Is) final magic=[$magic] size=$(stat -c%s "$OUT")"
[ "$magic" = "GGUF" ] && echo "OK: valid GGUF header" || echo "WARN: unexpected header"
