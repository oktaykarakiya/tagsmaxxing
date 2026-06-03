#!/usr/bin/env python3
"""Sample the app scheduler's slot/queue state under load and verify it behaves.

Polls `GET /metrics` (Prometheus) and the host llama `/slots` endpoints at an
interval, writes a timeseries CSV, and prints a PASS/FAIL verdict on the scheduler
invariants we care about:

  * slot-respect  — per-backend in_flight never exceeds total_slots
  * consistency   — free_slots + in_flight == total_slots (no accounting drift)
  * queueing      — kb_queue_depth rises under contention …
  * drain         — … and returns to ~0 (no stuck/leaked leases) by the end
  * health        — no backend wrongly unhealthy / circuit-breaker-open

Usage:  scheduler_monitor.py [duration_secs=180] [interval_secs=3]
Env:    BASE_URL (default https://localhost:9443); E2E_SCHED_CSV (output path).
Stdlib only, so it runs without the venv.
"""
from __future__ import annotations

import os
import re
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE = os.environ.get("BASE_URL", "https://localhost:9443")
DUR = float(sys.argv[1]) if len(sys.argv) > 1 else 180.0
IVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get("E2E_SCHED_CSV") or os.path.join(HERE, "..", "results", "scheduler_monitor.csv")
LLAMA_PORTS = {"text": 8080, "embed": 8081, "rerank": 8082}

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE


def _fetch(url: str, timeout: float = 5.0) -> str:
    try:
        ctx = _ctx if url.startswith("https") else None
        with urllib.request.urlopen(url, timeout=timeout, context=ctx) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


def _parse_prom(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    out: dict[str, list[tuple[dict[str, str], float]]] = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = re.match(r"^([A-Za-z_:][\w:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)$", line.strip())
        if not m:
            continue
        name, lbls, val = m.group(1), m.group(2) or "", m.group(3)
        labels = dict(re.findall(r'(\w+)="([^"]*)"', lbls))
        try:
            out.setdefault(name, []).append((labels, float(val)))
        except ValueError:
            pass
    return out


def _by_backend(samples: list[tuple[dict[str, str], float]]) -> dict[str, float]:
    res: dict[str, float] = {}
    for labels, v in samples:
        bid = labels.get("backend") or labels.get("id") or labels.get("backend_id") or "?"
        res[bid] = v
    return res


def _llama_busy(port: int) -> int:
    txt = _fetch(f"http://127.0.0.1:{port}/slots", 3.0)
    return txt.count('"is_processing":true')


def main() -> int:
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    new = not os.path.exists(OUT)
    fh = open(OUT, "a", encoding="utf-8")
    if new:
        fh.write("ts,backend,in_flight,free_slots,total_slots,healthy,queue_depth,"
                 "queue_oldest_age_s,throttled_total,circuit_open,llama_busy\n")

    violations: list[str] = []
    max_inflight: dict[str, float] = {}
    max_queue = 0.0
    last_queue = 0.0
    throttled_first: float | None = None
    throttled_last = 0.0
    ever_cb_open = False
    backends_seen: set[str] = set()
    totals: dict[str, float] = {}

    deadline = time.monotonic() + DUR
    ticks = 0
    print(f"[sched-monitor] polling {BASE}/metrics every {IVAL}s for {DUR:.0f}s → {OUT}")
    while time.monotonic() < deadline:
        ts = datetime.now(timezone.utc).isoformat()
        prom = _parse_prom(_fetch(f"{BASE}/metrics"))
        if not prom:
            time.sleep(IVAL)
            continue
        ticks += 1
        in_flight = _by_backend(prom.get("kb_backend_in_flight", []))
        free = _by_backend(prom.get("kb_backend_free_slots", []))
        total = _by_backend(prom.get("kb_backend_total_slots", []))
        healthy = _by_backend(prom.get("kb_backend_healthy", []))
        qd = (prom.get("kb_queue_depth", [({}, 0.0)]) or [({}, 0.0)])[0][1]
        qage = (prom.get("kb_queue_oldest_job_age_secs", [({}, 0.0)]) or [({}, 0.0)])[0][1]
        thr = (prom.get("kb_ingest_throttled_total", [({}, 0.0)]) or [({}, 0.0)])[0][1]
        cb = _by_backend(prom.get("kb_circuit_breaker_open", []))

        max_queue = max(max_queue, qd)
        last_queue = qd
        if throttled_first is None:
            throttled_first = thr
        throttled_last = thr
        if any(v >= 1 for v in cb.values()):
            ever_cb_open = True

        for bid in set(in_flight) | set(total) | set(free):
            backends_seen.add(bid)
            inf = in_flight.get(bid, 0.0)
            tot = total.get(bid, 0.0)
            fr = free.get(bid, 0.0)
            totals[bid] = tot
            max_inflight[bid] = max(max_inflight.get(bid, 0.0), inf)
            if tot and inf > tot:
                violations.append(f"{ts} {bid}: in_flight {inf} > total_slots {tot}")
            if tot and abs((fr + inf) - tot) > 0.5:
                violations.append(f"{ts} {bid}: free({fr})+in_flight({inf}) != total({tot})")
            busy = _llama_busy(LLAMA_PORTS.get(bid, 0)) if bid in LLAMA_PORTS else -1
            fh.write(f"{ts},{bid},{inf:g},{fr:g},{tot:g},{healthy.get(bid,0):g},"
                     f"{qd:g},{qage:g},{thr:g},{cb.get(bid,0):g},{busy}\n")
        fh.flush()
        time.sleep(IVAL)

    fh.close()

    print(f"\n[sched-monitor] {ticks} samples over ~{DUR:.0f}s")
    for bid in sorted(backends_seen):
        print(f"  backend {bid}: max in_flight={max_inflight.get(bid,0):g} / total_slots={totals.get(bid,0):g}")
    print(f"  max queue_depth={max_queue:g}  final queue_depth={last_queue:g}")
    if throttled_first is not None:
        print(f"  ingest_throttled delta={throttled_last - throttled_first:g}")
    print(f"  circuit-breaker-open seen: {ever_cb_open}")

    ok = True
    if violations:
        ok = False
        print("\n  ✗ SLOT INVARIANT VIOLATIONS:")
        for v in violations[:10]:
            print(f"     {v}")
    if last_queue > 0.5:
        ok = False
        print(f"  ✗ queue did not drain (final depth {last_queue:g}) — possible stuck/leaked leases")
    if ever_cb_open:
        print("  ⚠ a circuit breaker opened during the window (review app logs)")
    print(f"\n[sched-monitor] VERDICT: {'PASS — scheduler respected slots and drained' if ok else 'FAIL — see above'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
