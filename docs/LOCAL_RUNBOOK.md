# Local runbook — bring the whole app up (e.g. after a reboot)

How to get the **entire Local File Knowledge Base running on the dev box** (Framework 13,
AMD Ryzen + Radeon 760M, **Vulkan**, no NVIDIA). The local stack is:

```
host-native llama.cpp servers + ettin reranker sidecar + whisper   (inference, on the host)
        ▲  socat relays (9080-9082, 9093)  ▼
Podman: postgres · tika · app · caddy                              (the app stack, in containers)
        ▲  https://localhost:9443 (Caddy → app:9999)
```

The containerized app can't reach the host's `127.0.0.1` llama servers directly, so socat relays
re-expose them on `0.0.0.0:9080-9082` and the app config
([`test/config.e2e.host.toml`](../test/config.e2e.host.toml)) points at `host.containers.internal`.

> Production (a real GPU server) is different — use the **Quadlet/systemd** units in
> [`quadlet/`](../quadlet) (see the README). This runbook is the **dev/local** path.

## Current model stack

| Role | Model | Port | How it runs |
|---|---|---|---|
| text / vision / code | **Qwen3.6-35B-A3B-VL** (`Qwen3.6-35B-A3B-APEX-I-Compact.gguf` + `mmproj-Qwen3.6-35B-A3B.gguf`) | 8080 | `llama-server`, `--reasoning-budget 0` |
| embeddings | **Qwen3-Embedding-4B** (`Qwen3-Embedding-4B-Q4_K_M.gguf`, 2560-dim) | 8081 | `llama-server`, `--embedding --pooling last` |
| reranker | **ettin-reranker-400m-v1** | 8082 | torch + sentence-transformers sidecar (`tools/ettin-rerank/`) — **not** llama.cpp |
| transcription (audio/video) | **Whisper large-v3-turbo** (`ggml-large-v3-turbo.bin`) | 8083 | whisper.cpp (CPU); **optional** — only for media ingestion |

## What a reboot wipes (and what survives)

| Survives | Must be restarted/rebuilt |
|---|---|
| `./models/*.gguf`, `~/.venvs/ettin-rerank`, Podman images + named volumes | the host model servers (started by hand, not services), the socat relays, **the whisper.cpp build in `/tmp`** (ephemeral) |

---

## Step 0 — prerequisites (one-time, or after `/tmp` is wiped)

```bash
cd ~/Documents/projects/files_organizer

# socat (for the relays) + the model files must be present
command -v socat >/dev/null || sudo dnf install -y socat
ls models/Qwen3.6-35B-A3B-APEX-I-Compact.gguf models/Qwen3-Embedding-4B-Q4_K_M.gguf \
   models/mmproj-Qwen3.6-35B-A3B.gguf models/ggml-large-v3-turbo.bin

# ettin reranker venv (only if missing)
[ -x ~/.venvs/ettin-rerank/bin/python ] || {
  python3 -m venv ~/.venvs/ettin-rerank
  ~/.venvs/ettin-rerank/bin/pip install -U pip torch --index-url https://download.pytorch.org/whl/cpu
  ~/.venvs/ettin-rerank/bin/pip install sentence-transformers fastapi uvicorn
}

# whisper.cpp lives in /tmp → rebuild after a reboot (skip if you won't ingest audio/video)
[ -x /tmp/whisper.cpp/build/bin/whisper-server ] || {
  git clone https://github.com/ggml-org/whisper.cpp /tmp/whisper.cpp
  cmake -S /tmp/whisper.cpp -B /tmp/whisper.cpp/build -DGGML_NATIVE=ON -DWHISPER_BUILD_TESTS=OFF
  cmake --build /tmp/whisper.cpp/build -j"$(nproc)" --target whisper-server
}
```

## Step 1 — start the host model servers

`llama-server` is the Vulkan llama.cpp build at `~/.local/bin/llama-server` (`-ngl 99` = all layers
on the Radeon iGPU). Run each in the background; logs go to `/tmp/`.

```bash
cd ~/Documents/projects/files_organizer

# text / vision / code — Qwen3.6-35B-A3B-VL (reasoning OFF so json_schema tagging works)
nohup llama-server -ngl 99 --temp 0 -c 8192 --host 127.0.0.1 --port 8080 \
  -m models/Qwen3.6-35B-A3B-APEX-I-Compact.gguf \
  --mmproj models/mmproj-Qwen3.6-35B-A3B.gguf --reasoning-budget 0 \
  >/tmp/llama-text-8080.log 2>&1 & disown

# embeddings — Qwen3-Embedding-4B (2560-dim, last-token pooling)
nohup llama-server -ngl 99 -c 8192 --host 127.0.0.1 --port 8081 \
  -m models/Qwen3-Embedding-4B-Q4_K_M.gguf --embedding --pooling last \
  >/tmp/llama-embed-8081.log 2>&1 & disown

# reranker — ettin sidecar (self-restarting wrapper; defaults to PORT=8082)
PORT=8082 nohup bash tools/ettin-rerank/run.sh >/dev/null 2>&1 & disown

# (optional) transcription — whisper large-v3-turbo, CPU
nohup /tmp/whisper.cpp/build/bin/whisper-server -m models/ggml-large-v3-turbo.bin \
  --host 127.0.0.1 --port 8083 --inference-path /v1/audio/transcriptions -t 6 \
  >/tmp/whisper-8083.log 2>&1 & disown
```

Wait for them to load (the 35B takes a minute or two), then sanity-check:

```bash
for p in 8080 8081 8082; do printf 'port %s: ' "$p"; curl -sf "http://127.0.0.1:$p/health" && echo; done
```

## Step 2 — start the socat relays

```bash
# 9080→8080, 9081→8081, 9082→8082 (text/embed/rerank), for the container to reach the host
bash test/scripts/host_relays.sh start

# optional whisper relay 9093→8083 (only if you started whisper)
ss -tlnH 'sport = :9093' | grep -q LISTEN || \
  nohup socat TCP-LISTEN:9093,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:8083 >/dev/null 2>&1 & disown
```

## Step 3 — bring up the app stack (Podman)

If the image is already current (a plain reboot, no code change), just start the containers:

```bash
podman compose --env-file test/.env -f compose.yaml -f test/compose.e2e.host.yaml up -d
```

If you changed Rust code first, rebuild the image, then recreate:

```bash
podman build --ignorefile test/.containerignore.e2e -t kb:latest -f Containerfile .
podman compose --env-file test/.env -f compose.yaml -f test/compose.e2e.host.yaml down -v
podman compose --env-file test/.env -f compose.yaml -f test/compose.e2e.host.yaml up -d
```

> `--env-file test/.env` is **required** — without it the app skips seeding the bootstrap admin
> (`admin@local.kb` / `admin`) and every request 401s.
> `./test/run.sh up` does build + relays + up + health-wait for you, but its build is `--no-cache`
> (slow); the manual path above with a cached `podman build` is faster.

## Step 4 — verify

```bash
# health (Caddy → app); ~10s after the containers report healthy
until curl -ksf https://localhost:9443/health >/dev/null; do sleep 3; done; echo "healthy ✓"

# the running stack
podman ps --format '{{.Names}}\t{{.Status}}' | grep local-kb
```

Open <https://localhost:9443> (self-signed cert) and sign in with `admin@local.kb` / `admin`.

## Observability / single pane

There is **one** observability pane: **Prometheus** scrapes the app's `/metrics` endpoint and
**Grafana** renders it. Import `crates/metrics/grafana-dashboard.json` (its only `__input` is a
Prometheus datasource). A single dashboard serves **all tenants** — per-tenant series carry a
`tenant_id` label rather than a Grafana template variable, so no per-tenant duplication is needed.

Metrics surfaced on the dashboard (all live as of P14; full list + HELP text in
`crates/metrics/src/lib.rs`):

- **Backend / scheduler:** `kb_backend_healthy`, `kb_backend_free_slots` / `kb_backend_total_slots`,
  `kb_backend_in_flight`, `kb_queue_depth`, `kb_queue_oldest_job_age_secs`.
- **Requests:** `kb_requests_total`, `kb_request_duration_seconds`, `kb_request_errors_total`
  (and the HTTP RED trio `kb_http_requests_total` / `_duration_seconds` / `kb_http_errors_total`).
- **Per-tenant usage & cost** (label `tenant_id`): `kb_active_users`, `kb_storage_bytes_used`,
  `kb_tenant_tokens_monthly`, `kb_tenant_spend_monthly_micros`, `kb_tenant_budget_cents`,
  `kb_tenant_budget_exceeded`.
- **Tokens metered:** `kb_tokens_total{role,model}`, `kb_metering_write_failures_total`.
- **Limits / rejections:** `kb_quota_rejections_total{limit}`,
  `kb_rate_limit_rejections_total{kind}`.
- **Runtime health:** `kb_subsystem_degraded{subsystem}`, `kb_inflight_ingest`.

**Alerting:** load `prometheus_alerts.yml` via `rule_files:` in `prometheus.yml`. It fires on
budget ≥ 80% / exceeded, quota- and rate-limit-rejection spikes, subsystem degradation, in-flight
ingest saturation, and lost metering writes. Validate with
`promtool check rules prometheus_alerts.yml` (or, without promtool,
`python3 -c "import yaml; list(yaml.safe_load_all(open('prometheus_alerts.yml')))"`).

### Exact per-USER usage — Grafana Postgres datasource

Per-**user** usage is **intentionally not** a Prometheus label (a `user_id` label would explode
cardinality). Prometheus is the *ops* pane; for exact per-user accounting add a **second Grafana
datasource of type PostgreSQL** pointed at the app database and query the `usage_events` table
(`tenant_id, user_id, model, role, prompt_tokens, completion_tokens, created_at`). Example panel
query (tokens per user, last 30 days):

```sql
SELECT user_id,
       sum(coalesce(prompt_tokens,0) + coalesce(completion_tokens,0)) AS tokens
FROM   usage_events
WHERE  created_at >= now() - interval '30 days'
GROUP  BY user_id
ORDER  BY tokens DESC;
```

Use a **read-only** Postgres role for this datasource; it bypasses app-layer RLS, so scope it to
reporting only. (Aggregated per-tenant rollups also live in `tenant_monthly_usage`.)

## Teardown

```bash
# app stack (volumes wiped)
podman compose --env-file test/.env -f compose.yaml -f test/compose.e2e.host.yaml down -v
# host inference + relays (kill by port)
for p in 8080 8081 8082 8083 9080 9081 9082 9093; do
  pid=$(ss -tlnpH "sport = :$p" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
  [ -n "$pid" ] && kill "$pid"
done
```

## Troubleshooting

- **App 503 / search returns nothing right after a model hot-swap** — the rerank circuit breaker is
  briefly open; wait ~1-2 min for health probes to recover, or `down -v && up -d` for a clean start.
- **`host.containers.internal` unreachable from the app** — the relays aren't up; re-run Step 2 and
  confirm `ss -tlnp | grep -E ':908[0-2]'` shows socat listening on `0.0.0.0`.
- **Tagger 500 / empty content** — Qwen3.6-VL is a *reasoning* model; it must run with
  `--reasoning-budget 0` (Step 1), otherwise `response_format: json_schema` fails.
- **Whisper missing after reboot** — `/tmp` was wiped; redo the whisper build in Step 0.
- **Run the e2e suite against this stack:** `E2E_LLAMA=host E2E_RECORD=1 ./test/run.sh pytest tests -m "not judge and not perf"`.
