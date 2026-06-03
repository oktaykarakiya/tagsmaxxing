# `test/` — black-box end-to-end harness

A **self-contained, swappable** end-to-end test suite that drives the *real
deployed app* the way a user does: through a headless browser (Playwright) and
the JSON API. Everything lives in this folder — compose overlay, app config, its
own `.env`, the Python project, the runner. **Delete `test/` and the repo is
untouched.** It does not read from or write to `BUILD_LEDGER.toml` or the Rust
build; the ledger/plan were only a *reference* for deriving the test catalog.

## One command

```bash
./test/run.sh setup     # once: create the venv, install deps + Chromium
./test/run.sh           # cargo build → fresh app image (--no-cache) → bring up
                        # all ./models → wait for health → run the suite → teardown
```

Other subcommands: `up` (build + start, leave running), `down` (teardown `-v`),
`quality` (full run **including** the judge lane), `perf` (performance/load lane),
`pytest …` (run against an already-up stack, e.g. `./test/run.sh pytest -m smoke`).

`E2E_KEEP=1 ./test/run.sh` leaves the stack up for debugging.
*(Optional convenience: add `e2e:\n    ./test/run.sh` to the repo `justfile` for
`just e2e` — the only thing that would live outside `test/`.)*

## What it stands up

`run.sh` layers `test/compose.e2e.yaml` onto the repo's `compose.yaml`. The repo
ships a single placeholder `llama-server`; a real run of `./models` needs **one
llama.cpp server per role**, so the overlay adds them and `test/config.e2e.toml`
maps each role to the right one:

| Model in `./models` | Service | Role(s) |
|---|---|---|
| `Qwen3VL-30B-A3B-Instruct-Q4_K_M.gguf` + `mmproj-…-F16.gguf` | `llama-text` | text, vision, code |
| `bge-m3-q8_0.gguf` (1024-dim) | `llama-embed` | embed |
| `bge-reranker-v2-m3-q8_0.gguf` | `llama-rerank` | rerank |
| `ggml-large-v3-turbo.bin` | `whisper` | audio |

Plus the base stack: `postgres`, `tika`, `app`, `caddy`. The suite talks to the
app over **HTTPS via Caddy (`https://localhost:9443`)** because the session
cookie is `__Host-`-prefixed (requires `Secure`); TLS verification is disabled
for Caddy's local cert.

**GPU by default.** No NVIDIA GPU? Set `E2E_HW=cpu` in `test/.env` (slower).

## Layout

```
test/
  run.sh                 single entry point (build → up → wait → pytest → teardown)
  .env / .env.example    config + the DeepSeek key (.env is gitignored)
  compose.e2e.yaml       multi-model overlay (llama-text/embed/rerank + whisper)
  compose.e2e.cpu.yaml   CPU-only override
  config.e2e.toml        app backend config (role → server), secure_cookies=true
  pyproject.toml         pytest config + markers
  requirements.txt       Python deps
  conftest.py            fixtures: base_url, browser (HTTPS), api client, judge
  lib/                   config.py, api_client.py, flows.py, judge.py, fixtures_data/
  tests/
    smoke/               the one fully-implemented end-to-end test (step 1)
    catalog/             ~the "huge array": pending stubs grouped by feature (step 2)
  CATALOG.md             human index of the catalog by feature area
```

## The catalog (step 2)

`tests/catalog/` holds one **pending** (`@pytest.mark.skip`) test per capability,
grouped by feature area and marked (`-m auth`, `-m billing`, …). They show up as
*skipped* in `pytest --collect-only` and in `CATALOG.md`. Step 2 fills in the
bodies. Nondeterministic checks (e.g. "are these tags any good?") use the
**DeepSeek LLM-as-judge** (`lib/judge.py`, `deepseek-v4-pro` at max reasoning
effort), carry `@pytest.mark.judge`, and run only in the `quality` lane.

## Implementing the catalog in bulk

`./test/implement.sh` drives the catalog to completion with a swarm of headless
Claude Code agents — **one agent per file, up to `MAX_AGENTS` (default 10) in
parallel**. Status is derived live from the code, so it only works on pending
tests and skips anything already implemented (safe to re-run until done):

```bash
./test/implement.sh status         # done / pending / blocked per file
./test/run.sh up                   # bring the stack up so agents can verify
./test/implement.sh run            # spawn the swarm (<= MAX_AGENTS in parallel)
./test/implement.sh run --dry-run  # show the plan without spawning
MAX_AGENTS=6 IMPLEMENT_MODEL=sonnet ./test/implement.sh run
```

Each agent edits only its one file, implements the bodies, and verifies against
the running stack; logs land in `test/.implement-logs/`. Performance/load tests
(`tests/catalog/test_performance.py`, marker `perf`) are implemented by the same
swarm and run via `./test/run.sh perf`.

## Test philosophy & results history

These tests encode how the app **should** behave (its contract), not how it
currently behaves. **A failing test is a valid, expected result — it means an app
bug, not a broken test.** We never weaken an assertion, change app code, or skip a
test to make it green; failures are recorded and surfaced.

Every official run (`run`, `quality`, `perf` — which set `E2E_RECORD=1`) appends to:

- `test/results/history.csv` — one row per test per run:
  `run_id, timestamp_utc, nodeid, outcome, duration_s, error`.
- `test/results/runs.csv` — one row per run: totals of passed / failed / skipped / error.

Agent verification runs (`./test/run.sh pytest …`) don't record, so the history
reflects only full official runs, and it is append-only across runs.

## Notes & troubleshooting

- **Models are real**, so output is nondeterministic — the core tests assert on
  *behavior* ("a doc comes back with tags + a summary and is searchable"), not on
  exact AI text. Content quality is graded separately by the judge lane.
- First run is slow: `--no-cache` image build + loading an 18.5 GB model into VRAM.
  `STACK_HEALTH_TIMEOUT` (default 900s) covers the model load.
- **rerank server won't start?** Some llama.cpp builds also want
  `LLAMA_ARG_POOLING: rank` — add it to `llama-rerank` in `compose.e2e.yaml`.
- **whisper unhealthy?** It's not needed for the text-only smoke test; audio
  tests come in step 2. Adjust its healthcheck if your image lacks `/health`.
- **Change the DeepSeek key** anytime by editing `DEEPSEEK_API_KEY` in `test/.env`
  (read at call time). The key shared during setup should be rotated if leaked.
