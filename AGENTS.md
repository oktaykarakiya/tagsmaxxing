# Multi-Role Sweep Waves — Implementation Methodology

## Non-Negotiable Testing Rule

There is no valid excuse for not running ALL tests after ANY code change.
This machine is meant to execute everything. If a command times out, re-run
with a larger timeout. If a step genuinely cannot complete (OOM, disk full),
STOP and surface it explicitly — never silently proceed as if it succeeded.

"Slow" is never a reason to skip. "Probably fine" is never a reason to skip.
"Only changed one file" is never a reason to skip. Every code change, no
matter how small, requires the full sweep. Every single time.

If a command times out, the ONLY valid response is to re-run it with a longer
timeout. A timeout is NOT a "pass" — it means the command didn't finish. Never
treat a timeout as "no output = no failures." Re-run, re-run, re-run until it
completes. Never skip an E2E run, never skip a build, never skip a test suite.
If the process genuinely cannot complete (real disk full, real OOM), STOP
everything and surface the blocker — do not proceed past it silently.

### Mandatory sequence after EVERY code change, in this exact order:

1. **Baseline** (before changes) — capture pass/fail counts of the full suite.
   Without a baseline, pre-existing failures are indistinguishable from
   regressions.

2. **`cargo build --workspace --all-targets`** — verify every crate compiles,
   including tests, examples, and doc-tests. No warnings allowed.

3. **`cargo test --workspace --all-features`** — unit tests, integration tests,
   doctests across every crate. All must pass.

4. **`cargo clippy --workspace --all-features -- -D warnings`** — zero lints.
   Zero warnings. Clippy is as strict as the compiler.

5. **`just up --rebuild`** — rebuild the container image from source, start
   the FULL local stack: GPU model servers, socat relays, watchdog, Podman
   containers (postgres, tika, app, caddy). Wait for health check.

   The correct rebuild command is:
   ```
   just up --rebuild
   ```
   Or without `just`:
   ```
   bash scripts/dev-up.sh start --rebuild
   ```

6. **`E2E_LLAMA=host ./test/run.sh pytest tests -m "not judge and not perf"`** —
   run the full E2E Playwright test suite against the live production-equivalent
   stack. Every route, every workflow, every integration surface.

   For the full quality gate including LLM-as-judge:
   ```
   E2E_LLAMA=host ./test/run.sh quality
   ```

7. **Compare against baseline** — subtract the pre-change failure counts from
   the post-change failure counts. Only NEW failures (not in baseline) count
   as regressions. Infrastructure failures (connection refused, pool exhausted)
   are always excluded regardless of baseline.

### No step may be skipped.

No "targeted re-run only on the changed file." No "unit tests passed so E2E
can't be broken." No "the change is trivial." Full Sweep-and-Verify, every
time, or do not claim done. The final gate is: full suite passes with zero
new non-infrastructure failures vs baseline.

### Exit code classification

| Exit code | Outcome | Meaning |
|-----------|---------|---------|
| 0 | `passed` | Clean run, zero failures |
| 124 | `timed_out` | Shell timeout — re-run with larger timeout |
| 127 | `missing_command` | Command not found |
| other | `failed` | Real test failure — BLOCKS |

## Empirical proof — no assumptions
- Every issue must be proven empirically: reproduce the failure, root-cause it
  with concrete data, fix it, and prove the fix eliminates the failure.
- Never claim a failure is "pre-existing" without actual evidence — run a
  baseline and diff it against the current run.
- Test at the appropriate layer: replicate bugs with dedicated regression tests
  before fixing; verify fixes with end-to-end integration tests against the
  production-equivalent environment.
- Verify every build actually happened (container image timestamps, binary
  hashes) and every test actually ran green — never infer or claim success
  without evidence.

## Quality Assurance Patterns

These strategies were battle-tested across multiple waves and are responsible
for eliminating regressions.

- **Pre-Wave Baseline (Mandatory)**: Capture full test suite pass/fail counts
  before any code changes. Without this, pre-existing failures are
  indistinguishable from regressions. Infrastructure failures (unreachable
  services, connection refused) are always excluded regardless of baseline.
- **Parallel Agent Research**: Launch 2-3 explore agents simultaneously to
  investigate different subsystems. Agents return structured findings that feed
  the planner. Cuts investigation time by 3x.
- **Validation Gate Before Code**: Have at least two agents independently review
  the plan — one for completeness (did we miss any files/bugs?), one for security
  (any credential exposure? unsafe operations?). Every finding becomes a task.
  Only then does development begin.
- **Empirical Proof — Reproduce Before Fixing**: Every bug must be reproduced
  empirically before writing a fix. Trace the code line-by-line. Check the
  database schema. Run a targeted test to capture the exact error. Never fix
  based on assumptions or documentation alone.
- **Full Sweep After Every Change**: After each round of fixes, run ALL affected
  test files — never just the file that was edited. Changes in one file cascade
  to other test files that assert on the affected behavior.
- **Categorize Every Failure**: Each test failure gets exactly one category:
  `infra` (unreachable services — ignore), `code bug` (unexpected errors — must
  fix), `test assertion` (mismatched response shape — fix test), `flaky`
  (non-deterministic — quarantine). Different fix strategy per category.
- **Iteration Gate**: The loop is: run suite → categorize all failures → fix one
  category → rerun FULL suite → repeat until zero non-infra failures. Never skip
  the full rerun. Never fix more than one category per iteration without
  re-running.
- **Deploy Gate**: Before deploying, verify: (a) fresh build completed,
  (b) container image timestamps are recent, (c) full test suite passes,
  (d) type checker / linter returns zero, (e) core modules pass against
  production-equivalent services.
- **Post-Deploy Read-Only (Never Touch Production)**: After deployment, verify
  via read-only commands: service health, HTTP 200 on endpoints, container logs,
  backup connectivity. Never exec into containers. Never modify files. Never
  patch on the server.

## Full-Suite Testing Strategy ("Sweep-and-Verify")

After ANY code change, the tester must execute the COMPLETE test suite — never
a targeted subset. Targeted runs hide regressions in unrelated files.

Must report zero failures. Database-connection failures (connection refused,
pool exhausted) are pre-existing infrastructure gaps — call these out separately
but do not treat them as regressions from the code change.

### Gate
- If ANY non-infrastructure test fails → fix → re-run FULL suite (not just the
  file that failed). Repeat until 100% green.
- Targeted re-runs (single file, filter patterns) are for development iteration
  ONLY. The final verification before declaring "done" MUST be a full sweep.
- Never skip slow steps — slowness is NEVER a valid reason to skip a test run,
  build, or verification. If a command times out, re-run it with a larger timeout
  and wait for completion. Never substitute assumptions or a partial run for an
  actual completed run.
- If a step genuinely cannot complete (real environment limit, e.g. out of memory
  or disk), STOP and surface it explicitly. Never silently proceed as if it
  succeeded.

## Multi-Role Sweep Waves (Standard Implementation Strategy)

Every implementation — whether bug fix, feature, or refactor — follows this
pattern. The goal: maximize parallelism, eliminate blind spots, and guarantee
zero regressions through disciplined role separation.

### Agent Role Categories

| Category | Role | Runs | When |
|----------|------|------|------|
| **Planning** | `planner` | One agent | Before developer on complex tasks. Produces `{approach, files_to_touch, files_to_NOT_touch, risks, acceptance_criteria}` |
| **Planning** | `pm` | Optional | Scale-adaptive: clarifies user need, scope boundaries, UX concerns |
| **Planning** | `architect-as-planner` | Optional | Scale-adaptive: structural decomposition, module boundaries, data models |
| **Planning** | `test-architect` | Optional | Scale-adaptive: test strategy, fixtures, edge cases |
| **Developer** | `developer` | One per discrete change | Always. Implements fix/feature in isolated worktree. Parallelize when changes are independent files |
| **Mandatory** | `tester` | One agent | Always. Empirically verifies change is correct. Runs Sweep-and-Verify (full suite). Returns verdict |
| **Mandatory** | `security` | One agent | Always. Audits diff for injection, authz/authn, secret exposure, SSRF, path traversal |
| **Mandatory** | `reviewer` | One agent | Always. Final sign-off on architecture, code quality, readability, conventions |
| **Conditional** | `architect` | Zero or one | Triggered by: new files, API/route/endpoint changes, DB schema changes, new dependencies, >3 files changed |
| **Conditional** | `risk` | Zero or one | Triggered by: >5 files, DB changes, auth changes, payment changes, infra changes, >3 files |
| **Conditional** | `performance` | Zero or one | Triggered by: DB changes, API changes, loop operations, build config changes |
| **Conditional** | `legal` | Zero or one | Triggered by: new dependencies, user data changes, auth changes, third-party API calls |
| **Conditional** | `devops` | Zero or one | Triggered by: Containerfiles, `.env`, new services, deploy/CI/CD config, migrations |
| **Post-Mortem** | `postmortem` | One agent | On iteration failure. Analyzes logs, produces rewritten task spec |
| **Bootstrap** | `repo-analyst` | One agent | First task in a new repo. Identifies languages, frameworks, commands, conventions |

### Verdict Contract

Every reviewer (tester, security, reviewer, architect, risk, performance,
legal, devops) MUST end their response with exactly one JSON block:

```json
{"verdict": "pass", "reason": "what you verified", "evidence": []}
```

For a problem:

```json
{"verdict": "fail", "reason": "one-paragraph objection",
 "evidence": [{"file": "relative/path.ext", "line": 42, "snippet": "the problematic code"}]}
```

Valid verdicts: `pass`, `fail`, `block`.

**Grounding rules — the orchestrator verifies these:**
- Every `file` MUST appear in the current task's diff — cannot reject code the
  developer did not touch.
- Every `file` MUST exist on disk; snippets must match current file contents.
- Findings without grounded evidence are downgraded and do NOT block the task.
- If unsure, return `pass` with a note — do not invent issues.

### Conditional Pipeline (Diff-Driven Trigger Map)

Conditional reviewers are selected by analyzing the diff. Any one trigger
matching fires the agent (short-circuit OR).

**Structural triggers (computed from diff):**

| Trigger | Condition |
|---------|-----------|
| `new_files` | Diff adds at least one new file |
| `files_gt_5` | More than 5 files changed |
| `complexity_medium_plus` | More than 3 files changed |

**Keyword triggers (matched case-insensitively against full diff text):**

| Trigger | Matches in diff |
|---------|-----------------|
| `api_change` | `api`, `route`, `endpoint`, `controller`, `handler` |
| `db_schema` | `migration`, `schema.sql`, `CREATE TABLE`, `ALTER TABLE`, `schema` |
| `db_change` | `migration`, `schema.sql`, `CREATE TABLE`, `ALTER TABLE`, `schema` |
| `migration` | `migration` |
| `new_dependency` | `package.json`, `Cargo.toml`, `Cargo.lock`, `requirements.txt`, `pom.xml`, `go.mod`, `pyproject.toml` |
| `auth_change` | `auth`, `login`, `password`, `token`, `jwt`, `session`, `oauth` |
| `payment_change` | `stripe`, `payment`, `checkout`, `billing`, `invoice` |
| `user_data_change` | `user`, `profile`, `gdpr`, `privacy`, `pii` |
| `third_party_api` | `fetch`, `reqwest`, `axios`, `http`, `stripe`, `twilio`, `mailgun` |
| `loop_operation` | `for `, `while `, `forEach`, `.map(`, `.reduce(`, `.iter()` |
| `build_config` | `package.json`, `Cargo.toml`, `Cargo.lock`, `webpack`, `vite`, `rollup`, `esbuild` |
| `containerfile` | `Dockerfile`, `Containerfile` |
| `compose_file` | `docker-compose`, `compose.yaml`, `compose.yml` |
| `env_var` | `.env` |
| `deploy_config` | `deploy`, `ci`, `cd`, `pipeline`, `.yml`, `.yaml` |
| `new_service` | `service`, `microservice`, `worker`, `daemon` |
| `infra_change` | `terraform`, `ansible`, `kubernetes`, `k8s`, `helm`, `podman`, `docker` |

**Agent trigger assignments (any one match fires the agent):**

| Agent | Triggers |
|-------|----------|
| **architect** | `new_files`, `api_change`, `db_schema`, `new_dependency`, `complexity_medium_plus` |
| **risk** | `complexity_medium_plus`, `db_change`, `auth_change`, `payment_change`, `infra_change`, `files_gt_5` |
| **performance** | `db_change`, `api_change`, `loop_operation`, `build_config` |
| **legal** | `new_dependency`, `user_data_change`, `auth_change`, `third_party_api` |
| **devops** | `containerfile`, `compose_file`, `env_var`, `new_service`, `deploy_config`, `migration` |

### Wave Structure

Wave N follows these phases. Phases run sequentially. Within a phase, agents
launch in parallel when their inputs are independent.

```
Wave N:
  ┌─ Phase 1: Plan (if complex task) ───────────────────────┐
  │  [planner] → [pm, architect-as-planner, test-architect]  │  parallel
  │  Output: {approach, files_to_touch, risks, criteria}      │
  └──────────────────────────────────────────────────────────┘
  ┌─ Phase 2: Develop ──────────────────────────────────────┐
  │  [developer(s)] — implement changes                      │  parallel per independent file
  └──────────────────────────────────────────────────────────┘
  ┌─ Phase 3: Review ───────────────────────────────────────┐
  │  Mandatory: [tester] [security] [reviewer]               │  parallel
  │  Conditional: diff triggers → [architect] [risk] ...     │  parallel
  └──────────────────────────────────────────────────────────┘
  ┌─ Phase 4: Gate ─────────────────────────────────────────┐
  │  Tester runs FULL Sweep-and-Verify                       │
  │  Reviewer gives final sign-off:                          │
  │    verdict=pass AND gate=green → wave ready               │
  └──────────────────────────────────────────────────────────┘
  ┌─ Phase 5: On Failure ───────────────────────────────────┐
  │  [postmortem] analyzes failure → rewrites task spec       │
  │  Wave N+1 starts from Phase 1 with refined spec           │
  └──────────────────────────────────────────────────────────┘
```

### Pre-Wave Baseline (Mandatory Before Phase 3)

Before any changes are made, the orchestrator or tester MUST capture a baseline
run of the full suite. This is the only way to distinguish pre-existing failures
from regressions introduced by the current change.

The baseline is captured by running the full test suite and recording pass/fail
counts. Infrastructure failures (unreachable services, connection refused) are
excluded.

**Baseline report format:**
```
Test baseline:  N suites / M tests (X failed, Y infra)
Lint/type baseline: PASS/FAIL
```

The tester's Phase 3-4 run is then compared against this baseline. Only NEW
failures (not in the baseline) count as regressions. Pre-existing infrastructure
failures are always excluded regardless of baseline.

### Pipeline Assembly (How to Derive the Reviewer Set)

Concrete, executable steps to assemble the agent pipeline from a diff:

```bash
# 1. Count structural triggers
git diff --stat HEAD | tail -1                                    # file count
git diff HEAD | grep -c "new file mode"                           # new_files

# 2. Extract keyword trigger text
DIFF_TEXT=$(git diff HEAD | tr '[:upper:]' '[:lower:]')

# 3. Fire conditional agents (any one trigger match → include)
#    architect: new_files>0 OR file_count>3 OR DIFF_TEXT matches (api|route|endpoint|controller|handler|migration|schema.sql|CREATE TABLE|ALTER TABLE|schema|package.json|Cargo.toml|requirements.txt|pom.xml|go.mod|pyproject.toml)
#    risk:      file_count>3 OR file_count>5 OR DIFF_TEXT matches (migration|schema.sql|CREATE TABLE|ALTER TABLE|auth|login|password|token|jwt|session|stripe|payment|checkout|billing|terraform|ansible|kubernetes|k8s|helm|podman|docker)
#    performance: DIFF_TEXT matches (migration|schema.sql|CREATE TABLE|ALTER TABLE|api|route|endpoint|handler|for |while |forEach|.map(|.reduce(|.iter(|package.json|Cargo.toml|webpack|vite|rollup|esbuild)
#    legal:     DIFF_TEXT matches (package.json|Cargo.toml|requirements.txt|pom.xml|go.mod|pyproject.toml|user|profile|gdpr|privacy|pii|auth|login|password|token|jwt|session|fetch|reqwest|axios|http|stripe|twilio|mailgun)
#    devops:    DIFF_TEXT matches (Dockerfile|Containerfile|docker-compose|compose.yaml|compose.yml|.env|service|microservice|worker|daemon|deploy|ci|cd|pipeline|.yml|.yaml|migration)

# 4. Final pipeline = [tester, security, reviewer] + fired_conditionals
```

For the orchestrator (human or agent): run steps 1-2, apply the trigger map from
the Conditional Pipeline section, and append matching agents to the mandatory set.

### Tester Requirements (no compromise)

The tester MUST:
1. Run the full test suite (ALL test files, not a subset)
2. Run the type checker / linter
3. Report exact pass/fail counts per file, not a summary
4. Compare against pre-wave baseline — only NEW failures count as regressions
5. Distinguish infrastructure failures from code regressions:
   - Connection refused / pool exhausted → pre-existing infrastructure gap, do not block
   - All other failures → code regression, BLOCKS
6. Never accept "targeted re-runs" as proof of success — final gate is FULL sweep
7. Never skip slow test runs — slowness is not a valid reason to skip. If a command
   times out, re-run with a larger timeout. If a step genuinely cannot complete,
   STOP and surface it explicitly.

**Exit code classification (from green-gate):**

| Exit code | Outcome | Meaning |
|-----------|---------|---------|
| 0 | `passed` | Clean run, zero failures |
| 124 | `timed_out` | Shell timeout — re-run with larger timeout |
| 127 | `missing_command` | Command not found |
| other | `failed` | Real test failure — BLOCKS |

### Flaky Test Quarantine

A single test that flips between pass and fail across runs (same code, same
environment) is **non-deterministic** and must be quarantined — not treated as
a regression.

**Quarantine procedure:**
1. Run the SUITE (not the single test) 3 times.
2. If the test passes in ANY run → it is flaky → do NOT block the wave.
3. If it fails in ALL 3 runs → it is a real failure → BLOCKS.
4. Document the flaky test in the wave report so it can be permanently excluded
   or fixed in a future task.

### Decline Consensus

All mandatory reviewers (tester, security, reviewer) must return `verdict: "pass"`.
If any returns `verdict: "fail"` or `verdict: "block"`, the developer must address
the evidence and the wave repeats from Phase 2.

### Sticky Approvals

A reviewer's `verdict: "pass"` stays valid for unchanged portions of the diff.
On re-run, reviewers only re-examine new or changed code — not the entire diff
from scratch.

### Wave Launch Template

When instructed to fix an issue:

```
Wave 1:
  Agent 1 (Developer):   [fix instructions with file paths and constraints]
  Agent 2 (Tester):      [verdict contract + sweep-and-verify commands]
  Agent 3 (Security):    [verdict contract + diff audit scope]
  Agent 4 (Reviewer):    [verdict contract + final sign-off gate]
  Agent 5 (Architect):   [conditional — triggered by diff]
  Agent 6 (Risk):        [conditional — triggered by diff]
  Agent 7 (Performance): [conditional — triggered by diff]
```

For complex tasks, prepend:

```
Wave 1:
  Agent 0 (Planner):     [produce {approach, files_to_touch, risks, criteria}]
  ...then Phase 2-5 as above...
```

### Iteration Gate

```
while tester.green === false OR any_mandatory.verdict !== "pass":
    [postmortem] → refined task spec → next Wave
```

Repeat until ALL mandatory verdicts are `pass` AND the full Sweep-and-Verify
reports zero non-infrastructure failures across all test files. Never stop
mid-wave — every bug, every test gap, every issue must be resolved before
declaring done. Never skip tests — fix either the test or the bug, but never
skip. Never assume — empirically verify every fix. Always run the full suite
after every change.

# Local Development Server — Full-Stack Startup & Teardown

## Single entrypoint: `just up` / `just down`

Everything is behind one command. No separate script invocations, no remembering
ports or compose files.

```bash
just up              # start the ENTIRE stack (models, relays, watchdog, containers)
just up --rebuild    # rebuild the container image first, then start
just down            # tear everything down (-v wipe)
```

If `just` is not on PATH (ramdisk wipe), use the raw script:

```bash
bash scripts/dev-up.sh              # start everything
bash scripts/dev-up.sh --rebuild    # rebuild + start
bash scripts/dev-up.sh down         # teardown
bash scripts/dev-up.sh status       # check what's running
```

## What `just up` does (in order, all idempotent)

1. **Prerequisite check + auto-bootstrap** — installs `socat` if missing,
   creates `ettin-rerank` Python venv if missing, builds `whisper-server` from
   source into `/tmp` if missing. Fails with clear errors if model files or
   `llama-server` binaries are absent (those can't be auto-bootstrapped).

2. **Host model servers** — runs `scripts/start-models.sh start --no-fan --no-whisper`
   (skips fan control and whisper; those are optional). Already-idempotent: skips
   any port already listening.

3. **Model watchdog** — starts `scripts/model-watchdog.sh` in background
   (auto-restarts dead model servers within ~10s). Skips if already running.

4. **Optional image rebuild** (only with `--rebuild`) — `podman build` with
   `test/.containerignore.e2e` to keep the ~21GB models/ dir out of the build
   context.

5. **Podman compose stack** — `podman compose --env-file test/.env -f compose.yaml -f test/compose.e2e.host.yaml up -d --force-recreate`.
    Starts: `postgres` (ParadeDB: pgvector + pg_search), `tika` (Apache Tika 3.3), `app` (kb binary),
   `caddy` (reverse proxy, TLS termination on :9443).

6. **Health wait** — polls `https://localhost:9443/health` up to 600s (10 min)
   for the 35B model to load into GPU memory. Prints container logs on timeout.

7. **Status summary** — model server ports, watchdog, containers, app health.

## What `just down` tears down

1. `podman compose down -v` (wipes all volumes — pgdata, kb-data, caddy-data)
2. Kills model watchdog
3. Kills host model servers + socat relays (`scripts/start-models.sh stop`)

## Architecture diagram (local/dev)

```
host-native llama.cpp servers + ettin reranker sidecar + whisper   (inference, on the host)
        ▲  socat relays (9080→8080, 9081→8081, 9082→8082, 9093→8083)  ▼
Podman: postgres · tika · app · caddy                              (the app stack, in containers)
        ▲  https://localhost:9443 (Caddy → app:9999)
```

The containerized app can't reach the host's `127.0.0.1` llama servers
directly, so socat relays re-expose them on `0.0.0.0:9080-9082`. The app config
(`test/config.e2e.host.toml`) points at `host.containers.internal:9080/9081/9082`
— podman injects `host.containers.internal` automatically on the compose
(netavark) network.

## Model stack (host-native, all GPU/Vulkan except ettin)

| Role | Model | Port | Runtime |
|------|-------|------|---------|
| text / vision / code | Qwen3.6-35B-A3B-VL (Q6_K_P GGUF + mmproj) | 8080 | llama.cpp b9592, GPU/Vulkan `-ngl 99`, `-c 262144 --parallel 4` |
| embeddings | Qwen3-Embedding-4B (Q4_K_M GGUF, 2560-dim) | 8081 | llama.cpp (old build), GPU/Vulkan, `--embedding --pooling last` |
| reranker | ettin-reranker-400m-v1 (safetensors) | 8082 | torch + sentence-transformers sidecar, CPU-only |
| transcription | Whisper large-v3-turbo (GGML) | 8083 | whisper.cpp, GPU/Vulkan (optional) |

> The embed server intentionally stays on the OLD llama.cpp binary so Qwen3-Embedding-4B
> vector numerics remain byte-stable with the 2560-dim vectors already stored in Postgres.
> The text server uses the newer b9592 build (fixes a SIGSEGV / empty-tagger-output on
> this model). Do not swap binaries casually.

## Key files and env vars

### Scripts (entrypoints)
| File | Purpose |
|------|---------|
| `scripts/dev-up.sh` | **Single entrypoint** — start/stop/status for the entire local stack |
| `scripts/start-models.sh` | Start/stop/status host model servers + relays (called by dev-up.sh) |
| `scripts/model-watchdog.sh` | Auto-restart dead model servers (background, called by dev-up.sh) |
| `test/scripts/host_relays.sh` | Manage socat relays for container→host (called by start-models.sh) |
| `test/run.sh` | Full E2E test harness (build + stack + pytest + teardown) |

### Config files
| File | Purpose |
|------|---------|
| `test/.env` | E2E env vars (`E2E_LLAMA=host`, bootstrap admin creds, model file names, timeouts) |
| `test/config.e2e.host.toml` | App config for host-llama mode (backends pointed at relays) |
| `test/compose.e2e.host.yaml` | Compose overlay — wires `WHISPER_URL` to relay, no inference containers |
| `compose.yaml` | Base compose file (postgres, tika, app, caddy, optional workers) |
| `compose/Caddyfile` | Caddy reverse proxy config (TLS, security headers, logging) |

### Env vars that control startup
| Var | Default | Purpose |
|-----|---------|---------|
| `E2E_LLAMA` | `host` | `host` = reuse host-native models via relays; `container` = containerized inference |
| `HEALTH_TIMEOUT` | `600` | Max seconds to wait for `https://localhost:9443/health` |
| `LLAMA_BIN` | `~/.local/bin/llama-server` | Path to llama-server (embed server) |
| `LLAMA_BIN_TEXT` | `~/.local/lib/llama-b9592/llama-server` | Path to newer llama-server (text server) |
| `MODEL_DIR` | `./models` | Directory containing GGUF model files |
| `ETTIN_VENV` | `~/.venvs/ettin-rerank` | Python venv for the ettin reranker sidecar |
| `APP_IMAGE` | `kb:latest` | Podman image tag for the app container |

## Web access

- **URL:** `https://localhost:9443`
- **Login:** `admin@local.kb` / `admin`
- **Cert:** Self-signed (Caddy, `test/Caddyfile.e2e`)

## Common troubleshooting

- **`just` not on PATH** — ramdisk was wiped. Re-run `bash scripts/bootstrap-dev.sh` to
  reinstall cargo tools, or just `cargo install just` for a quick fix, or use
  `bash scripts/dev-up.sh` directly.
- **App 503 / search returns nothing right after a model hot-swap** — the rerank
  circuit breaker is briefly open; wait ~1-2 min for health probes to recover, or
  `just down && just up` for a clean start.
- **`host.containers.internal` unreachable from the app** — relays aren't up.
  Run `bash scripts/dev-up.sh status` to check.
- **Tagger 500 / empty content** — Qwen3.6-VL is a reasoning model; the startup
  script passes `--reasoning-budget 0 --chat-template-kwargs '{"enable_thinking":false}'`
  which prevents this. If running manually, ensure those flags are present.
- **Whisper missing after reboot** — `/tmp` was wiped; the auto-bootstrap in
  `dev-up.sh` rebuilds it automatically.
- **App crash-loop: "migration 1 was previously applied but has been modified"** — the
  Postgres volume has stale migration state from a prior schema version. Run
  `just down && just up` to wipe and recreate. `just down` does `podman compose down -v`
  which destroys the `pgdata` volume; `just up` reinitialises it cleanly.
- **Postgres won't start** — `--data-checksums` is baked into the very first
  `initdb`. If changing `POSTGRES_INITDB_ARGS`, delete the `pgdata` volume first
  (`podman volume rm local-kb_pgdata`).

## Running the E2E test suite against the local stack

```bash
E2E_LLAMA=host E2E_RECORD=1 ./test/run.sh pytest tests -m "not judge and not perf"
```

For the full quality gate (includes LLM-as-judge lane):
```bash
E2E_LLAMA=host ./test/run.sh quality
```

## What survives a reboot (and what doesn't)

| Survives | Must be restarted |
|----------|-------------------|
| `./models/*.gguf` | Host model servers (started by hand, not systemd services) |
| `~/.venvs/ettin-rerank` | Socat relays |
| Podman images + named volumes | whisper.cpp build in `/tmp` (auto-bootstrapped by dev-up.sh) |
| `test/.env` | Model watchdog (auto-started by dev-up.sh) |

After a reboot, just run `just up` — the script handles everything that needs
restarting.
