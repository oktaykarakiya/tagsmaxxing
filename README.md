# TagsMaxxing — Local File Knowledge Base

**https://tagsmaxxing.com**

> ⚠️ **This project is experimental software in active development. It is not yet ready
> for production use — features, APIs, and data formats may change without notice.
> Do not rely on it for real data. Contributions and feedback are welcome.**

A self-hosted, modular tool that ingests files of **any** type, enriches them with an
LLM-generated title/summary/tags plus your notes and extracted metadata, stores everything in
Postgres, and retrieves it via **hybrid (vector + keyword) search with reranking** — all
inference on **local llama.cpp servers** behind a slot-aware, multi-host scheduler.

Full design: [`local-kb-plan.md`](./local-kb-plan.md). Architecture:
[`ARCHITECTURE.md`](./ARCHITECTURE.md). Build protocol:
[`CLAUDE.md`](./CLAUDE.md). Contributing:
[`CONTRIBUTING.md`](./CONTRIBUTING.md).

## Use cases

TagsMaxxing is built to be a **searchable knowledge base and AI memory** for teams drowning in
information — everything stays on your own infrastructure (your llama.cpp servers, your Postgres),
with per-tenant RLS isolation and at-rest encryption.

- **Company documentation that's actually findable.** Ingest every doc, PDF, spec, wiki export,
  spreadsheet, code file, image, and audio/video recording. Each is auto-titled, summarised, and
  tagged by the LLM (plus your own notes), then indexed for **hybrid (vector + keyword) search with
  reranking** — so you find the right passage even when you don't remember the exact wording or
  filename, with provenance deep-links back to the source. It solves the common failure mode where
  *"the documentation exists, but nobody can find anything in it."*
- **An AI memory for work.** Use it as a persistent, queryable context store for your team's AI
  workflows: drop in notes, transcripts, decisions, runbooks, and reference material, then retrieve
  exactly the relevant context on demand. Instead of re-pasting or re-deriving knowledge every
  session, your tooling reads from *your* curated memory — a more efficient way to put AI to work on
  real company knowledge.
- **A private, local alternative to cloud "chat-with-your-docs" tools.** No documents leave your
  network; embedding, tagging, reranking, and (optional) audio/video transcription all run on local
  models.

## Quickstart

> Running the full local model stack on the dev box (host-llama mode), or bringing everything back
> up after a reboot? See **[`docs/LOCAL_RUNBOOK.md`](docs/LOCAL_RUNBOOK.md)**.

```bash
# 1. Clone
git clone git@github.com:oktaykarakiya/tagsmaxxing.git
cd tagsmaxxing

# 2. Start the sidecar stack (Postgres + pgvector, Apache Tika)
cp .env.example .env           # optional: override ports / credentials
podman compose up -d           # healthchecks pass in ~9s

# 3. Create a config file (or copy and edit the example)
cp config.example.toml config.toml   # edit DB credentials if needed

# 4. Build and run the app
cargo run --bin kb -- serve --config config.toml
# Open https://localhost:9999 in your browser (HTTP without TLS: omit cert/key)

# 5. Run with TLS (self-signed cert for local dev)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost"
cargo run --bin kb -- serve --config config.toml \
  --tls-cert cert.pem --tls-key key.pem

# 6. Ingest a file via CLI
echo "Hello, world." > example.txt
cargo run --bin kb -- ingest example.txt --note "my first file"

# 7. Search
cargo run --bin kb -- search "hello"

# 8. Tear down
podman compose down -v
```

> **GPU inference** (llama.cpp, whisper.cpp) uses a separate profile:
> `podman compose --profile gpu up -d`. See `compose.cpu.yaml` for CPU-only overrides.
> All commands work under **Podman only** — no Docker engine required.

## Web UI

The app ships a full SaaS web interface at **https://tagsmaxxing.com** (or your own domain):

| Page | Path | What it does |
|------|------|-------------|
| Landing | `/` | Public hero, features, pricing, FAQ |
| Sign up | `/signup` | Create an account with email verification |
| Login | `/login` | Multi-tenant login (tenant slug + email + password) |
| Dashboard | `/dashboard` | Document/file/tag counts, quota bars, daily token chart, activity feed |
| Upload | `/upload` | Drag-and-drop or file-picker upload with voice-note recording, group-as-document toggle |
| Search | `/search` | Natural-language hybrid search with reranked results |
| Account | `/account` | Profile, plan, team, API tokens, danger zone |
| Admin | `/admin` | Tenants, users, providers, models, routes, jobs, tags, decrypt audit |

All pages are server-rendered with Askama templates and Tailwind CSS + HTMX for
interactivity. No JavaScript framework required on the client.

## CLI

```bash
# Ingest files
kb ingest [--as-document] [--note "…"] [--no-wait] FILES...

# Search
kb search [--kind image] [--tag invoice] [--limit 5] "quarterly report"

# Start the API + web server
kb serve [--port 9999] [--config config.toml]
         [--tls-cert cert.pem] [--tls-key key.pem]

# Run a standalone ingest worker (drains the shared queue with THIS
# machine's model backends; see "Queued ingestion" below)
kb worker [--config config.toml] [--concurrency 4]
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      User interfaces                          │
│  ┌─────────┐  ┌───────────┐  ┌──────────┐  ┌─────────────┐  │
│  │  Web UI  │  │  REST API │  │   CLI    │  │Folder Watch │  │
│  │(Askama   │  │(axum :9999│  │ (clap)   │  │ (notify)    │  │
│  │ +HTMX)   │  │           │  │          │  │             │  │
│  └────┬─────┘  └─────┬─────┘  └────┬─────┘  └──────┬──────┘  │
├───────┼──────────────┼─────────────┼────────────────┼────────┤
│       │         kb-api (orchestrates via kb-pipeline)  │       │
│       └──────────────┬──────────────┘─────────────────┘       │
│                      │                                        │
│  ┌───────────────────▼──────────────────────────────────┐    │
│  │                  kb-pipeline                           │    │
│  │  IngestPipeline   RetrievalPipeline   JobQueue         │    │
│  │  TagCanonicalizer Chunker+Embedder   ExportPipeline   │    │
│  └───┬───────────────┬───────────────┬──────────────────┘    │
│      │               │               │                       │
│  ┌───▼────┐  ┌───────▼──────┐  ┌─────▼──────┐               │
│  │kb-extract│ │   kb-llm    │  │  kb-store  │               │
│  │text/code │ │Tagger       │  │PgStore     │               │
│  │Tika      │ │LlamaClient  │  │LocalBlob   │               │
│  │image/EXIF│ │Reranker     │  │HybridSearch│               │
│  │audio/vid │ │             │  │SessionStore│               │
│  │binary    │ │             │  │(Postgres+  │               │
│  └──────────┘ └──┬──────────┘  │ pgvector)  │               │
│                  │             └────────────┘               │
│          ┌───────▼────────┐                                 │
│          │  kb-scheduler   │                                 │
│          │  Pool.acquire() │  slot-aware, multi-host         │
│          │  HealthLoop     │  load balancer                  │
│          └───────┬────────┘                                 │
│                  │                                           │
│  ┌───────────────▼──────────────────────────────────────┐   │
│  │  llama.cpp servers  (OpenAI-compatible HTTP)          │   │
│  │  chat · embed · rerank · vision · whisper             │   │
│  └──────────────────────────────────────────────────────┘   │
├──────────────────────────────────────────────────────────────┤
│  kb-core — domain types + capability traits (I/O-free)       │
│  kb-config — typed, validated, hot-swappable config          │
│  kb-metrics — Prometheus + optional OTLP                     │
│  kb-logging — structured logging, size-based rotation        │
└──────────────────────────────────────────────────────────────┘
```

**Queued ingestion (P15):** uploads return `202 {job_id, document_id}` in
milliseconds — the bytes are stored, a `pending` document is staged, and a
durable Postgres-backed job queue (leases, heartbeats, retries with backoff,
dead-letter + tenant retry) drives the processing on background workers.
`kb serve` embeds a worker pool; dedicated processes scale it out:

```bash
# same box:
podman compose --profile worker up -d --scale worker=2
# another machine (its own config.toml = its own model slots; requires
# [blob] backend = "s3" so it can fetch staged bytes):
kb worker --config /etc/kb/config.toml --concurrency 4
```

Rollback lever: set `[ingest] mode = "inline"` in config.toml (hot-reloaded,
no restart) to restore the synchronous pipeline.

**Document Source Sync (P17):** give a document a `source_url` and it stays
current automatically. The system periodically re-fetches the URL, compares
content hashes, and on change re-ingests the new content with an LLM-generated
"what changed" summary and an immutable version snapshot. Old chunks are
soft-deleted (tombstoned), search always uses the latest version. Configurable
per-document interval, global scanner loop, SSRF-guarded fetcher. See
`[source_sync]` in `config.example.toml`.

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for a full crate tour, data-flow diagrams,
and the rationale behind key design decisions.

## Workspace

```
crates/
├── core/           domain types + capability traits (stable, I/O-free)
├── config/         TOML config + env overlay + hot-reload
├── scheduler/      multi-host, slot-aware model pool
├── llm/            llama.cpp client (chat, embed, rerank) + tagger
├── extract/        Extractor implementations (text, code, Tika, image, audio, video, binary)
├── store/          Postgres + pgvector (migrations, PgStore, hybrid search, blob, sessions)
├── pipeline/       ingest + retrieval orchestration, job queue, tag canonicalizer
├── api/            axum HTTP API (:9999) + clap CLI + Askama/HTMX web UI
├── metrics/        Prometheus metrics + optional OTLP tracing
├── logging/        structured logging, size-based rotation, correlation spans
├── mock-backend/   in-process mock HTTP server for deterministic tests
├── testsupport/    shared harness: per-binary pgvector container, two-role DB
└── cov-gate/       per-group line-coverage floor enforcement (LCOV parser)
```

## Develop

Prerequisites: Rust (pinned to 1.92.0 via `rust-toolchain.toml`) and the gate tools:

```bash
just bootstrap-tools        # install just, cargo-deny, cargo-audit, cargo-llvm-cov
```

Run the full Definition-of-Done gate suite (identical to CI):

```bash
just ci                     # fmt-check, build, clippy -D warnings, test, deny, audit, coverage
```

Individual gates: `just fmt`, `just clippy`, `just test`, `just deny`, `just audit`,
`just cov`. List everything with `just`.

The Podman-backed integration lane (`just ci-integration`) runs the `#[ignore]`
testcontainers suites against a real Postgres and enforces higher coverage floors on
`kb-store` + auth paths.

## Run the sidecar stack

The stateful sidecars — Postgres (with **pgvector**, `data-checksums` on) and **Apache Tika** —
come up from a single [`compose.yaml`](./compose.yaml) via **Podman** (this project targets
Podman exclusively — no Docker):

```bash
cp .env.example .env        # optional: override ports / image tags / credentials
podman compose up -d
```

Both services declare healthchecks, so `podman compose ps` reports `healthy` once ready.
Postgres is published on `127.0.0.1:5432` and Tika on `127.0.0.1:9998` by default
(change `POSTGRES_PORT` / `TIKA_PORT`). The app serves on **port 9999**. Tear down with
`podman compose down` (add `-v` to also drop the data volume).

## Configuration

The app reads a TOML config file (default: `config.toml`). Multi-backend setups route
requests to different models on different ports by role:

```toml
[storage]
postgres_url = "postgres://kb:kb@127.0.0.1:5432/kb"
app_postgres_url = "postgres://kb_app:kb_app@127.0.0.1:5432/kb"

[api]
port = 9999
secure_cookies = true

[scheduler]
acquire_timeout_secs = 120
health_interval_secs = 10
max_retries = 3

[[backend]]
id = "qwen3.6-vl"
base_url = "http://127.0.0.1:8080/v1"
roles = ["text", "vision", "code"]
slots = 2
priority = 0

[[backend]]
id = "qwen3-embed-4b"
base_url = "http://127.0.0.1:8081/v1"
roles = ["embed"]
slots = 4
priority = 0

[[backend]]
id = "ettin-rerank"
base_url = "http://127.0.0.1:8082/v1"
roles = ["rerank"]
slots = 4
priority = 0
```

The scheduler acquires a free slot from the pool before forwarding requests, so backends
are never oversubscribed. Routing is hot-reloadable from the database (`/admin/routes`).

## Deploy with Quadlet (systemd)

For single-node Podman users, **Quadlet `.container` files** are the recommended production
method (plan §14). Quadlet translates `.container` / `.pod` / `.volume` files into native
systemd units, so your stack starts on boot, restarts on failure, and logs via `journalctl`.

All files are in [`quadlet/`](./quadlet). Copy them into the systemd user unit directory:

```bash
mkdir -p ~/.config/containers/systemd
cp quadlet/* ~/.config/containers/systemd/
```

### Units

| File | What it runs |
|------|-------------|
| `kb.pod` | Shared pod — all containers share a network namespace (services reach each other via `localhost`) |
| `kb-postgres.container` | Postgres 17 + pgvector (data-checksums on, §23), persistent volume |
| `kb-tika.container` | Apache Tika 3.3 (document text/metadata extraction, §2) |
| `kb-app.container` | The `kb` binary — API on `:9999` (`/health`, search, ingest, web UI) |
| `kb-llama.container` | llama.cpp server — optional GPU inference (chat + embed + rerank) |
| `kb-whisper.container` | whisper.cpp server — optional GPU transcription |
| `kb-pgdata.volume` | Persistent named volume for Postgres data |
| `kb-app-data.volume` | Persistent named volume for the app (blob store + logs) |

### Quick start

```bash
# 1. Build the app image
podman build -t kb:latest .

# 2. Place your GGUF models (optional — only for GPU inference)
mkdir -p ~/.config/containers/systemd/models
cp /path/to/model.gguf ~/.config/containers/systemd/models/model.gguf
cp /path/to/whisper-model.gguf ~/.config/containers/systemd/models/whisper-model.gguf

# 3. Edit the container files to set passwords / secrets:
#    - kb-postgres.container: POSTGRES_PASSWORD
#    - kb-app.container: SESSION_SECRET (≥32 bytes), POSTGRES_URL passwords

# 4. Reload systemd and start the core stack
systemctl --user daemon-reload
systemctl --user start kb-postgres.service kb-tika.service kb-app.service

# 5. (Optional) Enable GPU inference
systemctl --user start kb-llama.service kb-whisper.service

# 6. Verify
curl http://localhost:9999/health
journalctl --user -u kb-app.service -f

# 7. Enable on boot
systemctl --user enable kb-postgres.service kb-tika.service kb-app.service
```

> **Inside the pod** all containers share `localhost`, so the app connects to Postgres at
> `localhost:5432` and Tika at `localhost:9998`. The pod publishes ports to the host
> (loopback by default — remove `127.0.0.1:` in `kb.pod` for LAN access).
>
> **GPU services** have `AddDevice=nvidia.com/gpu=all`. Remove that line to run CPU-only,
> or just don't enable those two units. See `compose.cpu.yaml` for the equivalent compose
> override.

To validate your files before starting:

```bash
/usr/libexec/podman/quadlet -dryrun -user
```

## Logging

The app uses `tracing` with configurable format and size-based rotation enforcing a hard
`LOG_MAX_GB` cap (plan §18). Configure via `.env`:

```bash
LOG_LEVEL=info      # trace, debug, info, warn, error
LOG_FORMAT=json     # json or pretty
LOG_DIR=./logs
LOG_MAX_GB=5        # enforced hard cap via file-rotate
```

## SBOM — Software Bill of Materials

Generate a [CycloneDX](https://cyclonedx.org/) SBOM for dependency inventory and
compliance:

```bash
# Install once
cargo install cargo-cyclonedx

# Generate (JSON or XML)
cargo cyclonedx --format json --output-file sbom.cdx.json

# Or use the recipe
just sbom
```

The SBOM includes every Rust dependency with version, licence, and provenance metadata.
Intended as a CI artifact for release pipelines.

## Autonomous build loop

Development is **ledger-driven**: one small task at a time, each gated by `just ci`
(see [`CLAUDE.md`](./CLAUDE.md) / plan §31). Inspect and drive it:

```bash
just status                 # ledger summary + next eligible task
just next                   # just the next task id
just loop                   # run the loop until a checkpoint / blocked / phase complete
just loop --approve P1-T1   # clear a human-review checkpoint and continue
just loop --max 1           # do exactly one task
just loop --dry-run         # show the next task without invoking anything
```

The loop invokes Claude Code per task, then **independently re-runs the gates** and confirms
the task committed before advancing. It stops immediately on a red gate, a blocked task, or an
unapproved checkpoint — so progress is always safe and resumable.

## Bug-fix pipeline

A separate autonomous loop fixes bugs surfaced by the E2E test suite, using conflict-aware
parallel batching so multiple Claude Code instances fix independent bugs simultaneously
without file conflicts:

```bash
./scripts/fix-loop.sh                 # full run (baseline → fix → compare)
./scripts/fix-loop.sh --dry-run       # preview what would be fixed
./scripts/fix-loop.sh --yolo          # unattended
./scripts/fix-loop.sh --max-batches 3 # stop after 3 batches
./scripts/fix-loop.sh --batch-size 4  # max 4 parallel fixes per batch

python3 scripts/bug_ledger.py status  # summary of all tracked bugs
```

Bugs are tracked in [`BUG_LEDGER.toml`](./BUG_LEDGER.toml) (~72 entries, 17 conflict groups).
Each batch picks one bug from each non-conflicting group, fixes them in parallel via
`scripts/fix-bug.sh`, then independently verifies `just ci` + the full E2E suite against a
pre-fix baseline. If a fix introduces a regression (a previously-passing test now fails),
the loop halts. See [`scripts/fix-loop.sh`](./scripts/fix-loop.sh) for the full design.

## E2E test suite

A comprehensive black-box test harness lives in [`test/`](./test/). It drives the app over
HTTPS at `https://localhost:9443` (TLS verification off, via Caddy) and encodes the intended
contract: **tests assert correct behavior; failures are app bugs — never fix the test.**

```bash
./test/run.sh setup                  # create venv + install deps
./test/run.sh up                     # build + start the full stack (podman compose)
./test/run.sh run                    # functional suite (fast lane, no judge)
./test/run.sh quality                # functional + DeepSeek LLM-as-judge (semantic quality)
./test/run.sh perf                   # load / performance lane
./test/run.sh down                   # tear down

# Drive the implement.sh swarm to auto-build PENDING test stubs:
./test/implement.sh run              # spawn agents (MAX_AGENTS=5), one per catalog file
./test/implement.sh status           # count pending / blocked / done
```

Key fixtures (defined in [`test/conftest.py`](./test/conftest.py)): `api` (black-box HTTP
client), `page` (Playwright browser), `judge` (DeepSeek LLM-as-judge for nondeterministic
quality assertions). Results are recorded to `test/results/history.csv` when `E2E_RECORD=1`
is set — the `run`, `quality`, and `perf` lanes set it automatically.

The test catalog ([`test/CATALOG.md`](./test/CATALOG.md)) tracks ~324 tests across 35 files.
Stubs with `@pytest.mark.skip(reason="step2: ...")` are PENDING (the swarm picks them up);
stubs with `reason="blocked: ..."` require missing features (SSO, Stripe, B2) and stay
catalogued as acceptance criteria.

## License

**Open-source edition:** Licensed under the [GNU Affero General Public License v3.0 or
later](./LICENSE) — see the [`LICENSE`](./LICENSE) file for the full text. The AGPLv3 is a
strong copyleft license that closes the "network service loophole": if you modify the
software and make it available to users interacting with it over a computer network, you
must make your modifications available under the same license.

**Commercial license:** For enterprises that need to integrate this software into
proprietary products without the copyleft obligations of the AGPL, a commercial license is
available. See [`LICENSE.COMMERCIAL`](./LICENSE.COMMERCIAL) for details.

**Contributor License Agreement:** All contributions are accepted under the terms of
[`CLA.md`](./CLA.md). See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for instructions.

**Model weights are NOT covered by this license.** The LLM models (llama.cpp GGUF files,
whisper models, etc.) are separately licensed by their respective providers. This project
does not distribute any model weights — operators must obtain them from the model
providers under each model's own terms.
