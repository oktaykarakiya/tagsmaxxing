# Architecture — Local File Knowledge Base

This document describes the architecture of the Local File Knowledge Base: its crate
topology, the two main data flows (ingest + retrieval), and the rationale behind key
design choices. For the authoritative specification see
[`local-kb-plan.md`](./local-kb-plan.md); for build procedure see
[`CLAUDE.md`](./CLAUDE.md).

## Crate tour

```
crates/
├── core/           Domain types + capability traits (the stable, I/O-free contract)
├── config/         TOML config loader + env overlay + hot-reload (arc_swap + notify)
├── scheduler/      Multi-host, slot-aware model pool — the centrepiece of §6
├── llm/            llama.cpp OpenAI-compatible client (chat, embed, rerank) + tagger
├── extract/        Extractor implementations (text, code, Tika, image, audio, video, binary)
├── store/          Postgres + pgvector (migrations, PgStore, hybrid search, session store, blob)
├── pipeline/       Ingest orchestrator + retrieval pipeline + job queue + tag canonicalizer
├── api/            axum HTTP API (port 9999) + clap CLI + Askama/HTMX web UI
├── metrics/        Prometheus metrics + optional OTLP tracing (plan §15, §18)
├── logging/        size-based log rotation, JSON/pretty format, correlation spans (plan §18)
├── mock-backend/   In-process mock HTTP server for deterministic scheduler/llm tests
├── testsupport/    Shared test harness: per-binary pgvector container + two-role DB (P6-T14)
└── cov-gate/       Build tool enforcing per-group line-coverage floors from LCOV (P6-T0)
```

### `kb-core` — domain types + capability traits

This is the **stable, I/O-free contract**. Every other crate depends on it; it depends on
none of them. It holds:

- **Domain types** mirroring the Postgres schema (plan §5): `Tenant`, `User`, `Document`,
  `FileRecord`, `Chunk`, `Tag`, `TagAlias`, `Job`, `UsageEvent`, `Sha256`, and string-enums
  (`DocKind`, `JobStatus`, `UserRole`, `Role`).
- **Capability traits** (`Extractor`, `Tagger`, `Embedder`, `Reranker`, `Store`,
  `ProviderAdapter`, `Blob`, `SessionStore`) — interfaces that concrete crates implement.
  Depending on traits rather than concretions is what keeps each component swappable and
  independently testable.

*Key decision (plan §4):* `PageImage` carries encoded bytes, not `image::DynamicImage`
(keeps core codec-free). `TagOutput` has no `category` field (multi-label tags with
semantic canonicalization replace single-category bucketing — plan §1). `ProviderAdapter`
takes a core `ProviderConn` rather than the scheduler's `Backend` (avoids a dependency
cycle between `kb-scheduler` ↔ `kb-llm`).

### `kb-config` — typed, validated, hot-swappable configuration

Parses `config.toml` (plan §6) into typed structs, overlays environment variables,
validates the result, and exposes it behind an `arc_swap::ArcSwap<Config>` handle.
Call sites resolve `config.current()` per use so that runtime edits (model endpoints,
API keys, rates) take effect without a restart — the hot-swap rule from `CLAUDE.md`.

Also handles file-watch hot-reload via `notify` so editing `config.toml` on a live
system triggers `reload()`. Defaults `api.port` to **9999**.

### `kb-scheduler` — slot-aware, multi-host model pool

The centrepiece of the architecture (plan §6). Holds `Backend` objects (one per
llama.cpp server, each with a `tokio::sync::Semaphore` of N slots matching
`--parallel N`), indexed by `Role` (Chat, Embed, Rerank, Vision, …) in a `DashMap`.

Key components:

| Component | Role |
|-----------|------|
| `Backend` | One OpenAI-compatible server: base URL, roles, priority, semaphore, health flag |
| `Lease` | RAII guard holding exactly one slot — dropped at end of an LLM call |
| `Pool` | `acquire(role)` → walk healthy candidates sorted by priority then free slots; if none free, `await` the first available via `FuturesUnordered` bounded by timeout |
| `HealthLoop` | Background task polling `GET {base}/health` every interval, updating `Backend::healthy` |
| `AcquireError` | `NoBackend`, `Timeout`, `Closed` — why a slot could not be obtained |

### `kb-llm` — OpenAI-compatible client with failover

`LlamaClient` owns a `Pool` + `reqwest::Client` and routes every `chat()` / `embed()` /
`rerank()` through the failover loop (plan §6.4): acquire a slot → POST to backend →
on transport error/5xx, mark unhealthy + advance circuit-breaker + retry (bounded by
`max_retries`). Circuit-breaker: K consecutive failures → cooldown period during which
the backend is skipped even if the health loop re-marks it healthy.

Also houses `JsonSchemaTagger` (implements the `Tagger` trait — prompt + `json_schema`
with `strict=true`, bracketing untrusted user content with defence-in-depth against
prompt injection) and `LlamaReranker` (implements the `Reranker` trait for cross-encoder
scoring).

### `kb-extract` — `Extractor` implementations

Per-filetype extraction, routed by `DocKind`:

| Module | What it handles | How |
|--------|----------------|-----|
| `text` | `.txt`, `.md`, `.html`, `.log` | UTF-8 decode → `Extracted { text, meta:{}, page_images:[] }` |
| `code` | Source files | UTF-8 decode (tree-sitter chunking in P3) |
| `tika` | PDF, DOCX, PPTX, XLSX, ODT, RTF | HTTP PUT to Apache Tika `/tika`, hot-swappable base URL |
| `image` | JPEG, PNG, GIF, WebP, TIFF | Magic-byte validation + EXIF harvest via `kamadak-exif` → `PageImage` |
| `audio` | WAV, MP3, OGG, FLAC, M4A | ffmpeg transcode to 16kHz mono → whisper transcription via OpenAI-compatible `/v1/audio/transcriptions` |
| `video` | MP4, MKV, WebM, MOV | ffmpeg scene-change keyframes (cap ~40) + audio extraction → whisper + ffprobe metadata |
| `binary` | Unknown / archive | `tree_magic_mini` MIME, `file` subprocess, printable-string extraction, optional archive listing |

### `kb-store` — Postgres + pgvector persistence

The only crate that talks to the database. Ships:

- **Schema migrations** (plan §5): forward-only SQL under `migrations/` — extensions,
  tables, indexes (HNSW + GIN), RLS policies, the two-role model (privileged `kb` +
  non-superuser `kb_app` for tenant isolation, migration `0006`).
- **`PgStore`**: implements the `Store` trait (`upsert_file`, `upsert_chunks`,
  `upsert_document`, `insert_usage_event`, `transactional_ingest`, `hybrid_search`,
  tag CRUD methods, user CRUD, admin queries).
- **`LocalBlob`**: implements the `Blob` trait against the local filesystem —
  content-addressed key layout (`{root}/{prefix}/{blob_key}`). This is the dev
  backend; B2 replaces it in P8.
- **`PgSessionStore`**: implements `SessionStore` for cookie-based auth sessions
  (plan §13). `InMemorySessionStore` is a dev/test alternative.
- **`hybrid_search`**: runs vector (HNSW cosine via `<=>`) + keyword (ts_rank on
  `websearch_to_tsquery`) queries, fuses with RRF (k=60), and rolls up to
  `Hit` structs with deep-link provenance.

### `kb-pipeline` — ingest + retrieval orchestration

The "business logic" crate. Wires all other crates together:

| Module | Purpose |
|--------|---------|
| `document_builder` | Construct `Document` + `FileRecord`s from raw bytes (SHA-256, MIME, blob keys) |
| `metadata_merge` | Merge per-page `Extracted` outputs into document-level text + meta |
| `tag_canonicalizer` | Deduplicate raw LLM tags against a tenant's existing tag set (alias lookup + cosine similarity ≥ 0.85) |
| `chunker` | Split extracted text into overlapping chunks (~512 tokens, 64-char overlap) with provenance |
| `embedder` | Batch-embed chunk content + tag names (batches ≤ 32), verifying output dim = 1024 |
| `ingest` | `IngestPipeline`: full P3 flow (build → extract → merge → tag → canonicalize → chunk → embed → transactional upsert) |
| `retrieval` | `RetrievalPipeline`: embed query → hybrid search → RRF → rerank → top-K hits |
| `job_queue` | Durable Postgres-backed queue: enqueue, atomic claim (`SKIP LOCKED`), exponential backoff, dead-letter, worker pool |
| `export` | Streaming ZIP + JSONL export of a tenant's full data (data portability, plan §13) |
| `folder_watcher` | `notify`-based directory watcher: detect new/modified files, debounce, enqueue ingest jobs |
| `eval` | Retrieval evaluation: labelled query set, recall@k, reciprocal rank, regression check |
| `rrf` | Reciprocal Rank Fusion — re-exports from `kb_core::query` (placed in core to avoid a cycle) |

### `kb-api` — HTTP API + CLI + web UI

The user-facing interface crate:

- **axum HTTP server** on port **9999**: `POST /api/ingest` (multipart upload), `GET /api/search`,
  `GET /api/documents/:id`, `GET /api/documents/:id/file/:file_id`, `GET /api/jobs/:id`.
- **Auth middleware** + handlers for `POST /auth/login`, `POST /auth/register`, `POST /auth/logout`.
- **Web UI** with Askama templates + Tailwind CSS + HTMX: search page, upload page (drag-and-drop),
  document detail page (tag management, page reordering), admin panel.
- **clap CLI**: `kb ingest <FILES...>` and `kb search <QUERY>` for headless/scripted use.
- **Bootstrap**: first-run tenant + admin user creation from env vars.

### `kb-metrics` — Prometheus + optional OTLP

Installs a global Prometheus recorder, registers metrics (per-backend health, slot
usage, queue depth, p50/p95 latency, throughput, error rate, storage used, active users),
and renders the `GET /metrics` endpoint. The optional `otlp` feature enables
OpenTelemetry span export.

### `kb-logging` — structured logging + rotation

`tracing` subscriber with size-based rotation via `file-rotate` (`ContentLimit::Bytes` +
`FileLimit::MaxFiles`), enforcing `LOG_MAX_GB`. JSON or pretty format. Correlation spans
carrying `request_id`, `tenant_id`, `file_id`.

### Support crates

- **`kb-mock-backend`**: axum server on ephemeral port with scriptable `Scenario`
  (healthy/unhealthy/slow/5xx/429/chat/embed/rerank). Used pervasively in `kb-scheduler`
  and `kb-llm` tests for deterministic, no-flake testing (plan §31.3).
- **`kb-testsupport`**: shared harness for `#[ignore]` testcontainers integration suites:
  one pgvector container per test binary, per-test fresh database, admin + app role URLs.
- **`kb-cov-gate`**: parses LCOV coverage reports and enforces per-group line-coverage
  floors (P6-T0). Drives the `just ci-integration` lane.

## Data flow

### Ingest path (P0–P3)

```
                          ┌──────────────┐
   Raw bytes ────────────►│DocumentBuilder│──► (Document, FileRecords)
                          └──────┬───────┘
                                 │
                    ┌────────────▼──────────┐
                    │   Blob::put (LocalBlob)│       content-addressed storage
                    └────────────┬──────────┘
                                 │
              ┌──────────────────▼───────────────────┐
              │  Extract (route by DocKind)            │
              │  text/code │ tika │ image │ audio/video│
              └──────────────────┬───────────────────┘
                                 │
                          ┌──────▼──────┐
                          │MetadataMerge│──► merged text + meta
                          └──────┬──────┘
                                 │
                          ┌──────▼───────┐
                          │ JsonSchemaTagger │──► {title, summary, tags[]}
                          └──────┬───────┘
                                 │
                          ┌──────▼─────────┐
                          │TagCanonicalizer │──► canonical tag_ids
                          └──────┬─────────┘
                                 │
                          ┌──────▼──────┐
                          │  Chunker     │──► overlapping chunks
                          │  + Embedder  │      with 1024-dim vectors
                          └──────┬──────┘
                                 │
                          ┌──────▼────────────┐
                          │TransactionalIngest│──► document.status = 'ready'
                          │ (one DB txn)       │     + usage events written
                          └───────────────────┘
```

### Retrieval path (P4)

```
                          ┌────────────┐
   Query text ───────────►│Embed (Query)│──► 1024-dim vector
                          └──────┬─────┘
                                 │
                    ┌────────────▼────────────────┐
                    │ Store::hybrid_search          │
                    │ ┌──────────┐ ┌─────────────┐ │
                    │ │Vector N  │ │Keyword N     │ │
                    │ │(HNSW cos)│ │(ts_rank+tsv) │ │
                    │ └────┬─────┘ └──────┬──────┘ │
                    │      └───┬──┬───────┘        │
                    │     RRF fuse (k=60)           │
                    │     Document roll-up          │
                    └────────────┬────────────────┘
                                 │
                          ┌──────▼──────┐
                          │  Reranker    │──► re-scored top-K
                          └──────┬──────┘
                                 │
                          ┌──────▼──────┐
                          │  Top-n Hits  │──► {title, snippet, score, page_no, file_id, …}
                          └─────────────┘
```

## Design decisions (from plan §1)

### One sparse-MoE multimodal workhorse, not one model per filetype

A 30B-A3B VLM (3B active params) handles documents, code, images, and video frames at
~3B inference cost. Specialising only where it clearly pays (code, on a second host) and
relying on a single workhorse avoids VRAM fragmentation from loading N distinct models
into 32 GB. This also simplifies prompt-template maintenance.

### llama.cpp addressed only via its OpenAI-compatible HTTP server

No in-process bindings. Every backend is just a base URL, making "another machine"
trivial — the load-balancer already speaks HTTP. Vision uses `--mmproj`; embeddings/rerank
use dedicated instances with `--embedding`.

### Slot count is the load-balancing primitive

Each `llama-server` runs with `--parallel N`. The scheduler holds a semaphore of exactly
N permits per backend, so we never oversubscribe (which would silently serialise inside
llama.cpp). A `Lease` holds the slot and drops it when the call completes.

### No single "category" field — multi-label tags with semantic canonicalization

Free-text LLM categories fragment into synonyms ("invoice"/"bill"/"receipt") and force
one bucket onto multi-faceted files. Instead, multi-label tags are canonicalised via
alias lookup and cosine similarity (threshold 0.85), so identical items retrieve together
regardless of label. Embeddings already cluster synonyms.

### Multi-tenant from day one

Every data-bearing row carries `tenant_id`. Postgres Row-Level Security (RLS) enforces
isolation as a defence-in-depth layer below application-level checks. The connection-role
model (P6-T14) uses a non-superuser `kb_app` role for tenant-scoped queries so RLS cannot
be bypassed.

### The app node is stateless and disposable

All durable state lives in Postgres + a Backblaze B2 object store. Local disk holds only
cache + logs. Any node can be destroyed and rebuilt from `compose up` + a restore. This
is the primary resilience lever.

### Trait-per-capability rationale

Every externally-facing capability is defined as a trait in `kb-core`:

| Trait | Concrete implementors | Why a trait |
|-------|-----------------------|-------------|
| `Blob` | `LocalBlob` (P2), `B2Blob` (P8) | Swap storage backends without touching callers |
| `Store` | `PgStore` | Test with mock `IngestStore`/`RetrievalStore` |
| `Extractor` | Text, Code, Tika, Image, Audio, Video, Binary | One interface, many file types |
| `Tagger` | `JsonSchemaTagger` | Swap tagging strategies (different prompts/models) |
| `Embedder` | `LlamaClient` (Role::Embed) | Supports batch-embedding while callers stay generic |
| `Reranker` | `LlamaReranker` | Swap reranking models independently |
| `ProviderAdapter` | (P9) | Abstract over local vs. remote inference |
| `SessionStore` | `InMemorySessionStore`, `PgSessionStore` | Dev/test vs. production without changing middleware |

This pattern enables the mock-backend testing strategy: every component can be tested
against a controlled fake, so the full ingest+retrieval pipeline is testable without a
real GPU or Postgres instance in `just ci` (the fast gates). Integration tests against
real Postgres run in the Podman-backed `just ci-integration` lane.

## Cross-cutting concerns

### Hot-swappable configuration

Runtime parameters (model endpoints, API keys, model IDs, routes, rate limits, timeouts,
quotas) are held behind `arc_swap::ArcSwap` and resolved **at call time** — never captured
once at startup. File-watch reloads via `notify`. See `CLAUDE.md` for the full rule.

### Two-role database model (P6-T14)

A privileged role (`kb`) owns the schema, runs migrations, and handles cross-tenant
operations (job queue, admin queries). A non-superuser `NOBYPASSRLS` role (`kb_app`) is
used for all tenant-scoped data access. RLS is enforced as a defence-in-depth layer.
`PgStore` holds both an admin pool and an app pool; tenant methods begin a transaction
that sets `app.current_tenant` on the app-pool connection before queries run.

### Container runtime: Podman only

This project targets Podman exclusively — no Docker engine dependency. `compose.yaml`
uses fully-qualified image names (`docker.io/…`). Container-based tests target the
Podman socket. Quadlet `.container` files are shipped for systemd-native deployment.
