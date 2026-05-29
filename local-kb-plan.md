# Local File Knowledge Base — Implementation Plan

A self-hosted, modular tool that ingests files of **any** type, enriches them with an
LLM-generated title/summary/tags + your own notes + extracted metadata, stores everything
in Postgres, and retrieves it via **hybrid (vector + keyword) search with reranking**.
All inference runs on **local llama.cpp servers**, addressed by hostname, with a
**slot-aware load balancer** that picks a free server and waits if all are busy.

---

## 1. Design decisions (read first)

- **One sparse-MoE multimodal workhorse, not one model per filetype.** A 30B-A3B VLM
  (3B active params) handles documents, code, images, and video frames at ~3B inference
  cost. Loading a distinct generative model per filetype is appealing in theory but in
  32 GB it just steals VRAM you want for KV-cache/parallel slots, and multiplies
  prompt-template maintenance. Specialize only where it clearly pays (code), and put that
  specialist on a **second host** — which your multi-host requirement makes free.
- **llama.cpp is addressed only via its OpenAI-compatible HTTP server** (`llama-server`).
  No in-process bindings. This is what makes "another machine" trivial: it's just another
  base URL. Vision uses `--mmproj`; embeddings/rerank use dedicated `llama-server`
  instances with `--embedding`.
- **Slot count is the load-balancing primitive.** Each `llama-server` runs with
  `--parallel N`. The scheduler holds a semaphore of exactly `N` permits per backend, so we
  never oversubscribe (which would silently serialize inside llama.cpp).
- **Avoid the abliterated/"uncensored-aggressive" build.** This pipeline refuses nothing —
  it tags your own files. Aggressive de-alignment measurably degrades instruction-following
  and JSON-schema adherence, which is exactly the property the tagger depends on. Use stock
  Apache-2.0 weights.
- **Structured output is grammar-constrained**, not prompt-and-pray. `llama-server`
  supports `response_format: json_schema` / GBNF, so tags/metadata deserialize into Rust
  structs reliably.
- **No single hard "category" field.** Free-text LLM categories fragment into synonyms
  ("invoice"/"bill"/"receipt") and force one bucket onto multi-faceted files. Use
  **multi-label tags with semantic canonicalization** instead (see §6.5). Embeddings
  already cluster synonyms, so identical items retrieve together regardless of label.
- **Multi-tenant from day one.** `tenant_id` on every row + Postgres Row-Level Security,
  so a forgotten filter can't leak across tenants (see §13).
- **The app node is stateless and disposable.** All durable state lives in **Postgres + a
  Backblaze B2 object store** (§20); local disk holds only cache + logs. Any node can be
  destroyed and rebuilt from `compose up` + a restore. This is the primary resilience lever
  — everything below (HA, self-healing, DR) depends on it (§20–§25).

---

## 2. Model roster (fits 32 GB resident, with slot headroom)

| Role | Model | Quant | ~VRAM | Notes |
|---|---|---|---|---|
| Text + vision + code (workhorse) | **Qwen3-VL-30B-A3B-Instruct** + mmproj | Q4_K_M / UD-Q4_K_XL | ~18–20 GB | 3B active. Captions images, reads scanned-PDF pages, summarizes docs/code, drafts RAG answers. Official GGUF + mmproj. |
| Embeddings | **BGE-M3** (dense+sparse+colbert) | Q8 | ~0.6 GB | One model gives dense **and** lexical vectors → hybrid from a single embedder. Upgrade path: **Qwen3-Embedding-4B** (top MTEB, dense-only, pair with PG full-text). Dim: BGE-M3 = 1024. |
| Reranker | **bge-reranker-v2-m3** | Q8 | ~0.6 GB | High ROI on retrieval precision. Alt: Qwen3-Reranker-0.6B. |
| ASR (audio + video speech) | **whisper.cpp large-v3-turbo** | — | ~1.5 GB | Run as `whisper-server` (also OpenAI-style) or subprocess. |

Resident total ≈ **22–24 GB**, leaving ~8–10 GB for KV-cache / parallel slots. Comfortable.

**Optional second host** (routed purely via config — see §6):
- **Qwen3-Coder-30B-A3B-Instruct** (~17 GB) for code, if code-summary quality matters.
- **Qwen3.6-35B-A3B** for top-tier query-time RAG synthesis (it does not fit *alongside*
  the VLM in 32 GB, so it belongs on another box or as an on-demand swap).

### Per-filetype routing

| FileKind | Extractor | Model role used |
|---|---|---|
| document (pdf/docx/pptx/xlsx/md/html/txt) | Apache Tika (universal) or native (`calamine`, `lopdf`); scanned PDF → rasterize → VLM OCR | text (+ vision if scanned) |
| code | read as text; `tree-sitter` chunking | code (→ workhorse, or 2nd-host coder) |
| image | VLM caption (structured) + EXIF | vision |
| audio | `ffmpeg` → wav → whisper | ASR → text |
| video | `ffmpeg` keyframes → VLM + audio → whisper, merged | vision + ASR → text |
| archive | list/recurse contents, store manifest | (per inner file) |
| binary / unknown | `file`/magic + size + hashes + `strings` + your note | text (normalize tags only) |

> **Video note:** llama.cpp has **no native video** path yet — extract keyframes with
> ffmpeg and caption them as images. Do not depend on experimental temporal patches.

---

## 3. Architecture & data flow

```
            ┌─────────── ingest (CLI / API / folder-watch) ───────────┐
            ▼                                                          │
   detect kind (infer)                                                 │
            ▼                                                          │
   Extractor (Tika / native / ffmpeg+whisper / VLM)  → text + meta + page-images
            ▼
   Tagger (workhorse VLM, json_schema)  → {title, summary, tags[], category}
            ▼
   chunk → Embedder (BGE-M3) → vectors        ┌── all model calls go through ──┐
            ▼                                  │   the SCHEDULER (§6), which    │
   Store.upsert (Postgres + pgvector)          │   picks a free llama-server    │
                                               │   slot across hosts, or waits  │
   query → Embedder → hybrid search (vec+FTS)  └────────────────────────────────┘
            → RRF fuse → Reranker → (optional RAG answer via workhorse)
```

---

## 4. Rust workspace (Cargo, modular)

Trait-per-capability so any component is swappable without touching the rest.

```
kb/
├── core/        # domain types + traits (no I/O). The stable contract.
├── scheduler/   # multi-host, slot-aware model pool  (the centerpiece, §6)
├── llm/         # llama.cpp OpenAI client; impls Tagger/Embedder/Reranker via scheduler
├── extract/     # Extractor impls: tika, native (calamine/lopdf), media (ffmpeg/whisper), vision
├── store/       # Store impl: Postgres+pgvector (sqlx). Optional: qdrant impl.
├── pipeline/    # wires extract → models → store (ingest + query flows)
├── api/         # axum HTTP + clap CLI (ingest, query, serve)
└── config/      # serde + TOML loader, hot-reload (arc-swap + notify)
```

**Key crates:** `tokio`, `reqwest` (json+stream), `serde`/`serde_json`, `axum`, `clap`,
`sqlx` (postgres) + `pgvector`, `async-trait`, `anyhow`/`thiserror`, `tracing`,
`infer`/`tree_magic_mini`, `calamine`, `lopdf`, `kamadak-exif`, `tokio::process` (ffmpeg/whisper),
`dashmap`, `futures`, `arc-swap`, `notify`, `governor` (optional).

**Sidecars (not Rust, run as services):** Apache Tika server (universal doc/metadata
extraction — fills Rust's document-parsing gaps), `ffmpeg`, `whisper-server`, and the
`llama-server` instances.

### Core traits (`core/`)

```rust
#[async_trait]
pub trait Extractor: Send + Sync {
    async fn extract(&self, f: &RawFile) -> anyhow::Result<Extracted>;
}
// Extracted { text: String, meta: serde_json::Value, page_images: Vec<image::DynamicImage> }

#[async_trait]
pub trait Tagger: Send + Sync {     // json_schema-constrained
    async fn tag(&self, input: &TagInput) -> anyhow::Result<TagOutput>;
}
// TagOutput { title: String, summary: String, tags: Vec<String>, category: String }

#[async_trait]
pub trait Embedder: Send + Sync {
    fn dim(&self) -> usize;
    async fn embed(&self, texts: &[String], kind: EmbedKind) -> anyhow::Result<Vec<Vec<f32>>>;
}
// EmbedKind { Document, Query }  // Qwen3-Embedding/BGE want different instructions

#[async_trait]
pub trait Reranker: Send + Sync {
    async fn rerank(&self, query: &str, docs: &[String]) -> anyhow::Result<Vec<f32>>;
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn upsert_file(&self, rec: &FileRecord) -> anyhow::Result<i64>;
    async fn upsert_chunks(&self, file_id: i64, chunks: &[Chunk]) -> anyhow::Result<()>;
    async fn hybrid_search(&self, q: &Query) -> anyhow::Result<Vec<Hit>>;
}
```

---

## 5. Storage & schema (Postgres + pgvector)

Single store = metadata + tags + vectors + keyword index. Hybrid search lives in SQL.

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE tenants (
    id           BIGSERIAL PRIMARY KEY,
    slug         TEXT UNIQUE NOT NULL,
    name         TEXT NOT NULL,
    quota_bytes  BIGINT,                        -- NULL = unlimited
    quota_tokens BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,                -- argon2id
    role          TEXT NOT NULL DEFAULT 'member', -- owner|admin|member
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, email)
);

-- A DOCUMENT is the semantic unit the user retrieves (an ID, a contract, a 50-page manual,
-- a song). It has one or more member FILES (pages / sides / parts), each its own blob.
-- A single-file upload auto-creates a 1-page document. See §27.
CREATE TABLE documents (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    title       TEXT,
    summary     TEXT,                           -- synthesized across ALL pages
    user_note   TEXT,
    kind        TEXT NOT NULL,                  -- document|image|audio|video|identity_document|code|archive|binary
    meta        JSONB NOT NULL DEFAULT '{}',    -- union of member metadata + doc-level fields
    page_count  INT NOT NULL DEFAULT 1,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON documents USING GIN (meta jsonb_path_ops);

CREATE TABLE files (                            -- physical blob = one page/member of a document
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    page_no     INT NOT NULL DEFAULT 1,         -- order within the document
    page_label  TEXT,                           -- 'front' | 'back' | 'p3' | original filename
    sha256      BYTEA NOT NULL,
    blob_key    TEXT NOT NULL,                  -- content-addressed (= sha256), tenant-prefixed
    path        TEXT,                           -- original filename
    mime        TEXT,
    size_bytes  BIGINT,
    meta        JSONB NOT NULL DEFAULT '{}',    -- per-page exif / ffprobe / doc props (raw, namespaced)
    status      TEXT NOT NULL DEFAULT 'pending',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, sha256)                  -- blob-level dedup per tenant
);
CREATE INDEX ON files (document_id);
CREATE INDEX ON files USING GIN (meta jsonb_path_ops);

-- Canonical, multi-label, semantically-deduplicated tags (replaces a hard category; see §6.5)
CREATE TABLE tags (
    id        BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name      TEXT NOT NULL,
    embedding VECTOR(1024),                     -- for synonym detection
    UNIQUE (tenant_id, name)
);
CREATE TABLE tag_aliases (                      -- "bill" -> canonical "invoice"
    tenant_id BIGINT NOT NULL,
    alias     TEXT NOT NULL,
    tag_id    BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (tenant_id, alias)
);
CREATE TABLE document_tags (                    -- tags attach to the DOCUMENT (semantic unit), not a page
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    tag_id      BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (document_id, tag_id)
);

CREATE TABLE chunks (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,  -- retrieval rolls up to here
    file_id     BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,       -- provenance: which page
    page_no     INT,                            -- deep-link target (page within the document)
    idx         INT NOT NULL,
    content     TEXT NOT NULL,
    ts_offset   DOUBLE PRECISION,               -- seconds into audio/video (NULL otherwise) → jump-to-moment
    embedding   VECTOR(1024) NOT NULL,          -- MUST match embedder dim (BGE-M3 = 1024)
    tsv         TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
);
CREATE INDEX ON chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON chunks USING GIN (tsv);
CREATE INDEX ON chunks (document_id);
CREATE INDEX ON chunks (file_id);

-- Durable async ingestion jobs (retryable, with dead-letter); see §16
CREATE TABLE jobs (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    file_id    BIGINT,
    kind       TEXT NOT NULL,                   -- ingest|reembed|export
    priority   INT NOT NULL DEFAULT 100,        -- lower runs first; queries bypass jobs entirely
    status     TEXT NOT NULL DEFAULT 'queued',  -- queued|running|failed|dead|done
    attempts   INT NOT NULL DEFAULT 0,
    last_error TEXT,
    run_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON jobs (status, priority, run_after);

-- Per-call usage for admin + per-tenant quotas; see §15
CREATE TABLE usage_events (
    id                BIGSERIAL PRIMARY KEY,
    tenant_id         BIGINT NOT NULL,
    user_id           BIGINT,
    model             TEXT NOT NULL,
    role              TEXT NOT NULL,
    backend_id        TEXT,
    prompt_tokens     INT,
    completion_tokens INT,
    latency_ms        INT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON usage_events (tenant_id, created_at);

-- Row-Level Security: isolation even if a query forgets the tenant filter.
-- Apply to every tenant-scoped table (files, chunks, tags, file_tags, jobs, usage_events).
ALTER TABLE files ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON files
    USING (tenant_id = current_setting('app.current_tenant')::BIGINT);
-- Per transaction the app runs:  SELECT set_config('app.current_tenant', $tenant, true);
```

> **Embedder lock-in:** the `VECTOR(1024)` dim is baked into the schema. Store the embedder
> id + dim in a `meta`/`settings` table; changing embedders requires a `reembed` job over
> every chunk, not a manual reindex.

---

## 6. The model scheduler (multi-host, slot-aware) — centerpiece

Requirements satisfied: many hostnames; route by model role; pick a server with a **free
slot right now**; **wait** (async, fair) if all are busy; skip dead hosts; fail over.

> The skeleton below is the **local llama.cpp** case (slot semaphore). §26 generalizes a
> `Backend` to *any* provider — local or remote (Gemini, DeepSeek, Qwen, Claude, …) — via a
> pluggable **adapter** + **capacity guard** (slots *or* rate-limit/concurrency), and replaces
> the flat pool with **admin-managed tiered failover routes** (primary → fallback). The
> `acquire`/`Lease`/failover machinery here is reused unchanged; only `Backend` and candidate
> selection generalize.

### Mechanics
- Each backend = one `llama-server`/`whisper-server` instance with a known `--parallel N`
  (local case; remote backends use a rate-limit/concurrency guard instead — §26).
- `Backend.slots = Semaphore::new(N)` → permit count == server slot count.
- **Acquire:** fast path = least-loaded backend with a `try_acquire`-able slot; wait path =
  await *all* backends' `acquire_owned()` concurrently, take whichever frees first, with a
  timeout. The permit is RAII — dropping the `Lease` frees the slot.
- **Health:** background task polls `GET {base}/health` every few seconds; mark up/down.
  Local semaphore is the source of truth for *our* in-flight load (avoids `/slots` races).
  (Remote: passive health from a rolling error window + `Retry-After` cooldown — §26.)
- **Failover:** transport error / 5xx → drop lease, mark suspect, retry on another backend
  (bounded). Circuit-break after K consecutive failures with a cooldown.


### Skeleton (`scheduler/`)

```rust
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role { Text, Vision, Code, Embed, Rerank }

pub struct Backend {
    pub id: String,
    pub base_url: String,          // http://gpu2.lan:8080/v1
    pub roles: Vec<Role>,
    pub priority: u8,              // lower = preferred (e.g. local before remote)
    pub slots: Semaphore,         // permits == server --parallel N
    pub healthy: AtomicBool,
}
impl Backend { pub fn free(&self) -> usize { self.slots.available_permits() } }

pub struct Lease { pub backend: Arc<Backend>, _permit: OwnedSemaphorePermit }

pub struct Pool {
    by_role: DashMap<Role, Vec<Arc<Backend>>>,
    acquire_timeout: Duration,
}

#[derive(thiserror::Error, Debug)]
pub enum AcquireError {
    #[error("no backend serves this role")] NoBackend,
    #[error("timed out waiting for a free slot")] Timeout,
    #[error("pool closed")] Closed,
}

impl Pool {
    pub async fn acquire(&self, role: Role) -> Result<Lease, AcquireError> {
        let mut cands: Vec<Arc<Backend>> = self.by_role.get(&role)
            .map(|v| v.clone()).unwrap_or_default()
            .into_iter().filter(|b| b.healthy.load(Ordering::Relaxed)).collect();
        if cands.is_empty() { return Err(AcquireError::NoBackend); }

        // FAST PATH: least-loaded backend that has a free slot this instant.
        cands.sort_by_key(|b| (b.priority, usize::MAX - b.free()));
        for b in &cands {
            if let Ok(p) = b.slots.clone().try_acquire_owned() {
                return Ok(Lease { backend: b.clone(), _permit: p });
            }
        }

        // WAIT PATH: whichever backend frees a slot first wins; fair + bounded.
        let mut waiters = FuturesUnordered::new();
        for b in cands {
            waiters.push(async move {
                b.slots.clone().acquire_owned().await.map(|p| (b, p))
            });
        }
        match tokio::time::timeout(self.acquire_timeout, waiters.next()).await {
            Ok(Some(Ok((b, p)))) => Ok(Lease { backend: b, _permit: p }),
            Ok(_)  => Err(AcquireError::Closed),
            Err(_) => Err(AcquireError::Timeout),
        }
    }
}
```

### Call pattern with failover (`llm/`)

```rust
pub async fn chat(pool: &Pool, client: &reqwest::Client, role: Role, body: &serde_json::Value)
    -> anyhow::Result<serde_json::Value>
{
    let mut last_err = None;
    for _ in 0..=MAX_RETRIES {
        let lease = pool.acquire(role).await?;                 // waits here if all busy
        let url = format!("{}/chat/completions", lease.backend.base_url);
        match client.post(&url).json(body).send().await.and_then(|r| r.error_for_status()) {
            Ok(resp) => return Ok(resp.json().await?),         // slot freed when `lease` drops
            Err(e) => { lease.backend.healthy.store(false, Ordering::Relaxed); last_err = Some(e); }
        }
    }
    Err(anyhow::anyhow!(last_err.unwrap()))
}
```

### Health loop (one task, all backends)

```rust
// every health_interval: GET {base}/health, set b.healthy accordingly.
// On recovery a downed host is automatically eligible again on next acquire().
```

### Config — adding a machine is one entry (`config.toml`)

```toml
[storage]
postgres_url = "postgres://kb:kb@localhost/kb"

[scheduler]
acquire_timeout_secs = 120     # how long a task waits for a slot before erroring
health_interval_secs = 5
max_retries          = 2

# ── 32 GB box ───────────────────────────────────────────────
# Each backend = one llama-server. `slots` MUST equal its --parallel N.
[[backend]]                    # workhorse: Qwen3-VL-30B-A3B + mmproj
id = "local-vl"; base_url = "http://127.0.0.1:8080/v1"
roles = ["text", "vision", "code"]; slots = 4; priority = 0

[[backend]]                    # BGE-M3, started with --embedding
id = "local-embed"; base_url = "http://127.0.0.1:8082/v1"
roles = ["embed"]; slots = 8; priority = 0

[[backend]]                    # bge-reranker-v2-m3
id = "local-rerank"; base_url = "http://127.0.0.1:8083/v1"
roles = ["rerank"]; slots = 8; priority = 0

# ── second machine: just add entries, no code change ─────────
[[backend]]                    # Qwen3-Coder-30B-A3B → code routed here preferentially? 
id = "gpu2-coder"; base_url = "http://gpu2.lan:8080/v1"
roles = ["code"]; slots = 4; priority = 0     # priority 0 = preferred over local for code

[[backend]]                    # Qwen3.6-35B-A3B for query-time RAG synthesis
id = "gpu2-brain"; base_url = "http://gpu2.lan:8081/v1"
roles = ["text"]; slots = 2; priority = 1
```

> Launch each server with matching slots, e.g.
> `llama-server -m Qwen3-VL-30B-A3B-Instruct-Q4_K_M.gguf --mmproj mmproj-F16.gguf
> --n-gpu-layers 99 --parallel 4 --ctx-size 16384 --port 8080 --flash-attn on`.
> Optional: enable hot-reload (`notify` + `arc-swap`) so editing this file rebuilds the
> pool without a restart.

### 6.5 Taxonomy — tags, not categories

No single mutually-exclusive `category` field: free-text LLM categories fragment into
synonyms and force one bucket onto multi-faceted files. Files carry **many** canonical tags,
deduplicated semantically at write time:

1. LLM proposes raw tags (part of the json_schema tagging output).
2. For each: **exact alias lookup** in `tag_aliases` → if found, use that canonical `tag_id`.
3. Else **embed the tag** and cosine-match against existing `tags.embedding` for the tenant.
   If best match ≥ `TAG_MERGE_THRESHOLD` (~0.85), reuse the canonical tag and record the raw
   form as an alias. Otherwise create a new canonical tag.
4. Insert `file_tags`.

This stops "invoice/bill/receipt" splitting in the first place; semantic search is the
backstop (identical items embed adjacently regardless of label). Drift over time is handled
by an admin **merge** action (§15): merge tag B→A, repoint `document_tags`, fold B's name into
`tag_aliases`. For browsing UX, derive facets by clustering `tags.embedding` — do **not**
hand-maintain a tree.

---

## 7. Ingestion pipeline (`pipeline/`)

Operates on a **document** (one or more member files). A single-file upload creates a
1-page document; a multi-image/multi-file upload creates one document with N pages (§27).

1. **Submit** a document = 1..N files (CLI, API upload, folder-watch, or "these images are one
   document" from the UI) → enqueue a `jobs` row (§16).
2. **Per page:** `sha256` (blob dedup), magic-byte kind detection, **deterministic metadata
   harvest** (ExifTool / ffprobe / Tika → raw namespaced `files.meta`; §metadata-harvest), then
   content extraction (text natively; OCR/ASR only where flagged). Produces per-page
   `{ text, meta, page_images, ts_chunks? }`.
3. **Assemble the document:** order pages (`page_no`/`page_label`), **merge** member metadata
   into `documents.meta` (union; e.g. front+back fields combined), set `kind` and `page_count`.
4. **Tag once, over the whole document:** call the workhorse with `response_format: json_schema`
   over *all pages' text + metadata + user_note* (the note may be typed or **voice-transcribed**,
   §12) → `{title, summary, tags[]}`, then **canonicalize tags** (§6.5). One ID → one record
   summarizing both sides.
5. **Chunk + embed** every page's content (token-aware; tree-sitter for code; transcript chunks
   keep `ts_offset`); each chunk records its `document_id`, `file_id`, and `page_no`.
6. **Upsert** `documents` + `files` + `document_tags` + `chunks` in one transaction; set
   `status = 'ready'`.

Every model call flows through `pool.acquire(role)` → automatic slot/host selection + waiting.

### 7.1 Video specifically

- **Visual:** `ffmpeg` scene-change keyframes, capped — `select='gt(scene,0.3)'`, max ~40
  frames — sent to the VLM for captions. (A montage/grid in one VLM call is the
  token-cheap option for the overall gist.)
- **Speech:** extract audio → whisper → transcript with timestamps.
- **Metadata:** `ffprobe` → duration/resolution/codec/fps into `meta`.
- **Retrieval value:** transcript and caption chunks are embedded *individually with their
  `ts_offset`*, so search returns the **moment** in the video (deep-link to timestamp), not
  just the file.
- **Cost control is mandatory:** video is the most expensive kind. The frame cap + audio
  handling bound it; without them one long video can dominate the whole ingest queue.

---

## 8. Retrieval pipeline (`pipeline/`)

1. Query text + optional filters (`kind`, `tags`, date range).
2. **Embed** query (EmbedKind::Query — use the model's retrieval instruction).
3. **Hybrid search** in SQL: vector top-N (HNSW cosine) ∪ keyword top-N (`ts_rank` on `tsv`),
   filters applied in `WHERE`, fused with **Reciprocal Rank Fusion**:
   `score = Σ 1/(k + rank_i)` (k≈60) over the two rankings → top-K.
   *(With BGE-M3 you can additionally store its sparse vector and fuse three signals.)*
4. **Roll up to documents.** Chunk hits are grouped by `document_id` and scored by their best
   (or summed) chunk — so a match on *either* page of an ID surfaces the **one** ID document,
   never two half-results, and the same document never appears twice. Each result keeps the
   winning chunk's `file_id`/`page_no`/`ts_offset` for a **deep-link to the exact page or
   moment**.
5. **Rerank** the top-K documents (best-chunk text) with the cross-encoder → top-n.
6. **Optional RAG answer**: feed the top documents' relevant chunks (which may span multiple
   pages) to the workhorse to synthesize an answer citing `document_id` + page. So "expiry on
   my ID" is answered from the back even if the query matched the front. Otherwise return
   ranked **documents** with snippets + page deep-links.

---

## 9. Reliability of structured output

- Always send a JSON Schema (`response_format`) or GBNF grammar to `llama-server` for the
  tagger; deserialize straight into `TagOutput`. No regex-scraping of free text.
- Keep the tagging prompt + schema versioned in `core/` so output stays stable across
  model swaps. This is the concrete reason to use stock (non-abliterated) weights — they
  follow schemas far more reliably.

---

## 10. Implementation phases (for the agent)

> **Read §31 (Build strategy & autonomous execution protocol) before starting.** It defines
> *how* to execute these phases: a checked-in task ledger walked one item at a time, the
> Definition-of-Done quality gates, the test-per-function and modularity mandates, and the
> mandatory human-review checkpoints. Do **not** attempt to implement multiple phases in one
> pass.

- **P0 — Scaffold.** Cargo workspace, `config` loader, `core` types + traits, sqlx
  migrations (schema §5), `compose.yaml` (Postgres+pgvector, Tika) runnable under docker **and** podman.
- **P1 — Scheduler + llm client.** `Pool::acquire` (§6) with **priority arg**, health loop,
  failover wrapper. Test against a **mock HTTP backend** asserting: free-slot pick, fair
  wait-then-acquire, priority ordering, timeout, dead-host skip, failover.
- **P2 — Job queue + extractors.** Postgres `jobs` queue with retry/backoff/dead-letter
  (§16) and workers. Extractors in order: text/code → Tika docs → image (VLM) → audio
  (whisper) → video (ffmpeg frames §7.1) → binary metadata. Blob trait (local first).
- **P3 — Document ingestion** end-to-end: **document/page model** (single upload → 1-page doc;
  multi-file → one N-page doc, **explicit grouping only — §27**), per-page deterministic harvest
  + metadata merge, structured tagging (json_schema) + canonicalization (§6.5) over the whole
  document, chunking with page/`ts_offset` provenance, embedding, transactional upsert. Usage
  logged to `usage_events`.
- **P4 — Store + retrieval.** Hybrid SQL + RRF + reranker, **roll-up to documents** + dedup,
  page/timestamp deep-links (§8).
- **P5 — Multi-tenancy + auth.** `tenant_id` + RLS, sessions + Argon2, per-tenant quotas,
  streaming ZIP/JSONL export (§13). Security defaults (§17).
- **P6 — Interfaces.** `clap` CLI + `axum` API + folder watch; **web UI** (axum/Askama/HTMX/
  Tailwind §12) incl. multi-select grouping and **type-or-voice description** (MediaRecorder →
  whisper → editable transcript); **admin panel** + Prometheus metrics + Grafana dashboard (§15).
- **P7 — Packaging + OSS.** Containerfile (cargo-chef, rustls, non-root), compose + GPU
  profile + Quadlet units, `.env.example`, CI (clippy/fmt/test + scheduler tests),
  `cargo audit`/`deny`, docs (§14). Logging with `LOG_MAX_GB` cap (§18).
- **P8 — Storage + resilience.** B2 blob backend via `object_store` (content-addressed,
  envelope-encrypted, cached, presigned) + degraded-mode (§20); pgBackRest WAL/PITR to a
  locked B2 bucket + 30-day retention + automated restore-test (§21); Caddy + app replicas +
  PgBouncer + advisory-locked migrations + readiness/shutdown gates (§22); integrity/orphan-GC
  + maintenance scheduler + Alertmanager rules (§23, §25); DR runbook with RTO/RPO.
- **P9 — Pluggable providers + routing (§26).** `ProviderAdapter` trait (OpenAiCompat first;
  Anthropic/Gemini native optional), pluggable capacity guards (slots vs rate-limit/concurrency
  + cooldown), DB-driven `providers`/`models`/`routes` with encrypted keys + hot-reload,
  tiered failover in `acquire()`, per-tenant data-residency flag, cost-aware strategy + budgets,
  admin CRUD + test-connection. Extend the mock-backend tests with: tier spill, 429 cooldown,
  rate-limit throttle, capability gating, residency enforcement.
- **P10 — Confidentiality hardening (§28).** Per-tenant envelope keys via KMS/HSM (not `.env`),
  no plaintext at rest (encrypted blobs + encrypted DB volumes), crypto-shredding on tenant
  delete, decrypt-access auditing; optional confidential-computing deployment + session-scoped
  keys.
- **P11 — Billing & plans (§29).** Stripe Checkout + Customer Portal + Tax; `plans` table →
  quota/budget/features; signature-verified idempotent webhooks driving
  active/past_due/suspended; dunning; plan-driven quota enforcement at upload; optional metered
  overage from `usage_events`.
- **P12 — Public frontend (§30).** Marketing/pricing, signup → checkout → onboarding,
  account/billing (Stripe portal), usage dashboard, API tokens, legal pages; transactional
  email provider wired (verification/receipts/dunning/quota alerts).

Each phase is independently testable because dependencies are traits, not concretions.

---

## 11. Risks & locked decisions

- **Vision backend:** use **Qwen3-VL** (official GGUF + mmproj, works in `llama-server`
  now). Do **not** rely on Qwen3.6's new vision path or experimental video patches.
- **Video:** no native llama.cpp video → ffmpeg keyframes + whisper audio.
- **VRAM:** the VLM workhorse + tiny specialists fit 32 GB; a second 30B/35B does **not**
  fit alongside it → second host (already in the config model).
- **Embedder lock-in:** dim is baked into the schema; re-embedding is required to change it.
- **The `embed` role must pin to ONE model — it does not fail over like chat (corrects §26).**
  Every vector in an index must come from the same embedding model+version; routing `embed`
  across different models/providers silently corrupts the vector space (incompatible geometry,
  even at equal dim) and breaks retrieval. Only `text`/`vision`/`code` get free tiered failover.
  For embed, configure a single active model (a fallback may only be an *identical* model on
  another host); changing it triggers a full re-embed job, never live mixing. The router must
  enforce this (reject multi-model `embed` routes).
- **Rust doc-parsing gaps:** Tika sidecar is the universal fallback; native crates
  (`calamine`, `lopdf`) are optional fast paths, not prerequisites.
- **Model choice:** stock Apache-2.0 weights only — abliterated builds hurt the JSON
  reliability this whole pipeline rests on.
- **Resilience has a complexity cost.** Maximal HA (auto-failover clusters) adds moving
  parts that can *reduce* stability for a single operator. Default to a disposable stateless
  node + fast, tested PITR restore; add a hot standby only if RTO demands it; reach for
  Patroni only if you truly need zero-downtime failover (§22).
- **B2 is an external dependency.** Mitigated by local cache (reads), retrying job queue
  (writes), bucket versioning + Object Lock (deletes), and PITR backups in a separate
  locked bucket. The app degrades rather than crashes when B2 is unreachable (§20, §22).
- **Remote model providers = data leaves your infra.** Routing tenant content to
  Gemini/DeepSeek/Claude/etc. is a governance decision; enforce the per-tenant **data-residency
  flag** (§26). Remote calls also cost money (budget caps) and can train on inputs unless
  opted out — vet each provider's terms.
- **Streaming failover is pre-first-token only.** Once a remote starts streaming, a mid-stream
  failure surfaces to the caller; transparent retry/failover applies only before the first
  token (§26).
- **"Zero-knowledge" is not achievable with server-side AI.** Server-side extraction/embedding/
  search require plaintext, so the honest guarantee is *encrypted at rest, no plaintext at rest,
  per-tenant isolation, crypto-shredding* — not "only the user can ever decrypt." A
  fully-compromised live host can read data in flight; confidential computing narrows but does
  not eliminate that (§28). Don't market zero-knowledge.
- **PCI:** never touch card data — Stripe Checkout/Elements keeps you at SAQ-A (§29). Storing a
  PAN anywhere would be a severe, avoidable liability.
- **Transactional email is an added external dependency** (verification, receipts, dunning) —
  pick a provider (Resend/Postmark/SES) and accept it breaks pure self-hosting (§30).

---

## 12. Web UI (`api/`)

**Default stack: axum + Askama (server-rendered) + HTMX + Tailwind.** One toolchain, minimal
JS, trivial to open-source and maintain; "sleek minimalist" is achievable with Tailwind.
(Alternative: a SvelteKit SPA on the same JSON API if a richer client is later wanted.)

- **Search:** single bar → hybrid results (one row per **document**) with snippets + highlight;
  filters for kind / tag / date; hits deep-link to the matching **page** (`page_no`) or
  `ts_offset`. HTMX swaps the results list (no full reload).
- **Upload:** drag-and-drop with progress; **streamed** multipart to the blob store (never
  buffer large files in RAM); chunked/resumable via **tus** for big video. **Multi-select →
  "ingest as one document"** (the front/back-ID and multi-photo-scan flow); reorder pages
  before submit. Returns a job id; HTMX polls ingest status.
- **Description — type *or* speak (both offered):** alongside the text field, a mic button
  records via the browser `MediaRecorder` API; the short clip is transcribed by the **existing
  whisper ASR backend** (§2) — no new component — and fills `user_note`. The transcript is shown
  **editable** so the user fixes ASR errors before submit (reliability: the description drives
  tagging). Treated as **interactive** → bypasses the batch job queue and uses a query-priority
  `acquire()` (§16), not a background job. Voice clips are user content: encrypted like any
  blob, and under a tenant's `local_only` policy transcription stays on local whisper (never a
  remote ASR); the audio is discarded after transcription by default (keep-original is opt-in).
  Also an accessibility + mobile win.
- **Document detail:** combined metadata/summary/tags for the whole document, a **page
  thumbnail strip** (front/back/p1…pN) you can reorder, **add-page** to an existing document,
  editable tags (→ re-canonicalized), re-tag button, per-page original download.
- All routes tenant-scoped + auth-gated; CSRF token on mutating forms.

## 13. Multi-tenancy, auth & export (`store/`, `api/`)

- **Isolation:** `tenant_id` on every row + Postgres **RLS** (`app.current_tenant` set per
  transaction) so a missed filter can't leak across tenants. Shared schema (simplest to
  operate); schema-per-tenant only if a tenant demands physical separation.
- **Auth:** users belong to a tenant with roles (owner/admin/member). `tower-sessions`
  cookie sessions + **Argon2id** password hashing. OIDC optional for SSO.
- **Blob store:** `Blob` trait over the `object_store` crate → **Backblaze B2** (S3-compatible
  endpoint); local-disk impl kept for dev/tests. Content-addressed keys, client-side
  encryption, local cache, presigned downloads — full design in §20.
- **Quotas (plan-driven, §29):** per-tenant `quota_bytes` + token budget come from the
  subscribed **plan**. Storage is accounted incrementally and enforced at upload (hard cap →
  clear error + upsell); token budget gates remote/paid routing or bills as metered overage.
- **Export (data portability):** a `jobs` task that streams a ZIP = original files +
  `export.jsonl` (files, tags, meta, chunks) + `manifest.json` (schema version, embedder
  id/dim). Round-trippable via a matching import path. This doubles as the GDPR "give me my
  data" feature. **Note:** export ≠ backup — Postgres PITR + B2 versioning are the backups (§21).

## 14. Packaging (Docker / Podman) & open-source

- **Image:** multi-stage `Containerfile` with **cargo-chef** (dependency-layer caching) →
  `debian:slim` or distroless runtime, **non-root** user, **rustls** (no OpenSSL) for a
  clean portable binary. Same file builds under `docker build` and `podman build`.
- **Compose:** one `compose.yaml` (app + `pgvector/pgvector` Postgres + Tika) that runs
  under `docker compose` and `podman compose`. GPU inference (llama-server/whisper) is a
  separate **profile** / `compose.gpu.yaml` (Podman GPU via CDI, `--device nvidia.com/gpu=all`)
  so CPU-only users can still run the app.
- **Podman-native:** ship **Quadlet** `.container` units for systemd deployment.
- **Repo:** `.env.example`, README/ARCHITECTURE/CONTRIBUTING, **LICENSE** (Apache-2.0/MIT),
  note that model weights are separately licensed, CI (clippy + fmt + tests, incl. the
  mock-backend scheduler tests), `cargo audit` + `cargo deny`, SBOM.

## 15. Admin panel & observability (`api/`)

- **Token/usage accounting:** every model call writes a `usage_events` row (tenant, model,
  role, backend, prompt/completion tokens, latency). Local inference is "free," but tokens =
  capacity planning + the basis for per-tenant quotas. For remote backends, multiply by
  per-model pricing → **spend** (per tenant / provider / model), with budget alerts.
- **Model & routing management (§26):** CRUD for **providers** (type, endpoint, encrypted
  API key, headers), **models** (provider, model-id, capabilities, context, pricing, rate
  limits, concurrency), and **routes** (role → ordered tiers of backends + strategy +
  `spill_after`). A **"test connection"** action validates a provider/key; all routing edits
  are audit-logged.
- **Metrics:** export **Prometheus** (`metrics` + `metrics-exporter-prometheus`): per-backend
  health, free/used slots **or rate-limit headroom**, cooldown state, **queue depth /
  waiters**, p50/p95 latency, throughput, error rate, spend, storage used, active users. Ship
  a Grafana dashboard JSON + a built-in lightweight admin view for the basics.
- **Admin actions:** manage tenants/users; audit log; **tag merge** (§6.5); reprocess/re-tag;
  dead-letter inspect/replay; live backend + route status. Super-admin (cross-tenant) vs
  tenant-admin scoping.

## 16. Ingestion queue & reliability (`pipeline/`)

- **Durable queue:** the `jobs` table (or `pgmq`/`apalis`) with `status`, `attempts`,
  exponential-backoff `run_after`, and a **dead-letter** (`status='dead'`) that's inspectable
  and replayable from the admin panel. Ingestion *will* hit corrupt files, model timeouts,
  and OOM — this is required, not optional.
- **Concurrent workers:** claim jobs with `SELECT … FOR UPDATE SKIP LOCKED LIMIT n`, so N
  stateless worker instances pull safely from the same queue (this is also how ingestion
  scales out). Each claim takes a lease/visibility timeout; a crashed worker's job becomes
  reclaimable.
- **Priority lanes:** interactive **queries bypass the job queue entirely** and use a higher
  `acquire()` priority so a 5k-file bulk import can't starve search.
- **Idempotency:** `(tenant_id, sha256)` dedup; re-ingest of a changed file supersedes.
- **Graceful shutdown:** stop taking jobs, drain in-flight leases, then exit.
- **Startup readiness:** fail fast if no backend covers a required role or if embedder dim ≠
  schema; don't serve until ≥1 healthy backend per required role.

## 17. Security defaults

Upload size + MIME validation; path-traversal + SSRF guards (esp. if fetching URLs);
per-tenant authz on **every** resource (belt-and-suspenders with RLS); rate limiting
(`governor`); CSRF on the cookie UI; CSP/security headers; secrets only via `.env`
(document rotation); optional clamav scan on upload.

**PII concentration (identity documents).** A *complete* document (e.g. both sides of an ID:
name + number + address + DOB + signature) is far more sensitive than any single page — and
the grouping in §27 deliberately concentrates it. So a document whose `kind` is
`identity_document` (or detected as PII-heavy) should default to **`local_only`** routing
(§26) — never sent to a remote LLM — and is a prime target for the optional PII
redaction/field-masking policy. Grouping must key on the *document/identity*, not on per-image
visual similarity, so two different people's cards are never merged.

## 18. Logging

- `tracing` + `tracing-subscriber` **JSON**, with spans carrying `request_id` / `tenant_id` /
  `file_id` for correlation.
- **Hard GB cap:** `tracing-appender` rotates by time, not size — use the **`file-rotate`**
  crate (`ContentLimit::Bytes` + `FileLimit::MaxFiles`) to enforce `LOG_MAX_GB`, or a janitor
  task pruning oldest files past the cap.
- `.env`: `LOG_LEVEL`, `LOG_FORMAT` (json|pretty), `LOG_DIR`, `LOG_MAX_GB`.
- Logs are debug-only and may roll away; **usage + audit data live in Postgres** so they
  survive rotation and feed the admin panel.
- Optional: OpenTelemetry export (`tracing-opentelemetry`) to Jaeger/Tempo.

## 19. Retrieval quality

Build a small **labelled eval set** (queries → expected files) and tune RRF weights, rerank
top-K, and the `TAG_MERGE_THRESHOLD` against it. Leaderboard scores don't transfer — measure
on your own corpus. Keep it in CI so retrieval changes are regression-tested.

---

## 20. Storage backend — Backblaze B2

All file blobs live in **Backblaze B2** via its S3-compatible API, behind the `Blob` trait
(crate: **`object_store`** with a custom S3 endpoint; `rust-s3` is an alternative). Local
disk is cache + logs only — never the source of truth.

- **Content-addressed keys:** `blob_key = hex(sha256)` (tenant-prefixed namespace). Makes
  uploads **idempotent** and dedup-safe, and means a blob written by a transaction that later
  fails is a harmless orphan (swept by GC, §23) rather than a corruption.
- **Multipart + retry:** large files (video) use S3 multipart with bounded retries and
  per-request timeouts. Never buffer a whole file in RAM (stream upload/download).
- **Client-side encryption:** envelope encryption — a random per-file data key encrypts the
  blob; the data key is wrapped by a master key held by the app (§24). Tenants' files are
  never plaintext in a third party. (SSE-B2 is a weaker fallback.)
- **Local read-through cache:** size-capped on-disk LRU for hot blobs; B2 is a network hop,
  so avoid re-fetching. Cache stores decrypted plaintext only on the trusted node.
- **Presigned downloads:** the UI/API hands the client a short-lived presigned B2 URL so
  download bandwidth bypasses the app entirely (also good for media streaming with range
  requests).
- **Versioning + Object Lock** on the bucket → protection against accidental/malicious
  deletes and ransomware (prior versions retained for a lifecycle window).
- **Least privilege:** **separate buckets and application keys** for app-data vs backups; the
  data key cannot delete or read the backup bucket.
- **Degraded mode:** if B2 is unreachable, ingestion jobs pause and retry (no crash); reads
  served from cache where possible; a degraded banner is surfaced (§22).
- **Optional paranoia:** B2 bucket replication or a periodic copy to a second
  region/provider for cross-cloud durability.

## 21. Backups, PITR & disaster recovery

Replace plain hourly `pg_dump` with **continuous archiving** for true point-in-time recovery.

- **Tool:** **pgBackRest** (alt: WAL-G) with a **B2 (S3) repository**, repo **encryption**,
  and parallelism.
- **Strategy:** continuous **WAL archiving** (recover to any second) + scheduled backups
  (e.g. daily full + **hourly incrementals**, satisfying "hourly") with **30-day retention**.
- **Location:** the separate, **Object-Lock'd** backup bucket (immutable → ransomware can't
  delete backups).
- **Tested restores (non-negotiable):** a scheduled job restores the latest backup to a
  scratch instance, runs integrity checks, and **alerts on failure or stale WAL/backup**. An
  untested backup is not a backup.
- **What's backed up:** Postgres (the only critical mutable state). B2 blobs are protected by
  versioning + Object Lock rather than copied. Config/secrets are reproducible from `.env` /
  secret store (§24), kept out of the repo.
- **DR runbook** with explicit **RTO/RPO**: documented, scripted steps to (1) provision a
  node, (2) `compose up`, (3) pgBackRest restore to target time, (4) point the stateless app
  at it. Because the node is disposable (§1), recovery ≈ restore time.

## 22. High availability, degradation & self-healing

- **Stateless app replicas** behind a reverse proxy (**Caddy** = automatic TLS, self-hosted)
  with `restart: unless-stopped` / systemd `Restart=always`. Workers scale horizontally via
  the SKIP-LOCKED queue (§16).
- **Postgres (the real SPOF) — tiered by tolerance:**
  - *Baseline:* disposable node + fast **tested PITR restore** (low complexity).
  - *Better:* a **streaming hot standby** (warm failover, minutes RTO).
  - *Max:* **Patroni**-managed auto-failover cluster (only if zero-RTO is required — it adds
    failure modes; see §11).
- **PgBouncer** in front of Postgres for connection pooling under many workers/tenants.
- **Migrations** run on startup guarded by a **Postgres advisory lock** so concurrent
  instances can't race them; forward-only.
- **Graceful degradation matrix:** B2 down → ingest pauses/retries, reads from cache;
  a role with no healthy backend → queries return a clear error, jobs wait; DB read-replica
  lag → route reads accordingly. **Timeouts on every external call**; circuit breakers per
  backend (§6).
- **Backpressure:** cap queue depth / in-flight uploads; return 429 when saturated rather
  than collapsing.
- **Graceful shutdown:** stop claiming jobs, drain in-flight leases + slots, flush, exit.
- **Readiness gate:** don't serve traffic until DB reachable, migrations applied, and ≥1
  healthy backend per required role.

## 23. Data integrity & reconciliation

- **Postgres `data-checksums`** enabled at init (detect page corruption).
- **Blob integrity scan (job):** periodically re-hash a sample of B2 blobs and compare to
  stored `sha256` → detect bit-rot / tampering.
- **Orphan reconciliation GC (job):** find DB rows whose blob is missing and blobs with no DB
  row (from failed dual-writes), and resolve/clean. Content-addressing makes this safe.
- **Transactional outbox** (optional) if side effects beyond blob+DB are added later.

## 24. Secrets & key management

- B2 application keys (data + backup, separate), session secret, and DB creds supplied via
  `.env` **or file-based Docker/Podman secrets** (preferred in prod); never committed.
- The **encryption master key (KEK)** that wraps per-tenant data keys should live in a
  **KMS/HSM** (Vault Transit, cloud KMS, or an HSM) — not `.env` — so it can be rotated,
  access-audited, and (optionally) never exposed to the app process. Full confidentiality
  model + threat boundary in §28.
- **Remote provider API keys** (Gemini/DeepSeek/Qwen/Anthropic/… §26) are stored
  **encrypted at rest** in the DB, decrypted only in memory at call time, **never logged**,
  and rotatable from the admin panel without downtime.
- **Rotation documented.** Envelope encryption (§20/§28) means rotating the KEK only re-wraps
  per-tenant/per-file keys and provider-key blobs — no bulk re-encryption of file data.
- Least-privilege scoping on every credential; audit access in the admin audit log.

## 25. Scheduled maintenance & self-sufficiency

A built-in scheduler (or cron/systemd timers) runs, with success/freshness alerts:
- backups + **restore verification** (§21),
- log pruning to `LOG_MAX_GB` (§18) and blob-cache LRU eviction,
- **orphan GC** + integrity scans (§23),
- autovacuum/ANALYZE tuning and periodic `VACUUM`; HNSW/index maintenance as the corpus grows,
- B2 lifecycle (version expiry) enforcement,
- re-embed migrations when the embedder changes (§5).

First-run **bootstrap** seeds the initial admin tenant/user and validates config; the whole
stack comes up from a single `compose up` (or Quadlet) with migrations auto-applied — no
manual steps, no external SaaS beyond B2.

---

## 26. Pluggable providers & admin-managed routing

Generalizes §6 so **any** model — local llama.cpp or remote (Gemini, DeepSeek, Qwen,
Anthropic/Claude, OpenAI, …) — is a `Backend`, added and wired from the admin panel, with
flexible **primary → fallback** routing per role for resilience against load and failures.

### 26.1 Provider adapters
A trait normalizes calls across provider families; the `acquire`/`Lease`/failover machinery
of §6 is reused unchanged.

```rust
#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    async fn chat(&self, b: &Backend, req: ChatReq) -> anyhow::Result<ChatResp>;   // -> text + Usage
    async fn embed(&self, b: &Backend, req: EmbedReq) -> anyhow::Result<EmbedResp>;
    fn supports(&self, role: Role) -> bool;
}
```
- **`OpenAiCompat`** (base_url + optional key) — covers local llama.cpp/vLLM, **DeepSeek**,
  **Qwen/DashScope**, **Gemini** (OpenAI-compat endpoint), OpenAI, and most others. Build
  90% on this.
- **`Anthropic`** — Messages API (`x-api-key`, `anthropic-version`, system param). Add only
  for Claude-specific fidelity.
- **`GeminiNative`** — `generateContent`. Optional.
- Adapters return a normalized `Usage {prompt, completion}` so `usage_events`/cost works
  uniformly.

### 26.2 Capacity guards (pluggable — remote has no "slots")
```rust
pub enum Capacity {
    Slots(Semaphore),                          // local llama.cpp: == --parallel N
    Concurrency(Semaphore),                    // remote: max in-flight you choose
    Rated { conc: Semaphore, rpm: TokenBucket, tpm: TokenBucket }, // remote API limits
}
```
- Local: semaphore (as §6).
- Remote: bounded **concurrency** + **RPM/TPM token buckets** (`governor`). You can't observe
  remote free capacity, so saturation is **reactive**: a `429` sets
  `cooldown_until = now + Retry-After`; repeated `5xx`/timeouts trip the circuit breaker.
- `acquire()` becomes "take capacity from this backend's guard"; a backend in cooldown or
  over its buckets is simply skipped as a candidate.

### 26.3 Generalized backend
```rust
pub struct Backend {
    pub id: String,
    pub adapter: Arc<dyn ProviderAdapter>,
    pub endpoint: Option<String>,      // base_url (local / openai-compat)
    pub key: Option<SecretRef>,        // encrypted provider key id (§24)
    pub model_id: String,              // "deepseek-chat" | "claude-..." | local gguf alias
    pub caps: CapSet,                  // {Text,Vision,Code,Embed,Rerank}
    pub capacity: Capacity,
    pub pricing: Option<Pricing>,      // $/Mtok in,out (None = local/free)
    pub data_class: DataClass,         // Local | Remote  (for residency, §26.6)
    pub healthy: AtomicBool,
    pub cooldown_until: parking_lot::Mutex<Option<Instant>>,
}
```

### 26.4 Tiered failover routes (the primary/fallback model)
Per role, an **ordered list of tiers**; each tier is a backend set + a selection strategy.
```rust
pub struct Route { pub role: Role, pub tiers: Vec<Tier> }
pub struct Tier  { pub strategy: Strategy, pub backends: Vec<String>, pub spill_after: Duration }
pub enum Strategy { LeastLoaded, RoundRobin, CostAsc, Priority }
```
`acquire(role, tenant)` algorithm:
1. For each tier in order: candidates = backends in tier that are healthy, **not in cooldown**,
   **capable of `role`**, and **allowed for this tenant** (§26.6); order by `strategy`.
2. `try_acquire` capacity across candidates → first success wins (fast path).
3. If none free now, wait up to `spill_after` on this tier (await-first-free, §6 wait path);
   on timeout, **spill to the next tier**.
4. All tiers exhausted → wait on the union up to a global timeout, else return
   `CapacityExhausted` (interactive queries surface it; jobs retry with backoff).
On request failure: `5xx`/timeout → suspect + retry next candidate/tier (bounded);
`429` → set cooldown + retry next.

This expresses the target directly, e.g. for `text`:
`[Tier0 local-vl (LeastLoaded, spill_after 800ms)] → [Tier1 deepseek+qwen (CostAsc)] →
[Tier2 claude (Priority)]` = run local for free, **burst to cheap cloud under peak load**,
fall to a premium provider on hardware/provider failure.

### 26.5 DB-driven config (admin CRUD, hot-reload)
Runtime source of truth is Postgres (TOML stays a bootstrap seed). Scheduler holds an
`ArcSwap<RoutingTable>` rebuilt on change (LISTEN/NOTIFY or short poll). Edits are audit-logged.

```sql
CREATE TABLE providers (              -- global (or tenant-scoped if desired)
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL,        -- openai_compat | anthropic | gemini_native | local
    endpoint    TEXT,                 -- base_url (null for native SDK endpoints)
    api_key_enc BYTEA,                -- envelope-encrypted (§24); null for local
    headers     JSONB DEFAULT '{}',
    enabled     BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE models (                 -- a "backend" = provider + model + capacity
    id          BIGSERIAL PRIMARY KEY,
    provider_id BIGINT REFERENCES providers(id) ON DELETE CASCADE,
    model_id    TEXT NOT NULL,        -- provider's model name / local alias
    caps        TEXT[] NOT NULL,      -- {text,vision,code,embed,rerank}
    ctx_tokens  INT,
    max_conc    INT,                  -- slots (local) or concurrency cap (remote)
    rpm         INT, tpm INT,         -- remote rate limits (null = none)
    price_in    NUMERIC, price_out NUMERIC,  -- $/Mtok (null = free/local)
    data_class  TEXT NOT NULL DEFAULT 'remote',  -- local | remote
    enabled     BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE routes (                 -- ordered tiers per role (optionally per tenant)
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT,                -- null = global default
    role       TEXT NOT NULL,
    tier       INT NOT NULL,          -- 0 = primary, 1 = first fallback, …
    strategy   TEXT NOT NULL DEFAULT 'least_loaded',
    spill_ms   INT NOT NULL DEFAULT 800,
    model_id   BIGINT REFERENCES models(id) ON DELETE CASCADE,
    UNIQUE (tenant_id, role, tier, model_id)
);
```
Admin: CRUD on all three + a **test-connection** action (cheap call with the decrypted key).

### 26.6 Capability gating, residency & cost
- **Capability gating:** routing only considers backends whose `caps` include the role
  (UI blocks invalid routes, e.g. vision→text-only, or embeddings→Anthropic which has no
  embeddings endpoint).
- **Data residency (governance):** a per-tenant `residency = local_only | allow_remote`
  flag (and/or a per-route restriction). When `local_only`, candidates with
  `data_class = remote` are excluded for that tenant's content — "spill to cloud" can never
  silently override it. Surface, per tenant, which providers may touch their data.
- **Cost control:** `CostAsc` strategy prefers cheaper/local; remote calls accrue
  `usage_events × pricing` → per-tenant/provider **budgets** with alerts and optional hard
  caps (over budget → drop the paid tier, stay local or queue).
- **Streaming caveat:** failover is transparent only **before** the first streamed token.

---

## 27. Documents: multi-page & multi-image units

Unifies "a 50-page PDF," "a song," and "an ID photographed front + back" under one model: a
**document** is the unit the user retrieves; **files** are its ordered physical pages/members
(each its own blob). This is why §5 is document-centric, §7 tags per-document, and §8 rolls
hits up to documents.

**Forming a document (3 paths):**
- **One file → one document** (default): every plain upload auto-wraps in a 1-page document, so
  the common case needs no thought.
- **Many files → one document** (the ID/front-back case): the UI multi-select ("ingest as one
  document"), the CLI (`ingest --as-document a.jpg b.jpg`), or a manifest groups pages
  explicitly and orders them (`page_no`, `page_label='front'|'back'`). **Reliable and
  deterministic** — no guessing.
- **Add a page later:** "add page" on an existing document appends a member and re-runs the
  document-level tag/summary so the new side is incorporated.
- **Native multi-page files** (PDF/TIFF/multi-page DOCX) become one document with per-page
  chunks automatically.

**Grouping is explicit-only (v1 — decided).** Documents are formed only by deliberate user
action (multi-select / CLI / manifest / add-page); the system never auto-merges. **Heuristic
auto-pairing is deferred** to a later phase, and even then ships strictly as a *confirm-first
suggestion* (detecting `_front/_back`, `_1/_2`, burst timestamps, or a repeated ID number) —
never silent. Rationale: a false pair would fuse two people's records (a privacy + correctness
hazard), and grouping must key on the document/identity, not visual similarity (§17). Until
then, explicit grouping is the only path, which is unambiguous and safe.

**Why retrieval "just works" for an ID:**
- Front and back are chunks of the **same** `document_id`. A query matching either side
  (`name` on the front, `expiry` on the back) returns the **one** ID document — §8 rolls chunk
  hits up by document and dedups, so you never get two halves or a duplicate.
- The result deep-links to the **specific page** that matched (`page_no`), and the document
  detail shows both sides.
- A RAG question ("when does my ID expire?") is answered from the back even though intuition
  about "my ID" might surface the front — because relevant chunks across **all** pages of the
  top document are fed to the model, cited by page.
- The combined `summary` and the canonical `tags` describe the whole ID, not one side, so it's
  also findable by browsing/filtering.

**Governance:** a complete identity document concentrates PII → defaults to `local_only`
routing and is a prime PII-redaction target (§17).

---

## 28. Encryption & data confidentiality (threat model)

**The ceiling, stated plainly:** server-side extraction, embedding, and search require
plaintext, so a *zero-knowledge* guarantee ("only the user can ever decrypt") is impossible
without moving all AI + search to the client (which removes the product). The achievable,
honest posture is **encrypted-by-default with no plaintext at rest + per-tenant isolation +
cryptographic erasure**.

**Key hierarchy (envelope):**
- **KEK** (master) in a **KMS/HSM** (Vault Transit / cloud KMS / HSM) — rotatable, audited,
  ideally never in the app's address space.
- **Per-tenant DEK**, wrapped by the KEK. (Optionally a per-document key wrapped by the DEK.)
- Originals encrypted with the DEK **before** upload to B2; chunk content / derived text columns
  encrypted at rest too; the search **index** (vectors, `tsv`) sits on **encrypted volumes
  (LUKS)** — it must stay queryable, so it is protected at the disk/KMS/RLS layer, not
  individually user-locked.

**What this protects vs not:**
- **Protects:** stolen disks, leaked B2 bucket, DB dumps, cross-tenant leakage, sub-processor
  exposure of cold data. A raw storage breach yields ciphertext.
- **Does NOT protect:** a fully-compromised *running* host / malicious operator — plaintext is
  in RAM during processing and the KMS can unwrap. No server-side-AI system escapes this.

**Narrowing the residual gap (optional, by threat model):**
- **Confidential computing** (AMD SEV-SNP / Intel TDX confidential VMs) → encrypted RAM +
  attestation, so the host/operator can't read process memory.
- **Session-scoped, password-derived keys** (Argon2id → unlocks the DEK only during an
  authenticated session) → the cold store is useless without the user online.
- **Constraint:** background jobs (ingest, re-embed) run while the user is offline, so a
  **server-held wrapping key (KMS) is required** for async work — you cannot have both
  fully-async ingestion and a server that never holds a key. Decide per product: async-with-KMS
  (default) vs ingest-only-while-online (stricter).

**Deletion = crypto-shredding:** "delete tenant/document" destroys the tenant/doc key → data is
unrecoverable everywhere, **including immutable B2 versions and PITR backups** (which can't be
selectively purged). This is how GDPR erasure coexists with 30-day immutable backups.

**Honest marketing language:** "encrypted in transit and at rest, per-tenant keys, zero
plaintext at rest, instant cryptographic erasure" — never "zero-knowledge" or "no one can ever
see your data" while server-side AI is in use. Document the model in the DPA (§30).

## 29. Billing, subscriptions & plans (Stripe)

PCI stays at **SAQ-A** — card data never touches our servers (Stripe Checkout/Elements only).

- **Stripe products:** Billing (subscriptions), Checkout (signup), Customer Portal
  (self-service card/cancel/invoices), Tax (VAT/sales tax), webhooks.
- **Schema:**
```sql
CREATE TABLE plans (
    id            BIGSERIAL PRIMARY KEY,
    code          TEXT UNIQUE NOT NULL,        -- free|pro|team|…
    stripe_price  TEXT NOT NULL,
    quota_bytes   BIGINT NOT NULL,
    token_budget  BIGINT,
    features      JSONB NOT NULL DEFAULT '{}', -- remote-models allowed, max users, rate caps…
    price_cents   INT, currency TEXT
);
ALTER TABLE tenants ADD COLUMN plan_id BIGINT REFERENCES plans(id);
ALTER TABLE tenants ADD COLUMN stripe_customer_id TEXT;
ALTER TABLE tenants ADD COLUMN subscription_id    TEXT;
ALTER TABLE tenants ADD COLUMN billing_status     TEXT DEFAULT 'inactive'; -- active|past_due|canceled|suspended
ALTER TABLE tenants ADD COLUMN current_period_end TIMESTAMPTZ;
```
- **Flow:** signup → choose plan → Checkout → `checkout.session.completed` webhook activates
  the subscription → tenant gets `plan_id` + quota → use.
- **Lifecycle webhooks** (`customer.subscription.created/updated/deleted`,
  `invoice.paid`, `invoice.payment_failed`) set `billing_status`; **dunning** via Stripe Smart
  Retries; on final failure → `suspended` (read-only or blocked, your choice). Upgrades/downgrades
  re-map quota with proration.
- **Webhook hygiene:** verify signatures, **idempotent** handlers (Stripe retries + may
  duplicate), store `event_id` to dedupe. Stripe is the source of truth; we cache only the
  fields above.
- **Quota enforcement:** storage checked pre-upload against `plans.quota_bytes` (hard cap →
  clear error + upsell); token budget gates remote/paid routing (§26) or bills **metered
  overage** computed from `usage_events`.
- **Entitlements:** `plans.features` drives capability gates (e.g. remote models allowed,
  max seats, rate caps) — checked centrally.

## 30. Public frontend, onboarding & email

Beyond the in-app search/upload UI (§12) and admin (§15), the SaaS surface:
- **Marketing:** landing, **pricing** (tiers from `plans`), features, FAQ.
- **Auth & onboarding:** signup/login, **email verification**, password reset → plan choice →
  Checkout → first-upload onboarding.
- **Account & billing:** profile, seats/roles, **embedded Stripe Customer Portal**, plan
  change.
- **Usage dashboard:** storage used vs cap, token usage + spend (from `usage_events`), recent
  activity, **API tokens** for programmatic ingest.
- **Legal (required for a public multi-tenant service):** ToS, Privacy Policy, **DPA +
  sub-processor list** (B2, each remote LLM, email provider, Stripe), AUP, retention/erasure
  statement (ties to §28 crypto-shredding).
- **Transactional email** (Resend/Postmark/SES — **new external dependency**, breaks pure
  self-hosting): verification, Stripe receipts, **quota warnings** (e.g. 80%/100%),
  **payment-failure/dunning** notices, ingest-failed alerts.
- Stack stays §12's axum/Askama/HTMX/Tailwind for cohesion; marketing pages can be static.

---

## 31. Build strategy & autonomous execution protocol

This section is binding on the implementing agent. The objective is **high code quality,
maintainability, and regression-safety**, achieved through **small, individually-verified
increments** — never a single large generation pass. Code quality comes from the
compile → test → fix loop, not from emitting many files at once.

### 31.1 The task ledger (the autonomous, sequential driver)
The agent maintains a checked-in **task ledger** — `BUILD_LEDGER.toml` (or `.json`) at repo
root — as the single source of build progress. It is the "self-caller": the agent does not
free-form decide what to do next; it reads the ledger and advances it.

- The ledger decomposes §10's phases (P0–P12) into **small tasks** (target: a few hours each,
  one module/feature), each with: `id`, `phase`, `description`, `depends_on` (task ids),
  `plan_sections` (which §s to read), `acceptance` (what proves it done), `status`
  (`todo|in_progress|blocked|done`), and `notes`.
- The agent generates the full ledger for the **current phase** first (not the whole project),
  in dependency order, then executes.

**Execution loop (repeat until phase complete):**
1. Select the next `todo` task whose `depends_on` are all `done`.
2. Set `in_progress`. Read its `plan_sections` + the existing code it touches.
3. Implement the task **with its unit tests** (§31.3), following the modularity rules (§31.4).
4. Run the **Definition of Done gates** (§31.2). All must pass.
5. If green → commit (one task = one commit, §31.6), set `status = done`, append a one-line
   note. If a gate fails → fix and re-run; if genuinely blocked → set `blocked` with reason and
   pick the next eligible task.
6. If the task is a **checkpoint** (§31.5) → stop and request human review before continuing.

A thin orchestration convenience is allowed (a `just`/`make` target or shell loop that invokes
the agent per task and runs gates) — but it **orchestrates and verifies**; it never bulk-emits
code. The loop is **idempotent and resumable**: progress lives in the ledger, gates make
partial work safe, so the agent can stop and resume cleanly.

### 31.2 Definition of Done (quality gates — ALL required to mark a task `done`)
A task is not done until, on a clean checkout:
- `cargo build` succeeds (workspace);
- `cargo fmt --check` is clean;
- `cargo clippy --all-targets -- -D warnings` passes (**zero warnings**);
- **unit tests for every function added/changed** pass (§31.3);
- the phase's integration test(s) pass;
- `cargo test` is green for the whole workspace (no regressions);
- `cargo audit` and `cargo deny check` pass;
- every public item has a doc comment;
- **no stubs**: a task containing `todo!()`, `unimplemented!()`, `unreachable!()` (in
  non-genuinely-unreachable paths), or a hardcoded fake return is **not done** — implement it or
  mark `blocked`. (This is the rule that prevents a plausible-looking skeleton.)

CI enforces the same gates on every commit, so "done" means the same thing locally and in CI.

### 31.3 Testing mandate (reliability is paramount)
- **Every function carrying logic has at least one unit test**, colocated (`#[cfg(test)] mod
  tests`), covering the happy path + the meaningful edge/error cases. This is the primary
  regression net and is non-negotiable for the dangerous subsystems.
- **Only exception:** pure one-line delegators with no logic (e.g. `fn x(&self){ self.inner.x() }`)
  may rely on integration coverage — but anything with a branch, a calculation, parsing,
  encryption, or I/O orchestration gets a direct test.
- **TDD for the hard, pure logic** — write tests first for: scheduler acquire/spill/cooldown,
  token buckets, RRF fusion, tag-canonicalization threshold, quota math, envelope
  wrap/unwrap. Use **property-based tests** (`proptest`) for these where inputs are wide.
- **Mock-backend pattern** for the scheduler (already specified): an in-process HTTP server
  asserting free-slot pick, fair wait-then-acquire, priority/tier spill, 429 cooldown,
  rate-limit throttle, dead-host skip, failover, capability gating, residency enforcement.
- **Integration tests** per phase exercise the real seams (Postgres via testcontainers, MinIO
  for blobs) — e.g. ingest→search round-trip, cross-tenant isolation, webhook idempotency.
- **Coverage** tracked (`cargo llvm-cov`/`tarpaulin`) with a **floor (e.g. ≥85% lines, higher
  on crypto/auth/store)**; coverage drop fails CI. Coverage is a guardrail, not the goal —
  meaningful assertions over vanity percentage.
- Deterministic tests only (no network/time flakiness; inject clocks).

### 31.4 Modularity & maintainability standards
- **One responsibility per module**; the module tree mirrors the workspace crates (§4).
- **File-size discipline:** soft cap **~300 lines**, hard cap **~500** per `.rs`. Exceeding it
  is a signal to split by responsibility. No god-files. `mod.rs`/`lib.rs` stay thin
  (re-exports + wiring, not logic).
- **Small functions**, single level of abstraction; extract rather than nest deeply.
- **Trait-per-capability** (already in `core/`): depend on traits, not concretions, so every
  unit is swappable and testable in isolation.
- Errors via `thiserror` per crate; no `unwrap()`/`expect()` outside tests and provable
  invariants (clippy-enforced). Public API documented.
- Each new capability (extractor, provider adapter, store, blob backend) is added as a new
  module implementing the relevant trait — **never** by enlarging an existing file.

### 31.5 Mandatory human-review checkpoints (autonomy has limits)
Even in the autonomous loop, the agent **stops and requests human review before merging** any
task touching these seams — unreviewed code here is silently catastrophic, which is
incompatible with the reliability goal:
- **Tenant isolation / RLS** — must ship with a cross-tenant negative test suite (IDOR/leakage
  attempts) proving isolation; human sign-off required.
- **Encryption & key handling (§28)** — key hierarchy, no-plaintext-at-rest, crypto-shredding.
- **Auth & sessions** — login, password hashing, session/cookie handling, API tokens.
- **Billing webhooks (§29)** — Stripe signature verification + idempotency.
- **Extractor sandbox & prompt-injection boundary (§17, reviews)** — untrusted-bytes handling.
The agent may *implement and test* these autonomously but must pause at the review gate, not
self-merge.

### 31.6 Sequencing & commit discipline
- **Walking skeleton first:** complete the **P0–P4 vertical slice** (ingest one text file → tag
  → embed → store → hybrid search → result) before adding breadth. Prove the architecture
  end-to-end; only then proceed to encryption, multi-tenancy, billing, frontend.
- **One task = one commit/PR**, conventional-commit messages referencing the ledger `id`, so
  regressions bisect cleanly and history is reviewable.
- Update the ledger in the same commit as the work it tracks.
- Verify external versions (crate APIs, model names/quants) against current sources before use
  — the plan's names may have drifted; do not trust them blindly.
- Treat the plan as living: if implementation reveals a gap or a needed correction, note it in
  the ledger and surface it for the plan to be updated — don't silently diverge.
