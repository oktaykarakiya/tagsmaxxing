# Feature Gaps & Roadmap

**Date: 2026-05-31.** Maps the operator's stated requirements to the plan. Legend:
✅ already specified/built · 🔶 partial · ❌ genuine gap (new work).

---

## 1. Uploads never fail; async queue; status queued/failed/succeeded; re-queue transient failures; max-queue-per-endpoint → spill
- ✅ **The decoupling you want is already the design.** Upload streams bytes → encrypts →
  writes blob to B2 → commits a durable `jobs` row → returns a job id immediately (§7, §12, §16).
  Processing happens later. **"All LLMs crashed" still stores the file instantly** — it sits in
  `queued`. (Full treatment in `05-encryption-and-ingestion-model.md`.)
- ✅ Durable queue with `attempts`, exponential-backoff `run_after`, inspectable/replayable
  **dead-letter** (§16). Transient failures re-queue automatically.
- ✅ Crash-safe concurrent workers via `SELECT … FOR UPDATE SKIP LOCKED` (§16).
- 🔶 **Don't conflate two queues.** (a) the *ingestion job queue* (one global Postgres queue;
  where priority lives) vs (b) *per-endpoint inference capacity* (the scheduler). "Max queue per
  endpoint → next endpoint" is **(b)** and is **already built** (§26.4 tiers + `spill_after` +
  cooldown/circuit-break).
- 🔶 **User-facing status vocabulary.** Surface an explicit `queued | failed | succeeded` enum
  (thin view over `documents.status` + `jobs.status`), including the "stored but not yet
  processed / degraded" state.

## 2. Admin-set per-user priority (unbounded int, higher = first) + free/comp accounts
- ❌ **Genuine gap.** Today's priorities are *backend* priority (§6) and an *interactive-vs-batch*
  lane (§16); there is **no per-user/tenant scheduling priority**. Add a `priority INT` on `jobs`
  (derived from tenant/user), claim ordered by `priority DESC, run_after ASC`. To bite on
  *interactive* contention too, the scheduler's fair-semaphore wait needs a priority-aware waiter
  queue (real but bounded).
- ✅ Free/comp accounts fit cleanly: a `free` plan code or `billing_exempt` flag (§29 plans +
  `features` JSONB).
- **Also handle (not asked):** (a) **starvation** — unbounded top priority starves everyone; add
  **aging**. (b) Free ≠ free *to you* — keep per-plan rate caps/quotas on free accounts or one
  abusive friend tanks paying users. (c) Decide per-tenant vs per-user granularity. (d) **Audit**
  every priority change (§15). (e) Priority must apply to *both* queues or it's cosmetic.

## 3. Granular per-endpoint control: bandwidth, hour/tokens/prompts, per container/API
- ✅ **Strongest, already-built area** (P3 scheduler). `Capacity = Slots | Concurrency |
  Rated{conc, rpm, tpm}` (§26.2) + `models(max_conc, rpm, tpm, ctx_tokens, pricing)` (§26.5),
  **hot-reloaded from the DB** — adding/tuning a backend is a row edit, no restart. Each
  container/API = its own `Backend` row, so per-container control already exists.
- 🔶 **Widen the knobs you named:** per-**hour/day** windows (only per-minute today — a
  `TokenBucket` window variant); **time-of-day gates** ("heavy GPU at night," "cloud 9–5") — not
  present, a small scheduling predicate.
- ❌ **Bandwidth (MB/s) throttling** — not in the plan; needs a rate-limited HTTP client/reader
  per backend (the one piece that isn't just a config row).
- **Decentralized home-server reality (plan now):** residential ISPs mean NAT, dynamic IPs,
  upload caps → put backends on a **mesh VPN (Tailscale/WireGuard)**, never expose inference
  ports publicly. Consider GPU thermal/duty-cycle limits, cold-start/warm-up, and
  electricity-cost-aware routing (extends the `CostAsc` strategy).

## 4. Load all user data into memcached on login?
- ❌ **Don't.** It fights the stateless/disposable-node design (§1), doesn't scale (a 100k-doc
  tenant), and metadata reads aren't the bottleneck — **LLM inference and B2 egress are.**
- **Instead:** cache *expensive derived* things — session data, rendered thumbnails, presigned
  URLs, short-TTL search-result pages. Use **PgBouncer** (already §22) + good indexes (HNSW/GIN).
  Legitimate micro-opt: on login, prefetch *only* the first page of recent docs + thumbnails so
  the dashboard paints instantly. That's "warm the cache," not "load everything."

## 5. Lazy-load files; cache to avoid B2 3× free egress
- ✅ Already correct: presigned direct-from-B2 downloads (bypass the app), local read-through LRU
  cache, lazy loading (§20, §12).
- ❌ **Two big additions:**
  1. **Put Cloudflare in front of B2** — they're in the **Bandwidth Alliance, so B2→Cloudflare
     egress is free** and edge-cached. This is *the* B2 cost trick and isn't in the plan.
  2. **Generate small thumbnails/previews at ingest** and serve *those* for grid/browse — casual
     browsing then never pulls full originals from B2. Originals fetched only on explicit click.
  Together these largely eliminate the egress worry.

## 6. Manual tag curation after upload
- ✅ Specified: editable tags + re-canonicalize + re-tag button (§12); admin merge/rename
  (§6.5/§15).
- 🔶 **Add tag provenance:** mark each `file_tags` row `source = llm | user` and **lock user
  tags** so a later LLM re-tag never deletes human-curated tags. Without this, "re-tag" silently
  clobbers manual work.

## 7. (operator left blank)
Placeholder — the 7th item was not provided. Fill in and re-map.

## Resiliency note — survive cloud/package outages
Good instinct; ~70% there already (committed `Cargo.lock`, pinned toolchain, `cargo-deny`
source allowlist). To be genuinely cloud-crash-proof:
- `cargo vendor` + `.cargo/config.toml` source replacement (or run a `panamax`/`kellnr` mirror).
- **Mirror model weights** — GGUFs aren't on crates.io and HF can rate-limit/remove them.
- Mirror base **container images** (`debian:slim`, `pgvector`, Tika) into a local registry
  (§14 pulls from docker.io).
- Vendor the RustSec advisory DB for offline `cargo audit`.
Aligns with the self-sufficiency goal (§25).

---

## Prioritized new work (the actual deltas)
| Priority | Item | Where | Size |
|---|---|---|---|
| High | Per-user priority on the job queue **+ aging/fairness** | new; `jobs` + scheduler | M |
| High | Cloudflare-in-front-of-B2 + **ingest-time thumbnails** | extends §20/§12 | S–M |
| High | Tag **provenance + lock** (`source=llm|user`) | extends §6.5/§12 | S |
| Med | Per-**hour/day** limits + **time-of-day** routing windows | extends §26.2 `TokenBucket` | M |
| Med | **Bandwidth (MB/s)** throttle per backend | new rate-limited client | M |
| Med | Explicit user-facing `queued/failed/succeeded` status + degraded state | extends §16/§12 | S |
| Med | Quota/rate caps enforced on **free** accounts | extends §29 | S |
| Low | Mesh-VPN guidance + electricity/thermal-aware routing | ops + extends §26 | S–M |
| Low | Supply-chain vendoring (crates, weights, images, advisory DB) | §25 | M |

> Drop the memcached idea (#4). Encrypt at *write*, not after processing (see doc 05).
