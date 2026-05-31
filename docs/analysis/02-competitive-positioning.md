# Competitive Positioning

**Date: 2026-05-31.** Compares the Local File Knowledge Base to two products it is often
mentally grouped with — **Eagle** and **TagStudio** — and extracts honest marketing messaging.

## The framing that matters most: this is a different *species*
Eagle and TagStudio are **local, single-user desktop applications** for organizing files. This
project is a **multi-tenant, server-based, AI-native knowledge platform**. Treating them as
direct competitors will mislead both the roadmap and the marketing.

The closest mental model for this project is **not** a design-asset manager — it's a
self-hostable blend of "Paperless-ngx + semantic search + a model-routing gateway," i.e. a
category Eagle and TagStudio deliberately avoid.

## Side-by-side
| | **Eagle** | **TagStudio** | **Local File KB (this project)** |
|---|---|---|---|
| Deployment | Local desktop (Win/Mac) | Local desktop (Python/Qt) | **Multi-tenant server / self-hostable** |
| Target user | One designer, their machine | One privacy-purist, their folders | **Many tenants, many users, remote** |
| Privacy model | Local-only (your disk) | Local-only **by ideology** — explicitly refuses cloud + non-local LLMs | Encrypted cloud + per-tenant keys; honest "not zero-knowledge" |
| AI / search | Image-centric AI search + auto-tag (new in v4) | **None** — string/boolean tag search | **Hybrid vector+FTS, RAG answers, OCR/ASR over every filetype, reranking** |
| File understanding | Great previews; design assets | Previews; all types, no extraction | **Extracts + understands** docs/code/images/audio/video |
| Tag model | Folders, smart folders, auto-tag | **Excellent** — inheritance, aliases, parents, disambiguation | Semantic canonicalization + manual curation |
| Multi-tenant / billing / quotas | No | No | **Yes** — RLS, Stripe, plans, quotas, admin |
| Resilience / DR | N/A (desktop) | N/A | **PITR backups, HA tiers, degradation matrix, crypto-shred** |
| Heterogeneous self-hosted inference | No | No | **Yes** — multi-host scheduler, primary→fallback routing, cost control |
| UX maturity | **Outstanding** — 400k users, plugins, browser extension | Usable alpha, 7k★, 141 contributors | **~none yet** (HTMX UI is a later phase) |
| Cost / license | Paid, one-time, closed | Free, GPL-3, open | TBD (plan leans Apache/MIT core + paid SaaS) |
| Maturity | Shipped v4 | Alpha v9.5 | **Vertical slice done; SaaS surface unbuilt** |

## Where we genuinely win
- **AI retrieval across heterogeneous files.** Semantic + keyword hybrid search, reranking, and
  RAG answers over documents, code, images, audio, and video — with OCR and ASR built in.
  Neither competitor does this. Eagle's AI is image-centric and recent; TagStudio refuses LLMs.
- **Self-hosted model orchestration.** A slot/rate-aware scheduler that load-balances and fails
  over across many local GPUs *and* remote APIs, with per-backend cost/residency control. This
  is a serious differentiator and is **already built** (plan §6/§26).
- **Operability for teams/orgs.** Multi-tenancy with RLS, quotas, billing, audit, backups — a
  real platform, not a single-user tool.
- **Privacy honesty + governance.** Per-tenant encryption keys, crypto-shredding, and
  `local_only` routing that keeps PII-heavy content (e.g. ID documents) off remote LLMs (§17/§28).

## Where we lose (be honest)
- **UX polish.** Eagle is the benchmark: hover previews, color search, browser extension, font
  management, plugin ecosystem, 400k-user-tested flows. This project has essentially **no UI yet**.
- **Privacy by construction.** TagStudio is *categorically* more private: it never sends data
  anywhere and has no operator to compromise. Our own §28 admits a fully-compromised running host
  can see plaintext (true of any server-side-AI system). We cannot out-privacy a pure-local tool.
- **Zero-setup single-user simplicity.** Both competitors are "download and run." We require
  Postgres, a blob store, and model backends.
- **Maturity & track record.** They ship; we have a vertical slice.

## Marketing messaging — claims you can stand behind
**Use:**
- "Find anything in your files by *meaning*, not just filename or tag — across documents, code,
  images, audio, and video."
- "Your own models, your own servers: route inference across local GPUs and cloud APIs with
  automatic failover and cost control."
- "Encrypted in transit and at rest, per-tenant keys, **zero plaintext at rest**, instant
  cryptographic erasure." (Exact wording sanctioned by §28.)
- "PII-aware: identity documents stay on local models and never leave your infrastructure."
- "Self-hostable and open-format — your data is portable and exportable (GDPR-ready)."

**Do NOT use (will not survive scrutiny):**
- ❌ "Zero-knowledge" or "no one can ever see your data." Server-side AI needs plaintext in RAM;
  §28 forbids this claim. TagStudio could make it; we cannot.
- ❌ "More private than [local tool]." We are more private than *cloud* products; a local-only
  tool is more private than us by construction. Frame it as "private cloud AI," not "most private."
- ❌ "Enterprise ready" as an unqualified claim today — see `03-enterprise-readiness.md`.

## Positioning one-liner
> A **private, AI-native knowledge base for all your files** — self-hostable, multi-tenant, and
> built to run your own models across your own machines. Eagle organizes design assets;
> TagStudio tags local files; this *understands and retrieves* anything, at team scale.
