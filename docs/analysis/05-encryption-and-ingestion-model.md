# Encryption & the "Uploads Never Fail" Ingestion Model

**Date: 2026-05-31.** Answers two recurring questions: *when* should encryption happen, and how
do uploads "never fail" even when every model backend is down. Grounds the design in plan
§7, §16, §20, §22, §26, §28.

---

## TL;DR
- **Encrypt at *write*, not "after processing."** The original is encrypted *before* it ever
  touches B2. Encrypting "after processing" would leave plaintext in B2 during the whole queue
  wait — exactly the "all LLMs down" window you want to avoid.
- **Encryption is orthogonal to the queue.** It sits on the blob write path (cheap,
  deterministic, no LLM), so even with every model offline, the file is encrypted + stored +
  queued instantly.
- **Two distinct queues.** The durable *ingestion job queue* (what to process next) is separate
  from *per-endpoint inference capacity* (which GPU/API serves a given model call). Don't
  conflate them.

---

## How encryption works here (envelope, per §20/§28)
Key hierarchy:
- **KEK** (master) in a **KMS/HSM** (Vault Transit / cloud KMS / HSM) — rotatable, audited,
  ideally never in the app's address space.
- **Per-tenant DEK**, wrapped by the KEK. (Optionally a per-document key under the DEK.)
- A random **per-file data key** encrypts the blob; it is wrapped by the DEK.

What is encrypted, and where:
- **Originals:** encrypted with the data key **before** upload to B2 → B2 holds only ciphertext.
- **Derived text / chunk content columns:** encrypted at rest too.
- **Search index** (vectors, `tsv`): must stay queryable, so it is protected at the **disk
  layer (LUKS) + KMS + RLS**, not individually user-locked.

What this protects vs not (stated honestly, §28):
- **Protects:** stolen disks, leaked B2 bucket, DB dumps, cross-tenant leakage, sub-processor
  exposure of cold data — a raw storage breach yields ciphertext.
- **Does NOT protect:** a fully-compromised *running* host / malicious operator — plaintext is in
  RAM during processing and the KMS can unwrap. No server-side-AI system escapes this. (This is
  why we never claim "zero-knowledge.")

**Deletion = crypto-shredding:** destroying the tenant/doc key makes data unrecoverable
everywhere, including immutable B2 versions and PITR backups. This is how GDPR erasure coexists
with 30-day immutable backups.

---

## Why "encrypt at write" (not "after processed")
| | Encrypt at **write** (chosen) | Encrypt "after processing" (rejected) |
|---|---|---|
| Plaintext window in B2 | None | From upload until processing finishes — unbounded if LLMs are down |
| Behavior when all LLMs down | File already encrypted + safe | File sits in B2 **as plaintext** |
| Coupling | Independent of the model queue | Coupled to a slow, failure-prone stage |

The only availability dependency for "encrypt at write" is the KMS to unwrap the DEK. Mitigate by
**caching unwrapped DEKs for active tenants in memory**, so a KMS blip doesn't stall ingest.
Tradeoff to decide per §28: *async-with-KMS* (default; background jobs run while the user is
offline) vs *ingest-only-while-online* (stricter; session-scoped, password-derived keys).

---

## The ingestion flow (how "uploads never fail")
The upload path and the processing path are **decoupled** so acceptance never depends on model
availability:

```
UPLOAD (synchronous, fast, no LLM):
  receive stream → validate (size/MIME) → encrypt → write ciphertext to B2
  → INSERT jobs row (status=queued) → return job id        ← user sees "queued" immediately

PROCESS (asynchronous, later, needs plaintext in RAM):
  worker claims job (SELECT … FOR UPDATE SKIP LOCKED)
  → fetch ciphertext → decrypt in memory (DEK via KMS)
  → extract → tag → embed   (every model call via scheduler.acquire(role))
  → write derived artifacts (encrypted) + set documents.status=ready/succeeded
  on failure → attempts++, exponential backoff run_after; exhausted → dead-letter (status=dead)
```

Properties (per §16, §22):
- **Durable queue:** `attempts`, backoff `run_after`, inspectable + replayable **dead-letter**.
- **Crash-safe:** a dead worker's lease expires; the job becomes reclaimable.
- **Backpressure:** cap queue depth / in-flight uploads; return **429** when saturated rather
  than collapsing.
- **Degraded mode:** B2 unreachable → ingest pauses and retries (no crash); a role with no
  healthy backend → jobs wait, queries return a clear error. A degraded banner is surfaced.

## The two queues (don't conflate)
1. **Ingestion job queue** — *one* global Postgres queue. Workers pull via SKIP LOCKED;
   sequential per worker, concurrent across workers. **Priority/aging lives here** (see
   `04-feature-gaps-and-roadmap.md` §2).
2. **Per-endpoint inference capacity** — the **scheduler** (§6/§26). Each backend has a capacity
   guard (`Slots | Concurrency | Rated{conc,rpm,tpm}`). "Max queue per endpoint → spill to the
   next endpoint" is **this** layer: §26.4 ordered tiers + `spill_after` + cooldown/circuit-break.
   **Already built (P3).**

## User-facing status (recommended explicit enum)
Expose a thin three-state view over the internal `documents.status` + `jobs.status`:
- **queued** — accepted + stored (encrypted), not yet processed (covers "backend crashed / all
  LLMs busy or down").
- **failed** — processing exhausted retries → dead-letter (admin can inspect/replay).
- **succeeded** — processed, indexed, retrievable.

This gives the operator the at-a-glance status they asked for while preserving the richer
internal lifecycle (`queued → processing → ready/failed/dead`).
