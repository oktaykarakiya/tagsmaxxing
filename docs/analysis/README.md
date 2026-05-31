# Analysis & Positioning — Local File Knowledge Base

This folder captures a structured assessment of the project: where the code stands,
how it compares to adjacent products, whether it is "enterprise ready," and the concrete
feature gaps surfaced while discussing the operator's requirements.

It exists to be reused — for **marketing** (positioning, honest claims), **future
implementation** (roadmap gaps with plan references), and **investor/enterprise**
conversations (readiness verdict).

> **Snapshot date: 2026-05-31.** Anything describing *current build state* (coverage %,
> task counts, "not yet built") is point-in-time and will drift. The *analysis* (positioning,
> architecture judgment, gaps) is durable. Each doc dates its state claims.

## Build state at time of writing
- **Ledger:** 40/44 tasks done. P0–P4 complete; P5 (multi-tenancy/auth) at **T1–T4 done**,
  parked at the **P5-T5 auth-middleware checkpoint** (mandatory human review, plan §31.5).
- **Gates (`just ci`):** all green — `fmt`, `build`, `clippy -D warnings` (0 warnings),
  **709 tests** (0 failed, 72 `#[ignore]`d DB-integration), `deny`, `audit`, coverage **86.29% lines**.
- **Scope built:** the P0–P4 vertical slice (ingest → tag → embed → store → hybrid search →
  rerank → retrieve) + the multi-host model scheduler with failover. The SaaS/enterprise
  surface (billing, public frontend, admin panel, observability, B2 end-to-end, KMS encryption,
  backups/DR, HA) is specified in the plan but **not yet built**.

## Contents
| File | What it's for | Primary audience |
|---|---|---|
| [`01-code-quality-audit.md`](./01-code-quality-audit.md) | Gate-by-gate validation, architecture, strengths & risks | Eng / due diligence |
| [`02-competitive-positioning.md`](./02-competitive-positioning.md) | vs Eagle & TagStudio, category framing, honest messaging | Marketing / strategy |
| [`03-enterprise-readiness.md`](./03-enterprise-readiness.md) | "Plan-grade vs product-ready" verdict, enterprise gaps | Sales / enterprise / founders |
| [`04-feature-gaps-and-roadmap.md`](./04-feature-gaps-and-roadmap.md) | Operator's requirements mapped to the plan + new work | Eng / planning |
| [`05-encryption-and-ingestion-model.md`](./05-encryption-and-ingestion-model.md) | Encryption timing + "uploads never fail" queue design | Eng / security / marketing |

## Source
Derived from a code-quality validation run and a strategy discussion on 2026-05-31, cross-
referenced against [`../../local-kb-plan.md`](../../local-kb-plan.md) (the binding spec) and
[`../../BUILD_LEDGER.toml`](../../BUILD_LEDGER.toml). Section references (§N) point into the plan.
