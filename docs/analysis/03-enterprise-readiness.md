# Enterprise Readiness

**Date: 2026-05-31.**

## Verdict in one line
**The plan is enterprise-*grade*. The product is not enterprise-*ready* yet — and those are
different claims.** The architecture covers controls most commercial products in this space
don't even consider; but "ready" means *shipped, verified, operated*, and most of the
enterprise surface is still unbuilt.

## What is genuinely impressive (rare for a solo build)
The spec already designs for: multi-tenant **RLS**, **envelope encryption + crypto-shredding**,
**PITR backups + tested restores**, **HA tiers**, a **graceful-degradation matrix**, **audit
logs**, **Prometheus** metrics, **SBOM** + supply-chain gates (`cargo deny`/`audit`), **GDPR
export/erasure**, and **DPA / sub-processor** discipline. The §28 honesty ("no zero-knowledge
while server-side AI runs") is itself a maturity signal. This is a serious enterprise checklist.

## Why it is not "ready" today
1. **Only the vertical slice exists.** P0–P5 (ingest→tag→embed→store→search→multi-tenancy/auth)
   is done or in progress; the **entire SaaS + enterprise surface is unbuilt**: Stripe billing,
   public frontend, admin panel, observability wiring, B2 end-to-end, KMS/encryption,
   backups/DR, HA. These are later phases in the plan.
2. **Security-critical tests don't run in CI.** The cross-tenant RLS and auth suites exist but
   are `#[ignore]`d (need Postgres/Podman). "The test exists but never runs automatically" is the
   single biggest risk for a multi-tenant boundary. Fix before any enterprise conversation.
   (See `01-code-quality-audit.md`.)
3. **Enterprise checklist items the plan is light on:**
   - **SSO/SAML + SCIM provisioning** — plan mentions only optional OIDC; enterprises expect
     SAML federation and SCIM user lifecycle.
   - **Fine-grained RBAC / per-document ACLs** — current model is coarse (owner/admin/member);
     enterprises want custom roles and resource-level permissions.
   - **Customer-managed keys (BYOK/CMK)** — per-tenant DEKs exist (§28); BYOK would extend them.
   - **Region / data-residency selection** per tenant.
   - **Certifications** — **SOC 2 Type II, ISO 27001, HIPAA/BAA**. These are a *year-long org
     effort*, not code; the plan's controls (audit, encryption, access control, backups) are the
     substrate, but certification is the real wall for a small team.
4. **The untrusted-bytes boundary needs real sandboxing.** Feeding untrusted file bytes through
   Tika/ffmpeg/image parsers (historically CVE-prone) and untrusted *text* into an LLM (prompt
   injection) is correctly flagged as a checkpoint (§31.5) with MIME/size/clamav guards (§17) —
   but enterprise-grade means extractors in a **locked-down sandbox** (seccomp/gVisor/isolated
   container), not just validation.
5. **The vendor-risk wall (founders underrate this).** A solo-operated SaaS fails enterprise
   vendor review on *organizational* maturity alone: support, SLA, on-call, business continuity
   ("bus factor = 1"), third-party pen-test reports. However well-built, this is real.

## Where it would win an enterprise evaluation today
- Data governance design (per-tenant keys, crypto-shred, `local_only` PII routing).
- Cost/usage governance (per-tenant quotas, token budgets, spend tracking, per-backend pricing).
- Self-hostability (stateless, disposable node, `compose up`, no mandatory SaaS beyond B2).
- Honest, documented threat model (§28) — buyers' security teams respect this.

## Strategic recommendation
For a solo operator, the realistic enterprise path is **"ship excellent self-hostable
open-source software"** (enterprises run it themselves, own their keys and compliance) rather
than "sell multi-tenant SaaS into enterprise security reviews." The architecture is *already*
optimized for the former — stateless, `compose up`, Apache/MIT core (§14). Lead with that;
treat hosted multi-tenant SaaS as a later, separate motion once org maturity and certifications
can follow.

## Readiness checklist (living)
- [ ] Cross-tenant RLS + auth suites run in an automated CI lane (Podman).
- [ ] Coverage gate raised to 85% with per-crate floors on store/auth.
- [ ] Extractor sandboxing (seccomp/gVisor/isolated container).
- [ ] SSO (SAML) + SCIM.
- [ ] Fine-grained RBAC / resource-level ACLs.
- [ ] BYOK/CMK option.
- [ ] Backups/PITR + tested restore shipped and scheduled (§21).
- [ ] Observability shipped (Prometheus + dashboards, §15).
- [ ] Third-party pen test + security audit.
- [ ] SOC 2 Type II / ISO 27001 program (long-horizon).
- [ ] Support / SLA / on-call / business-continuity story.
