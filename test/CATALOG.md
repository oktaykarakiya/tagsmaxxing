# Test catalog

The referenceable "huge array" of user-facing tests, **derived from** the app's
capability map (P0–P12) but **independent** of the ledger. Each entry is a
**pending stub** in `tests/catalog/` (a `@pytest.mark.skip` test) to be
implemented in step 2. The smoke test in `tests/smoke/` is already implemented.

> **Philosophy:** a test asserts the *intended* behavior. A failing test means an
> app bug — it is recorded, not fixed and not weakened. Outcomes are appended to
> `test/results/history.csv` on every official run (`./test/run.sh` / `quality` / `perf`).

List everything (including pending) without running the stack:

```bash
cd test && .venv/bin/python -m pytest --collect-only            # all
cd test && .venv/bin/python -m pytest --collect-only -m billing # one feature area
```

Conventions: every test carries a feature **marker** (run a slice with `-m <marker>`);
nondeterministic-quality checks also carry **`judge`** (DeepSeek LLM-as-judge,
`quality` lane only). "Did it do its job" behavioral checks are deterministic;
content-quality checks are judged.

**Status legend:** ✅ implemented · ⏳ pending (`step2:` — the `implement.sh` swarm builds these) ·
⛔ blocked (`blocked:` — feature absent or not drivable black-box; catalogued, not built).
Live status: `python3 scripts/catalog_status.py summary`.

### Original capability map (P0–P12) — implemented
| File | Marker | Status | Covers |
|---|---|---|---|
| `tests/smoke/test_smoke_pipeline.py` | `smoke` | ✅ | ingest → pipeline → search (API + browser) round-trip |
| `tests/catalog/test_auth.py` | `auth` | ✅ | register, login, logout, sessions, cookie attributes |
| `tests/catalog/test_ingest.py` | `ingest` | ✅ | text/pdf/image/audio/video/code/binary, jobs, idempotency |
| `tests/catalog/test_search.py` | `search` | ✅ | hybrid search, filters, rerank, deep-links, doc detail |
| `tests/catalog/test_tagging.py` | `tagging`,`judge` | ✅ | tag/summary/title quality (judged), bounds, locking |
| `tests/catalog/test_multitenant_isolation.py` | `isolation` | ✅ | cross-tenant **read** 404s, RLS, token scoping |
| `tests/catalog/test_quotas.py` | `quotas` | ✅/⛔ | storage quota, cost budget, rate limits, job priority |
| `tests/catalog/test_billing.py` | `billing` | ✅ | checkout, webhook signature + idempotency, dunning |
| `tests/catalog/test_encryption.py` | `encryption` | ✅/⛔ | at-rest encryption, key rotation, crypto-shred, audit |
| `tests/catalog/test_admin.py` | `admin` | ✅ | dashboard, tenants/users, jobs, tag merge, audit, RBAC |
| `tests/catalog/test_providers_routing.py` | `providers` | ✅ | provider/model/route CRUD, failover, hot-reload, cost |
| `tests/catalog/test_email_signup.py` | `email` | ✅ | signup, verification, password reset, templates |
| `tests/catalog/test_account_team.py` | `account` | ✅ | account, team, plan, dashboard, danger zone, legal |
| `tests/catalog/test_api_tokens.py` | `tokens` | ✅ | token create/list/revoke, Bearer auth |
| `tests/catalog/test_degradation.py` | `degradation` | ✅/⛔ | LLM/Tika/blob down, /health 503, graceful shutdown |
| `tests/catalog/test_observability.py` | `observability` | ✅ | health, readiness, Prometheus metrics |
| `tests/catalog/test_performance.py` | `perf` | ✅ | concurrent uploads/searches, throughput, latency, burst drain |

### Web / security / UX expansion (2026-06) — implemented
| File | Marker | Status | Covers |
|---|---|---|---|
| `tests/catalog/test_ui_journeys.py` | `ui` | ✅ | nav, click-through to doc detail, empty-state, kind-filter, tag removal, navbar logout, marketing/pricing |
| `tests/catalog/test_security_headers.py` | `security` | ✅ | CSP/HSTS/clickjacking/nosniff/Referrer/Permissions, CSRF, Cache-Control, no Server leak |
| `tests/catalog/test_api_contract.py` | `api` | ✅ | 404/405/400/4xx, clamped limit, HEAD/OPTIONS, wrong-field, content-type |
| `tests/catalog/test_input_validation.py` | `security` | ✅ | XSS escaping, SQLi safety, path-traversal, unicode, oversized/NUL bytes |
| `tests/catalog/test_session_security.py` | `auth` | ✅ | concurrent/distinct sessions, per-session revoke, throttle, weak-pw |
| `tests/catalog/test_accessibility.py` | `ui` | ✅ | title/lang, single h1, labels, alt text, dup ids, native controls |
| `tests/catalog/test_search_ux.py` | `search` | ✅ | query persists, snippet+tags, provenance deeplink, tag filter, count, ranking order |
| `tests/catalog/test_scheduler_load.py` | `perf` | ✅ | slot-respect/accounting/drain/fairness/health invariants, backpressure, throttle |
| `tests/catalog/test_search_quality.py` | `search`,`judge` | ✅ | semantic recall (paraphrase), conceptual ranking, cross-lingual (judged) |

### Enterprise readiness & core correctness (2026-06) — derived from a 10-agent gap audit
⏳ = swarm-buildable now · ⛔ = feature-absent / not drivable black-box (catalogued so the gap is visible).
| File | Marker | Status | Covers |
|---|---|---|---|
| `tests/catalog/test_tenant_isolation_deep.py` | `isolation` | ⏳ ×15 | cross-tenant **writes**/IDOR, RLS-only tag+vector search, presigned-URL scope, super-admin & global-infra bugs |
| `tests/catalog/test_rbac.py` | `admin` | ⏳ ×9 ⛔ ×2 | last-owner/self-demote guards, member-can't-self-promote, audit-writes; (⛔ audit viewer/export, suspend) |
| `tests/catalog/test_account_security.py` | `auth` | ⏳ ×9 ⛔ ×7 | reset-invalidates-sessions, step-up delete, role propagation, breached-pw; (⛔ MFA, lockout, session mgmt) |
| `tests/catalog/test_sso_saml_oidc.py` | `sso` | ⛔ ×14 | SAML/OIDC/SCIM — **entirely absent** (deferred P13); the #1 enterprise blocker |
| `tests/catalog/test_data_governance.py` | `governance` | ⏳ ×2 ⛔ ×10 | DPA consent, decrypt-audit; (⛔ export, per-doc erasure, residency, retention, legal hold, key rotation, BYOK) |
| `tests/catalog/test_billing_enterprise.py` | `billing` | ⏳ ×3 ⛔ ×9 | seat-limit-500 bug, metering accuracy, rate-cap; (⛔ invoice/PO, annual, tax, trials — most need signed webhooks) |
| `tests/catalog/test_upgrade_journey.py` | `billing` | ⏳ ×1 ⛔ ×7 | dead upgrade-CTA (405); (⛔ existing-data preserve, free-cap-unblock, reprocess under better models) |
| `tests/catalog/test_kb_correctness.py` | `ingest` | ⏳ ×11 ⛔ ×5 | provenance accuracy, exact-match ranking, dedup-orphan bug, formats, 0-byte; (⛔ delete, pagination, OCR, re-embed) |
| `tests/catalog/test_api_platform.py` | `api` | ⏳ ×9 ⛔ ×5 | token scopes/expiry/last-used, Bearer parity, rate-limit, error envelope; (⛔ pagination, webhooks, versioning, OpenAPI) |
| `tests/catalog/test_reliability_ops.py` | `observability` | ⏳ ×7 ⛔ ×5 | backup/DR metrics, dead circuit-breaker metric, RED metrics, liveness vs readiness, retry/dead-letter; (⛔ dep fault-injection, log capture) |

## How step 2 works

For each stub: remove `@pytest.mark.skip`, implement the body using the helpers in
`lib/` (`api_client`, `flows`, and for the browser the pytest-playwright `page`
fixture), and assert behavior. For nondeterministic output, use the `judge`
fixture:

```python
def test_tags_are_relevant(api, judge):
    marker, _ = flows.ingest_and_wait(api, "...invoice/budget text... {marker}")
    doc = api.get_document(api.search(marker)["hits"][0]["document_id"])
    v = judge.verdict(
        rubric="The tags concisely and accurately describe a financial/invoice document.",
        output=", ".join(t["name"] for t in doc["tags"]),
        context=doc.get("summary", ""),
    )
    assert v.passed, v.reason
```

Then run the core suite with `./test/run.sh` (excludes `judge` + `perf`), the judged
checks with `./test/run.sh quality`, or the load lane with `./test/run.sh perf`.

## Implementing the catalog in bulk

`./test/implement.sh` drives this catalog to completion with a swarm of headless
Claude Code agents — **one agent per file, up to `MAX_AGENTS` (default 10) in
parallel**. Status is read live from the code (AST), so the swarm only works on
PENDING tests and skips anything already implemented; it is safe to re-run until
everything is `DONE`.

```bash
./test/implement.sh status         # done / pending / blocked, per file + totals
./test/implement.sh list           # every test with its status
./test/run.sh up                   # bring the stack up so agents can verify
./test/implement.sh run            # spawn agents for all files with pending tests
./test/implement.sh run --dry-run  # show the plan without spawning
MAX_AGENTS=6 IMPLEMENT_MODEL=sonnet ./test/implement.sh run   # tune fan-out / model
```

Each agent edits **only its one file** (no concurrent edits to the same file),
removes the `@pytest.mark.skip`, implements the body using `lib/` + the `page`
fixture, and verifies against the running stack. Per-agent logs are written to
`test/.implement-logs/`. A test an agent finds genuinely blocked is left skipped
with a `blocked: …` reason so it is not retried.
