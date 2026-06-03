"""Catalog — API platform & integrations (how companies integrate).

Beyond the covered token create/list/revoke + Bearer-401 + api-contract basics. A
code audit found API tokens have no scopes (a Bearer token = the user's full role),
no per-token rate limit, untested expiry/last-used, a top-100 search ceiling with no
pagination, and no CORS/idempotency/webhooks/versioning/OpenAPI. Tests that assert
the intended contract against current behavior are PENDING (they fail = findings);
wholly-absent surfaces with no endpoint to call are BLOCKED.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx
import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.api


# ── Local helpers (allowed: defined here, the only file we may edit) ──────────────
#
# These mirror a real API integrator: a raw httpx client (so error statuses,
# headers and redirects are observable, unlike the ApiClient wrapper which raises),
# Bearer-only and cookie-only variants for auth-parity checks, and a couple of
# token CRUD shortcuts over the documented /api/tokens endpoints.


def _anon_client() -> httpx.Client:
    """A raw, unauthenticated HTTP client (no redirects → statuses are observable)."""
    return httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        follow_redirects=False,
        timeout=config.http_timeout(),
    )


def _bearer_client(token: str) -> httpx.Client:
    """A raw client that authenticates *only* via ``Authorization: Bearer`` (no cookie)."""
    c = _anon_client()
    c.headers["Authorization"] = f"Bearer {token}"
    return c


def _cookie_client() -> httpx.Client:
    """A raw client logged in as the bootstrap admin via a ``__Host-session`` cookie."""
    c = _anon_client()
    r = c.post(
        "/auth/login",
        json={
            "tenant_slug": config.tenant_slug(),
            "email": config.admin_email(),
            "password": config.admin_password(),
        },
    )
    assert r.status_code == 200, f"admin login failed: {r.status_code} {r.text}"
    return c


def _login_admin(api: ApiClient) -> None:
    """Authenticate the ApiClient fixture as the bootstrap admin."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())


def _create_token(api: ApiClient, name: str, *, ttl_secs: int = 0, **extra: Any) -> dict[str, Any]:
    """Create an API token via ``POST /api/tokens`` and return the parsed response.

    ``extra`` lets a test send aspirational fields the contract *should* honour
    (e.g. a ``scopes`` array); unknown fields are simply ignored by the current
    server, which is exactly what some of these tests are designed to surface.
    """
    body: dict[str, Any] = {"name": name}
    if ttl_secs:
        body["ttl_secs"] = ttl_secs
    body.update(extra)
    r = api._c.post("/api/tokens", json=body)
    assert r.status_code == 201, f"token create failed: {r.status_code} {r.text}"
    return r.json()


def _list_tokens(api: ApiClient) -> list[dict[str, Any]]:
    """Return the metadata list from ``GET /api/tokens``."""
    r = api._c.get("/api/tokens")
    assert r.status_code == 200, f"token list failed: {r.status_code} {r.text}"
    return r.json().get("tokens", [])


def _find_token(items: list[dict[str, Any]], token_id: int) -> dict[str, Any] | None:
    """Find a token's metadata by id in a list response."""
    return next((t for t in items if t.get("id") == token_id), None)


def _is_error_envelope(resp: httpx.Response) -> tuple[bool, str]:
    """Whether a response body is the app's ``{error, message}`` JSON envelope.

    Returns ``(ok, detail)`` where ``detail`` describes the violation when not ok.
    """
    try:
        body = resp.json()
    except Exception:
        return False, f"{resp.status_code} non-JSON body {resp.text[:120]!r}"
    if not isinstance(body, dict):
        return False, f"{resp.status_code} body is not an object: {body!r}"
    if "error" not in body or "message" not in body:
        return False, f"{resp.status_code} keys={sorted(body)}"
    return True, ""


# ── Tests ─────────────────────────────────────────────────────────────────────────


def test_api_token_scopes_enforced(api):
    """A read/search-scoped token cannot ingest or perform admin actions.

    Audit: no scope column; ``validate_token`` returns the creator's full role. Create
    a token intended as read-only and assert it is REFUSED on ``POST /api/ingest`` —
    expected to FAIL, surfacing that a leaked token grants full account power.
    """
    _login_admin(api)
    # Ask for a least-privilege, read/search-only token (fields the contract *should*
    # honour). The current server ignores them, so the token carries full account power.
    created = _create_token(
        api,
        "read-only-search",
        scopes=["search", "documents:read"],
        scope="read",
    )
    raw = created["token"]

    with _bearer_client(raw) as bearer:
        resp = bearer.post(
            "/api/ingest",
            files=[("files", ("scope-probe.txt", b"scope enforcement probe", "text/plain"))],
        )

    assert resp.status_code in (401, 403), (
        f"a read/search-only token was allowed to POST /api/ingest (status "
        f"{resp.status_code}); API tokens carry no scopes, so a leaked read token "
        "grants full write/admin power over the account"
    )


def test_api_token_expiry_enforced(api):
    """A token created with a short ttl_secs is rejected (401) after it expires."""
    _login_admin(api)
    created = _create_token(api, "expiry-probe", ttl_secs=1)
    raw = created["token"]

    with _bearer_client(raw) as bearer:
        # Immediately valid.
        fresh = bearer.get("/api/tokens")
        assert fresh.status_code == 200, (
            f"a freshly created token should authenticate, got {fresh.status_code}"
        )

        # Wait past the 1-second TTL, then the same token must be refused.
        time.sleep(3)
        expired = bearer.get("/api/tokens")

    assert expired.status_code == 401, (
        f"an expired (ttl_secs=1) token still authenticated with status "
        f"{expired.status_code}; token TTL is not enforced on validation"
    )


def test_api_token_last_used_tracked(api):
    """A successful Bearer request updates the token's last_used_at (rotation hygiene)."""
    _login_admin(api)
    created = _create_token(api, "last-used-probe")
    token_id = created["id"]
    raw = created["token"]

    # A brand-new token has never been used.
    before = _find_token(_list_tokens(api), token_id)
    assert before is not None, "created token not found in the token list"
    assert before.get("last_used_at") is None, (
        f"a token that was never used reports last_used_at={before.get('last_used_at')!r}"
    )

    # Make one successful Bearer-authenticated call with the token.
    with _bearer_client(raw) as bearer:
        used = bearer.get("/api/tokens")
        assert used.status_code == 200, (
            f"Bearer call should succeed, got {used.status_code} {used.text}"
        )

    after = _find_token(_list_tokens(api), token_id)
    assert after is not None, "token disappeared from the list after use"
    assert after.get("last_used_at") is not None, (
        "last_used_at did not advance after a successful Bearer call — operators "
        "cannot tell which tokens are active for rotation"
    )


def test_bearer_token_parity_across_api(api):
    """A Bearer-only client (no cookie) can drive ingest/search/documents/file/jobs/tokens.

    Enterprises integrate via tokens; assert each protected ``/api/*`` route behaves
    identically under Bearer auth as under a cookie session (the existing test only
    proves search).
    """
    _login_admin(api)
    created = _create_token(api, "parity-probe")
    raw = created["token"]

    with _bearer_client(raw) as bearer, _cookie_client() as cookie:
        # 1. Ingest must be drivable by a Bearer-only client (no cookie at all).
        marker = flows.unique_marker()
        content = (
            f"Bearer parity probe document covering quarterly revenue and invoices. "
            f"marker {marker}."
        ).encode("utf-8")
        ingest = bearer.post(
            "/api/ingest",
            files=[("files", (f"{marker}.txt", content, "text/plain"))],
        )
        assert ingest.status_code == 202, (
            f"a Bearer-only client could not ingest: {ingest.status_code} {ingest.text}"
        )
        doc_id = ingest.json().get("document_id")
        assert doc_id is not None, f"ingest returned no document_id: {ingest.json()}"

        # Discover a file id to exercise the file-download route under both auths.
        detail = bearer.get(f"/api/documents/{doc_id}")
        assert detail.status_code == 200, (
            f"Bearer document fetch failed: {detail.status_code} {detail.text}"
        )
        files = detail.json().get("files", [])
        assert files, "document detail returned no files to download"
        file_id = files[0]["id"]

        # 2. Every protected GET route must behave identically under Bearer vs cookie.
        cases: list[tuple[str, str, dict[str, Any]]] = [
            ("GET", "/api/search", {"params": {"q": marker, "limit": 5}}),
            ("GET", f"/api/documents/{doc_id}", {}),
            ("GET", f"/api/documents/{doc_id}/file/{file_id}", {}),
            ("GET", "/api/jobs/999999999", {}),  # nonexistent → both 404, proves reachability
            ("GET", "/api/tokens", {}),
        ]
        for method, path, kw in cases:
            rb = bearer.request(method, path, **kw)
            rc = cookie.request(method, path, **kw)
            assert rb.status_code != 401, (
                f"Bearer auth was rejected (401) on {method} {path}; the route is not "
                "reachable with a token, only with a cookie"
            )
            assert rb.status_code == rc.status_code, (
                f"{method} {path}: Bearer status {rb.status_code} != cookie status "
                f"{rc.status_code} (auth methods are not at parity)"
            )

        # 3. Search via Bearer must actually find the just-ingested document.
        search = bearer.get("/api/search", params={"q": marker, "limit": 5})
        assert search.status_code == 200, f"Bearer search failed: {search.status_code}"
        assert search.json().get("hits"), (
            f"Bearer search returned no hits for {marker!r}; token auth is not at "
            "functional parity with cookie auth on /api/search"
        )


def test_per_token_rate_limit(api):
    """Rapid requests on one token are eventually throttled (429 + Retry-After).

    Audit: only a concurrency limiter exists; no frequency limit reads the plan
    rate_caps. Burst a token and assert 429 — expected to FAIL, surfacing the gap.
    """
    _login_admin(api)
    created = _create_token(api, "rate-limit-probe")
    raw = created["token"]

    statuses: list[int] = []
    retry_after: str | None = None
    with _bearer_client(raw) as bearer:
        for _ in range(120):
            r = bearer.get("/api/tokens")  # cheap, token-authed, tenant-scoped
            statuses.append(r.status_code)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                break

    assert 429 in statuses, (
        f"no request was throttled after {len(statuses)} rapid calls on one token "
        f"(statuses observed: {sorted(set(statuses))}); per-token request rate "
        "limiting is absent"
    )
    assert retry_after is not None, (
        "a 429 throttle response did not include a Retry-After header"
    )


def test_error_envelope_consistent(api):
    """4xx/5xx across every route family return the same {error,message} JSON shape.

    Trigger error paths on search/jobs/ingest/tokens/documents and diff the keys; also
    assert the middleware 401 returns a JSON body (not an empty body) for parity.
    """
    _login_admin(api)

    responses: dict[str, httpx.Response] = {}
    # 400 — search with a missing required `q`.
    responses["search_400"] = api._c.get("/api/search")
    # 404 — job that does not exist for this tenant.
    responses["jobs_404"] = api._c.get("/api/jobs/999999999")
    # 404 — document that does not exist for this tenant.
    responses["documents_404"] = api._c.get("/api/documents/999999999")
    # 400 — ingest with a part not named `files`.
    responses["ingest_400"] = api._c.post(
        "/api/ingest",
        files=[("not_files", ("x.txt", b"hi", "text/plain"))],
    )
    # 4xx — token create with the required `name` omitted (validation error).
    responses["tokens_validation"] = api._c.post("/api/tokens", json={"ttl_secs": 5})
    # 401 — the auth middleware rejecting an unauthenticated request to /api/*.
    with _anon_client() as anon:
        responses["middleware_401"] = anon.get("/api/search")

    violations: list[str] = []
    for label, resp in responses.items():
        assert resp.status_code >= 400, (
            f"{label}: expected an error status, got {resp.status_code} {resp.text[:120]!r}"
        )
        ok, detail = _is_error_envelope(resp)
        if not ok:
            violations.append(f"{label}: {detail}")

    assert not violations, (
        "error responses do not share the {error, message} JSON envelope:\n  "
        + "\n  ".join(violations)
    )


def test_idempotency_key_dedupes_retry(api):
    """Two identical POST /api/ingest with the same Idempotency-Key return the same job/document.

    Audit: no idempotency handling. Assert the intended contract (same document) —
    expected to FAIL, surfacing that retried uploads duplicate work.
    """
    _login_admin(api)
    marker = flows.unique_marker()
    key = str(uuid.uuid4())
    content = (
        f"Idempotency probe — a client retried this exact upload. marker {marker}."
    ).encode("utf-8")
    files = [("files", (f"{marker}.txt", content, "text/plain"))]

    r1 = api._c.post("/api/ingest", files=files, headers={"Idempotency-Key": key})
    r2 = api._c.post("/api/ingest", files=files, headers={"Idempotency-Key": key})

    assert r1.status_code == 202, f"first ingest failed: {r1.status_code} {r1.text}"
    assert r2.status_code == 202, f"retried ingest failed: {r2.status_code} {r2.text}"

    doc1 = r1.json().get("document_id")
    doc2 = r2.json().get("document_id")
    assert doc1 is not None and doc2 is not None, (
        f"ingest responses missing document_id: {r1.json()} / {r2.json()}"
    )
    assert doc1 == doc2, (
        f"two POST /api/ingest with the same Idempotency-Key {key!r} created distinct "
        f"documents ({doc1} != {doc2}); the retry was not de-duplicated and the upload "
        "was processed twice"
    )


def test_api_cors_policy(api):
    """A cross-origin preflight to /api/* returns a defined CORS policy (not *+credentials).

    Audit: no CorsLayer anywhere. Send an OPTIONS preflight with Origin +
    Access-Control-Request-Method and assert the intended policy (locked-down or
    explicit allow-list) — documents the absent/!defined policy.
    """
    with _anon_client() as client:
        resp = client.request(
            "OPTIONS",
            "/api/search",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "authorization",
            },
        )

    acao = resp.headers.get("access-control-allow-origin")
    acac = (resp.headers.get("access-control-allow-credentials") or "").lower()

    # A credentialed wildcard is the one combination that must never appear.
    assert not (acao == "*" and acac == "true"), (
        "CORS preflight returned 'Access-Control-Allow-Origin: *' together with "
        "credentials — any site could drive the authenticated API"
    )

    # A deliberate, defined policy answers the preflight with an ACAO header. The
    # app ships none, so this surfaces the absent cross-origin policy.
    assert acao is not None, (
        f"OPTIONS /api/search preflight returned status {resp.status_code} with no "
        "Access-Control-Allow-Origin header — there is no defined CORS policy "
        "governing cross-origin API access"
    )
    # And a locked-down policy must not blanket-allow an arbitrary untrusted origin.
    assert acao != "https://evil.example.com", (
        "CORS policy reflected an arbitrary untrusted Origin "
        "(https://evil.example.com) — the allow-list is not locked down"
    )


def test_job_status_exposes_document_link(api, page):
    """Polling a job to terminal state lets a client reach the resulting document.

    Audit: the job-status response has no document_id, so a token client polling a job
    can't learn the produced document. Assert the job→document linkage in the contract.
    """
    _login_admin(api)

    # Create a document, then enqueue a real (async) job against it via the re-tag
    # action — the upload path itself uses sentinel job_id 0 and is not pollable.
    marker, doc = flows.ingest_and_wait(
        api, "Document used to exercise job→document linkage. marker {marker}"
    )
    doc_id = doc["id"]

    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto(f"/documents/{doc_id}")
    page.click(f"form[action='/documents/{doc_id}/retag'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # Find the newest re-tag job id from the admin jobs table (ordered newest-first).
    page.goto("/admin/jobs")
    page.wait_for_selector("table tbody tr")
    rows = page.locator("table tbody tr")
    job_id: int | None = None
    for i in range(rows.count()):
        cells = rows.nth(i).locator("td")
        if cells.count() >= 2 and cells.nth(1).inner_text().strip().lower() == "retag":
            job_id = int(cells.nth(0).inner_text().strip())
            break
    assert job_id is not None, (
        "no re-tag job appeared in /admin/jobs after triggering a re-tag"
    )

    # The job-status contract must expose the document the job produced/affected.
    job = api.get_job(job_id)
    assert "document_id" in job, (
        f"GET /api/jobs/{job_id} response exposes no document link (keys={sorted(job)}); "
        "a token client polling a job cannot reach the resulting document"
    )
    assert job["document_id"] == doc_id, (
        f"job {job_id} reports document_id={job['document_id']!r}, expected {doc_id}"
    )


@pytest.mark.skip(reason="blocked: search has no offset/cursor + no list endpoints (top-100 ceiling)")
def test_deep_pagination_and_list_endpoints(api):
    """A client can page past the top-100 and enumerate all their documents/jobs.

    BLOCKED: no offset/cursor on search and no ``GET /api/documents`` / jobs list — the
    tail of a large corpus is unreachable via the API.
    """
    ...


@pytest.mark.skip(reason="blocked: no bulk/batch endpoint with per-item results")
def test_bulk_operations_partial_failure(api):
    """A bulk ingest reports per-item success/failure (not all-or-nothing/opaque 202).

    BLOCKED: multi-file ingest returns a single job_id/document_id with no per-file
    status array; no bulk-delete.
    """
    ...


@pytest.mark.skip(reason="blocked: no outbound-webhook subsystem")
def test_outbound_webhooks_signed_delivery(api):
    """Registering a webhook yields signed, retried delivery on job.completed/document.ready.

    BLOCKED: the only webhook is inbound Stripe; no event emission/signing/delivery for
    customer systems.
    """
    ...


@pytest.mark.skip(reason="blocked: no API versioning / deprecation strategy")
def test_api_versioning(api):
    """A version namespace (/api/v1) or version header exists for stable integrations.

    BLOCKED: routes are unversioned; breaking changes would silently break integrators.
    """
    ...


@pytest.mark.skip(reason="blocked: no served OpenAPI/contract document")
def test_openapi_spec_served_and_accurate(api):
    """A served OpenAPI spec matches the actual routes/schemas (SDK generation).

    BLOCKED: no openapi/utoipa/api-docs endpoint; no machine-readable contract.
    """
    ...
