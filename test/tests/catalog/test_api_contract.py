"""Catalog (step 2) — JSON API contract: status codes, methods, error shapes.

Black-box checks that the ``/api`` and ``/auth`` JSON surface behaves like a
well-formed HTTP API on the *unhappy* paths that other files don't cover:
unknown routes, wrong methods, missing resources, malformed/oversized bodies and
bad content types. The existing files assert the happy paths and a few specific
negatives (``test_auth`` → 401/409, ``test_ingest`` → empty-payload 400,
``test_search`` → empty query); this file fills the remaining contract gaps and
must not duplicate them.

The key contract under test: **bad input is rejected with a sane 4xx, never a
500 / panic / hang, and never a stack trace**. A failing assertion is a real app
bug and is left failing. Status/headers are read via the raw ``httpx`` client
(``api._c``) because ``ApiClient`` hides them.
"""

from __future__ import annotations

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.api


def _login(api: ApiClient) -> None:
    """Authenticate the client as the bootstrap admin (helper for authed probes)."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())


def test_unknown_api_route_returns_404(api):
    """An authenticated request to a non-existent /api path returns 404.

    Log in, then GET a path that no route matches (e.g. ``/api/does-not-exist``).
    The router must answer 404 (route not found) — not 401 (it's authed), not 200,
    and never 500.
    """
    _login(api)
    r = api._c.get("/api/does-not-exist")
    assert r.status_code == 404, (
        f"unknown /api path must return 404 (not 401/200/500), got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_method_not_allowed_returns_405(api):
    """Using the wrong method on an existing route returns 405 Method Not Allowed.

    ``/api/search`` is GET-only and ``/api/ingest`` is POST-only. Authenticated,
    send a DELETE/PUT to one of them and assert 405 with an ``Allow`` header
    listing the permitted method(s) — not 404 and not 500.
    """
    _login(api)
    r = api._c.request("DELETE", "/api/search")
    assert r.status_code == 405, (
        f"DELETE on GET-only /api/search must be 405 (not 404/500), got "
        f"{r.status_code}: {r.text[:200]}"
    )
    allow = r.headers.get("allow", "")
    assert "GET" in allow.upper(), (
        f"405 must advertise the permitted method(s) in an Allow header, got {allow!r}"
    )


def test_nonexistent_resource_returns_404(api):
    """Fetching a document/job that does not exist returns 404 for an authed user.

    Authenticated, ``GET /api/documents/{huge_id}`` and ``GET /api/jobs/{huge_id}``
    for ids that cannot exist must each return 404 (resource not found) with a
    JSON body — proving the not-found path is handled, not a 500 or a leak of
    another tenant's row.
    """
    _login(api)
    # i64::MAX — a syntactically valid id that no row can occupy.
    huge_id = 9223372036854775807
    for path in (f"/api/documents/{huge_id}", f"/api/jobs/{huge_id}"):
        r = api._c.get(path)
        assert r.status_code == 404, (
            f"GET {path} for a nonexistent id must be 404 (not 500/leak), got "
            f"{r.status_code}: {r.text[:200]}"
        )
        ct = r.headers.get("content-type", "")
        assert "application/json" in ct.lower(), (
            f"{path} 404 must carry a JSON error body, got content-type {ct!r}"
        )


def test_login_malformed_json_returns_400(api):
    """POST /auth/login with a body that isn't valid JSON returns 400.

    Send ``Content-Type: application/json`` but a body of broken JSON (e.g.
    ``{"tenant_slug":``). The app must reject it with 400 Bad Request, not crash
    with a 500.
    """
    r = api._c.post(
        "/auth/login",
        content='{"tenant_slug":',
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 400, (
        f"malformed JSON to /auth/login must be 400 (not 500), got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_login_missing_fields_returns_4xx(api):
    """POST /auth/login with required fields missing returns a client error.

    Send valid JSON that omits a required field (e.g. only ``{"email": "x"}``).
    The app must answer a 4xx (400/422 deserialization error), never a 500.
    """
    r = api._c.post("/auth/login", json={"email": "nobody@example.com"})
    assert 400 <= r.status_code < 500, (
        f"login with missing required fields must be a 4xx (400/422), not a 500, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_unsupported_content_type_rejected(api):
    """POST /auth/login with a non-JSON content type is rejected with a 4xx.

    Send a well-formed key=value body but ``Content-Type: text/plain``. A JSON
    endpoint must reject the unsupported media type with a 4xx (415/400), not
    attempt to parse it and 500.
    """
    r = api._c.post(
        "/auth/login",
        content="tenant_slug=default&email=admin@local.kb&password=admin",
        headers={"Content-Type": "text/plain"},
    )
    assert 400 <= r.status_code < 500, (
        f"non-JSON content-type on /auth/login must be a 4xx (415/400), not a 500, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_search_invalid_limit_handled(api):
    """An out-of-range ``limit`` on /api/search is handled, never a 500.

    Authenticated, call ``/api/search`` with ``limit=0``, a negative limit and an
    absurdly large limit. Each must yield a sane response — either a 400 or a 200
    with a clamped, bounded number of hits — but never a 500 or an unbounded
    result set. Complements ``test_search_limit_param`` (valid limits only).
    """
    _login(api)
    q = flows.unique_marker()
    for limit in ("0", "-1", "999999999"):
        r = api._c.get("/api/search", params={"q": q, "limit": limit})
        assert r.status_code != 500, (
            f"search with limit={limit} must not 500, got "
            f"{r.status_code}: {r.text[:200]}"
        )
        assert r.status_code in (200, 400), (
            f"search with limit={limit} must be 200 (clamped) or 400, got {r.status_code}"
        )
        if r.status_code == 200:
            hits = r.json().get("hits", [])
            assert isinstance(hits, list) and len(hits) <= 100, (
                f"search with limit={limit} returned an unbounded result set: "
                f"{len(hits) if isinstance(hits, list) else hits!r}"
            )


def test_search_missing_query_param_defined(api):
    """``/api/search`` with no ``q`` parameter has defined behavior, not a 500.

    Distinct from ``test_search_empty_query`` (``q=""``): omitting the ``q``
    parameter entirely must produce a defined result — a 400 (param required) or
    a 200 with an empty/zero-hit body — never a 500.
    """
    _login(api)
    r = api._c.get("/api/search")
    assert r.status_code != 500, (
        f"search with no q param must not 500, got {r.status_code}: {r.text[:200]}"
    )
    assert r.status_code in (200, 400), (
        f"search with no q param must be 400 (required) or 200 (empty), got {r.status_code}"
    )
    if r.status_code == 200:
        body = r.json()
        assert not body.get("hits") and body.get("count", 0) == 0, (
            f"search with no query must yield zero hits, got {body!r}"
        )


def test_head_and_options_handled(api):
    """HEAD and OPTIONS requests return sane responses, not 500.

    ``HEAD /health`` should return the health status code with no body, and an
    ``OPTIONS`` preflight to an API route should return a non-5xx response
    (204/200/405 as the framework dictates). Assert neither yields a 500.
    """
    get_health = api._c.get("/health")
    head_health = api._c.request("HEAD", "/health")
    assert head_health.status_code != 500, (
        f"HEAD /health must not 500, got {head_health.status_code}"
    )
    assert head_health.status_code == get_health.status_code, (
        f"HEAD /health ({head_health.status_code}) must mirror GET /health "
        f"({get_health.status_code})"
    )
    assert head_health.content == b"", (
        f"HEAD /health must have an empty body, got {head_health.content[:200]!r}"
    )

    _login(api)
    opt = api._c.request("OPTIONS", "/api/search")
    assert opt.status_code < 500, (
        f"OPTIONS /api/search must be a non-5xx response (204/200/405), got {opt.status_code}"
    )


def test_ingest_wrong_multipart_field_rejected(api):
    """POST /api/ingest with the wrong multipart field name is a clean 4xx.

    The ingest endpoint expects a ``files`` part. Authenticated, submit a
    multipart body whose file part is named something else (e.g. ``upload``). The
    app must reject it with a 4xx (no file to ingest) — never a 500/panic.
    """
    _login(api)
    marker = flows.unique_marker()
    # File part named "upload" instead of the expected "files".
    files = [("upload", (f"{marker}.txt", marker.encode("utf-8"), "text/plain"))]
    r = api._c.post("/api/ingest", files=files)
    assert r.status_code != 500, (
        f"ingest with a wrong multipart field must not 500, got "
        f"{r.status_code}: {r.text[:200]}"
    )
    assert 400 <= r.status_code < 500, (
        f"ingest with a wrong multipart field name must be a clean 4xx (no file to "
        f"ingest), got {r.status_code}: {r.text[:200]}"
    )
