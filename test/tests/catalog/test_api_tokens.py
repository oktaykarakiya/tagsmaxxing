"""Catalog (step-2) — API tokens + Bearer auth.

Drives the real app's API-token lifecycle as a programmatic consumer would:
create a token (the plaintext is shown once), list metadata, revoke, and use the
token for `Authorization: Bearer` auth on `/api/*` without a session cookie.

Behavioral contract (from crates/api/src/handlers/api_tokens.rs + middleware.rs):
  * POST   /api/tokens      → 201 Created, body {id, token (64 hex, shown once),
                              name, expires_at}. The raw token is never returned
                              again.
  * GET    /api/tokens      → 200, body {tokens: [{id, name, last_used_at,
                              expires_at, created_at}, …]} — metadata only.
  * DELETE /api/tokens/{id} → 200, body {id, message} ("Token revoked").
  * Auth middleware: a valid Bearer token authenticates /api with no cookie; an
    invalid/unknown/revoked Bearer token is a HARD 401 with NO fallback to the
    session cookie.

List with `pytest --collect-only -m tokens`.
"""

from __future__ import annotations

import string

import httpx
import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.tokens

# Marked text so the Bearer-auth search has a deterministic doc to find.
BEARER_SAMPLE = (
    "Engineering runbook for the deploy pipeline; covers rollout, rollback, and "
    "on-call escalation. Unique Bearer-auth marker: {marker}."
)


def _login_admin(api: ApiClient) -> None:
    """Authenticate the shared client as the bootstrap admin (cookie session)."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())


def _create_token(api: ApiClient, name: str, ttl_secs: int | None = None) -> httpx.Response:
    """POST /api/tokens via the session-authenticated client; return the raw response."""
    body: dict[str, object] = {"name": name}
    if ttl_secs is not None:
        body["ttl_secs"] = ttl_secs
    return api._c.post("/api/tokens", json=body)


def _list_tokens(api: ApiClient) -> httpx.Response:
    """GET /api/tokens via the session-authenticated client; return the raw response."""
    return api._c.get("/api/tokens")


def _revoke_token(api: ApiClient, token_id: int) -> httpx.Response:
    """DELETE /api/tokens/{id} via the session-authenticated client; return the raw response."""
    return api._c.delete(f"/api/tokens/{token_id}")


def _bearer_client(token: str | None = None) -> ApiClient:
    """A fresh ApiClient with an empty cookie jar, optionally carrying a Bearer header.

    Reuses the harness ApiClient (so base_url / TLS-verify / timeout match) but
    never logs in — proving that any successful call is authorized by the Bearer
    token alone, not a session cookie.
    """
    client = ApiClient(config.base_url())
    if token is not None:
        client._c.headers["Authorization"] = f"Bearer {token}"
    return client


def test_create_token_returns_plaintext_once(api):
    """POST /api/tokens returns the raw token exactly once."""
    _login_admin(api)
    name = flows.unique_marker("tok")

    resp = _create_token(api, name)
    assert resp.status_code == 201, f"expected 201 Created, got {resp.status_code}: {resp.text}"
    body = resp.json()

    assert body.get("name") == name, f"token name not echoed back: {body}"
    assert isinstance(body.get("id"), int) and body["id"] > 0, f"missing/invalid id: {body}"
    assert body.get("expires_at"), f"create response missing expires_at: {body}"

    raw = body.get("token")
    assert isinstance(raw, str) and raw, "create response did not include the plaintext token"
    assert len(raw) == 64 and all(c in string.hexdigits for c in raw), (
        f"plaintext token should be 64 hex chars, got {raw!r}"
    )

    # "Exactly once": the raw token must never be recoverable after creation.
    listed = _list_tokens(api)
    assert listed.status_code == 200, f"list failed: {listed.status_code} {listed.text}"
    mine = [t for t in listed.json().get("tokens", []) if t.get("id") == body["id"]]
    assert mine, f"newly created token {body['id']} not present in listing"
    assert "token" not in mine[0], "listing must not expose a 'token' field"
    assert raw not in listed.text, "the plaintext token must never be returned again after creation"


def test_list_tokens_metadata_only(api):
    """GET /api/tokens lists metadata, never the raw token."""
    _login_admin(api)
    name = flows.unique_marker("tok")

    created = _create_token(api, name)
    assert created.status_code == 201, f"create failed: {created.status_code} {created.text}"
    created_body = created.json()
    raw = created_body["token"]

    resp = _list_tokens(api)
    assert resp.status_code == 200, f"list failed: {resp.status_code} {resp.text}"
    mine = [t for t in resp.json().get("tokens", []) if t.get("id") == created_body["id"]]
    assert mine, f"created token {created_body['id']} absent from listing"
    item = mine[0]

    assert item.get("name") == name, f"listed name mismatch: {item}"
    # Metadata fields the contract promises are all present.
    for field in ("id", "name", "expires_at", "created_at", "last_used_at"):
        assert field in item, f"list item missing metadata field {field!r}: {item}"
    # The raw token must NEVER appear in the listing — not as a field, not anywhere.
    assert "token" not in item, "list item leaked a 'token' field"
    assert raw not in resp.text, "list response leaked the raw token value"


def test_revoke_token(api):
    """DELETE /api/tokens/{id} revokes the token."""
    _login_admin(api)
    created = _create_token(api, flows.unique_marker("tok"))
    assert created.status_code == 201, f"create failed: {created.status_code} {created.text}"
    token_id = created.json()["id"]

    resp = _revoke_token(api, token_id)
    assert resp.status_code == 200, f"revoke failed: {resp.status_code} {resp.text}"
    body = resp.json()
    assert body.get("id") == token_id, f"revoke echoed wrong id: {body}"
    assert "revoke" in str(body.get("message", "")).lower(), f"unexpected revoke message: {body}"

    # A revoked token is no longer an active token: it drops out of the listing.
    listed = _list_tokens(api)
    assert listed.status_code == 200, f"list failed: {listed.status_code} {listed.text}"
    assert all(t.get("id") != token_id for t in listed.json().get("tokens", [])), (
        f"revoked token {token_id} still appears in the active token list"
    )


def test_bearer_auth_with_token(api):
    """A valid Bearer token authenticates /api calls without a session."""
    _login_admin(api)

    # Ingest a uniquely-marked doc as the admin (via the cookie session).
    marker, _doc = flows.ingest_and_wait(api, BEARER_SAMPLE)

    # Mint an API token for this same user/tenant.
    created = _create_token(api, flows.unique_marker("tok"))
    assert created.status_code == 201, f"create failed: {created.status_code} {created.text}"
    raw = created.json()["token"]

    # A fresh client with NO session cookie, carrying only the Bearer header.
    bearer = _bearer_client(raw)
    try:
        resp = bearer._c.get("/api/search", params={"q": marker, "limit": 10})
        assert resp.status_code == 200, (
            f"Bearer token did not authenticate /api/search: {resp.status_code} {resp.text}"
        )
        hits = resp.json().get("hits", [])
        assert hits, (
            f"Bearer-authenticated search for {marker!r} returned no hits "
            "(token did not inherit the creator's tenant scope)"
        )
    finally:
        bearer.close()


def test_revoked_token_rejected(api):
    """A revoked token returns 401."""
    _login_admin(api)
    created = _create_token(api, flows.unique_marker("tok"))
    assert created.status_code == 201, f"create failed: {created.status_code} {created.text}"
    raw = created.json()["token"]
    token_id = created.json()["id"]

    bearer = _bearer_client(raw)
    try:
        # The token authenticates before revocation.
        before = bearer._c.get("/api/search", params={"q": "kbtokenprobe", "limit": 1})
        assert before.status_code == 200, (
            f"token should authenticate before revocation: {before.status_code} {before.text}"
        )

        # Revoke it via the session-authenticated client.
        revoked = _revoke_token(api, token_id)
        assert revoked.status_code == 200, f"revoke failed: {revoked.status_code} {revoked.text}"

        # The same Bearer token must now be rejected with a hard 401.
        after = bearer._c.get("/api/search", params={"q": "kbtokenprobe", "limit": 1})
        assert after.status_code == 401, (
            f"a revoked Bearer token must return 401, got {after.status_code}: {after.text}"
        )
    finally:
        bearer.close()


def test_invalid_bearer_401(api):
    """A malformed/unknown Bearer token returns 401 (no cookie fallback)."""
    # (a) An unknown/malformed Bearer token, with no session present, is a hard 401.
    anon = _bearer_client("0" * 64)
    try:
        resp = anon._c.get("/api/search", params={"q": "x", "limit": 1})
        assert resp.status_code == 401, (
            f"unknown Bearer token must return 401, got {resp.status_code}: {resp.text}"
        )
    finally:
        anon.close()

    # (b) No cookie fallback: a valid session cookie must NOT rescue a request that
    #     also carries an invalid Bearer header — the middleware treats a bad Bearer
    #     token as a hard 401 instead of falling through to the cookie.
    _login_admin(api)
    cookie_ok = api._c.get("/api/search", params={"q": "x", "limit": 1})
    assert cookie_ok.status_code == 200, (
        f"cookie session should authenticate on its own: {cookie_ok.status_code} {cookie_ok.text}"
    )

    with_bad_bearer = api._c.get(
        "/api/search",
        params={"q": "x", "limit": 1},
        headers={"Authorization": "Bearer " + "deadbeef" * 8},
    )
    assert with_bad_bearer.status_code == 401, (
        "an invalid Bearer header must not fall back to the session cookie; "
        f"expected 401, got {with_bad_bearer.status_code}: {with_bad_bearer.text}"
    )
