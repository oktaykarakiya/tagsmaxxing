"""Catalog (step 2) — session lifecycle & login-abuse hardening.

Covers the session/auth contracts that ``test_auth`` does **not**: multiple
simultaneous sessions, per-session revocation, anti-fixation (a fresh token each
login), login brute-force pushback, and the registration password-strength
floor. ``test_auth`` already covers single login/logout, cookie attributes,
remember-me TTL, 401s and user-enumeration — this file must not repeat those.

Per the suite philosophy a failing assertion means a real app bug and is left
failing. Negative paths read raw status/headers via ``api._c`` (``ApiClient``
hides them); cross-session tests use TWO independent ``ApiClient`` instances.
"""

from __future__ import annotations

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.auth

_PROBE = "/api/jobs/999999999"  # behind auth: 404 when authed, 401 when not
_PASSWORD = "E2e-Session-Passw0rd!"


def _fresh_user(api: ApiClient) -> tuple[str, str]:
    """Register a brand-new user in the bootstrap tenant; return (email, password)."""
    email = f"{flows.unique_marker('sess')}@e2e.test"
    api.register(config.tenant_slug(), email, _PASSWORD)
    return email, _PASSWORD


def _new_client() -> ApiClient:
    """A second, independent API client (own cookie jar) against the same app."""
    return ApiClient(config.base_url())


def _session_cookie(client: ApiClient) -> str | None:
    """Return the value of the client's stored ``__Host-session`` cookie, if any."""
    for cookie in client._c.cookies.jar:
        if cookie.name == "__Host-session":
            return cookie.value
    return None


def _response_set_session(resp) -> bool:
    """True iff an HTTP response issues a *new* (non-empty) ``__Host-session`` cookie.

    A logout/clearing cookie (``__Host-session=;``) is not a session grant.
    """
    for value in resp.headers.get_list("set-cookie"):
        if value.startswith("__Host-session=") and not value.startswith("__Host-session=;"):
            return True
    return False


def _probe_status_with_token(token: str) -> int:
    """Hit the auth-gated probe carrying ``token`` explicitly; return the status code.

    Uses a throwaway client with an empty jar so the only credential sent is the
    supplied token — this proves *server-side* validity, not just a cleared jar.
    """
    client = _new_client()
    try:
        resp = client._c.get(_PROBE, headers={"Cookie": f"__Host-session={token}"})
        return resp.status_code
    finally:
        client.close()


def test_concurrent_sessions_are_independent(api):
    """A user can hold two valid sessions at once (e.g. two devices).

    Register a user, then log in as that user on TWO separate ``ApiClient``
    instances. Both clients must authenticate a protected route simultaneously
    (each gets 404 on the never-existing job probe, not 401) — logging in on one
    device must not invalidate the other.
    """
    email, password = _fresh_user(api)
    tenant = config.tenant_slug()
    a, b = _new_client(), _new_client()
    try:
        a.login(tenant, email, password)
        b.login(tenant, email, password)
        # An authed request to the probe gets 404 (job missing); 401 means the
        # session was not accepted. Both must be live at the same time.
        assert a._c.get(_PROBE).status_code == 404, "session A failed to authenticate"
        assert b._c.get(_PROBE).status_code == 404, "session B failed to authenticate"
    finally:
        a.close()
        b.close()


def test_login_issues_distinct_session_tokens(api):
    """Logging in twice mints two different session tokens (no fixation/reuse).

    Log in as the same user twice (fresh clients) and capture each
    ``__Host-session`` cookie value. The two tokens must differ — the server must
    mint a fresh session per login rather than reusing/accepting a fixed one.
    """
    email, password = _fresh_user(api)
    tenant = config.tenant_slug()
    a, b = _new_client(), _new_client()
    try:
        a.login(tenant, email, password)
        b.login(tenant, email, password)
        token_a, token_b = _session_cookie(a), _session_cookie(b)
        assert token_a, "first login issued no __Host-session cookie"
        assert token_b, "second login issued no __Host-session cookie"
        assert token_a != token_b, (
            "two logins reused the same session token — session-fixation risk"
        )
    finally:
        a.close()
        b.close()


def test_logout_revokes_only_the_current_session(api):
    """Logging out one session leaves the user's other sessions valid.

    Register a user and log in on two clients A and B. When A logs out, A's token
    must be rejected (401) but B's session must keep working (404 on the probe) —
    revocation is per-session, not a global sign-out. (``test_auth`` only checks
    that the single logged-out session dies.)
    """
    email, password = _fresh_user(api)
    tenant = config.tenant_slug()
    a, b = _new_client(), _new_client()
    try:
        a.login(tenant, email, password)
        b.login(tenant, email, password)
        token_a = _session_cookie(a)
        assert token_a, "client A login issued no session token"

        a.logout()

        # A's token is dead server-side (re-presenting it explicitly → 401).
        assert _probe_status_with_token(token_a) == 401, (
            "logging out did not revoke the caller's own session token"
        )
        # B's independent session is untouched by A's logout.
        assert b._c.get(_PROBE).status_code == 404, (
            "logging out one session also revoked the user's other session"
        )
    finally:
        a.close()
        b.close()


def test_repeated_failed_logins_are_throttled(api):
    """A burst of wrong-password logins triggers abuse pushback, never a bypass.

    Fire many (~20) rapid ``/auth/login`` attempts with the wrong password for a
    real account. The contract: brute force is bounded — at least one attempt in
    the burst is rejected with 429 (rate limited), and **no** attempt ever returns
    200 or sets a session cookie. (Distinct from ``test_quotas`` per-endpoint
    limits: this is the auth brute-force protection specifically.)
    """
    email, password = _fresh_user(api)
    tenant = config.tenant_slug()
    attacker = _new_client()
    statuses: list[int] = []
    granted_session = False
    try:
        for _ in range(20):
            resp = attacker._c.post(
                "/auth/login",
                json={"tenant_slug": tenant, "email": email, "password": password + "-wrong"},
            )
            statuses.append(resp.status_code)
            if _response_set_session(resp):
                granted_session = True
    finally:
        attacker.close()

    assert 200 not in statuses, f"a wrong-password login unexpectedly succeeded: {statuses}"
    assert not granted_session, "a wrong-password login issued a session cookie (auth bypass)"
    assert _session_cookie(attacker) is None, "the brute-force burst left a live session"
    assert 429 in statuses, (
        f"brute-force burst was never rate-limited — no 429 in {statuses}"
    )


def test_weak_password_rejected_on_register(api):
    """Registering with a too-weak password is rejected (4xx), no account created.

    ``POST /auth/register`` with a password below the documented minimum (the
    signup flow requires ≥ 8 characters) — e.g. ``"123"`` — must be rejected with
    a 4xx and must not create a usable account or set a session cookie.
    """
    tenant = config.tenant_slug()
    email = f"{flows.unique_marker('weakpw')}@e2e.test"
    weak = "123"

    resp = api._c.post(
        "/auth/register",
        json={"tenant_slug": tenant, "email": email, "password": weak},
    )
    assert 400 <= resp.status_code < 500, (
        f"register accepted a password below the 8-char floor — expected 4xx, "
        f"got {resp.status_code}: {resp.text}"
    )
    assert not _response_set_session(resp), "a rejected registration must not set a session cookie"

    # The account must not have been created: it cannot be logged into.
    probe = _new_client()
    try:
        login = probe._c.post(
            "/auth/login",
            json={"tenant_slug": tenant, "email": email, "password": weak},
        )
        assert login.status_code != 200, (
            "a weak-password account is usable — registration created it despite the floor"
        )
    finally:
        probe.close()


def test_login_after_logout_issues_fresh_session(api):
    """After logout, the user can log in again and the new session works.

    Register, log in, log out (old token now dead), then log in again. The second
    login must succeed, mint a *new* ``__Host-session`` token (different from the
    revoked one), and that new session must authenticate a protected route.
    """
    email, password = _fresh_user(api)
    tenant = config.tenant_slug()
    client = _new_client()
    try:
        client.login(tenant, email, password)
        first = _session_cookie(client)
        assert first, "first login issued no session token"

        client.logout()
        assert _probe_status_with_token(first) == 401, (
            "the pre-logout token is still accepted after logout"
        )

        # Second login succeeds and mints a fresh, different token …
        client.login(tenant, email, password)
        second = _session_cookie(client)
        assert second, "second login issued no session token"
        assert second != first, "re-login after logout reused the revoked session token"

        # … and that fresh session authenticates a protected route.
        assert client._c.get(_PROBE).status_code == 404, (
            "the post-logout session does not authenticate"
        )
    finally:
        client.close()
