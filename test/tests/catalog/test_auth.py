"""Catalog (step 2) — authentication, sessions, registration.

Black-box tests that drive the real app the way a user / API consumer does.
They assert the *intended* auth contract (register → session, login + logout,
session rejection, cookie hardening). Per the suite philosophy a failing
assertion means an app bug and is left failing — never weakened.

The harness ``ApiClient`` intentionally hides raw status codes and response
headers (it raises on non-2xx), so the negative paths (401/409) and the
Set-Cookie attribute checks reach the underlying ``httpx`` client via
``api._c``. That is the only way to observe these contracts black-box; no
assertion intent is relaxed by doing so.
"""

from __future__ import annotations

import time

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.auth

# A protected JSON-API route used purely as an auth probe: it sits behind the
# auth middleware, so an unauthenticated/invalid caller gets 401 while an
# authenticated caller reaches the handler and gets 404 (this job id will never
# exist). We assert on the *auth decision*, not on the job lookup itself.
_PROBE_PATH = "/api/jobs/999999999"

# A password comfortably above any plausible minimum length.
_PASSWORD = "E2e-Auth-Passw0rd!"


# ── Local helpers ────────────────────────────────────────────────────────────


def _unique_slug() -> str:
    """A fresh, valid tenant slug for registration tests."""
    return flows.unique_marker("t")


def _unique_email() -> str:
    """A fresh, unique login email."""
    return f"{flows.unique_marker('u')}@e2e.test"


def _session_set_cookies(resp) -> list[str]:
    """All Set-Cookie header values that set the ``__Host-session`` cookie."""
    return [c for c in resp.headers.get_list("set-cookie") if c.startswith("__Host-session=")]


def _has_session_cookie(resp) -> bool:
    """True when the response sets a *non-empty* ``__Host-session`` cookie."""
    for c in _session_set_cookies(resp):
        value = c[len("__Host-session=") :].split(";", 1)[0].strip()
        if value:
            return True
    return False


def _probe_authed(api: ApiClient) -> int:
    """Status of the auth-probe route using the client's own cookie jar."""
    return api._c.get(_PROBE_PATH).status_code


def _probe_with_token(token: str) -> int:
    """Status of the auth-probe route using an explicit ``__Host-session`` token.

    Uses a throwaway client (empty jar) so *only* the supplied token is sent —
    this isolates server-side session validation from any ambient cookie.
    """
    client = ApiClient()
    try:
        return client._c.get(
            _PROBE_PATH, headers={"Cookie": f"__Host-session={token}"}
        ).status_code
    finally:
        client.close()


# ── Tests ────────────────────────────────────────────────────────────────────


def test_register_new_tenant_and_user(api):
    """POST /auth/register creates a tenant+user and returns a session cookie."""
    slug, email = _unique_slug(), _unique_email()
    resp = api._c.post(
        "/auth/register",
        json={"tenant_slug": slug, "email": email, "password": _PASSWORD},
    )
    assert resp.status_code == 201, (
        f"registering a new tenant+user should return 201 Created; "
        f"got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body.get("user_id"), f"register response missing user_id: {body}"
    assert body.get("tenant_id"), f"register response missing tenant_id: {body}"
    assert _has_session_cookie(resp), (
        "register must set a non-empty __Host-session cookie (start a session)"
    )
    # The session cookie just set must authenticate protected /api routes.
    assert _probe_authed(api) == 404, (
        "the session from register should authenticate /api routes "
        "(expected 404 job-not-found, not 401)"
    )


def test_register_duplicate_email_conflict(api):
    """Registering an existing email in a tenant is rejected (409)."""
    tenant = config.tenant_slug()  # the bootstrap tenant — guaranteed to exist
    email = _unique_email()

    first = api._c.post(
        "/auth/register",
        json={"tenant_slug": tenant, "email": email, "password": _PASSWORD},
    )
    assert first.status_code == 201, (
        f"first registration of a fresh email in an existing tenant should "
        f"succeed (201); got {first.status_code}: {first.text}"
    )

    # Re-registering the SAME email in the SAME tenant must be a conflict.
    second = api._c.post(
        "/auth/register",
        json={"tenant_slug": tenant, "email": email, "password": _PASSWORD},
    )
    assert second.status_code == 409, (
        f"duplicate email in a tenant must be rejected with 409 Conflict; "
        f"got {second.status_code}: {second.text}"
    )


def test_login_valid_sets_session_cookie(api):
    """POST /auth/login with good credentials returns 200 + session cookie."""
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    resp = api._c.post(
        "/auth/login",
        json={"tenant_slug": tenant, "email": email, "password": password},
    )
    assert resp.status_code == 200, (
        f"valid login should return 200; got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body.get("user_id"), f"login response missing user_id: {body}"
    assert body.get("tenant_id"), f"login response missing tenant_id: {body}"
    assert _has_session_cookie(resp), "login must set a non-empty __Host-session cookie"
    # The session authenticates protected /api routes.
    assert _probe_authed(api) == 404, (
        "the login session should authenticate /api routes (expected 404, not 401)"
    )


def test_login_wrong_password_401(api):
    """Bad password returns 401 with a generic message."""
    tenant, email = config.tenant_slug(), config.admin_email()
    resp = api._c.post(
        "/auth/login",
        json={"tenant_slug": tenant, "email": email, "password": "not-the-password"},
    )
    assert resp.status_code == 401, (
        f"a wrong password must return 401; got {resp.status_code}: {resp.text}"
    )
    assert not _has_session_cookie(resp), "a failed login must not set a session cookie"


def test_login_unknown_tenant_no_user_enumeration(api):
    """Unknown tenant/email returns the same 401 as a wrong password."""
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    # (a) correct tenant + email, wrong password.
    wrong_pw = api._c.post(
        "/auth/login",
        json={"tenant_slug": tenant, "email": email, "password": "wrong-password"},
    )
    # (b) a tenant slug that does not exist.
    unknown_tenant = api._c.post(
        "/auth/login",
        json={"tenant_slug": _unique_slug(), "email": email, "password": password},
    )
    # (c) an email that does not exist within a real tenant.
    unknown_email = api._c.post(
        "/auth/login",
        json={"tenant_slug": tenant, "email": _unique_email(), "password": password},
    )

    for label, r in (("wrong-password", wrong_pw), ("unknown-tenant", unknown_tenant), ("unknown-email", unknown_email)):
        assert r.status_code == 401, f"{label} login should be 401; got {r.status_code}: {r.text}"
        assert not _has_session_cookie(r), f"{label} login must not set a session cookie"

    # No enumeration: the three failures must be indistinguishable.
    assert unknown_tenant.status_code == wrong_pw.status_code, (
        "unknown tenant must return the same status as a wrong password (no enumeration)"
    )
    assert unknown_tenant.json() == wrong_pw.json(), (
        f"unknown tenant must be indistinguishable from a wrong password; "
        f"wrong_pw={wrong_pw.json()} unknown_tenant={unknown_tenant.json()}"
    )
    assert unknown_email.json() == wrong_pw.json(), (
        f"unknown email must be indistinguishable from a wrong password; "
        f"wrong_pw={wrong_pw.json()} unknown_email={unknown_email.json()}"
    )


def test_logout_revokes_session(api):
    """After logout, the old session cookie no longer authenticates."""
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    api.login(tenant, email, password)
    token = api._c.cookies.get("__Host-session")
    assert token, "login should have stored a __Host-session cookie"
    # Sanity: the live session authenticates before logout.
    assert _probe_authed(api) == 404, "session should authenticate before logout"

    api.logout()

    # The client's own jar no longer carries the cookie (it was cleared).
    assert _probe_authed(api) == 401, "after logout the client must no longer be authenticated"
    # And, crucially, replaying the OLD token must be rejected server-side.
    assert _probe_with_token(token) == 401, (
        "the revoked session token must be rejected (401) after logout"
    )


def test_protected_route_without_session_401(api):
    """An /api call with no session/bearer returns 401."""
    # The `api` fixture client has never logged in — its cookie jar is empty.
    assert api._c.get(_PROBE_PATH).status_code == 401, (
        "a protected /api route without a session must return 401"
    )
    assert api._c.get("/api/search", params={"q": "anything"}).status_code == 401, (
        "the search API must require authentication (401 without a session)"
    )


def test_expired_or_invalid_session_rejected(api):
    """A tampered/expired session token is rejected with 401."""
    # A token that was never issued by the server (the deterministic, black-box
    # equivalent of a tampered/forged or expired-and-purged session).
    forged = "z" + flows.unique_marker("") + flows.unique_marker("")
    assert _probe_with_token(forged) == 401, (
        "a tampered/invalid session token must be rejected with 401"
    )


def test_remember_me_extends_ttl(page):
    """Logging in with 'remember me' yields a long-lived session (30d)."""
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    page.goto("/login")
    page.fill("#tenant_slug", tenant)
    page.fill("#email", email)
    page.fill("#password", password)
    page.check("input[name='remember_me']")
    page.click("form[action='/login'] button[type='submit']")
    # A successful login redirects to the app's search page.
    page.wait_for_url("**/search", timeout=30_000)

    cookies = page.context.cookies()
    session = next((c for c in cookies if c["name"] == "__Host-session"), None)
    assert session is not None, "remember-me login must set a __Host-session cookie"

    expires = session.get("expires", -1)
    assert expires and expires > 0, (
        "a 'remember me' session cookie must be persistent (carry an expiry), "
        f"not a session cookie; got expires={expires}"
    )
    remaining_days = (expires - time.time()) / 86_400
    # Remember-me TTL is 30 days; assert it is clearly far beyond the 1-day
    # default so a regression to the short TTL is caught.
    assert remaining_days > 25, (
        f"remember-me session should be long-lived (~30 days); "
        f"cookie expires in {remaining_days:.1f} days"
    )


def test_session_cookie_attributes(api):
    """The Set-Cookie carries __Host- prefix + Secure + HttpOnly + SameSite=Lax."""
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    resp = api._c.post(
        "/auth/login",
        json={"tenant_slug": tenant, "email": email, "password": password},
    )
    assert resp.status_code == 200, (
        f"valid login should return 200; got {resp.status_code}: {resp.text}"
    )
    cookies = _session_set_cookies(resp)
    assert cookies, f"login must set a __Host-session cookie; got {resp.headers.get_list('set-cookie')}"
    cookie = cookies[0]
    lower = cookie.lower()

    # __Host- prefix mandates Secure + Path=/ + no Domain.
    assert "secure" in lower, f"session cookie must be Secure; got {cookie!r}"
    assert "path=/" in lower, f"session cookie must set Path=/; got {cookie!r}"
    assert "domain=" not in lower, f"__Host- cookies must not set a Domain; got {cookie!r}"
    # Defence-in-depth attributes.
    assert "httponly" in lower, f"session cookie must be HttpOnly; got {cookie!r}"
    assert "samesite=lax" in lower, f"session cookie must be SameSite=Lax; got {cookie!r}"


def test_browser_login_flow(page):
    """A user can log in through the rendered /login form and reach the app."""
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    flows.browser_login(page, tenant, email, password)
    # A successful form login lands the user on the authenticated app shell.
    assert "/search" in page.url, (
        f"a successful login should reach the app (/search); landed on {page.url}"
    )
    page.wait_for_selector("input[name='q']", timeout=15_000)
    assert page.locator("input[name='q']").count() >= 1, (
        "the authenticated search UI should be visible after login"
    )
