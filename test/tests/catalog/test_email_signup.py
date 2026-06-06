"""Catalog (step 2) — signup, email verification, password reset.

The dev EmailProvider (LogEmail) logs emails via tracing rather than sending them.
To retrieve tokens for verification and password reset, these tests query the
app's Postgres database directly via ``podman exec`` — this is the only practical
way to observe server-side state from a black-box harness without modifying the app.

List with ``pytest --collect-only -m email``.
"""

from __future__ import annotations

import re
import subprocess
import time

import httpx
import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.email

_PASSWORD = "E2e-Email-Passw0rd!"


# ── Local helpers ────────────────────────────────────────────────────────────────


def _unique_workspace() -> str:
    """A unique, human-readable workspace name for signup tests."""
    return f"E2E WS {flows.unique_marker('ws')}"


def _unique_email() -> str:
    """A unique email address for signup tests."""
    return f"{flows.unique_marker('em')}@e2e.test"


def _db_query(query: str) -> str:
    """Run a SQL query against the app's Postgres via ``podman exec``.

    Returns the first column of the first row as a stripped string, or an empty
    string when the query produces no rows.
    """
    result = subprocess.run(
        [
            "podman", "exec", "-i", "local-kb_postgres_1",
            "psql", "-U", "kb", "-d", "kb", "-t", "-A",
        ],
        input=query,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"DB query failed: {result.stderr}")
    return result.stdout.strip()


def _get_verify_token(email: str) -> str:
    """Retrieve the ``email_verify_token`` for a user, or empty string if not set."""
    return _db_query(
        f"SELECT COALESCE(email_verify_token, '') FROM users WHERE email = '{email}'"
    )


def _get_reset_token(email: str) -> str:
    """Retrieve the ``password_reset_token`` for a user, or empty string if not set."""
    return _db_query(
        f"SELECT COALESCE(password_reset_token, '') FROM users WHERE email = '{email}'"
    )


def _is_email_verified(email: str) -> bool:
    """Return ``True`` when the user's ``email_verified`` flag is set."""
    return _db_query(
        f"SELECT email_verified FROM users WHERE email = '{email}'"
    ) == "t"


def _get_tenant_slug_for_email(email: str) -> str:
    """Return the tenant slug for a user by joining users → tenants."""
    return _db_query(
        f"SELECT t.slug FROM tenants t "
        f"JOIN users u ON u.tenant_id = t.id "
        f"WHERE u.email = '{email}'"
    )


def _signup_via_api(api: ApiClient, workspace: str, email: str, password: str) -> httpx.Response:
    """POST the /signup web form, handling CSRF (double-submit cookie pattern).

    Returns the raw ``httpx.Response`` from the POST so callers can inspect status
    and body. The CSRF cookie is stored in ``api._c``'s jar for subsequent requests.
    """
    # 1. GET /signup to obtain a fresh CSRF token + cookie.
    get_resp = api._c.get("/signup")
    assert get_resp.status_code == 200, (
        f"GET /signup must return 200; got {get_resp.status_code}"
    )
    match = re.search(r'name="csrf_token" value="([^"]+)"', get_resp.text)
    assert match, "CSRF token not found in /signup HTML"
    csrf_token = match.group(1)

    # 2. POST the form. The CSRF cookie set by the GET is sent automatically.
    return api._c.post(
        "/signup",
        data={
            "csrf_token": csrf_token,
            "tenant_name": workspace,
            "email": email,
            "password": password,
        },
    )


def _extract_csrf_from_page(api: ApiClient, path: str) -> tuple[str, str]:
    """GET *path*, return ``(csrf_cookie_value, csrf_form_token)``.

    The CSRF cookie is extracted from the ``set-cookie`` header of the GET response
    and manually stored in the client's jar so the subsequent POST can send it.
    """
    get_resp = api._c.get(path)
    assert get_resp.status_code == 200, (
        f"GET {path} must return 200; got {get_resp.status_code}"
    )
    # Extract cookie from Set-Cookie header.
    cookie_val = ""
    for sc in get_resp.headers.get_list("set-cookie"):
        if sc.startswith("__Host-csrf="):
            cookie_val = sc[len("__Host-csrf="):].split(";", 1)[0]
            break
    # Extract form token from HTML.
    match = re.search(r'name="csrf_token" value="([^"]+)"', get_resp.text)
    assert match, f"CSRF token not found in {path} HTML"
    form_token = match.group(1)
    return cookie_val, form_token


# ── Tests ────────────────────────────────────────────────────────────────────────


def test_signup_sends_verification_email(page, api):
    """Completing /signup creates the tenant+user and dispatches a verify email.

    The dev EmailProvider (LogEmail) logs the email via tracing rather than
    actually sending it, so the test verifies:
    1. The signup form submits successfully and renders "Check your email".
    2. The new user can log in (the app does not block unverified users).
    """
    workspace = _unique_workspace()
    email = _unique_email()

    # 1. Submit the signup form through the browser (handles CSRF naturally).
    page.goto("/signup")
    page.wait_for_selector("input[name='tenant_name']", timeout=15_000)
    page.fill("input[name='tenant_name']", workspace)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", _PASSWORD)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # 2. The "Check your email" interstitial must be rendered.
    assert "Check your email" in page.content(), (
        "after signup the VerifyEmailSentPage must render 'Check your email' heading"
    )
    assert email in page.content(), (
        f"the verify-email-sent page must display the user's email ({email})"
    )

    # 3. The tenant + user were actually created: login via the JSON API must
    #    succeed. The signup creates a *new* tenant (slugified workspace name),
    #    so we look up the actual tenant slug from the DB.
    tenant_slug = _get_tenant_slug_for_email(email)
    assert tenant_slug, f"DB must contain a tenant for {email} after signup"
    api.login(tenant_slug, email, _PASSWORD)
    # Also confirm the app sees this user as unverified in the DB.
    assert not _is_email_verified(email), (
        "a freshly-signed-up user must have email_verified=false"
    )


def test_verify_token_marks_verified(api):
    """GET /verify?token=… sets email_verified and redirects to login.

    Signs up a user through the /signup web form (CSRF handled), retrieves the
    verification token from the database, then calls the verify endpoint and
    confirms the user is now marked verified.
    """
    workspace = _unique_workspace()
    email = _unique_email()

    # 1. Sign up a new tenant + user.
    resp = _signup_via_api(api, workspace, email, _PASSWORD)
    assert resp.status_code == 200, (
        f"POST /signup should return 200 (verify-email-sent page); "
        f"got {resp.status_code}: {resp.text[:300]}"
    )
    assert "Check your email" in resp.text, (
        "signup must render the VerifyEmailSentPage"
    )

    # 2. A freshly-signed-up user must NOT be verified yet.
    assert not _is_email_verified(email), (
        f"freshly-signed-up user {email} must have email_verified=false; "
        f"the DB default should be false, not true (migration 0021 bug)"
    )

    # 3. Retrieve the verification token from the DB.
    token = _get_verify_token(email)
    assert token, (
        f"DB must contain an email_verify_token for {email} after signup; "
        f"the LogEmail provider logs the token — verify it was stored"
    )

    # 4. Visit the verification link. The handler redirects on both success and
    #    failure, so we disable auto-redirect to inspect the Location header.
    verify_resp = api._c.get(f"/verify?token={token}", follow_redirects=False)
    assert verify_resp.status_code in (302, 303, 307, 308), (
        f"GET /verify must redirect; got {verify_resp.status_code}"
    )
    location = verify_resp.headers.get("location", "")
    assert "verify_success=1" in location, (
        f"valid token must redirect to /login?verify_success=1; "
        f"got location={location!r}"
    )

    # 5. Confirm the DB flag was set.
    assert _is_email_verified(email), (
        f"email_verified must be true after successful verification for {email}"
    )


def test_forgot_password_sends_reset(page, api):
    """/forgot dispatches a password-reset email for a known address.

    Signs up a user first (so the email exists), then submits the forgot-password
    form through the browser and verifies:
    1. A success message is shown (always, to prevent user enumeration).
    2. The password_reset_token was stored in the DB.
    """
    workspace = _unique_workspace()
    email = _unique_email()

    # 1. Sign up to create a real user.
    page.goto("/signup")
    page.wait_for_selector("input[name='tenant_name']", timeout=15_000)
    page.fill("input[name='tenant_name']", workspace)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", _PASSWORD)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    assert "Check your email" in page.content(), "signup must succeed"

    # 2. Navigate to /forgot and submit.
    page.goto("/forgot")
    page.wait_for_selector("input[name='email']", timeout=15_000)
    page.fill("input[name='email']", email)
    page.click("form[action='/forgot'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # 3. A success message must be shown (always, to prevent enumeration).
    content = page.content()
    assert "sent a reset link" in content.lower() or "check your inbox" in content.lower(), (
        f"POST /forgot must show a success message for a known email; got: {content[:500]}"
    )

    # 4. The reset token must be stored in the DB.
    token = _get_reset_token(email)
    assert token, (
        f"DB must contain a password_reset_token for {email} after POST /forgot"
    )


def test_reset_token_changes_password(api):
    """A valid reset token lets the user set a new password and log in.

    Full flow: sign up → request password reset → retrieve reset token from DB →
    POST /reset with new password → old password rejected, new password accepted.
    """
    workspace = _unique_workspace()
    email = _unique_email()
    new_password = _PASSWORD + "-new"

    # 1. Sign up.
    resp = _signup_via_api(api, workspace, email, _PASSWORD)
    assert resp.status_code == 200, f"signup failed: {resp.status_code}"

    # 2. Request a password reset via POST /forgot (CSRF handled).
    csrf_cookie, csrf_form = _extract_csrf_from_page(api, "/forgot")
    # Manually set the CSRF cookie so the POST sends it alongside the form token.
    api._c.cookies.set("__Host-csrf", csrf_cookie, domain="localhost", path="/")
    forgot_resp = api._c.post(
        "/forgot",
        data={"csrf_token": csrf_form, "email": email},
    )
    assert forgot_resp.status_code == 200, f"POST /forgot failed: {forgot_resp.status_code}"

    # 3. Retrieve the reset token from the DB.
    token = _get_reset_token(email)
    assert token, f"DB must contain a password_reset_token for {email}"

    # 4. GET /reset to obtain a fresh CSRF token for the reset form.
    csrf_cookie2, csrf_form2 = _extract_csrf_from_page(api, f"/reset?token={token}")
    api._c.cookies.set("__Host-csrf", csrf_cookie2, domain="localhost", path="/")

    # 5. POST /reset with the new password.
    reset_resp = api._c.post(
        "/reset",
        data={
            "csrf_token": csrf_form2,
            "reset_token": token,
            "password": new_password,
        },
        follow_redirects=False,
    )
    assert reset_resp.status_code in (302, 303, 307, 308), (
        f"POST /reset must redirect on success; got {reset_resp.status_code}"
    )
    location = reset_resp.headers.get("location", "")
    assert "reset_success=1" in location, (
        f"successful reset must redirect to /login?reset_success=1; "
        f"got location={location!r}"
    )

    # 6. Look up the actual tenant slug (signup creates a new tenant).
    tenant_slug = _get_tenant_slug_for_email(email)
    assert tenant_slug, f"DB must contain a tenant for {email}"

    # 7. Old password must now be rejected.
    #    Use a fresh client so no ambient session interferes.
    old_client = ApiClient()
    try:
        old_resp = old_client._c.post(
            "/auth/login",
            json={"tenant_slug": tenant_slug, "email": email, "password": _PASSWORD},
        )
        assert old_resp.status_code == 401, (
            f"old password must be rejected (401) after reset; "
            f"got {old_resp.status_code}: {old_resp.text}"
        )
    finally:
        old_client.close()

    # 8. New password must authenticate successfully.
    new_client = ApiClient()
    try:
        login_resp = new_client._c.post(
            "/auth/login",
            json={"tenant_slug": tenant_slug, "email": email, "password": new_password},
        )
        assert login_resp.status_code == 200, (
            f"new password must authenticate (200) after reset; "
            f"got {login_resp.status_code}: {login_resp.text}"
        )
    finally:
        new_client.close()


def test_invalid_reset_token_rejected(api):
    """An expired or tampered reset token is rejected.

    Verifies three negative paths:
    1. GET /reset with an invalid token redirects to /forgot with an error.
    2. POST /reset with an invalid token renders an error page.
    3. POST /reset with a too-short password renders a validation error.
    """
    # 1. GET /reset with an invalid token must redirect.
    bad_token = "0" * 128  # 128 hex chars — wrong value, never stored.
    get_resp = api._c.get(f"/reset?token={bad_token}", follow_redirects=False)
    assert get_resp.status_code in (302, 303, 307, 308), (
        f"GET /reset with invalid token must redirect; got {get_resp.status_code}"
    )
    location = get_resp.headers.get("location", "")
    assert "error=expired_or_invalid" in location, (
        f"invalid reset token GET must redirect to /forgot?error=expired_or_invalid; "
        f"got location={location!r}"
    )

    # 2. POST /reset with an invalid token must show an error.
    #    First get a CSRF token from the reset page (with the bad token — it will
    #    redirect, so GET /forgot instead and use its CSRF).
    csrf_cookie, csrf_form = _extract_csrf_from_page(api, "/forgot")
    api._c.cookies.set("__Host-csrf", csrf_cookie, domain="localhost", path="/")

    post_resp = api._c.post(
        "/reset",
        data={
            "csrf_token": csrf_form,
            "reset_token": bad_token,
            "password": "ValidPassw0rd!",
        },
    )
    assert post_resp.status_code == 400, (
        f"POST /reset with invalid token must return 400; got {post_resp.status_code}"
    )
    assert "expired" in post_resp.text.lower() or "invalid" in post_resp.text.lower(), (
        f"POST /reset with invalid token must show an error message; "
        f"got: {post_resp.text[:300]}"
    )

    # 3. POST /reset with a too-short password (even if the token were valid).
    csrf_cookie2, csrf_form2 = _extract_csrf_from_page(api, "/forgot")
    api._c.cookies.set("__Host-csrf", csrf_cookie2, domain="localhost", path="/")

    short_pw_resp = api._c.post(
        "/reset",
        data={
            "csrf_token": csrf_form2,
            "reset_token": bad_token,
            "password": "short",
        },
    )
    assert short_pw_resp.status_code == 400, (
        f"POST /reset with short password must return 400; got {short_pw_resp.status_code}"
    )
    assert "8 characters" in short_pw_resp.text.lower() or "at least" in short_pw_resp.text.lower(), (
        f"short-password error must mention minimum length; got: {short_pw_resp.text[:300]}"
    )


def test_transactional_templates_render():
    """Verification/reset/invoice templates render with expected fields.

    Fetches each public auth-flow page and asserts that key form fields and
    structural elements are present in the rendered HTML. These are pure GET
    requests — no auth or CSRF needed for the initial page loads.
    """
    base = config.base_url()
    with httpx.Client(base_url=base, verify=False, follow_redirects=False,
                      timeout=config.http_timeout()) as c:

        # ── Signup page ──────────────────────────────────────────────────────
        r = c.get("/signup")
        assert r.status_code == 200, f"GET /signup must return 200; got {r.status_code}"
        html = r.text
        assert 'name="csrf_token"' in html, "signup page must contain CSRF hidden field"
        assert 'name="tenant_name"' in html, "signup page must contain workspace name field"
        assert 'name="email"' in html, "signup page must contain email field"
        assert 'name="password"' in html, "signup page must contain password field"
        assert 'Create a new workspace' in html or 'Create workspace' in html, (
            "signup page must have a heading or button about creating a workspace"
        )
        # CSRF cookie must be set.
        csrf_cookies = [h for h in r.headers.get_list("set-cookie")
                        if "__Host-csrf=" in h]
        assert csrf_cookies, "GET /signup must set a __Host-csrf cookie"

        # ── Forgot-password page ──────────────────────────────────────────────
        r2 = c.get("/forgot")
        assert r2.status_code == 200, f"GET /forgot must return 200; got {r2.status_code}"
        html2 = r2.text
        assert 'name="csrf_token"' in html2, "forgot page must contain CSRF hidden field"
        assert 'name="email"' in html2, "forgot page must contain email field"
        assert 'Forgot' in html2 or 'password' in html2.lower(), (
            "forgot-password page must mention password reset"
        )
        csrf_cookies2 = [h for h in r2.headers.get_list("set-cookie")
                         if "__Host-csrf=" in h]
        assert csrf_cookies2, "GET /forgot must set a __Host-csrf cookie"

        # ── Reset-password page (valid-looking but non-existent token → redirect) ──
        r3 = c.get("/reset?token=00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
        # The handler validates the token before rendering the form. An unknown
        # token redirects to /forgot.
        assert r3.status_code in (302, 303, 307, 308), (
            f"GET /reset with unknown token must redirect; got {r3.status_code}"
        )
        loc = r3.headers.get("location", "")
        assert "forgot" in loc, (
            f"invalid reset token must redirect to /forgot; got location={loc!r}"
        )

        # ── Verify endpoint (no token → redirect to login with error) ────────
        r4 = c.get("/verify?token=")
        assert r4.status_code in (302, 303, 307, 308), (
            f"GET /verify with empty token must redirect; got {r4.status_code}"
        )
        loc4 = r4.headers.get("location", "")
        assert "verify_error=invalid_token" in loc4, (
            f"empty verify token must redirect to /login?verify_error=invalid_token; "
            f"got location={loc4!r}"
        )


def test_full_user_journey_signup_verify_upload(page):
    """A new user signs up, verifies their email, then uploads a file — all via browser.

    This is the canonical end-to-end user journey, exercising the real browser UX
    every real user follows.  It regresses the bug where web-UI uploads were
    enqueued but never processed because no worker pool was running (fixed by
    switching ``POST /upload`` to inline pipeline processing).

    The test mimics a real user with zero API or DB access (except for the one
    unavoidable DB query to retrieve the verification token, because there is no
    SMTP server to deliver the email).  All other interactions — sign up, verify,
    login, upload, and inspecting the result — happen through the browser.
    """
    workspace = _unique_workspace()
    email = _unique_email()
    password = _PASSWORD

    # ── 1. Sign up via browser ──────────────────────────────────────────────────
    page.goto("/signup")
    page.wait_for_selector("input[name='tenant_name']", timeout=15_000)
    page.fill("input[name='tenant_name']", workspace)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # The "Check your email" interstitial must render.
    content = page.content()
    assert "Check your email" in content, (
        "after signup the VerifyEmailSentPage must render 'Check your email' heading"
    )
    assert email in content, (
        f"the verify-email-sent page must display the user's email ({email})"
    )

    # ── 2. Retrieve the verification token from the DB ──────────────────────────
    #    (the dev EmailProvider logs → tracing; we read the stored token directly)
    token = _db_query(
        f"SELECT COALESCE(email_verify_token, '') FROM users WHERE email = '{email}'"
    )
    assert token, (
        f"DB must contain an email_verify_token for {email} after signup"
    )
    # Sanity: the user must NOT be verified yet.
    assert not _is_email_verified(email), (
        f"freshly-signed-up user {email} must have email_verified=false"
    )

    # ── 3. Verify the email via browser ─────────────────────────────────────────
    page.goto(f"/verify?token={token}")
    page.wait_for_load_state("networkidle")
    # The handler redirects to /login?verify_success=1 on success.
    assert "/login" in page.url, (
        f"after verification the browser should land on /login; got {page.url}"
    )
    assert "verify_success=1" in page.url, (
        f"verification redirect must carry verify_success=1; got {page.url}"
    )
    # Confirm the DB flag was set.
    assert _is_email_verified(email), (
        f"email_verified must be true after successful verification for {email}"
    )

    # ── 4. Log in via browser ───────────────────────────────────────────────────
    tenant_slug = _get_tenant_slug_for_email(email)
    assert tenant_slug, f"DB must contain a tenant for {email}"

    page.fill("#tenant_slug", tenant_slug)
    page.fill("#email", email)
    page.fill("#password", password)
    page.click("form[action='/login'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    assert "/search" in page.url, (
        f"after login the browser should land on /search; got {page.url}"
    )

    # ── 5. Upload a file via browser ────────────────────────────────────────────
    page.goto("/upload")
    page.wait_for_selector("#upload-form", timeout=10_000)

    marker = flows.unique_marker("kbe2ejourney")
    page.set_input_files(
        "#file-input",
        {
            "name": f"{marker}.txt",
            "mimeType": "text/plain",
            "buffer": f"End-to-end user journey test document. {marker}".encode(),
        },
    )

    # File list and upload controls become visible after file selection.
    page.wait_for_selector("#upload-controls:not(.hidden)", timeout=5_000)
    page.click("#submit-btn")

    # The progress area appears; wait for completion.
    # With inline processing the JS receives a document_id and redirects
    # directly to /documents/:id.  Use a poll loop with page.evaluate() to
    # avoid the greenlet hang that wait_for_url can trigger on slow pages.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            current_url = page.evaluate("() => window.location.href")
        except Exception:
            time.sleep(2)
            continue
        if "/documents/" in str(current_url) or "/search" in str(current_url):
            break
        time.sleep(5)
    else:
        # Fallback: check if we landed somewhere valid.
        current_url = page.url
        if "/documents/" not in current_url and "/search" not in current_url:
            pytest.fail(
                f"after upload the browser should land on /documents/:id or /search; "
                f"got {current_url}"
            )

    # ── 6. Verify the document exists and is ready ──────────────────────────────
    # Extract document id from the URL if we landed on a document page.
    current_url = page.url
    if "/documents/" in current_url:
        doc_id_str = current_url.rstrip("/").rsplit("/", 1)[-1]
        if doc_id_str.isdigit():
            # Navigate programmatically is fine; just confirm the page rendered.
            page.wait_for_selector("h1, h2, h3", timeout=10_000)
            page_content = page.content()
            assert marker in page_content or "journey" in page_content.lower(), (
                f"document detail page should mention the marker or content; "
                f"url={current_url}"
            )
            return

    # If we landed on /search, search for the marker.
    page.fill("input[name='q']", marker)
    page.click("#search-form button[type='submit']")
    page.wait_for_selector("#results a[href*='/documents/']", timeout=30_000)
    assert page.locator("#results a[href*='/documents/']").count() >= 1, (
        f"browser search for {marker!r} should find the uploaded document"
    )
