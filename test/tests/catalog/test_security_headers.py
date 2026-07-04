"""Catalog (step 2) — web security hardening: HTTP headers, CSRF, caching.

Black-box assertions on the **security posture** of the deployed endpoint as a
browser/HTTP client observes it through Caddy (``https://localhost:9443``). The
app's web middleware (``crates/api/src/web/security.rs``) sets the
Content-Security-Policy and Permissions-Policy on HTML responses; the TLS edge
(``test/Caddyfile.e2e``) adds HSTS / X-Frame-Options / X-Content-Type-Options /
Referrer-Policy and strips the ``Server`` header. These tests assert the
*intended* end-to-end contract regardless of which layer provides it.

These are pure header/contract checks not covered elsewhere (``test_auth`` only
checks the **session cookie** attributes). A failing assertion means a real
hardening gap and is left failing — never weakened. Header lookups use the raw
``httpx`` client (``api._c``) since ``ApiClient`` hides headers.
"""

from __future__ import annotations

import pytest

from lib import config, flows
from lib.api_client import ApiClient  # noqa: F401  (re-exported for harness symmetry)

pytestmark = pytest.mark.security


# ── Local helpers (allowed: defined inside this file only) ─────────────────────


def _get_html(api, path: str = "/login"):
    """Fetch ``path`` over the raw httpx client and assert it is an HTML page.

    Uses ``api._c`` because :class:`~lib.api_client.ApiClient` hides response
    headers. ``/login`` is public, so no authentication is needed for the pure
    header checks.
    """
    resp = api._c.get(path)
    assert resp.status_code == 200, f"GET {path} -> {resp.status_code}: {resp.text[:200]}"
    ctype = resp.headers.get("content-type", "")
    assert ctype.startswith("text/html"), f"GET {path} is not an HTML response: {ctype!r}"
    return resp


def _require_csp(resp) -> str:
    """Return the response's Content-Security-Policy, asserting it is present."""
    csp = resp.headers.get("content-security-policy")
    assert csp, "HTML response carries no Content-Security-Policy header"
    return csp


def _csp_directive(csp: str, name: str) -> list[str]:
    """Return the source tokens for a CSP directive (``[]`` if the directive is absent)."""
    for part in csp.split(";"):
        tokens = part.split()
        if tokens and tokens[0].lower() == name.lower():
            return tokens[1:]
    return []


def _hsts_max_age(value: str) -> int:
    """Parse the ``max-age`` (seconds) out of a Strict-Transport-Security value (-1 if absent)."""
    for part in value.split(";"):
        part = part.strip()
        if part.lower().startswith("max-age="):
            try:
                return int(part.split("=", 1)[1])
            except ValueError:
                return -1
    return -1


# ── Tests ───────────────────────────────────────────────────────────────────────


def test_csp_allows_htmx_and_tailwind_on_html(api):
    """An HTML page's CSP allows the scripts the UI actually needs.

    Regression guard for the CSP-vs-HTMX bug: fetch a rendered HTML page (e.g.
    ``/login``) and assert it carries a ``Content-Security-Policy`` whose
    ``script-src`` includes ``'self'`` and ``'unsafe-inline'``. All assets
    (HTMX, Tailwind CSS, Chart.js) are self-hosted — no CDN domains needed.
    """
    csp = _require_csp(_get_html(api, "/login"))
    script_src = _csp_directive(csp, "script-src")
    for needed in ("'self'", "'unsafe-inline'"):
        assert needed in script_src, (
            f"CSP script-src is missing {needed!r} (got {script_src}); "
            "the HTMX/Tailwind web UI would be blocked in a compliant browser"
        )


def test_csp_locks_down_object_frame_base_form(api):
    """The CSP restricts objects, framing, base-uri and form targets.

    Beyond allowing the needed scripts, the HTML CSP must also harden the page:
    ``object-src 'none'``, ``frame-ancestors 'none'`` (clickjacking),
    ``base-uri 'self'`` (base-tag injection) and ``form-action 'self'``
    (exfiltration via form posts). Assert each directive is present.
    """
    csp = _require_csp(_get_html(api, "/login"))
    assert "'none'" in _csp_directive(csp, "object-src"), f"CSP must set object-src 'none'; got: {csp!r}"
    assert "'none'" in _csp_directive(csp, "frame-ancestors"), (
        f"CSP must set frame-ancestors 'none' (clickjacking); got: {csp!r}"
    )
    assert "'self'" in _csp_directive(csp, "base-uri"), (
        f"CSP must set base-uri 'self' (base-tag injection); got: {csp!r}"
    )
    assert "'self'" in _csp_directive(csp, "form-action"), (
        f"CSP must set form-action 'self' (form exfiltration); got: {csp!r}"
    )


def test_permissions_policy_disables_sensitive_features(api):
    """HTML responses disable powerful browser features via Permissions-Policy.

    A rendered page must carry a ``Permissions-Policy`` that disables features the
    app does not use — at minimum ``camera``, ``microphone``, ``geolocation`` and
    ``payment`` set to the empty allowlist ``()``.
    """
    resp = _get_html(api, "/login")
    pp = resp.headers.get("permissions-policy")
    assert pp, "HTML response carries no Permissions-Policy header"
    norm = pp.replace(" ", "")
    for feature in ("camera", "microphone", "geolocation", "payment"):
        assert f"{feature}=()" in norm, (
            f"Permissions-Policy must disable {feature} with the empty allowlist (); got {pp!r}"
        )


def test_hsts_enforced_over_https(api):
    """HTTPS responses carry Strict-Transport-Security with a long max-age.

    Any response from the HTTPS endpoint must include a
    ``Strict-Transport-Security`` header with a substantial ``max-age`` (≥ ~6
    months / 15552000 s) and ``includeSubDomains`` so browsers pin HTTPS. Assert
    on a normal page fetch.
    """
    resp = api._c.get("/login")
    assert resp.status_code == 200, f"GET /login -> {resp.status_code}"
    hsts = resp.headers.get("strict-transport-security")
    assert hsts, "HTTPS response carries no Strict-Transport-Security header"
    max_age = _hsts_max_age(hsts)
    assert max_age >= 15552000, (
        f"HSTS max-age too short ({max_age}s); want >= 15552000 (~6 months); header={hsts!r}"
    )
    assert "includesubdomains" in hsts.lower(), (
        f"HSTS must include includeSubDomains so subdomains are pinned; got {hsts!r}"
    )


def test_clickjacking_nosniff_referrer_on_web(api):
    """Web pages carry X-Frame-Options, X-Content-Type-Options and Referrer-Policy.

    A rendered HTML response must include ``X-Frame-Options: DENY`` (framing),
    ``X-Content-Type-Options: nosniff`` (MIME sniffing) and a ``Referrer-Policy``
    of ``strict-origin-when-cross-origin`` (referrer leakage). Assert all three.
    """
    resp = _get_html(api, "/login")
    assert resp.headers.get("x-frame-options", "").upper() == "DENY", (
        f"web page must send X-Frame-Options: DENY; got {resp.headers.get('x-frame-options')!r}"
    )
    assert resp.headers.get("x-content-type-options", "").lower() == "nosniff", (
        f"web page must send X-Content-Type-Options: nosniff; got {resp.headers.get('x-content-type-options')!r}"
    )
    assert resp.headers.get("referrer-policy", "") == "strict-origin-when-cross-origin", (
        f"web page must send Referrer-Policy: strict-origin-when-cross-origin; "
        f"got {resp.headers.get('referrer-policy')!r}"
    )


def test_json_api_also_hardened(api):
    """The JSON API surface carries the baseline hardening headers too.

    Defense-in-depth must not stop at HTML: an API/JSON response (e.g.
    ``/health`` or an authenticated ``/api/search``) must still carry the
    edge-applied baseline — ``Strict-Transport-Security``, ``X-Frame-Options``
    and ``X-Content-Type-Options: nosniff`` — so a tricked browser cannot sniff
    or frame API responses either.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    resp = api._c.get("/api/search", params={"q": flows.unique_marker(), "limit": 1})
    assert resp.status_code == 200, f"/api/search -> {resp.status_code}: {resp.text[:200]}"
    assert resp.headers.get("content-type", "").startswith("application/json"), (
        f"expected a JSON API response; got content-type {resp.headers.get('content-type')!r}"
    )
    assert resp.headers.get("strict-transport-security"), "JSON API response is missing Strict-Transport-Security"
    assert resp.headers.get("x-frame-options", "").upper() == "DENY", (
        f"JSON API response must send X-Frame-Options: DENY; got {resp.headers.get('x-frame-options')!r}"
    )
    assert resp.headers.get("x-content-type-options", "").lower() == "nosniff", (
        f"JSON API response must send X-Content-Type-Options: nosniff; "
        f"got {resp.headers.get('x-content-type-options')!r}"
    )


def test_server_header_not_disclosed(api):
    """Responses do not leak the backend server or framework via headers.

    To avoid handing attackers a fingerprint, responses must not advertise the
    backend: no ``Server`` header revealing the proxy/app version and no
    ``X-Powered-By`` header. Assert both are absent (or, for ``Server``, carry no
    version token).
    """
    resp = api._c.get("/login")
    assert resp.status_code == 200, f"GET /login -> {resp.status_code}"
    server = resp.headers.get("server")
    assert not server or not any(ch.isdigit() for ch in server), (
        f"Server header discloses a version token (fingerprint): {server!r}"
    )
    assert "x-powered-by" not in resp.headers, (
        f"X-Powered-By header leaks the backend framework: {resp.headers.get('x-powered-by')!r}"
    )


def test_csrf_required_on_web_form_post(page):
    """A web form POST without the page's CSRF token is rejected.

    The web layer uses a double-submit ``__Host-csrf`` token on mutating forms.
    Log in via the browser, then POST to a state-changing form action (e.g.
    ``/account/team/invite``) **without** the token (forge a bare request that
    omits/!mangles the ``__Host-csrf`` value) and assert it is rejected
    (403/400). The legitimately rendered form must include the token (so the
    normal UI flow still works).
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    # The legitimately rendered form must carry the CSRF token (normal UI works).
    page.goto("/account/team")
    token = page.locator("input[name='csrf_token']").first.get_attribute("value")
    assert token, "the rendered team form carries no csrf_token field — the legitimate UI flow would break"

    # Forge a state-changing POST that submits a CSRF token NOT matching the
    # page's __Host-csrf cookie. page.request shares the browser's session +
    # cookie jar, so this is a logged-in request whose only defect is the bad
    # token — exactly what double-submit CSRF protection must reject.
    forged = "0" * 64  # 64 hex chars; deliberately not the page's real token
    resp = page.request.post(
        "/account/team/invite",
        form={"csrf_token": forged, "email": "csrf-probe@example.com", "role": "member"},
    )
    assert resp.status in (400, 403), (
        f"a form POST with a forged/mismatched CSRF token was accepted (status {resp.status}) — "
        "double-submit CSRF protection is not enforced on the mutating web form"
    )


def test_authenticated_html_not_shared_cacheable(api):
    """Authenticated pages forbid shared/proxy caching of their HTML.

    A page that renders tenant-private data (e.g. ``/account`` or ``/dashboard``)
    must instruct caches not to store it for other users: a ``Cache-Control``
    that is ``private`` and/or ``no-store`` (not ``public``). This prevents a
    shared proxy from serving one tenant's authenticated HTML to another.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    resp = api._c.get("/account")
    assert resp.status_code == 200, f"/account -> {resp.status_code}: {resp.text[:200]}"
    assert resp.headers.get("content-type", "").startswith("text/html"), (
        f"expected an authenticated HTML page; got content-type {resp.headers.get('content-type')!r}"
    )
    cache_control = resp.headers.get("cache-control", "")
    cc = cache_control.lower()
    assert cc, (
        "authenticated /account sets no Cache-Control — a shared proxy may store and "
        "serve one tenant's private HTML to another"
    )
    assert "public" not in cc, f"authenticated page is marked publicly cacheable: {cache_control!r}"
    assert ("private" in cc) or ("no-store" in cc), (
        f"authenticated page must be private/no-store (not shared-cacheable); Cache-Control={cache_control!r}"
    )
