"""Reusable plumbing for the *limits* e2e lane (ledger P14-T14).

Provisions a dedicated, low-cap **limits-e2e** tenant whose effective storage and
token caps are shrunk via the super-admin quota-override endpoint
(``POST /admin/tenants/:id/quota-override``). Because an admin override takes
precedence over the (absent) plan value, the tiny caps apply even though the
signup flow creates a grandfathered, no-plan tenant — so the upcoming E2/E3 tests
can drive the app into ``413`` (storage) / ``429`` (tokens) enforcement.

The super-admin is the platform operator's **bootstrap tenant** (the lowest-id
tenant — the only principal ``is_platform_admin_tenant`` accepts); locally that is
the ``KB_ADMIN_*`` / ``KB_TENANT_SLUG`` account the e2e config already exposes.

State is reset between cases the same way the rest of the harness reads
server-side state: straight against Postgres over ``podman exec … psql`` (the JSON
API exposes no "wipe my usage" route, by design).
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field

from . import config, flows
from .api_client import ApiClient

# The app's Postgres container (compose project ``local-kb``), matching the DB
# access used by the other black-box tests (e.g. test_account_security.py).
_PG_CONTAINER = "local-kb_postgres_1"

# Tiny caps the limits lane runs under: ~1 MB of storage, ~1000 tokens. Small
# enough that a single modest ingest / a handful of model calls trips the cap.
DEFAULT_QUOTA_BYTES = 1_000_000
DEFAULT_QUOTA_TOKENS = 1_000


def _db_exec(sql: str) -> str:
    """Run *sql* against the app's Postgres via ``podman exec``; return stdout.

    Returns the first column of the first row, stripped (empty string when there
    are no rows). Mirrors the ``_db_query`` helper other catalog tests use.
    """
    result = subprocess.run(
        ["podman", "exec", "-i", _PG_CONTAINER,
         "psql", "-U", "kb", "-d", "kb", "-t", "-A"],
        input=sql,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"DB exec failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _slugify(name: str) -> str:
    """Replicate the server-side slugify (lowercase, non-alnum runs → single hyphen)."""
    out: list[str] = []
    for c in name.strip().lower():
        if c.isalnum():
            out.append(c)
        elif out and out[-1] != "-":
            out.append("-")
    return "".join(out).rstrip("-") or "workspace"


def _csrf_form_token(client: ApiClient, path: str) -> str:
    """GET *path* (storing the ``__Host-csrf`` cookie) and return the hidden token.

    This is the double-submit-cookie dance the browser does automatically; doing
    it by hand lets the JSON :class:`ApiClient` drive the CSRF-protected web forms.
    """
    r = client._c.get(path)
    if r.status_code != 200:
        raise AssertionError(f"GET {path} expected 200, got {r.status_code}: {r.text[:200]}")
    m = re.search(r'name="csrf_token" value="([^"]+)"', r.text)
    if not m:
        raise AssertionError(f"no csrf_token hidden field found in {path}")
    return m.group(1)


def _signup(client: ApiClient, workspace: str, email: str, password: str) -> None:
    """Create a fresh tenant + Owner via the web ``/signup`` form (CSRF handled)."""
    token = _csrf_form_token(client, "/signup")
    r = client._c.post(
        "/signup",
        data={
            "csrf_token": token,
            "tenant_name": workspace,
            "email": email,
            "password": password,
        },
    )
    if r.status_code != 200:
        raise AssertionError(f"signup failed: {r.status_code} {r.text[:200]}")


def _tenant_id_for_slug(slug: str) -> int:
    """Resolve a tenant's numeric id by slug (DB read)."""
    raw = _db_exec(f"SELECT id FROM tenants WHERE slug = '{slug}'")
    if not raw:
        raise AssertionError(f"no tenant row found for slug {slug!r}")
    return int(raw)


def _verify_email_in_db(slug: str, email: str) -> None:
    """Mark a freshly-signed-up user's email verified directly in the DB.

    Signup leaves the user unverified (a verification email would be sent). The
    limits account must be able to log in and exercise the app, so flip the flag
    server-side — the same shortcut other black-box tests take for server-only
    state. Scoped by both tenant slug and email so it can never touch another row.
    """
    _db_exec(
        "UPDATE users SET email_verified = TRUE "
        f"WHERE email = '{email}' "
        f"AND tenant_id = (SELECT id FROM tenants WHERE slug = '{slug}')"
    )


@dataclass
class LimitsAccount:
    """A provisioned low-cap tenant for the limits lane, with reset support.

    Holds an authenticated :class:`ApiClient` (``client``) logged in as the
    account's Owner, the resolved ``tenant_id``/``slug``/``email``/``password``,
    and the caps it was clamped to. :meth:`reset_usage` returns the tenant to a
    clean slate between cases.
    """

    client: ApiClient
    tenant_id: int
    slug: str
    email: str
    password: str
    quota_bytes: int = DEFAULT_QUOTA_BYTES
    quota_tokens: int = DEFAULT_QUOTA_TOKENS
    _markers: list[str] = field(default_factory=list)

    def reset_usage(self) -> None:
        """Clear this tenant's accrued usage so a case starts from zero.

        Token usage is summed from ``usage_events`` (deleted directly). Storage is
        summed from ``files``; deleting this tenant's ``documents`` rows cascades
        (``ON DELETE CASCADE``) to ``files``, ``chunks``, and ``document_tags``, so
        one delete clears all storage sources. Every delete is scoped by
        ``tenant_id`` (this tenant only). The quota override is re-asserted in case
        a prior case cleared or changed it. Idempotent.
        """
        tid = self.tenant_id
        # Token usage source.
        _db_exec(f"DELETE FROM usage_events WHERE tenant_id = {tid}")
        # Storage source: deleting documents cascades to files/chunks/document_tags.
        _db_exec(f"DELETE FROM documents WHERE tenant_id = {tid}")
        # Re-assert the tiny caps (a previous case may have cleared the override).
        self.apply_caps()

    def apply_caps(
        self,
        admin: ApiClient | None = None,
        *,
        quota_bytes: int | None = None,
        quota_tokens: int | None = None,
    ) -> None:
        """(Re-)apply the quota override via the super-admin endpoint.

        Uses *admin* when supplied, else a fresh short-lived super-admin client.
        ``None`` cap arguments fall back to this account's configured caps.
        """
        b = self.quota_bytes if quota_bytes is None else quota_bytes
        t = self.quota_tokens if quota_tokens is None else quota_tokens
        self.quota_bytes, self.quota_tokens = b, t
        own_admin = admin is None
        admin = admin or _login_super_admin()
        try:
            admin.set_quota_override(
                self.tenant_id, quota_override_bytes=b, quota_override_tokens=t
            )
        finally:
            if own_admin:
                admin.close()

    def remember(self, marker: str) -> None:
        """Record a search marker ingested under this account (test bookkeeping)."""
        self._markers.append(marker)


def _login_super_admin() -> ApiClient:
    """Return an :class:`ApiClient` logged in as the platform operator (super-admin).

    The platform-admin is the bootstrap tenant; locally that is the ``KB_ADMIN_*``
    account the e2e config exposes. The caller owns the returned client and must
    close it.
    """
    admin = ApiClient()
    admin.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    return admin


def provision_limits_account(
    *,
    quota_bytes: int = DEFAULT_QUOTA_BYTES,
    quota_tokens: int = DEFAULT_QUOTA_TOKENS,
) -> LimitsAccount:
    """Provision a fresh, low-cap limits-e2e tenant and return a ready account.

    Steps: sign up a unique ``limits-e2e`` tenant + Owner via ``/signup``, verify
    its email (DB shortcut) so it can log in, resolve its tenant id, clamp its
    caps via the super-admin quota-override endpoint, and log the account's client
    in. The returned :class:`LimitsAccount` is reusable across cases via
    :meth:`LimitsAccount.reset_usage`.
    """
    workspace = f"Limits E2E {flows.unique_marker('lim')}"
    email = f"{flows.unique_marker('limits')}@e2e.test"
    password = "Limits-Passw0rd!23"
    slug = _slugify(workspace)

    client = ApiClient()
    _signup(client, workspace, email, password)
    _verify_email_in_db(slug, email)
    tenant_id = _tenant_id_for_slug(slug)

    account = LimitsAccount(
        client=client,
        tenant_id=tenant_id,
        slug=slug,
        email=email,
        password=password,
        quota_bytes=quota_bytes,
        quota_tokens=quota_tokens,
    )
    # Clamp caps as super-admin, then authenticate the account's own client.
    account.apply_caps()
    client.login(slug, email, password)
    return account
