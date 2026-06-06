"""Catalog (step 2) — RBAC enforcement & audit-log writes (admin panel + team).

The role *gates* exist (admin-can't-grant-owner, owner-only team mutations,
last-owner protection on the team page) but are almost entirely untested as
negative / forced-request cases — and a code audit suggests ``/admin/users/role``
is MISSING the last-owner + self-demotion guards its ``/account/team`` twin has.
These tests assert least-privilege server-side (not just hidden UI), and that
privileged actions are written to the audit log. Failures are real findings.

Setup: most need a member-role and an admin-role user — invite them into a fresh
tenant via the team page, then drive forced POSTs with that user's session +
their own CSRF token.
"""

from __future__ import annotations

import re
from collections import namedtuple

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.admin

# ── Helpers ──────────────────────────────────────────────────────────────────
#
# These tests forge POSTs to CSRF-protected Web UI routes (``/admin/*`` and
# ``/account/team/*``) as a chosen role. The Playwright ``page`` is used only to
# create a brand-new tenant + Owner via the public ``/signup`` form (there is no
# JSON signup endpoint — ``/auth/register`` only adds a Member to an *existing*
# tenant). Everything else is driven through a per-user ``ApiClient`` so each
# principal has its own cookie jar (session + ``__Host-csrf``).

CSRF_COOKIE = "__Host-csrf"

# A logged-in principal: its ApiClient, tenant slug, credentials, and user id.
Principal = namedtuple("Principal", ["client", "slug", "email", "password", "user_id"])


def _password(marker: str) -> str:
    """A signup/invite-valid password (>= 8 chars) derived from a unique marker."""
    return f"Pw-{marker}!"


def _signup_owner(page) -> Principal:
    """Create a fresh tenant + Owner via the web ``/signup`` form; return a
    logged-in Owner :class:`Principal`.

    The marker (``unique_marker()``) is already lowercase-alphanumeric, so the
    server-side slugify leaves it untouched and the tenant slug equals the marker.
    """
    marker = flows.unique_marker()
    email = f"{marker}@e2e.test"
    password = _password(marker)

    page.goto("/signup")
    page.fill("input[name='tenant_name']", marker)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("form button[type='submit']")
    page.wait_for_load_state("networkidle")

    client = ApiClient(config.base_url())
    resp = client.login(marker, email, password)  # slug == marker
    return Principal(client, marker, email, password, int(resp["user_id"]))


def _csrf_token(client: ApiClient) -> str:
    """Prime and return the double-submit CSRF token from the client's cookie jar.

    ``GET /account`` is authed-only (no role gate) and always sets ``__Host-csrf``,
    so any logged-in principal — Owner, Admin, or Member — can obtain a token.
    """
    client._c.get("/account")
    token = client._c.cookies.get(CSRF_COOKIE)
    assert token, f"expected a {CSRF_COOKIE} cookie after GET /account"
    return token


def _web_post(client: ApiClient, path: str, fields: dict):
    """Forge a form POST to a CSRF-protected Web UI route as ``client``.

    A fresh CSRF token is primed before every POST (the double-submit cookie is
    rotated on each page render), so call sites need not manage it.
    """
    data = {"csrf_token": _csrf_token(client)}
    data.update({k: str(v) for k, v in fields.items()})
    return client._c.post(path, data=data)


def _web_get(client: ApiClient, path: str):
    """GET a Web UI route as ``client`` (no redirects beyond ApiClient's default)."""
    return client._c.get(path)


def _invite(owner: Principal, *, email: str, role: str, password: str):
    """Owner-invites a user via ``/admin/users/invite`` (the only path that lets
    the inviter set a known password, so the invitee can later log in)."""
    return _web_post(
        owner.client,
        "/admin/users/invite",
        {"email": email, "role": role, "password": password},
    )


def _user_id_for_email(html: str, email: str) -> int | None:
    """Extract a user's id from the ``/admin/users`` table given their email.

    The row renders ``<td …>{id}</td>`` immediately followed by
    ``<td …>{email}</td>`` (see ``templates/admin_users.html``).
    """
    m = re.search(
        r"<td[^>]*>\s*(\d+)\s*</td>\s*<td[^>]*>\s*" + re.escape(email) + r"\s*</td>",
        html,
    )
    return int(m.group(1)) if m else None


def _owner_badge_count(admin_users_html: str) -> int:
    """Count Owner-role users shown on ``/admin/users``.

    Owner is the only role rendered with the purple badge (``templates/
    admin_users.html``); the role-change ``<select>`` options do not use it, so
    this counts owners reliably without per-row parsing.
    """
    return admin_users_html.count('bg-purple-100 text-purple-800">Owner</span>')


def _invite_member_and_login(page, *, target_role: str = "member") -> tuple[Principal, Principal]:
    """Sign up a fresh Owner and invite one user of ``target_role`` who is then
    logged in. Returns ``(owner, invitee)``."""
    owner = _signup_owner(page)
    marker = flows.unique_marker()
    email = f"{target_role}-{marker}@e2e.test"
    password = _password(marker)
    _invite(owner, email=email, role=target_role, password=password)

    # Confirm the invite actually created the user (the invite handler re-renders
    # with HTTP 200 on failure too, so status alone is not proof).
    users_html = _web_get(owner.client, "/admin/users").text
    uid = _user_id_for_email(users_html, email)
    assert uid is not None, f"invite of {email} (role={target_role}) created no user"

    invitee_client = ApiClient(config.base_url())
    resp = invitee_client.login(owner.slug, email, password)
    return owner, Principal(invitee_client, owner.slug, email, password, int(resp["user_id"]))


def _recent_audit_actions(admin_dashboard_html: str) -> list[str]:
    """Return the list of audit action strings from the ``/admin`` dashboard feed.

    The "Recent Audit Events" table renders each action in a
    ``text-gray-900`` cell; isolating the region after that heading avoids
    matching the (earlier) jobs table (``templates/admin_dashboard.html``).
    """
    idx = admin_dashboard_html.find("Recent Audit Events")
    region = admin_dashboard_html[idx:] if idx != -1 else admin_dashboard_html
    return re.findall(
        r'<td class="px-4 py-2 text-sm text-gray-900">\s*([a-z_]+)\s*</td>', region
    )


# ── Tests ────────────────────────────────────────────────────────────────────


def test_admin_role_endpoint_cannot_demote_last_owner(page):
    """Demoting the sole Owner via /admin/users/role is rejected (no lockout).

    The handler (admin_handlers.rs) now includes a last-owner guard that mirrors
    the account_change_role twin. POST /admin/users/role setting the only Owner to
    member; assert rejection (403/409) and the user is still Owner.
    """
    owner = _signup_owner(page)

    # An Admin (not the owner) drives the demotion, so this exercises the
    # last-Owner guard specifically — not self-demotion.
    marker = flows.unique_marker()
    admin_email = f"admin-{marker}@e2e.test"
    admin_pw = _password(marker)
    _invite(owner, email=admin_email, role="admin", password=admin_pw)
    admin = ApiClient(config.base_url())
    admin.login(owner.slug, admin_email, admin_pw)

    # The acting admin can view /admin/users and resolve the owner's id.
    users_html = _web_get(admin, "/admin/users").text
    owner_id = _user_id_for_email(users_html, owner.email)
    assert owner_id is not None, "could not find the owner row on /admin/users"
    assert _owner_badge_count(users_html) == 1, "expected exactly one Owner before demotion"

    resp = _web_post(admin, "/admin/users/role", {"user_id": owner_id, "role": "member"})

    # Intended contract: the only Owner must not be demotable (avoids a tenant
    # with zero owners). The /account/team twin returns 409 for this.
    after_html = _web_get(admin, "/admin/users").text
    assert _owner_badge_count(after_html) >= 1, (
        "the sole Owner was demoted via /admin/users/role — the tenant now has no "
        "Owner (lockout). The last-owner guard is missing from admin_change_user_role."
    )
    assert resp.status_code in (403, 409), (
        f"expected /admin/users/role to refuse demoting the last Owner "
        f"(403/409); got {resp.status_code}"
    )


def test_admin_role_endpoint_cannot_self_demote(page):
    """An owner cannot demote themselves via /admin/users/role (avoid self-lockout)."""
    owner = _signup_owner(page)

    resp = _web_post(
        owner.client, "/admin/users/role", {"user_id": owner.user_id, "role": "member"}
    )

    # Intended contract: refuse self-demotion to avoid locking yourself out of
    # admin. admin_change_user_role has no such guard today.
    assert resp.status_code in (403, 409), (
        f"expected /admin/users/role to refuse owner self-demotion (403/409); "
        f"got {resp.status_code}"
    )
    # No self-lockout: the owner must still hold admin access afterwards.
    assert _web_get(owner.client, "/admin").status_code == 200, (
        "owner self-demoted via /admin/users/role and lost admin access (lockout)"
    )


def test_admin_cannot_grant_owner_role(page):
    """An admin (not owner) POSTing role/invite with role=owner is forbidden (403).

    Drive both ``/admin/users/role`` and ``/admin/users/invite`` (and the
    ``/account/team`` equivalents) with ``role=owner`` as an admin-role user; assert
    403 and no new Owner is created.
    """
    owner = _signup_owner(page)
    marker = flows.unique_marker()

    # An admin actor + a member target to attempt to promote.
    admin_email = f"admin-{marker}@e2e.test"
    admin_pw = _password(marker)
    _invite(owner, email=admin_email, role="admin", password=admin_pw)
    member_email = f"member-{marker}@e2e.test"
    member_pw = _password(flows.unique_marker())
    _invite(owner, email=member_email, role="member", password=member_pw)

    member_id = _user_id_for_email(_web_get(owner.client, "/admin/users").text, member_email)
    assert member_id is not None, "could not resolve the member's id"

    admin = ApiClient(config.base_url())
    admin.login(owner.slug, admin_email, admin_pw)

    # (a) admin → /admin/users/role promote member to owner → must be 403.
    resp_a = _web_post(admin, "/admin/users/role", {"user_id": member_id, "role": "owner"})
    assert resp_a.status_code == 403, (
        f"admin promoting a member to Owner via /admin/users/role should be 403; "
        f"got {resp_a.status_code}"
    )

    # (b) admin → /account/team/role promote member to owner → must be 403.
    resp_b = _web_post(admin, "/account/team/role", {"user_id": member_id, "new_role": "owner"})
    assert resp_b.status_code == 403, (
        f"admin promoting a member to Owner via /account/team/role should be 403; "
        f"got {resp_b.status_code}"
    )

    # (c) admin → /admin/users/invite a new Owner → must not create an owner.
    _web_post(
        admin,
        "/admin/users/invite",
        {"email": f"esc-admin-{marker}@e2e.test", "role": "owner", "password": _password(marker)},
    )
    # (d) admin → /account/team/invite a new Owner → must not create an owner.
    _web_post(
        admin,
        "/account/team/invite",
        {"email": f"esc-team-{marker}@e2e.test", "role": "owner"},
    )

    # Whole-tenant invariant: a non-owner admin must never produce a new Owner.
    final_html = _web_get(owner.client, "/admin/users").text
    assert _owner_badge_count(final_html) == 1, (
        "a non-owner admin created a new Owner (privilege escalation) — likely via "
        "/account/team/invite, which does not cap the grantable role to the actor's."
    )


def test_member_cannot_self_promote_via_team_endpoint(page):
    """A member POSTing /account/team/role to elevate themselves is 403 (server-side).

    The UI hides the control; the endpoint must still reject a forged POST from a
    member targeting their own user_id with role=admin/owner.
    """
    _owner, member = _invite_member_and_login(page, target_role="member")

    resp = _web_post(
        member.client, "/account/team/role", {"user_id": member.user_id, "new_role": "admin"}
    )
    assert resp.status_code == 403, (
        f"a member self-promoting via /account/team/role must be 403; got {resp.status_code}"
    )
    # No privilege gained: still denied the admin panel.
    assert _web_get(member.client, "/admin").status_code == 403, (
        "member gained admin access after a forged /account/team/role self-promotion"
    )


def test_member_cannot_self_promote_via_admin_endpoint(page):
    """A member directly POSTing /admin/users/role to elevate themselves is denied."""
    _owner, member = _invite_member_and_login(page, target_role="member")

    resp = _web_post(
        member.client, "/admin/users/role", {"user_id": member.user_id, "role": "admin"}
    )
    assert resp.status_code == 403, (
        f"a member self-promoting via /admin/users/role must be 403; got {resp.status_code}"
    )
    assert _web_get(member.client, "/admin").status_code == 403, (
        "member gained admin access after a forged /admin/users/role self-promotion"
    )


def test_owner_only_pages_deny_admin_role(page):
    """An admin-role user is denied owner-only surfaces.

    Invite an admin-role user; as them GET ``/admin/tenants`` (owner-only) and
    attempt ``/account/danger`` actions; assert 403. (Also check whether
    ``/admin/decrypt-audit`` is owner-only vs admin-accessible — a possible
    over-grant.)
    """
    _owner, admin = _invite_member_and_login(page, target_role="admin")

    # Owner-only super-admin surface: an admin must be forbidden.
    assert _web_get(admin.client, "/admin/tenants").status_code == 403, (
        "/admin/tenants (owner-only) must reject an admin-role user with 403"
    )

    # Sanity: the admin *is* a valid admin (the dashboard is admin-accessible).
    assert _web_get(admin.client, "/admin").status_code == 200

    # Owner-only destructive action: deleting the workspace must be denied to an
    # admin (and must not actually delete anything).
    resp = _web_post(admin.client, "/account/danger/delete", {"confirm_slug": admin.slug})
    assert 400 <= resp.status_code < 500, (
        f"admin attempting the owner-only workspace deletion must be denied (4xx); "
        f"got {resp.status_code}"
    )
    assert "Workspace Deleted" not in resp.text, (
        "an admin-role user was able to trigger workspace deletion (owner-only action)"
    )

    # The decrypt-access audit log is intentionally admin-accessible (tenant-scoped
    # to the admin's own tenant per the handler contract), i.e. NOT owner-only.
    assert _web_get(admin.client, "/admin/decrypt-audit").status_code == 200, (
        "/admin/decrypt-audit should be reachable by a tenant admin (own-tenant scope)"
    )


def test_team_mutations_enforced_server_side_for_member(page):
    """A member's forged POST to /account/team/remove (and /invite) is 403.

    Confirms server-side enforcement, not just template hiding.
    """
    _owner, member = _invite_member_and_login(page, target_role="member")

    # Remove is Owner-only; a member's forged POST must be 403.
    resp_remove = _web_post(member.client, "/account/team/remove", {"user_id": member.user_id})
    assert resp_remove.status_code == 403, (
        f"a member forging /account/team/remove must be 403; got {resp_remove.status_code}"
    )

    # Invite is Owner/Admin-only; a member's forged POST must be 403.
    resp_invite = _web_post(
        member.client,
        "/account/team/invite",
        {"email": f"sneak-{flows.unique_marker()}@e2e.test", "role": "member"},
    )
    assert resp_invite.status_code == 403, (
        f"a member forging /account/team/invite must be 403; got {resp_invite.status_code}"
    )


def test_privileged_action_writes_audit_event(page):
    """A role change / tag merge / job action appears in the audit log.

    Perform a role change as owner, then assert a corresponding row (actor + action
    + target, ideally before→after) is recorded and visible (the /admin recent-audit
    feed is the only surface). Note: role-change details may omit the old role.
    """
    owner = _signup_owner(page)
    marker = flows.unique_marker()

    # An admin actor (so the /admin audit feed it later reads is tenant-scoped),
    # plus a member to act upon.
    admin_email = f"admin-{marker}@e2e.test"
    admin_pw = _password(marker)
    _invite(owner, email=admin_email, role="admin", password=admin_pw)
    member_email = f"member-{marker}@e2e.test"
    member_pw = _password(flows.unique_marker())
    _invite(owner, email=member_email, role="member", password=member_pw)
    member_id = _user_id_for_email(_web_get(owner.client, "/admin/users").text, member_email)
    assert member_id is not None, "could not resolve the member's id"

    admin = ApiClient(config.base_url())
    admin.login(owner.slug, admin_email, admin_pw)

    # Privileged action: the admin promotes the member to admin.
    resp = _web_post(admin, "/admin/users/role", {"user_id": member_id, "role": "admin"})
    assert resp.status_code == 200, f"role change unexpectedly returned {resp.status_code}"

    # The action must be recorded in the audit log and visible on /admin.
    actions = _recent_audit_actions(_web_get(admin, "/admin").text)
    assert "user_role_changed" in actions, (
        f"role change wrote no 'user_role_changed' audit event; saw actions={actions}"
    )
    region = _web_get(admin, "/admin").text
    assert f"user #{member_id}" in region, (
        f"audit feed has no row targeting the changed user #{member_id}"
    )


def test_team_self_service_actions_are_audited(page):
    """Inviting/removing/role-changing via /account/team writes audit events.

    The account_* handlers (handlers_account.rs) now write audit events for
    invite, change-role, and remove-user actions. Perform a team invite via
    /account/team and assert it produces an audit-log entry.
    """
    owner = _signup_owner(page)
    marker = flows.unique_marker()

    # An admin viewer so the /admin audit feed is tenant-scoped (own tenant only).
    admin_email = f"admin-{marker}@e2e.test"
    admin_pw = _password(marker)
    _invite(owner, email=admin_email, role="admin", password=admin_pw)
    admin = ApiClient(config.base_url())
    admin.login(owner.slug, admin_email, admin_pw)

    before = len(_recent_audit_actions(_web_get(admin, "/admin").text))

    # Self-service invite via /account/team (Owner-driven).
    invitee_email = f"teaminv-{flows.unique_marker()}@e2e.test"
    inv_resp = _web_post(owner.client, "/account/team/invite", {"email": invitee_email, "role": "member"})
    assert inv_resp.status_code == 200, f"team invite unexpectedly returned {inv_resp.status_code}"
    # Precondition: the invite actually created the user, so a missing audit row
    # is a genuine gap (not a no-op invite).
    assert _user_id_for_email(_web_get(owner.client, "/admin/users").text, invitee_email) is not None, (
        "the /account/team invite created no user — cannot assess auditing"
    )

    after = len(_recent_audit_actions(_web_get(admin, "/admin").text))
    assert after > before, (
        "a /account/team self-service invite wrote no audit event (before="
        f"{before}, after={after}); account_* team handlers do not audit, unlike "
        "their /admin twins."
    )


@pytest.mark.skip(reason="blocked: no /admin/audit viewer/filter/export route exists (feature gap)")
def test_audit_log_full_viewer_filter_export(page):
    """The full audit trail is viewable, filterable, and exportable (SIEM).

    BLOCKED: there is no ``/admin/audit`` route — only the dashboard's last-10 rows;
    no filter by actor/action/date, no pagination, no export. A SIEM-grade audit log
    is an enterprise requirement; cataloguing the gap.
    """
    ...


@pytest.mark.skip(reason="blocked: no suspend/reactivate route wired (admin_disable_user is orphaned)")
def test_user_suspend_and_reactivate(page):
    """An admin can suspend a user (sessions/logins blocked) and reactivate them.

    BLOCKED: ``admin_disable_user`` exists in the store but no web route wires it and
    ``users`` has no surfaced active/disabled flag — instant offboarding without
    deletion is unreachable.
    """
    ...
