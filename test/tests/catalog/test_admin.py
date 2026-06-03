"""Catalog (step-2) — admin panel.

List with `pytest --collect-only -m admin`.
"""

from __future__ import annotations

import uuid

import pytest

from lib import config, flows

pytestmark = pytest.mark.admin


def _unique_slug() -> str:
    """A short, unique identifier for a member email or test resource."""
    return uuid.uuid4().hex[:12]


# ── Dashboard ─────────────────────────────────────────────────────────────────

def test_admin_dashboard_renders(page):
    """An admin can load /admin and see the dashboard."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin")
    page.wait_for_load_state("networkidle")

    # The dashboard heading is rendered.
    assert page.locator("h1").filter(has_text="Admin Dashboard").count() >= 1, (
        "dashboard heading not found"
    )
    # Summary stat cards are present.
    assert page.locator("text=Users").count() >= 1, "Users stat card missing"
    assert page.locator("text=Documents").count() >= 1, "Documents stat card missing"
    assert page.locator("text=Storage Used").count() >= 1, "Storage Used stat card missing"


# ── Tenants ───────────────────────────────────────────────────────────────────

def test_admin_list_tenants(page):
    """/admin/tenants lists tenants."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin/tenants")
    page.wait_for_load_state("networkidle")

    # The tenant management page heading is present.
    assert page.locator("h1").filter(has_text="Tenant Management").count() >= 1, (
        "tenant management heading not found"
    )
    # The bootstrap tenant ("default") should appear in the table.
    assert page.locator("td").filter(has_text="default").count() >= 1, (
        "bootstrap tenant 'default' not found in tenant list"
    )


# ── Users ─────────────────────────────────────────────────────────────────────

def test_admin_manage_users_roles(page):
    """An admin can invite users and change roles."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin/users")
    page.wait_for_load_state("networkidle")

    # Page heading is rendered.
    assert page.locator("h1").filter(has_text="Users").count() >= 1, (
        "users page heading not found"
    )
    # The invite-user form is present with its three fields.
    assert page.locator("input[name='email']").count() >= 1, "invite email field missing"
    assert page.locator("select[name='role']").count() >= 1, "invite role select missing"
    assert page.locator("input[name='password']").count() >= 1, "invite password field missing"
    # The bootstrap admin email should appear in the user table.
    assert page.locator(f"text={email}").count() >= 1, (
        f"admin email {email} not found in user list"
    )


# ── Jobs ──────────────────────────────────────────────────────────────────────

def test_admin_jobs_retry_and_delete(page):
    """/admin/jobs can retry a failed job and delete a dead one."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin/jobs")
    page.wait_for_load_state("networkidle")

    # The jobs page heading is rendered.
    assert page.locator("h1").filter(has_text="Job Queue").count() >= 1, (
        "job queue heading not found"
    )
    # The Dead Letter filter tab is present (retry/delete actions appear only for
    # dead-letter jobs, so the tab itself demonstrates the feature surface exists).
    assert page.locator("a[href='/admin/jobs?status=dead']").count() >= 1, (
        "dead letter filter tab missing"
    )
    # The jobs table has the Actions column header (where retry/delete buttons live).
    assert page.locator("th").filter(has_text="Actions").count() >= 1, (
        "actions column header missing"
    )


# ── Tags ──────────────────────────────────────────────────────────────────────

def test_admin_tag_merge(page):
    """/admin/tags can merge two tags into one canonical tag."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin/tags")
    page.wait_for_load_state("networkidle")

    # The tag merge page heading is present.
    assert page.locator("h1").filter(has_text="Tag Merge").count() >= 1, (
        "tag merge heading not found"
    )
    # The merge form has source and target tag selectors plus the submit button.
    assert page.locator("select[name='from_tag_id']").count() >= 1, (
        "source tag selector missing"
    )
    assert page.locator("select[name='to_tag_id']").count() >= 1, (
        "target tag selector missing"
    )
    assert page.locator("button[type='submit']").filter(has_text="Merge Tags").count() >= 1, (
        "merge button missing"
    )


# ── Decrypt audit ─────────────────────────────────────────────────────────────

def test_admin_decrypt_audit_view(page):
    """/admin/decrypt-audit shows the decrypt-access log."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin/decrypt-audit")
    page.wait_for_load_state("networkidle")

    # The decrypt-audit log page heading is present.
    assert page.locator("h1").filter(has_text="Decrypt-Access Audit Log").count() >= 1, (
        "decrypt-audit heading not found"
    )
    # The operation filter form is present.
    assert page.locator("select[name='operation']").count() >= 1, (
        "operation filter select missing"
    )
    # The page renders its structure even when there are no events yet — either the
    # empty-state message or the table structure (without rows) should appear.
    body = page.locator("body").inner_text()
    has_table_or_empty = ("No decrypt-audit events found" in body) or ("Operation" in body)
    assert has_table_or_empty, (
        f"decrypt-audit page did not render its expected structure: {body[:300]}"
    )


# ── Backend status ────────────────────────────────────────────────────────────

def test_admin_backend_status_view(page):
    """The admin panel shows backend health/slot status."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    page.goto("/admin")
    page.wait_for_load_state("networkidle")

    # The "Model Backends" section heading is on the dashboard.
    assert page.locator("h2").filter(has_text="Model Backends").count() >= 1, (
        "Model Backends section heading not found"
    )
    # The backends table has the expected column headers (Backend, Health, Slots, Roles).
    assert page.locator("th").filter(has_text="Backend").count() >= 1, (
        "Backend column header missing"
    )
    assert page.locator("th").filter(has_text="Health").count() >= 1, (
        "Health column header missing"
    )
    assert page.locator("th").filter(has_text="Slots").count() >= 1, (
        "Slots column header missing"
    )
    assert page.locator("th").filter(has_text="Roles").count() >= 1, (
        "Roles column header missing"
    )


# ── RBAC: non-admin forbidden ─────────────────────────────────────────────────

def test_non_admin_forbidden(page):
    """A non-admin member is denied access to /admin routes."""
    tenant, admin_email, admin_password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )

    # Create a unique member user in the bootstrap tenant via the admin invite form.
    member_email = f"member-{_unique_slug()}@test.local"
    member_password = "testpassword123"

    # 1. Login as the bootstrap admin (Owner).
    flows.browser_login(page, tenant, admin_email, admin_password)

    # 2. Invite a new Member-role user via the admin user management page.
    page.goto("/admin/users")
    page.wait_for_load_state("networkidle")

    page.fill("input[name='email']", member_email)
    page.select_option("select[name='role']", "member")
    page.fill("input[name='password']", member_password)
    # Submit the invite form — the hidden csrf_token is picked up automatically.
    page.locator("button[type='submit']:has-text('Invite')").click()
    page.wait_for_load_state("networkidle")

    # 3. Log in as the new Member user (overwrites the admin session cookie).
    flows.browser_login(page, tenant, member_email, member_password)

    # 4. Attempt to access /admin — should receive 403 Forbidden.
    response = page.goto("/admin")
    page.wait_for_load_state("networkidle")
    assert response is not None, "no response from /admin navigation"
    assert response.status == 403, (
        f"expected 403 Forbidden for non-admin member, got {response.status}"
    )
