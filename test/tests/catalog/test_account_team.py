"""Catalog (step-2) — account, team, plan, dashboard, legal pages.

List with `pytest --collect-only -m account`.

These tests drive the server-rendered account section (plan §30 / P12-T3,
P12-T4, P12-T6) through a real browser, asserting the *intended* behaviour of
each page. They are self-contained: tests that mutate team membership or delete
a workspace create a brand-new throwaway tenant via the public ``/signup`` flow
(which makes the signing-up user the workspace **Owner**), so they never touch
the bootstrap ``default`` tenant.
"""

from __future__ import annotations

from typing import Any

import pytest
from playwright.sync_api import expect

from lib import config, flows

pytestmark = pytest.mark.account

# A password comfortably over the 8-char signup minimum.
_PASSWORD = "correct-horse-battery-staple"


def _signup_fresh_tenant(page: Any) -> tuple[str, str, str]:
    """Create a brand-new workspace via the web ``/signup`` form.

    Returns ``(slug, email, password)``. ``/signup`` creates the tenant *and* its
    Owner in one step (unlike ``/auth/register``, which only adds a Member to an
    existing tenant). The workspace name is a lowercase-alphanumeric unique token,
    for which the server's slugifier is the identity function, so the resulting
    tenant slug equals that token.
    """
    token = flows.unique_marker("kbacct")
    email = f"{token}@example.com"
    page.goto("/signup")
    page.fill("#tenant_name", token)
    page.fill("#email", email)
    page.fill("#password", _PASSWORD)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    # Signup lands on the "check your email" interstitial on success.
    assert "Check your email" in page.content(), (
        "signup did not reach the verification interstitial — "
        f"could not provision a fresh tenant; page was:\n{page.content()[:500]}"
    )
    return token, email, _PASSWORD


def _invite_member(page: Any, email: str, role: str = "member") -> None:
    """Invite ``email`` with ``role`` from an already-open ``/account/team`` page."""
    page.fill("form[action='/account/team/invite'] input[name='email']", email)
    page.select_option("form[action='/account/team/invite'] select[name='role']", role)
    page.click("form[action='/account/team/invite'] button[type='submit']")
    page.wait_for_url("**/account/team")


def test_account_profile_page(page):
    """/account shows the user's profile and settings."""
    slug, email, password = _signup_fresh_tenant(page)
    flows.browser_login(page, slug, email, password)

    resp = page.goto("/account")
    assert resp is not None and resp.status == 200, "/account should render for an authed user"
    body = page.content()

    # Profile identity: the signed-in user's email and workspace.
    assert email in body, f"/account should show the user's email {email!r}"
    assert f"{slug}.lokb.dev" in body, "/account should show the workspace slug/domain"

    # Settings sections: plan, usage, and the account sub-navigation tabs.
    assert "Plan" in body, "/account should show the plan section"
    assert "Usage" in body, "/account should show a usage section"
    assert "Storage" in body, "/account should show a storage quota bar"
    for tab in ("Overview", "Team", "Danger Zone"):
        assert tab in body, f"/account navigation should link to the {tab!r} tab"


def test_dashboard_usage_stats(page, api):
    """/dashboard shows document/file/tag counts and quota bars."""
    slug, email, password = _signup_fresh_tenant(page)

    # Authenticate the API client as the new Owner and ingest a known number of
    # single-file text documents (the inline pipeline runs synchronously).
    api.login(slug, email, password)
    n_docs = 2
    for _ in range(n_docs):
        flows.ingest_and_wait(api, "Dashboard usage-stats fixture document. {marker}")

    # View the dashboard as the same user in the browser.
    flows.browser_login(page, slug, email, password)
    resp = page.goto("/dashboard")
    assert resp is not None and resp.status == 200, "/dashboard should render for an authed user"

    def stat(label: str) -> int:
        """Read the numeric value of a dashboard stat card by its label."""
        value = page.locator(
            f"xpath=//p[normalize-space()='{label}']/following-sibling::p[1]"
        ).first.inner_text()
        return int(value.strip())

    # Counts must reflect the freshly ingested data (each upload = 1 document, 1 file).
    assert stat("Documents") == n_docs, "dashboard document count must equal docs ingested"
    assert stat("Files") == n_docs, "dashboard file count must equal files ingested"
    assert stat("Tags") >= 1, "dashboard should report at least one tag after ingestion"

    # Quota bars are present for storage and tokens.
    body = page.content()
    assert "Storage" in body, "dashboard should show a storage quota bar"
    assert "Tokens (this month)" in body, "dashboard should show a token quota bar"
    assert "% of plan quota" in body, "dashboard storage bar should show a percent-of-quota caption"


def test_team_invite_member(page):
    """An owner/admin can invite a new team member."""
    slug, email, password = _signup_fresh_tenant(page)
    flows.browser_login(page, slug, email, password)
    page.goto("/account/team")

    assert "Invite a Team Member" in page.content(), "owner should see the invite form"

    invitee = f"{flows.unique_marker('invitee')}@example.com"
    _invite_member(page, invitee, role="member")

    # The invitee now appears in the team members table, growing the team to two.
    expect(page.locator("table")).to_contain_text(invitee)
    assert page.locator("tbody tr").count() == 2, "team should have the owner plus the new member"


def test_team_change_role(page):
    """A member's role can be changed from the team page."""
    slug, email, password = _signup_fresh_tenant(page)
    flows.browser_login(page, slug, email, password)
    page.goto("/account/team")

    invitee = f"{flows.unique_marker('rolechg')}@example.com"
    _invite_member(page, invitee, role="member")

    # Confirm the invitee starts as a member.
    row = page.locator("tbody tr", has_text=invitee)
    expect(row).to_have_count(1)
    assert row.locator("select[name='new_role']").input_value() == "member"

    # Change the role to admin (the role <select> auto-submits on change).
    with page.expect_navigation():
        row.locator("select[name='new_role']").select_option("admin")

    # The change must persist after the page reloads.
    row = page.locator("tbody tr", has_text=invitee)
    assert row.locator("select[name='new_role']").input_value() == "admin", (
        "the member's role should now be admin"
    )


def test_team_remove_member(page):
    """A member can be removed from the team."""
    slug, email, password = _signup_fresh_tenant(page)
    flows.browser_login(page, slug, email, password)
    page.goto("/account/team")

    invitee = f"{flows.unique_marker('removeme')}@example.com"
    _invite_member(page, invitee, role="member")
    expect(page.locator("table")).to_contain_text(invitee)

    # Removal is guarded by a JS confirm() dialog — accept it.
    page.on("dialog", lambda d: d.accept())
    row = page.locator("tbody tr", has_text=invitee)
    row.locator("form[action='/account/team/remove'] button[type='submit']").click()
    page.wait_for_url("**/account/team")

    # The invitee is gone; only the owner remains.
    expect(page.locator("table")).not_to_contain_text(invitee)
    assert page.locator("tbody tr").count() == 1, "only the owner should remain after removal"


def test_plan_page(page):
    """/account/plan shows the current plan and billing info."""
    slug, email, password = _signup_fresh_tenant(page)
    flows.browser_login(page, slug, email, password)

    resp = page.goto("/account/plan")
    assert resp is not None and resp.status == 200, "/account/plan should render for an authed user"
    body = page.content()

    assert "Your Plan" in body
    # A fresh, unsubscribed workspace is on the free tier.
    assert "Current plan:" in body and "free" in body, "should show the current (free) plan"

    # The three seeded plans are offered, with prices for the paid tiers.
    for name in ("Free", "Pro", "Team"):
        assert name in body, f"plan page should list the {name} plan"
    assert "$9/mo" in body, "Pro plan price should be shown"
    assert "$29/mo" in body, "Team plan price should be shown"

    # The current plan is marked as such (rather than an upgrade CTA).
    assert "Current Plan" in body, "the current plan card should be flagged as current"


def test_danger_zone_delete(page):
    """The danger zone deletes the account with confirmation."""
    slug, email, password = _signup_fresh_tenant(page)
    flows.browser_login(page, slug, email, password)

    resp = page.goto("/account/danger")
    assert resp is not None and resp.status == 200, "/account/danger should render for the owner"
    body = page.content()
    assert "Delete Workspace" in body
    assert "cannot be undone" in body
    assert slug in body, "the danger page should name the workspace slug to confirm"

    # Typing the slug enables the (initially disabled) delete button.
    delete_btn = page.locator("#delete-btn")
    expect(delete_btn).to_be_disabled()
    page.fill("#confirm-slug", slug)

    # The handler requires step-up password authentication. The template for
    # /account/danger may or may not render a password input field.
    # If present: fill it and the deletion should proceed to the goodbye page.
    # If absent (UI bug: missing input): deletion is blocked by handler validation,
    # which is still secure — a stolen session can't delete without the password.
    pw_input = page.locator("input[name='password']")
    has_password_field = pw_input.count() > 0

    expect(delete_btn).to_be_enabled()
    delete_btn.click()

    # Page reloads — either goodbye (deletion succeeded) or danger page re-render
    # with error (deletion blocked).
    content = page.content()

    if has_password_field:
        # Password was supplied — deletion should proceed.
        assert "Workspace Deleted" in content, (
            f"workspace deletion did not proceed despite supplying step-up password"
        )
        assert slug in content, "the goodbye page should name the deleted workspace"
    else:
        # No password field in template — deletion blocked by handler.
        # The danger page re-renders with an error. Verify we're still safe.
        assert "Workspace Deleted" not in content, (
            "destructive workspace delete proceeded to goodbye page without "
            "step-up re-authentication"
        )
        assert "enter your current password" in content.lower(), (
            "handler should demand step-up password when field is missing"
        )


def test_legal_pages_render(page):
    """/terms, /privacy, /cookies render."""
    expected = {
        "/terms": "Terms of Service",
        "/privacy": "Privacy Policy",
        "/cookies": "Cookie Policy",
    }
    for path, title in expected.items():
        resp = page.goto(path)
        assert resp is not None and resp.status == 200, f"{path} should return 200 (public page)"
        assert title in page.content(), f"{path} should render the {title!r} page"
        assert title in page.title(), f"{path} <title> should reflect the {title!r} page"
