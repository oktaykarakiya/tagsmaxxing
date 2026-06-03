"""Catalog — the in-account UPGRADE journey for an EXISTING user with files.

The scenario that converts free users to paying customers: someone who already
signed up and uploaded files then upgrades. A code audit found the journey is
broken/unproven at the ends that matter most — the only "Upgrade" CTA is a dead GET
link to a POST-only endpoint (405), and the headline paid differentiator
(remote/better models) is gated by dead code, so upgrading unlocks nothing and
existing docs are never re-processed. The dead-CTA check is PENDING (drivable, no
Stripe); the rest need a signed webhook + a plan-assignment path → BLOCKED.
"""

from __future__ import annotations

import pytest

from lib import config, flows

pytestmark = pytest.mark.billing


def test_upgrade_cta_starts_checkout_not_405(page):
    """Clicking 'Upgrade' on /account/plan reaches checkout instead of erroring.

    Audit: the CTA is ``<a href="/billing/checkout?plan=pro">`` but the route is
    POST-only → a GET 405s. As an existing free tenant, click the Upgrade control and
    assert it does NOT 405 (lands on a Stripe URL / posts correctly) — the entire
    monetization entry point. Expected to FAIL, surfacing the dead button.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)
    page.goto("/account/plan")
    page.wait_for_load_state("networkidle")

    # The Upgrade CTA is an <a> linking to /billing/checkout?plan=<code>.
    # For a free tenant whose current plan is "free", non-current plan cards
    # have a label "Upgrade" when priced, "Switch" when free.
    upgrade = page.locator("a:has-text('Upgrade')")
    assert upgrade.count() >= 1, (
        "a free tenant should see at least one Upgrade CTA on the /account/plan page"
    )
    href = upgrade.first.get_attribute("href")
    assert href is not None, "Upgrade CTA must have an href attribute"
    assert "checkout" in href, (
        f"Upgrade CTA href should point to /billing/checkout, got: {href!r}"
    )

    # Clicking the Upgrade button starts the checkout flow.
    # The intended contract: navigating to the link target must NOT return
    # 405 Method Not Allowed.  page.goto() follows the link as a browser
    # would; its return value carries the main-response status code.
    response = page.goto(href)
    assert response is not None, "navigating to the Upgrade link must produce a response"
    assert response.status != 405, (
        f"Upgrade CTA (href={href!r}) returned 405 Method Not Allowed. "
        f"The /billing/checkout route is POST-only but the CTA is a plain GET "
        f"<a href> — the Upgrade button is dead and blocks the monetization "
        f"entry point. Fix: accept GET /billing/checkout?plan=pro or use a "
        f"form/POST button with HTMX."
    )


@pytest.mark.skip(reason="blocked: signed checkout.session.completed webhook required (Stripe unconfigured)")
def test_upgrade_applies_to_existing_tenant_preserving_data(api):
    """An upgrade mutates the EXISTING tenant in place; its documents/tags/search survive.

    BLOCKED: needs a signed ``checkout.session.completed`` with the tenant's
    client_reference_id. Asserts plan=pro on the same tenant (no duplicate), and all
    pre-existing docs still searchable.
    """
    ...


@pytest.mark.skip(reason="blocked: needs signed webhook + a way to assign the free plan first")
def test_free_cap_blocked_ingest_unblocked_after_upgrade(api):
    """An ingest blocked at the free cap succeeds after upgrade; prior usage recounts vs the new cap.

    BLOCKED: free plan isn't assigned to fresh tenants (plan_id NULL → unlimited), and
    the upgrade needs a signed webhook. The 'blocked → pay → unblocked' transition is
    the #1 reason users upgrade.
    """
    ...


@pytest.mark.skip(reason="blocked: no bulk reindex; remote-models gate is dead code")
def test_existing_docs_reprocessed_under_better_models_after_upgrade(api):
    """After upgrade, existing free-tier docs get re-tagged/re-embedded with better models.

    BLOCKED: only a per-doc retag (tagger-only, plan-agnostic) exists — no bulk
    reindex, and ``is_remote_models_allowed`` has no call sites, so existing content
    is stuck at free-tier quality. A core paid-value gap.
    """
    ...


@pytest.mark.skip(reason="blocked: remote-models gate (is_remote_models_allowed) has zero call sites")
def test_remote_models_gate_flips_on_upgrade(api):
    """Free tenants are restricted to local models; upgrading unlocks remote inference.

    BLOCKED: the gate is dead code; free tenants already use remote models and upgrade
    flips nothing. Needs the gate wired + routing introspection to observe.
    """
    ...


@pytest.mark.skip(reason="blocked: signed webhook required to reach the upgraded state")
def test_account_pages_reflect_new_plan_after_upgrade(page):
    """After upgrade, /account + /account/plan show the new plan, new quota bars, 'Current Plan'.

    BLOCKED: rendering is implemented but only the free default is tested; reaching
    the upgraded state needs a signed webhook.
    """
    ...


@pytest.mark.skip(reason="blocked: signed webhook required; double-upgrade idempotency")
def test_double_upgrade_is_idempotent(api):
    """Replaying the upgrade webhook / double-clicking Upgrade does not double-charge or corrupt plan.

    BLOCKED: event-id idempotency is covered for fresh tenants; re-applying pro over
    pro and double-checkout on an already-subscribed tenant need a signed webhook.
    """
    ...


@pytest.mark.skip(reason="blocked: signed webhook sequence required")
def test_downgrade_then_reupgrade_preserves_data(api):
    """pro → cancel → free → upgrade back to pro preserves data and restores quotas/customer id.

    BLOCKED: needs a completed→deleted→updated signed webhook sequence.
    """
    ...
