"""Catalog — data governance & compliance (residency, retention, export, erasure, keys).

Enterprise procurement gates. The crypto primitives exist (envelope encryption,
shred + export *pipelines*, residency enum, decrypt-audit) but the controls
companies actually buy are unwired or absent — and a code audit found the ``serve``
binary never starts a worker pool, so even the implemented crypto-shred/export jobs
are enqueued and never executed in production. Drivable checks are PENDING; the rest
are BLOCKED (feature absent or needs DB/blob inspection / fault injection).
"""

from __future__ import annotations

import pytest

from lib import config, flows

pytestmark = pytest.mark.governance


def _decrypt_audit_row_ids(page) -> set[str]:
    """Return the set of event ids currently shown in the /admin/decrypt-audit table.

    Loads the page fresh, asserts it actually rendered (so a redirect/403 surfaces
    as a clear harness error rather than an empty set), and harvests the first
    ``<td>`` (the append-only DB id) of every events-table row. Comparing id *sets*
    across two reads is robust to the page's 100-row cap and to ordering: a genuine
    new decrypt operation gets a fresh, higher id that appears at the top.
    """
    page.goto("/admin/decrypt-audit")
    page.wait_for_load_state("networkidle")
    assert page.locator("h1").filter(has_text="Decrypt-Access Audit Log").count() >= 1, (
        "decrypt-audit page did not render — admin login or role resolution failed"
    )
    rows = page.locator("main table tbody tr")
    ids: set[str] = set()
    for i in range(rows.count()):
        cell = (rows.nth(i).locator("td").first.inner_text() or "").strip()
        if cell:
            ids.add(cell)
    return ids


def test_dpa_terms_acceptance_required_at_signup(page):
    """Signup without accepting Terms/DPA is rejected; consent (who/when/version) is recorded.

    Audit: no acceptance checkbox/validation at /signup. Submit signup without
    acceptance and assert rejection — expected to FAIL (a DPA the customer never
    affirmatively accepted is unenforceable).
    """
    workspace = f"E2E DPA {flows.unique_marker('ws')}"
    email = f"{flows.unique_marker('dpa')}@e2e.test"
    password = "E2e-Dpa-Passw0rd!"

    page.goto("/signup")
    page.wait_for_selector("input[name='tenant_name']", timeout=15_000)

    page.fill("input[name='tenant_name']", workspace)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)

    # Submit WITHOUT affirmatively accepting Terms/Privacy/DPA: leave every consent
    # checkbox unticked. (The current form has none at all — submitting the bare form
    # is itself "without acceptance".)
    consent = page.locator("form[action='/signup'] input[type='checkbox']")
    for i in range(consent.count()):
        cb = consent.nth(i)
        if cb.is_checked():
            cb.uncheck()

    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    content = page.content()
    # Intended behavior: a signup that does not affirmatively accept Terms/Privacy/DPA
    # must be REJECTED — the "Check your email" success interstitial must NOT render and
    # no workspace/account is created. Unique workspace+email rule out a collision as the
    # reason for any rejection, so an absent interstitial means a consent gate stopped it.
    assert "Check your email" not in content, (
        "signup completed without the user affirmatively accepting Terms/Privacy/DPA "
        "(no acceptance checkbox or server-side validation at /signup) and rendered the "
        "'Check your email' success page; a DPA the customer never accepted is unenforceable"
    )


def test_decrypt_access_audit_records_real_events(page, api):
    """A genuine decrypt (ingest/search of an encrypted doc) adds a decrypt-audit row.

    The existing test passes even on an empty page. Capture the /admin/decrypt-audit
    row count, ingest+search a marked doc, and assert the count increased with a row
    tied to that activity (not just 'page renders').
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()

    # The API client (ingest/search) and the browser (admin page) hold independent
    # sessions — authenticate both as the bootstrap admin (Owner → may view the audit log).
    api.login(tenant, email, password)
    flows.browser_login(page, tenant, email, password)

    # 1. Baseline: which decrypt-audit rows exist before any activity of ours.
    before_ids = _decrypt_audit_row_ids(page)

    # 2. Perform a genuine decrypt. Storing an encrypted document wraps a DEK; tagging,
    #    embedding, and search all unwrap it (DEK unwrap / provider-key decrypt) — every
    #    such operation must append a confidentiality-audit row.
    marker, _doc = flows.ingest_and_wait(api, "Confidential decrypt-audit probe. {marker}")
    hits = api.search(marker).get("hits", [])
    assert hits, f"search for {marker!r} returned no hits — the retrieval/decrypt path did not run"

    # 3. Re-read the audit log after the activity.
    after_ids = _decrypt_audit_row_ids(page)

    new_ids = after_ids - before_ids
    assert new_ids, (
        "ingesting and searching an encrypted document recorded no new decrypt-access "
        "audit rows; a genuine DEK unwrap / provider-key decrypt must append a row "
        f"(before={len(before_ids)} rows, after={len(after_ids)} rows)"
    )


@pytest.mark.skip(reason="blocked: enqueued DeleteTenant/Export jobs are never processed by `serve` (no worker pool)")
def test_deletion_and_export_jobs_actually_run_in_serve(page):
    """Tenant-delete / export jobs actually execute under a vanilla `kb serve`.

    BLOCKED (and a severe finding): ``run_worker_pool`` has zero call sites in the
    serve binary, so crypto-shred/export jobs are enqueued and forgotten. Verifying
    needs DB polling against a plain serve process — not expressible purely over HTTP.
    """
    ...


@pytest.mark.skip(reason="blocked: no per-document delete route (whole-tenant delete only)")
def test_per_document_erasure_dsar(api):
    """A single document can be erased (blobs/chunks/embeddings/search) on request.

    BLOCKED: there is no ``DELETE /api/documents/{id}`` — only whole-tenant delete.
    Per-record erasure (DSAR / mistaken-PII upload) is unreachable.
    """
    ...


@pytest.mark.skip(reason="blocked: no data-export route + worker never runs")
def test_tenant_data_export_portability(api):
    """A tenant can export all their data (originals + jsonl + manifest).

    BLOCKED: a full export pipeline exists but has no UI/API entry point and no
    worker dispatch (GDPR Art. 20 portability / exit clause).
    """
    ...


@pytest.mark.skip(reason="blocked: crypto-shred unrecoverability needs DB/blob inspection")
def test_crypto_shred_unrecoverable_across_index_and_blobs(api):
    """After tenant delete, search index + chunks + embeddings + blob ciphertext are gone.

    BLOCKED: the existing shred test only asserts 'login fails'. Proving
    unrecoverability requires direct DB/blob inspection (the GDPR-erasure claim rests
    on this), not black-box reads (RLS/login already block those).
    """
    ...


@pytest.mark.skip(reason="blocked: residency not settable per-tenant + not enforced (ingest hardcodes local_only=false)")
def test_data_residency_local_only_enforced(api):
    """A local_only tenant's content never reaches a remote provider.

    BLOCKED: no residency column/route to set it, and the real ingest path hardcodes
    ``local_only: false`` — the primitive is unwired. Would need a recording sink +
    a settable flag.
    """
    ...


@pytest.mark.skip(reason="blocked: no retention-policy / auto-delete feature exists")
def test_retention_policy_auto_deletes_expired_data(api):
    """Data older than the configured retention period is auto-deleted; nothing earlier.

    BLOCKED: no tenant-configurable retention anywhere (only backup retention + a
    static /retention marketing page that promises a lifecycle that isn't enforced).
    """
    ...


@pytest.mark.skip(reason="blocked: no legal-hold concept exists")
def test_legal_hold_blocks_deletion(api):
    """A tenant/document under legal hold cannot be deleted or auto-purged until released.

    BLOCKED: no legal_hold anywhere; it directly conflicts with crypto-shred-on-delete
    (litigation-hold is a hard requirement for regulated buyers).
    """
    ...


@pytest.mark.skip(reason="blocked: no operator key-rotation entry point")
def test_key_rotation_backward_readable(api):
    """After a KEK rotation, old data still decrypts and DEKs are re-wrapped under the new KEK.

    BLOCKED: re-wrap exists only at startup KEK-swap; there is no runtime rotation
    route/admin action (SOC2/ISO key-rotation control).
    """
    ...


@pytest.mark.skip(reason="blocked: BYOK / customer-managed keys not offered (single global KEK)")
def test_byok_customer_managed_keys(api):
    """A tenant supplies/controls its own KEK and can revoke to render data unreadable.

    BLOCKED: single shared KEK wrapping per-tenant DEKs by design; a common
    security-questionnaire item.
    """
    ...


@pytest.mark.skip(reason="blocked: no PII detection/redaction or classification routing")
def test_pii_document_defaults_to_local_only(api):
    """An identity/PII-heavy document defaults to local-only routing / redaction flagging.

    BLOCKED: only a ``DocKind::IdentityDocument`` enum exists — no detection,
    redaction, or residency enforcement to back it.
    """
    ...
