"""Catalog — tenant jobs list API (P15-T10).

``GET /api/jobs`` lists the caller's own jobs (most recent first) with an
``active`` count of in-flight ingest work (the nav indicator's feed). The
jobs table sits outside RLS, so the tenant-scoping negative here guards the
endpoint's explicit tenant filter.

List with `pytest --collect-only -m ingest`.
"""

from __future__ import annotations

import pytest

from lib import config, flows
from lib import file_factory as ff
from lib.api_client import ApiClient

pytestmark = pytest.mark.ingest


def _login_admin(api):
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    return api


def test_jobs_list_shape_and_contains_own_upload(api):
    """The list returns {jobs, active}; a fresh upload's job appears in it."""
    _login_admin(api)
    marker = flows.unique_marker("jobslist")
    out = ff.upload(api._c, [(f"{marker}.txt", f"jobs list probe {marker}".encode(), "text/plain")])
    assert out["status"] == 202, f"upload failed: {out['status']}"
    job_id = (out["body"] or {}).get("job_id")
    assert job_id, "202 must carry job_id"

    r = api._c.get("/api/jobs?limit=100")
    assert r.status_code == 200, f"jobs list failed: {r.status_code} {r.text[:200]}"
    body = r.json()
    assert isinstance(body.get("jobs"), list), f"missing jobs array: {body}"
    assert isinstance(body.get("active"), int), f"missing active count: {body}"
    ids = [j["id"] for j in body["jobs"]]
    assert job_id in ids, f"own upload's job {job_id} missing from the list: {ids[:20]}"
    mine = next(j for j in body["jobs"] if j["id"] == job_id)
    for field in ("status", "attempts", "created_at", "document_id"):
        assert field in mine, f"job entry missing {field}: {mine}"

    # Settle the queue so later tests start clean.
    flows.wait_until_ready(api, (out["body"] or {})["document_id"], job_id=job_id)


def test_jobs_list_status_filter_and_invalid_status(api):
    """status= filters to that status only; an unknown status is a clean 400."""
    _login_admin(api)
    r = api._c.get("/api/jobs?status=done&limit=50")
    assert r.status_code == 200
    assert all(j["status"] == "done" for j in r.json()["jobs"]), "filter leak"

    r = api._c.get("/api/jobs?status=bogus")
    assert r.status_code == 400, f"unknown status must 400, got {r.status_code}"
    assert r.json().get("error") == "invalid_status"


def test_jobs_list_is_tenant_scoped(api, page):
    """§31.5 negative: another tenant's list never contains my jobs."""
    _login_admin(api)
    marker = flows.unique_marker("jobsiso")
    out = ff.upload(api._c, [(f"{marker}.txt", f"isolation probe {marker}".encode(), "text/plain")])
    assert out["status"] == 202
    my_job = (out["body"] or {})["job_id"]

    # Fresh tenant via the web signup form.
    name = flows.unique_marker("jobsintr")
    password = "Intruder-Passw0rd!42"
    page.goto("/signup")
    page.fill("input[name='tenant_name']", name)
    page.fill("input[name='email']", f"{name}@e2e.local")
    page.fill("input[name='password']", password)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    intruder = ApiClient()
    intruder.login(name, f"{name}@e2e.local", password)
    try:
        r = intruder._c.get("/api/jobs?limit=100")
        assert r.status_code == 200
        body = r.json()
        foreign_ids = [j["id"] for j in body["jobs"]]
        assert my_job not in foreign_ids, (
            f"tenant isolation violated: my job {my_job} visible to another tenant"
        )
        # A brand-new tenant has no in-flight ingest work.
        assert body["active"] == 0, f"fresh tenant active count must be 0: {body['active']}"
    finally:
        intruder.close()

    flows.wait_until_ready(api, (out["body"] or {})["document_id"], job_id=my_job)
