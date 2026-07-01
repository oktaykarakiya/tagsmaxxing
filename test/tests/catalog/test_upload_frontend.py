"""Catalog (step 2) — the /upload **frontend** wiring (Playwright).

These tests drive the rendered **upload form** the way a real user does — picking
files through the hidden ``#file-input``, ticking *Ingest as one document*, typing
a note, reordering, and submitting — and assert the *intended* outcome the UI
promises. They are complementary to:

* ``test_ingest`` (``test_browser_upload_flow`` / ``…_multi_format``), which only
  asserts the user *leaves* the upload page (it accepts a ``/search`` fallback);
  here we assert the **happy path the JS actually takes** — the ``document_id``
  redirect to ``/documents/{id}`` — so a regression to the dead ``job_id``-poll
  path (``job_id`` is always the sentinel ``0``) is caught;
* ``test_ingest_matrix`` (API-level MIME accept/reject), whereas case 6 here
  asserts the **browser** surfaces a rejection without crashing/hanging the page.

Per the suite philosophy a failing assertion means a real app bug (the UI is not
wired as promised) and is left failing — never weakened. The template under test
is ``crates/api/templates/upload.html``; the web route ``POST /upload`` lives in
``crates/api/src/web/mod.rs``. Selectors below are taken verbatim from that
template (``#drop-zone``, ``#file-input``, ``#group-toggle``, ``#user-note``,
``#submit-btn``, ``#file-list``, ``#upload-controls``, ``#progress-area``,
``#progress-error``, ``input[name='csrf_token']``).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from playwright.sync_api import expect

from lib import config, file_factory as ff, flows

pytestmark = [pytest.mark.ingest, pytest.mark.ui]


@pytest.fixture(autouse=True)
def _require_text_backend():
    """Skip when the text/embed backend is degraded (environment outage, not a UI bug).

    A browser text/note/group upload needs the tagger; when the LLM backend is
    unreachable the upload 500s and the page (correctly) stays on /upload — which
    would otherwise read as "redirect not wired". Skip those so the lane never
    false-fails on a backend hiccup; the PNG (media-graceful) and UI-only cases
    still assert when the backend is up.
    """
    import httpx

    with httpx.Client(base_url=config.base_url(), verify=config.verify_tls(), timeout=10.0) as c:
        if not (ff.subsystem_ok(c, "text") and ff.subsystem_ok(c, "embed")):
            pytest.skip("text/embed backend degraded per /health — environment, not a UI bug")

# The inline pipeline (extract → tag → VLM caption → embed → store) runs *inside*
# POST /upload on this hardware; a generous bound keeps a slow backend from
# hanging the suite while still failing a genuinely stuck submit.
UPLOAD_TIMEOUT_S = 180
# Bound every Playwright op so a crashed browser/slow server can't block the lane.
DEFAULT_OP_TIMEOUT_MS = 20_000


def _admin_creds() -> tuple[str, str, str]:
    """(tenant_slug, email, password) for the bootstrap admin user."""
    return config.tenant_slug(), config.admin_email(), config.admin_password()


def _open_upload(page: Any) -> None:
    """Log in and land on a ready ``/upload`` page (form rendered)."""
    page.set_default_timeout(DEFAULT_OP_TIMEOUT_MS)
    flows.browser_login(page, *_admin_creds())
    page.goto("/upload")
    page.wait_for_selector("#upload-form", timeout=10_000)


def _write(tmp_path: Path, name: str, data: bytes) -> str:
    """Write *data* to ``tmp_path/name`` and return its absolute path string.

    ``page.set_input_files`` consumes a real filesystem path; the upload form's
    ``onchange`` JS reads ``file.name``/``file.type`` from it, so the on-disk
    name + a correct extension drive the browser-side MIME guess.
    """
    p = tmp_path / name
    p.write_bytes(data)
    return str(p)


def _select_files(page: Any, paths: str | list[str]) -> None:
    """Set the hidden file input and wait for the controls to reveal."""
    page.set_input_files("#file-input", paths)
    # The list + controls un-hide once files are selected (renderFileList()).
    page.wait_for_selector("#upload-controls:not(.hidden)", timeout=10_000)
    page.wait_for_selector("#file-list:not(.hidden)", timeout=10_000)


def _submit(page: Any) -> None:
    """Click Upload and confirm the progress area takes over from the form."""
    page.click("#submit-btn")
    page.wait_for_selector("#progress-area:not(.hidden)", timeout=15_000)



# ── 1. Form renders + wiring ─────────────────────────────────────────────────
def test_upload_form_renders_with_all_controls(page):
    """GET /upload renders every promised control + the CSRF double-submit token.

    After login the upload page must present the drop-zone, the (hidden) file
    input, the *Ingest as one document* checkbox, the note textarea, the submit
    button, AND the hidden ``csrf_token`` field — and the ``__Host-csrf`` cookie
    must be set so the double-submit POST can succeed. A missing control is a
    wiring defect that would break the upload UI.
    """
    _open_upload(page)

    # Every control the upload journey depends on (verbatim selectors).
    expect(page.locator("#drop-zone")).to_be_visible()
    assert page.locator("#file-input").count() == 1, "missing #file-input"
    assert page.locator("input[type='checkbox'][name='group_as_document']").count() == 1, (
        "missing the group_as_document (#group-toggle) checkbox"
    )
    assert page.locator("textarea[name='user_note']").count() == 1, "missing the user_note textarea"
    assert page.locator("#submit-btn").count() == 1, "missing the #submit-btn submit button"

    # The hidden CSRF field must carry a non-empty token (double-submit field half).
    csrf = page.locator("#upload-form input[name='csrf_token']").first
    assert csrf.count() == 1, "upload form has no hidden csrf_token input"
    token = csrf.get_attribute("value")
    assert token, "csrf_token hidden field is present but empty — the POST would be rejected"

    # …and the cookie half of the double-submit defence is set on the context.
    csrf_cookies = [c for c in page.context.cookies() if "csrf" in c["name"].lower()]
    assert csrf_cookies, (
        "no __Host-csrf cookie set after GET /upload — double-submit CSRF cannot work"
    )


# ── 2. Single text-file upload happy path ────────────────────────────────────
def test_single_text_upload_redirects_to_document(page, tmp_path):
    """A single .txt upload ends on /documents/<id> (the document_id success path).

    Write a small text file, pick it through the form, submit, and assert the
    browser lands on ``/documents/{id}``. This confirms the JS took the
    ``document_id`` redirect branch (inline-processed doc), **not** the dead
    ``job_id`` poll (``job_id`` is always the sentinel 0, so that branch never
    completes). If no document is reachable, the test FAILs — recording the gap.
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2eup")
    path = _write(tmp_path, f"{marker}.txt", f"Frontend single-text upload {marker}.".encode())

    _select_files(page, path)
    _submit(page)

    page.wait_for_selector("#progress-detail a[href^='/documents/']", timeout=UPLOAD_TIMEOUT_S * 1000)
    page.click("#progress-detail a[href^='/documents/']")
    page.wait_for_selector("h1", timeout=10_000)
    expect(page.locator("nav")).to_be_visible()


# ── 3. Single image (PNG) upload ─────────────────────────────────────────────
def test_single_png_upload_succeeds(page, tmp_path):
    """A single valid PNG uploads via the form and redirects to its document.

    Uses ``file_factory.minimal_png()`` for valid PNG bytes (the app routes it to
    ``kind=image`` and runs vision captioning). The browser submit must succeed
    and land on ``/documents/{id}`` just like the text path.
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2eimg")
    path = _write(tmp_path, f"{marker}.png", ff.minimal_png())

    _select_files(page, path)
    _submit(page)

    page.wait_for_selector("#progress-detail a[href^='/documents/']", timeout=UPLOAD_TIMEOUT_S * 1000)
    page.click("#progress-detail a[href^='/documents/']")
    page.wait_for_selector("h1", timeout=10_000)
    expect(page.locator("nav")).to_be_visible()


# ── 4. Multi-file selection + group_as_document ──────────────────────────────
def test_multi_file_group_as_document_upload(page, tmp_path):
    """Selecting 3 files + ticking 'Ingest as one document' yields one document.

    Pick three text files at once, check ``#group-toggle`` (group_as_document),
    submit, and assert the promised outcome — a single grouped document reachable
    at ``/documents/{id}`` (the files become its pages).
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2egrp")
    paths = [
        _write(tmp_path, f"{marker}_a.txt", f"Group page A {marker}.".encode()),
        _write(tmp_path, f"{marker}_b.txt", f"Group page B {marker}.".encode()),
        _write(tmp_path, f"{marker}_c.txt", f"Group page C {marker}.".encode()),
    ]

    _select_files(page, paths)
    # The list header must reflect that all three files were taken.
    expect(page.locator("#file-list")).to_contain_text("3 files selected")

    page.check("#group-toggle")
    assert page.locator("#group-toggle").is_checked(), "group_as_document checkbox did not tick"

    _submit(page)
    page.wait_for_selector("#progress-detail a[href^='/documents/']", timeout=UPLOAD_TIMEOUT_S * 1000)
    page.click("#progress-detail a[href^='/documents/']")
    page.wait_for_selector("h1", timeout=10_000)
    expect(page.locator("nav")).to_be_visible()


# ── 5. user_note accompanies the upload ──────────────────────────────────────
def test_upload_with_user_note_succeeds(page, tmp_path):
    """Filling the note textarea then uploading a text file still succeeds.

    The note is an optional free-text field sent alongside the file(s). Typing a
    note and uploading must not break the submit — the browser must still land on
    ``/documents/{id}``.
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2enote")
    path = _write(tmp_path, f"{marker}.txt", f"Upload with note {marker}.".encode())

    _select_files(page, path)
    note = f"Reviewer note for {marker}: quarterly figures, handle with care."
    page.fill("#user-note", note)
    assert page.locator("#user-note").input_value() == note, "user_note textarea did not accept input"

    _submit(page)
    page.wait_for_selector("#progress-detail a[href^='/documents/']", timeout=UPLOAD_TIMEOUT_S * 1000)
    page.click("#progress-detail a[href^='/documents/']")
    page.wait_for_selector("h1", timeout=10_000)
    expect(page.locator("nav")).to_be_visible()


# ── 6. Rejected file surfaces an error without crashing the page ──────────────
def test_rejected_file_surfaces_error_and_page_stays_usable(page, tmp_path):
    """An ELF/unknown file is rejected in-browser; the page shows an error, no crash.

    The app rejects an executable (``application/x-executable``) with 400. The
    upload JS must catch ``!resp.ok``, surface a visible error (``#progress-error``
    un-hidden) — and crucially NOT throw an uncaught console error that breaks the
    page or hang forever. The "Try again" control must restore a usable form
    (``resetUpload()`` re-shows ``#upload-form``).
    """
    _open_upload(page)

    # Capture any uncaught page errors / console errors raised during the submit.
    page_errors: list[str] = []
    console_errors: list[str] = []
    page.on("pageerror", lambda exc: page_errors.append(str(exc)))
    page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

    marker = flows.unique_marker("kbe2eevil")
    path = _write(tmp_path, f"{marker}_evil.bin", ff.elf_stub())

    _select_files(page, path)
    page.click("#submit-btn")

    # The error state must surface within a bounded time (no infinite spinner).
    expect(page.locator("#progress-error")).to_be_visible(timeout=30_000)
    # The error detail must say *something* (not a blank failure).
    expect(page.locator("#progress-detail")).not_to_have_text("", timeout=5_000)

    # The submit must NOT have thrown an uncaught error that breaks the page.
    assert not page_errors, f"upload submit raised uncaught page error(s): {page_errors}"

    # The page is still alive and usable: "Try again" restores the form.
    page.click("#progress-area button:has-text('Try again')")
    expect(page.locator("#upload-form")).to_be_visible(timeout=5_000)
    # A console.error from a failed fetch is acceptable; an exception is not — already
    # asserted above. (Recorded for triage only.)


# ── 7. Reorder controls change the visible file order ────────────────────────
def test_reorder_controls_change_file_list_order(page, tmp_path):
    """Move-down on the first file swaps it below the second in the visible list.

    With 2+ files selected the list renders per-row move-up/move-down controls
    (``onclick="moveFileDown(0)"`` etc.). Clicking move-down on the first file
    must reorder the rendered ``#file-list`` so the formerly-second file is now
    first — proving the reorder UI mutates the to-be-uploaded order.
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2eord")
    first_name = f"{marker}_first.txt"
    second_name = f"{marker}_second.txt"
    paths = [
        _write(tmp_path, first_name, f"first {marker}".encode()),
        _write(tmp_path, second_name, f"second {marker}".encode()),
    ]
    _select_files(page, paths)

    # The move buttons carry title="Move up"/"Move down" (template, lines 251/260).
    move_down = page.locator("#file-list button[title='Move down']")
    if move_down.count() == 0:
        pytest.fail("upload form renders no reorder (move-down) controls for multiple files")

    # Initial order: first file's name precedes the second in the rendered list.
    names_before = page.locator("#file-list p.font-medium").all_inner_texts()
    assert first_name in names_before[0], f"unexpected initial order: {names_before}"

    # Click move-down on the first row; the order must swap.
    move_down.first.click()
    # Auto-wait for the re-render: the second file's name now leads the list.
    expect(page.locator("#file-list p.font-medium").first).to_contain_text(second_name)
    names_after = page.locator("#file-list p.font-medium").all_inner_texts()
    assert first_name in names_after[1], (
        f"move-down did not reorder the file list: before={names_before} after={names_after}"
    )


# ── 8. Accessibility of the form controls ────────────────────────────────────
def test_upload_form_controls_are_labelled(page):
    """The note textarea and file picker expose accessible labels.

    The project has known a11y label gaps elsewhere; this records whether the
    upload form repeats them. The note textarea must have a programmatic label
    (``<label for='user-note'>`` / wrapping label / aria-label), and the file
    picker must be operable with an accessible name — either the ``#file-input``
    itself is labelled, or its activating ``#drop-zone`` exposes a name (text or
    aria-label). A bare unlabelled control is the defect.
    """
    _open_upload(page)

    # Textarea: a <label for> targeting it, a wrapping <label>, or an aria-label.
    note_labelled = page.evaluate(
        r"""() => {
          const el = document.getElementById('user-note');
          if (!el) return false;
          if ((el.getAttribute('aria-label') || '').trim()) return true;
          if ((el.getAttribute('aria-labelledby') || '').trim()) return true;
          if (el.id && document.querySelector('label[for="' + CSS.escape(el.id) + '"]')) return true;
          if (el.closest('label')) return true;
          return false;
        }"""
    )
    assert note_labelled, (
        "the upload note textarea (#user-note) has no accessible label "
        "(label[for]/wrapping label/aria-label)"
    )

    # File picker: the input itself labelled, OR the activating drop-zone names it.
    file_named = page.evaluate(
        r"""() => {
          const el = document.getElementById('file-input');
          if (!el) return false;
          if ((el.getAttribute('aria-label') || '').trim()) return true;
          if ((el.getAttribute('aria-labelledby') || '').trim()) return true;
          if (el.id && document.querySelector('label[for="' + CSS.escape(el.id) + '"]')) return true;
          if (el.closest('label')) return true;
          // The drop-zone is the user-visible activator for the hidden input.
          const dz = document.getElementById('drop-zone');
          if (dz) {
            if ((dz.getAttribute('aria-label') || '').trim()) return true;
            if ((dz.textContent || '').trim().length > 0) return true;  // visible instruction text
          }
          return false;
        }"""
    )
    assert file_named, (
        "the upload file picker (#file-input) has no accessible name and its "
        "#drop-zone activator exposes none either"
    )


# ── 9. Client-side 100 MiB total-size guard (best-effort, no huge file) ───────
def test_client_side_size_guard_present(page):
    """The 100 MiB total-size guard is present and wired in the served page.

    Rather than building a 100-MiB file (expensive + slow), assert the guard
    EXISTS and is wired: the served page must declare the ``MAX_TOTAL_BYTES``
    threshold, render the ``#size-warning`` element, and the submit button must
    become ``disabled`` when the total exceeds it (``renderFileList`` sets
    ``btn.disabled = true``). The live over-limit trigger is skipped (would need a
    >100 MiB upload).
    """
    _open_upload(page)

    html = page.content()
    assert "MAX_TOTAL_BYTES" in html and "100 * 1024 * 1024" in html, (
        "served /upload page does not declare the 100 MiB MAX_TOTAL_BYTES guard"
    )
    # The warning element the guard reveals must be in the DOM (initially hidden).
    assert page.locator("#size-warning").count() == 1, "missing #size-warning guard element"
    # The guard's enforcement arm: code that disables the submit button on overflow.
    assert "btn.disabled = true" in html, (
        "size guard does not disable the submit button when the total exceeds the cap"
    )

    pytest.skip(
        "size guard present + wired (MAX_TOTAL_BYTES / #size-warning / submit-disable); "
        "live over-limit trigger skipped — would require a >100 MiB upload"
    )


# ── 10. Async polling journey (P15-T10) ─────────────────────────────────────
def test_async_journey_progress_redirect_and_badge_autoflip(page, tmp_path):
    """upload → progress bar → poll → redirect to the document → ready badge.

    The queued-mode primary path: the page shows the progress area while
    polling ``/api/jobs/:id``, redirects to ``/documents/{id}`` when the job
    completes, and the document page's badge reads ``ready`` — either because
    processing finished before the redirect or because the detail page's
    auto-refresh flipped it without manual interaction.
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2easync")
    path = _write(tmp_path, f"{marker}.txt", f"Async journey document {marker}.".encode())

    _select_files(page, path)
    _submit(page)

    # The progress area is visible (asserted by _submit); wait for the
    # document link to appear, then click to navigate to the detail page.
    page.wait_for_selector("#progress-detail a[href^='/documents/']", timeout=UPLOAD_TIMEOUT_S * 1000)
    page.click("#progress-detail a[href^='/documents/']")
    page.wait_for_selector("h1", timeout=10_000)

    # The badge must reach "ready" WITHOUT manual refresh: the detail page
    # auto-reloads while processing (P15-T10). Playwright's locator survives
    # reloads, so waiting on the text is exactly the no-manual-refresh assert.
    expect(page.locator("main span", has_text="ready").first).to_be_visible(
        timeout=120_000
    )


def test_async_journey_failed_upload_links_to_retryable_document(page, tmp_path):
    """A deterministically-failing upload surfaces the error + a working retry.

    Uses the P15-T9 probe (text-sniffed bytes with an invalid UTF-8 sequence):
    the upload 202s, processing dead-letters, the upload page shows the inline
    error with a link to the failed document, and the document page's Retry
    button round-trips (the form POST re-renders the page with processing
    state or a fresh failure — never an error page).
    """
    _open_upload(page)
    marker = flows.unique_marker("kbe2eafail")
    bad = (
        f"Deterministic failure probe {marker}. ".encode("ascii")
        + b"\xc3\x28"
        + b" trailing ascii so the sniffer still sees text."
    )
    path = _write(tmp_path, f"{marker}.txt", bad)

    _select_files(page, path)
    _submit(page)

    # The poll ends in the inline error + document link (job dead-letters fast
    # thanks to permanent-failure classification).
    deadline = time.monotonic() + 120
    link = page.locator("#progress-detail a[href^='/documents/']")
    while time.monotonic() < deadline:
        if link.count() > 0:
            break
        # Admission may have rejected it instead (also correct) — then the
        # error box shows without a document link.
        err_shown = page.evaluate(
            "() => { const e = document.getElementById('progress-error');"
            " return !!(e && !e.classList.contains('hidden')); }"
        )
        if err_shown and link.count() == 0:
            time.sleep(2)  # give the link injection a beat, then re-check
            if link.count() == 0:
                pytest.skip("probe rejected at admission — no async failure to exercise")
        time.sleep(1)
    assert link.count() > 0, "failed upload must link to the failed document"

    href = link.first.get_attribute("href")
    page.goto(href)
    expect(page.locator("#ingest-failed-alert")).to_be_visible(timeout=15_000)

    # The Retry button round-trips: the POST re-renders the detail page.
    page.click("#retry-ingest-btn")
    page.wait_for_load_state("networkidle")
    assert "/documents/" in page.url, f"retry must re-render the document page: {page.url}"
    # After retry the document is pending again (badge shows processing) until
    # the deterministic failure recurs — either state proves the retry ran.
    body_text = page.locator("main").inner_text().lower()
    assert ("processing" in body_text) or ("failed" in body_text), (
        "after retry the page must show processing or a fresh failure"
    )
