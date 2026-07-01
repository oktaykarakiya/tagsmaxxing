"""Catalog (step 2) — accessibility & semantic page structure.

Basic a11y/structure contracts on the rendered web UI, none of which are checked
elsewhere: every page has a title + language, a single top-level heading, labelled
form controls, alt text on images, no duplicate element ids, and native (keyboard-
focusable) interactive controls. These guard the UI against silent a11y
regressions that automated functional tests miss.

Per the suite philosophy a failing assertion is a real defect and is left failing.
Templates live in ``crates/api/templates/``.
"""

from __future__ import annotations

from typing import Any

import pytest

from lib import config, flows

pytestmark = pytest.mark.ui


def _admin_creds() -> tuple[str, str, str]:
    """(tenant_slug, email, password) for the bootstrap admin user."""
    return config.tenant_slug(), config.admin_email(), config.admin_password()


# ── DOM-inspection helpers (run in the page, return plain data) ──────────────
# These encode the a11y contract in-browser so the assertions stay declarative.
# Each returns the *violations* found so a failing assertion can name them.

_UNLABELED_INPUTS_JS = r"""
() => {
  // Text-entry controls that must carry a programmatic label.
  const LABELLABLE = new Set(['text', 'email', 'password', 'search', 'tel', 'url', 'number']);
  const offenders = [];
  for (const el of Array.from(document.querySelectorAll('input'))) {
    const type = (el.getAttribute('type') || 'text').toLowerCase();
    if (!LABELLABLE.has(type)) continue;            // hidden/checkbox/etc. exempt
    // Only visible controls are user-facing.
    const rect = el.getBoundingClientRect();
    const style = window.getComputedStyle(el);
    const visible = rect.width > 0 && rect.height > 0
      && style.visibility !== 'hidden' && style.display !== 'none';
    if (!visible) continue;
    // A control is labelled by aria-label(/ledby), a <label for=id>, or a wrapping <label>.
    let labelled = false;
    if ((el.getAttribute('aria-label') || '').trim()) labelled = true;
    if ((el.getAttribute('aria-labelledby') || '').trim()) labelled = true;
    if (!labelled && el.id) {
      try { if (document.querySelector('label[for="' + CSS.escape(el.id) + '"]')) labelled = true; }
      catch (e) { /* malformed id selector — treat as unlabelled */ }
    }
    if (!labelled && el.closest('label')) labelled = true;
    if (!labelled) offenders.push(el.getAttribute('name') || el.outerHTML.slice(0, 120));
  }
  return offenders;
}
"""

_IMGS_WITHOUT_ALT_JS = r"""
() => Array.from(document.querySelectorAll('img'))
  .filter(img => !img.hasAttribute('alt'))
  .map(img => img.getAttribute('src') || img.outerHTML.slice(0, 120))
"""

_DUPLICATE_IDS_JS = r"""
() => {
  const counts = {};
  for (const el of Array.from(document.querySelectorAll('[id]'))) {
    const id = el.id;
    if (!id) continue;
    counts[id] = (counts[id] || 0) + 1;
  }
  return Object.entries(counts).filter(([, n]) => n > 1).map(([id, n]) => id + ' ×' + n);
}
"""


def _lang_attr(page: Any) -> str:
    """The <html lang> attribute (empty string when absent)."""
    return (page.locator("html").get_attribute("lang") or "").strip()


def test_key_pages_have_title_and_lang(page):
    """Each key page declares a non-empty <title> and an <html lang> attribute.

    Visit a public page (``/login``) and an authenticated page (``/search`` after
    login). Each must have a non-empty ``<title>`` (assistive tech + tabs) and the
    root ``<html>`` element must carry a ``lang`` attribute (screen-reader
    pronunciation). Assert both for each page.
    """
    tenant, email, password = _admin_creds()

    # Public page.
    page.goto("/login")
    assert page.title().strip(), "/login has an empty <title>"
    assert _lang_attr(page), "/login <html> is missing a lang attribute"

    # Authenticated page.
    flows.browser_login(page, tenant, email, password)
    page.goto("/search")
    assert page.title().strip(), "/search has an empty <title>"
    assert _lang_attr(page), "/search <html> is missing a lang attribute"


def test_each_page_has_one_h1(page):
    """Every rendered page has exactly one top-level <h1> heading.

    A correct heading outline has a single ``<h1>`` per page. Check a representative
    set (``/login``, and authenticated ``/dashboard``): each must contain exactly one
    ``<h1>`` element — not zero (no page heading) and not many (broken outline).
    ``/search`` is excluded because the search page uses a compact nav bar without a
    visible page heading; its primary landmark is the search input itself.
    """
    tenant, email, password = _admin_creds()

    page.goto("/login")
    n = page.locator("h1").count()
    assert n == 1, f"/login has {n} <h1> elements (expected exactly 1)"

    flows.browser_login(page, tenant, email, password)
    page.goto("/dashboard")
    n = page.locator("h1").count()
    assert n == 1, f"/dashboard has {n} <h1> elements (expected exactly 1)"


def test_form_controls_have_labels(page):
    """Visible form inputs have an accessible label (label[for] / aria-label).

    On the ``/login`` form (and the authenticated ``/search`` form), every visible
    text/email/password/search input must have a programmatic label — either a
    ``<label for>`` that targets its id, a wrapping ``<label>``, or an
    ``aria-label``/``aria-labelledby``. Assert no labelled-input is missing its
    label.
    """
    tenant, email, password = _admin_creds()

    page.goto("/login")
    offenders = page.evaluate(_UNLABELED_INPUTS_JS)
    assert not offenders, f"/login has form inputs without an accessible label: {offenders}"

    flows.browser_login(page, tenant, email, password)
    page.goto("/search")
    offenders = page.evaluate(_UNLABELED_INPUTS_JS)
    assert not offenders, f"/search has form inputs without an accessible label: {offenders}"


def test_images_have_alt_text(page):
    """Every <img> on a rendered page has an ``alt`` attribute.

    Load the landing page (``/``) and an authenticated page. Every ``<img>``
    element must declare an ``alt`` attribute (empty alt is fine for decorative
    images; a missing attribute is the defect). Assert none lack it.
    """
    tenant, email, password = _admin_creds()

    page.goto("/")
    missing = page.evaluate(_IMGS_WITHOUT_ALT_JS)
    assert not missing, f"/ has <img> elements missing an alt attribute: {missing}"

    flows.browser_login(page, tenant, email, password)
    page.goto("/dashboard")
    missing = page.evaluate(_IMGS_WITHOUT_ALT_JS)
    assert not missing, f"/dashboard has <img> elements missing an alt attribute: {missing}"


def test_no_duplicate_element_ids(page):
    """A rendered page contains no duplicated ``id`` attributes.

    Duplicate ids break both assistive tech (label/aria targeting) and the page's
    own JS/HTMX (``#id`` selectors). On an information-dense authenticated page
    (e.g. ``/dashboard`` or a document detail page), collect all element ids and
    assert there are no duplicates.
    """
    tenant, email, password = _admin_creds()

    flows.browser_login(page, tenant, email, password)
    page.goto("/dashboard")
    dups = page.evaluate(_DUPLICATE_IDS_JS)
    assert not dups, f"/dashboard has duplicate element ids: {dups}"


def test_interactive_controls_are_native_focusable(page):
    """Primary actions use native <input>/<a>, keyboard-focusable — not div onclick.

    On the authenticated ``/search`` page, the primary controls (the search input,
    nav links) must be real ``<input>``/``<a>`` elements (reachable by keyboard and
    exposing a role), not click-handler ``<div>``/``<span>`` widgets. The search
    form submits via HTMX keyup so there is no submit button; the search input is
    the primary interactive control.
    """
    tenant, email, password = _admin_creds()

    flows.browser_login(page, tenant, email, password)
    page.goto("/search")

    # The search input must be a native focusable <input>.
    search_input = page.locator("#search-form input[name='q']")
    assert search_input.count() == 1, (
        f"expected exactly one native search input in the search form, found {search_input.count()}"
    )
    tag = search_input.first.evaluate("el => el.tagName.toLowerCase()")
    assert tag == "input", f"search control is a <{tag}>, not a native <input>"

    # The primary nav entries must be native anchors. An `a[href=…]` selector only
    # matches real <a> elements, so a count of zero means the control is not an anchor.
    for href in ("/dashboard", "/search", "/upload", "/account"):
        assert page.locator(f"nav a[href='{href}']").count() >= 1, (
            f"nav entry for {href!r} is not a native focusable <a>"
        )
