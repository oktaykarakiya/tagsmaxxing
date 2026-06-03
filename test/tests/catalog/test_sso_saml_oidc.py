"""Catalog — SSO & Identity Federation (SAML / OIDC / SCIM). ENTERPRISE.

A code audit confirms the entire domain is **absent**: no SAML/OIDC/OAuth/SCIM
code, schema, or dependencies (parked as deferred ``P13``). Password-only auth is
the sole login path. SSO/SCIM is the #1 enterprise procurement requirement, so the
catalog must record the full suite that will be needed once it is built.

Every test here is **BLOCKED** (the underlying feature does not exist and cannot be
driven black-box — there are no ACS/metadata/SCIM endpoints to call). They are
recorded so the gap is visible and so the swarm does not try to "implement" tests
for nonexistent endpoints. Promote a test to a ``step2:`` reason once the
corresponding endpoint exists.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.sso

_BLOCKED = "blocked: SSO/SAML/OIDC/SCIM not implemented (deferred P13) — drive once the endpoint exists"


@pytest.mark.skip(reason=_BLOCKED)
def test_sso_enforcement_blocks_password_login():
    """For a tenant requiring SSO, password login is rejected (must use the IdP).

    The core enterprise control: if password login still works when SSO is required,
    IdP-side MFA/conditional-access/offboarding are all bypassable.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_saml_assertion_signature_validated():
    """A SAML Response with an invalid/absent/wrong-cert or tampered assertion is rejected.

    A signature-validation flaw (XSW, unsigned-assertion acceptance) is full auth
    bypass / tenant takeover.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_saml_cross_tenant_assertion_rejected():
    """A validly-signed assertion for tenant A cannot authenticate into tenant B.

    Federation must not become a cross-tenant bypass (Issuer/Audience/Destination
    bound to the tenant).
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_saml_replay_and_expiry_rejected():
    """A re-POSTed (replayed) or NotOnOrAfter-expired assertion is rejected.

    Without replay/expiry checks a captured assertion is a reusable credential.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_scim_deprovision_revokes_access_immediately():
    """SCIM DELETE/active=false disables the user AND kills live sessions + API tokens.

    Offboarding is a SOC2/termination must; a deprovision that leaves sessions valid
    is an audit failure.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_scim_token_authn_and_tenant_scoping():
    """SCIM requires a valid per-tenant bearer token and cannot touch another tenant.

    A mis-scoped SCIM endpoint is a cross-tenant user-management hole.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_jit_provisioning_and_stable_nameid_mapping():
    """First SSO login auto-creates the user; a stable NameID links (not duplicates) on re-login.

    Unstable NameID mapping → duplicate accounts + broken deprovisioning.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_idp_group_to_role_mapping_least_privilege():
    """IdP group claims map to roles; an unmapped group yields least privilege, never admin.

    Crafted group claims must not escalate to admin-by-default.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_oidc_login_validates_state_nonce_pkce_idtoken():
    """OIDC code login validates state (CSRF), nonce (replay), id_token sig/iss/aud/exp, PKCE.

    Missing state/nonce checks = CSRF / replay login.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_sp_and_idp_initiated_login_with_safe_relaystate():
    """Both SP- and IdP-initiated flows authenticate; RelayState cannot be an open redirect."""
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_sp_metadata_and_idp_cert_rotation():
    """SP metadata is well-formed; rotating the IdP cert takes effect with no restart.

    Cert rollover is routine; brittle rotation causes auth outages.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_single_logout_propagates():
    """SP- or IdP-initiated logout terminates the app session; it cannot be reused.

    Today logout is local-only; SLO is a compliance expectation.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_domain_capture_requires_verification():
    """A tenant can auto-join users by email domain only after verifying the domain.

    Unverified domain capture lets one tenant hijack another company's users.
    """
    ...


@pytest.mark.skip(reason=_BLOCKED)
def test_scim_group_provisioning_and_conformance():
    """SCIM Groups + membership sync work, and endpoints meet RFC 7644 (filter/paginate/idempotent).

    Okta/Entra clients fail provisioning silently if SCIM conformance is off.
    """
    ...
