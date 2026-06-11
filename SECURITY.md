# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in the Local File Knowledge Base, please
**do not** open a public GitHub issue. Instead, report it privately to the Maintainer:

- **Email:** oktaykarakiya@protonmail.com
- **Subject:** `[SECURITY] Local File Knowledge Base — brief description`

## What to include

- A description of the vulnerability and its impact
- Steps to reproduce, including affected versions and configurations
- Any proof-of-concept code or screenshots (if available)
- Whether you believe the issue is publicly exploitable

## Response timeline

| Phase | Expected |
|-------|----------|
| Acknowledgment | Within 72 hours |
| Initial assessment | Within 5 business days |
| Fix released | Within 30 days (critical: within 7 days) |

The Maintainer will keep you informed of progress and will credit you in the release
notes (unless you prefer to remain anonymous).

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` branch (latest) | ✅ |
| Tagged releases | ✅ |
| Older commits | ❌ |

## Scope

This policy covers the Local File Knowledge Base application code and its first-party
workspace crates (`kb-*`). It does not cover:

- Third-party dependencies (please report those upstream)
- Model weights (these are not distributed by this project)
- Infrastructure / deployment configurations specific to any operator

## Disclosure policy

The Maintainer follows **coordinated disclosure**: the fix is released before the
vulnerability is publicly discussed. Once a fix is available, a security advisory
will be published on GitHub.

## Hall of Fame

Contributors who responsibly disclose validated vulnerabilities will be listed here
(with permission).

---

*No bug bounty program is currently offered. This may change in the future.*
