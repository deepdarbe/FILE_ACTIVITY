# Security Policy

## Supported versions

Only the latest release on `master` is supported. Security fixes are applied
forward — we do not backport to older tags.

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Preferred channel:

- Private GitHub Security Advisory:
  https://github.com/deepdarbe/FILE_ACTIVITY/security/advisories/new

Include in the report:

- Impact and severity assessment (what an attacker can do)
- Steps to reproduce or a minimal PoC
- Affected versions / commit SHAs
- Any proposed mitigation

## Response timeline

- **Acknowledgement**: within 3 business days
- **Initial status update**: within 7 days
- **Public disclosure**: coordinated with you after a fix is released

## Scope

In scope:

- Command / shell injection in API endpoints or CLI
- Path traversal in archive / restore / export endpoints
- SSRF in any HTTP caller
- Archive integrity issues (silent data corruption, checksum bypass)
- Authentication / authorization flaws (when auth is added)
- Dependency vulnerabilities in pinned packages

Out of scope:

- Theoretical vulnerabilities without a working PoC
- Upstream CVEs in third-party dependencies already tracked by Dependabot
- Self-XSS requiring the victim to paste attacker-supplied input
- Attacks requiring physical access to the Windows host
- Attacks against shares the tool is configured to scan (those are the
  domain of the underlying Windows ACL / SMB configuration)

## Safe harbor

Good-faith security research is welcome. If you follow the policy above
(private disclosure, no data exfiltration, no disruption of others'
deployments), we will not pursue any legal action.
