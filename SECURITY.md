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

## Production deployment

The dashboard is designed for trusted operator use. The defaults assume a
single-host RDP-only deployment; LAN exposure requires opt-in hardening.
The audit notes in `docs/architecture/security-audit-2026-04-28.md` give
the full rationale per setting.

- **Default bind 127.0.0.1 + bearer token**. Do not expose the dashboard
  to a network without first enabling the bearer-token middleware. See
  the operator runbook for the exact env var and rotation cadence
  (`docs/runbooks/dashboard-token.md`).
- **LAN exposure procedure**: start with `--bind 0.0.0.0` *and* export
  `FILEACTIVITY_DASHBOARD_TOKEN=<secret>` in the same shell. Without the
  token the server refuses to bind to a non-loopback interface.
- **Audit chain**: set `audit.chain_enabled: true` so every audit row is
  cryptographically linked to the previous row. Tamper detection is
  available only when this is on.
- **Approval framework**: set `approvals.identity_source: 'windows'` (AD
  / SSPI) or `'header'` (reverse-proxy injected). Never use
  `'client_supplied'` outside development — it lets the same operator
  approve their own request.

## Safe harbor

Good-faith security research is welcome. If you follow the policy above
(private disclosure, no data exfiltration, no disruption of others'
deployments), we will not pursue any legal action.
