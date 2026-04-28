---
status: Accepted (action plan in flight)
date: 2026-04-28
authors: research subagent + main thread review
context: v1.9.0-rc1, prod test on customer LAN
---

# Security Audit & Improvement Plan

## Executive summary

The repo demonstrates **defence-in-depth posture in pockets** — path-resolution + localhost gate on `list-dir`/`open-folder`, regex-validated PowerShell argv in SMB session kill, hash-chained audit log, SHA-256 verification on snapshot restore, two-person approval framework, whitelist SQL guard, no `shell=True`, no hardcoded secrets, all user data parameter-bound — but has **one structural weakness that dominates the threat model**: the FastAPI dashboard binds to `0.0.0.0:8085` with **no authentication, no CORS guard, no security headers, and many state-mutating POST endpoints lacking confirm/safety-token gates**.

Combined with stored-XSS-amenable rendering (`innerHTML = ${untrusted}` over file/source names) and an **insecure default for `archiving.dry_run` (false)**, an unauthenticated attacker on the LAN can move/quarantine/restore arbitrary files. Every other finding is downstream.

**Net rating before fixes**: High risk in shared/LAN deployments; acceptable in single-host RDP-only deployments.

## Findings

### Critical

| ID | Title | Effort |
|---|---|---|
| C-1 | Unauthenticated, network-bound dashboard | 6h |
| C-2 | `archiving.dry_run: false` shipped as default | 2h |

### High

| ID | Title | Effort |
|---|---|---|
| H-1 | Stored XSS via file/source/owner names rendered with `innerHTML` | 8h |
| H-2 | Two-person approval bypass when `identity_source: client_supplied` | 2h |
| H-3 | Audit chain bypassable via non-chained insert APIs (FIXED) | 4h |

### Medium

| ID | Title | Effort |
|---|---|---|
| M-1 | Restore silently skips integrity check when manifest sha256 empty | 0.5h |
| M-2 | SQL admin panel `query_panel.enabled: true` by default | 0.5h |
| M-3 | Snapshot restore needs only `confirm: true`, no safety_token | 1h |
| M-4 | CodeQL uses default queries + `continue-on-error: true` | 0.5h |

### Low / Info

L-1 config-only SQL/DuckDB f-string interpolation (no user input today, document + test).
L-2 dead `create_windows_task` schtasks `/TR` builder — delete or guard.
L-3 audit-report.html innerHTML XSS — covered by H-1 sweep.
I-1 README/SECURITY.md hardening note for LAN deployments.
I-2 Notification email resolved server-side via AD — safe.
I-3 SQL f-string in `database.py` (~25 sites) structurally safe — `?` binds user values.
I-4 No SSRF surface (telemetry + AD lookups only, config-derived endpoints).
I-5 No CORS — same-origin policy is the only defence and sufficient for LAN deployment model.

## Improvement plan

### Phase 1 — this week (Critical + High, ~22h)

1. **C-1 + C-2 as one PR**: bearer-middleware (`FILEACTIVITY_DASHBOARD_TOKEN` env), default-bind 127.0.0.1 (require explicit `--bind 0.0.0.0`), flip `archiving.dry_run: true` default, require `confirm: true` on `/api/archive/run` and `/api/archive/selective`.
2. **H-1 + L-3**: `escapeHtml()` helper sweep across `index.html` + `audit-report.html`, CSP middleware.
3. **H-2**: refuse boot when `approvals.enabled=true` AND `identity_source=client_supplied`.
4. **H-3** (FIXED — issue #158): the public `insert_audit_event` / `insert_audit_event_simple` now auto-route to `insert_audit_event_chained` when `audit.chain_enabled` is true; the raw-INSERT variants are renamed `_insert_audit_event_unchained` / `_insert_audit_event_simple_unchained` and guarded by CI test `tests/test_audit_chain_no_unchained_callsites.py`. Existing call-sites (scanner / archiver / dashboard / retention) require zero changes. Routing covered by `tests/test_audit_chain_routing.py`.

### Phase 2 — next sprint (Medium, ~4h)

5. M-1: hard-fail empty manifest sha256.
6. M-2: default `query_panel.enabled: false`.
7. M-3: add `safety_token: "RESTORE"` body field to snapshot restore.
8. M-4: `queries: security-extended,security-and-quality` + flip `continue-on-error: false` after first triage cycle.

### Phase 3 — backlog (Low + Info)

9. L-1: comments + reachability test on f-string SQL/DuckDB sites.
10. L-2: delete dead schtasks builder or add regex guard.
11. I-1: SECURITY.md hardening note.
12. I-3: `# noqa: S608` annotations to suppress CodeQL false positives in `database.py`.

## Won't fix (rationale)

- **No CORS configuration**: adding CORS on intentionally-LAN-only API enlarges surface; same-origin policy is correct.
- **No CSRF tokens**: once C-1 lands (bearer-token gate), CSRF is structurally prevented because the browser cannot read the token.
- **localStorage usage**: limited to view-toggle state and ETA history; no PII / credentials.
- **Username in logs**: operationally required; not PII at rest in admin-only log files.
- **Path traversal in `list-dir` without a jail**: localhost gate + realpath + `follow_symlinks=False` is the design. A jail would break legitimate UNC-path picking.

## CodeQL integration plan (24-48h after first scan)

**Day 0 (scan completes)**:
Triage the Security tab. Expected categories: `py/unsafe-html-construction` (matches H-1), `py/sql-injection-via-format` (false positives — see I-3), `js/xss-through-dom` (matches H-1/L-3), `py/command-line-injection` (matches L-2 if it lights up).

**Day 1**: open sub-issues per finding under the matching §3 Phase. Critical/High CodeQL findings preempt Phase 2 items. Dismiss the `database.py` f-string findings as **Won't fix → False positive** with comment linking I-3.

**Day 2**: update workflow:
- `queries: security-extended,security-and-quality`
- Keep `continue-on-error: true` until baseline is empty.

**Sprint cadence**: once baseline is at zero open Critical/High, flip `continue-on-error: false`. Weekly Monday scan feeds 30-min triage slot. Aged-14d findings escalate to `security` label.

## References

- ADR `docs/architecture/storage-decision-2026-04-28.md` — companion decision.
- Hash-chain audit log: issue #38, `src/storage/database.py::insert_audit_event_chained`.
- Two-person approval framework: PR #115 / issue #112.
- Snapshot restore SHA verification: PR #106 / issue #77 Phase 2.
- Quarantine triple-gate (PURGE / QUARANTINE token): PR #109 / issue #83.
