# CLAUDE.md — FILE_ACTIVITY agent context

> **Read me first.** Compressed brief for any Claude Code session opened on this repo.
> Goal: skip the "what is this codebase" warmup. ROADMAP.md is the canonical roadmap;
> this file is the operator's mental model + agent playbook.

---

## ✅ Stabilization week (2026-04-28 → 2026-05-05) — CLOSED

[Issue #194](https://github.com/deepdarbe/file_activity/issues/194) closed
2026-05-20. The 7-day plan executed; the four-times-recurring WAL leak
chapter (#132/#174/#181/#185) is sealed by the per-query DuckDB conn
contract and the read-only pool. The followup audit lives in
[#29 comment 2026-05-22](https://github.com/deepdarbe/file_activity/issues/29) — read that first when resuming.

### Discipline retained from stabilization (now permanent)
- **`node --check` is mandatory** for any `index.html` edit (PR #193 regression
  was a JS parse error — would have been caught in 1 second). Wired via
  `.github/workflows/ci.yml` and `scripts/ci_guards.py`.
- **No parallel agents on the same file.** Wave-of-agents on `index.html` produced
  the JS regression. One agent per file per PR.
- **`scripts/ci_guards.py` (12 mechanical guards: D-YAML / S-YAML / LOADERS /
  HTML-BUDGET / D-CHAIN / SVC-PARITY / R-CACHE / A-AWAIT / C-CURSOR / P-PAGE /
  A-AUDIT / S-SHAPE)** runs on every PR. Adds belong here, not in ad-hoc PR diffs.
- **Customer interactions still benefit from the #194 log format** even though
  the week is over: `Customer msg / What tested / Outcome ✅⏳❌ / Action / Next`.

The stabilization decision came after an honest assessment: 35 PRs in one session
included **4 separate fixes for the same WAL leak root cause** (#132, #174, #181,
#185) and a JS regression that broke every menu (#193). The pattern of
"customer reports → emergency hotfix → next regression" was structural, not
incidental. Plan A worked — the codebase is in a more honest state now than
when the week began.

### Post-stabilization wave (2026-05-22)
- **PR #215** — event-loop starvation root-cause fix. 166 dashboard endpoints
  were `async def` calling sync DB code, blocking the FastAPI loop. Converted
  to plain `def` so Starlette dispatches to anyio worker threads. Customer's
  "every page is waiting" symptom maps to this.
- **PR #216** — `D-CHAIN` ci-guard plus 18 `document.getElementById(...).innerHTML =`
  call-sites migrated to `_setHtmlSafe`. Prevents the #200/#201/#202 null-deref
  class permanently. `INNERHTML_BUDGET` tightened 180 → 140.
- **PR #217** — `scripts/bench_storage.py` harness. Customer can run on their
  real 3.1M-row DB to settle "is DuckDB actually faster than SQLite?" — 10k
  synthetic rows already shows DuckDB 9–37× slower because ATTACH overhead
  (~50 ms/call) dominates.

---

## 🔖 SESSION HANDOFF — read this first when resuming (as of master `ec2d79c`, 2026-06-14)

The 2026-06-14 session was a **customer empty-page triage**, diagnosed **on the
prod box via bridge** (`burculogo` — 31 GB DB, 2.9M files, E:\ source). Three
"page comes up empty" reports → 3 root-caused fixes (PRs #293/#294/#295,
issues #290/#291/#292 all closed):
- **#291 Growth empty** → `scan_runs.total_size` COLUMN was 0 (written pre-enrich,
  never back-filled); growth reads `MAX(total_size)`. Fix: `compute_scan_summary`
  back-fills the column (#293). On-box: back-filled the 3 existing scans already,
  so growth works there NOW.
- **#292 PII empty** → feature off (`compliance.pii.enabled` absent → default
  false) + **no UI way to launch a scan**. Fix: "PII Tara" button + `background=true`
  scan mode (#294). On-box: enabled `compliance.pii.enabled: true` in
  `config\config.yaml` (the ACTIVE config — NOT root `config.yaml`).
- **#290 Duplicates empty** → endpoint tried DuckDB-ATTACH first; that
  GROUP-BY-over-2.9M-rows ran **>25 min / OOM-died**, so the request never
  returned. Fix: route through indexed SQLite + read totals from the summary +
  new index `idx_sf_scan_name_size` (#295). (Data was fine: 414k dup groups.)

**⚠️ DEPLOY STILL OWED for #290 + #292** — see NEXT SESSION #1. Growth (#291) is
already live on-box via the back-fill; duplicates (index build at first start) and
the PII button need the new code on the box.

**BRIDGE GOTCHA (burculogo):** heavy/long bridge-spawned **detached** child procs
get killed mid-op (NOT OOM — 109 GB RAM free). The dashboard's own process is
unaffected. Validate heavy things post-deploy, not via bridge probes.
**RUNTIME GOTCHA:** the dashboard runs as a **manual `python main.py … dashboard`**
(system Python 3.12, PID held 8085), NOT the NSSM service (Stopped). So plain
`update.cmd` (service-aware) updates files + starts the *service* → **8085 conflict**
and the manual python keeps serving OLD code. The manual python must be stopped &
restarted (or switch to the service) for new code to load.

---

The 2026-06-13 session was a **follow-up wave** that cleared the top four
NEXT-SESSION items left by the 2026-06-12 security+compliance wave — 5 PRs:
**#283** (held M1 list-dir auth → closes #278), **#285** (R-6 wave 3,
A_AUDIT_ALLOWLIST 31→23), **#286** (handoff refresh), **#287** (pytest floor
≥9.0.3 → last open advisory cleared), **#288** (treemap "Wasted %", punch-list
#3). The prior 2026-06-12 wave was a security + compliance hardening wave (9
PRs: #275 install.ps1, #277 CVE floors, #280+#276 R-6 audit waves, #281 STH
export, #282 secret-scrub, #270–#274 the R-5 guard series + docs, plus
Dependabots #264–#268), driven by an OSV scan + a defensive source audit + a
competitive-research punch list.

**▶ NEXT SESSION — start here:**
1. **Deploy #290/#292 to burculogo + verify all 3 fixes** (owed). Because the
   dashboard is a manual python (see RUNTIME GOTCHA above), the safe sequence is:
   stop the manual `python main.py … dashboard` proc(s), run `update.cmd` on the
   box (RDP — bridge can't reliably drive the 31 GB pre-update snapshot), then
   restart (service or manual). First start builds `idx_sf_scan_name_size`
   (~minutes, one-time). Then verify: Growth shows the size series; Kopya page
   loads (indexed SQLite); PII page shows the "PII Tara" button (config already
   enabled on-box) — optionally run a bounded PII scan. `config\config.yaml`
   already has `compliance.pii.enabled: true` (preserved across update).
2. **Research punch-list 2/4/5** (deep-research report, 2026-06-12; #3 shipped
   in #288): #2 Lynis-style hardening-index score, #4 Presidio PII
   context-boosting, #5 Sleuth Kit mactime timeline. All net-new features —
   each wants its own design pass.
3. **Customer on-box smoke** still owed from the 2026-06-12 wave (#262 CSV +
   Adlandırma, #263 PDQ option) — gated on `update.cmd` pulling current master.
4. **R-6 "later pass" candidates** (optional): the 23-entry allowlist is all
   justified, but `create_snapshot`, `duplicates_delete`, `duplicates_quarantine`,
   `notify_users_run_now`, `notifications_send_to` are real-ish actions that could
   be triaged next (verify whether the engine already audits them before emitting).
5. **Parquet-reports** (ADOPT, deferred); **PILOT** (ETW/Tantivy); **#114** ES
   (deferred until 500 GB / 200 M-row).

`git log --oneline -15` confirms the real tip.

### Where we are
- **master = `ec2d79c`**. ci_guards **12/12**; A-AUDIT allowlist = **23**. Per-PR CI:
  the usual non-blocking `Pytest (Linux, Docker)` flake (`continue-on-error`, the
  Docker image's `apt-get install` times out before pytest runs) — do NOT chase.
  NOTE: `CodeQL` now genuinely scans on PRs (it flagged real issues on #281 —
  fixed). Read CodeQL annotations; don't blanket-dismiss them as the old umbrella
  flake. **CodeQL triage tip**: to prove an alert is pre-existing vs PR-introduced,
  use the per-alert `/code-scanning/alerts/{n}/instances` endpoint, NOT the
  `?ref=refs/heads/master` filter (the ref filter matches `most_recent_instance`,
  which moves to the PR ref and hides master-side alerts).
- **Open PRs: #203 only** (old D2/DuckDB bundle, do NOT merge as-is). #293/#294/
  #295 merged 2026-06-14; #283/#285/#286/#287/#288 the day before.
- **Security posture**: dependency floors **fully clean vs OSV** — the last open
  advisory (dev-only pytest GHSA-6w46-j5rx-g56g) was cleared by #287
  (`pytest>=9.0.3,<10`); Dependabot auto-closes it on its next master rescan.
  Source audit found **no reachable Critical/High**. Both Mediums shipped
  (M2 = #282 secret-scrub, M1 = #283 list-dir auth). The 5 HIGH CodeQL
  `py/path-injection` alerts on the picker `realpath` sinks were dismissed
  `won't fix` (localhost-gated + #278 scope guard + symlink-escape-required;
  PR #283 strictly reduces exposure).

### What shipped 2026-06-14 (customer empty-page triage, diagnosed on-box via bridge)
- **#293** — **growth size series flat-zero** (#291). `compute_scan_summary` now
  back-fills `scan_runs.total_files`/`total_size` (written pre-enrich = 0, never
  updated) in the same UPDATE as `summary_json`. On-box: the 3 existing scans were
  back-filled directly (summary_json had the real 17.8 TB), so growth works there now.
- **#294** — **PII page unusable from UI** (#292). New "PII Tara" button (shown when
  `compliance.pii.enabled`) + `pii_scan(background=true)` daemon-thread mode so the
  multi-hour content scan doesn't block the request; findings appear incrementally.
  On-box: enabled in `config\config.yaml` (the ACTIVE config).
- **#295** — **duplicates report empty at scale** (#290). The DuckDB-ATTACH dup
  query ran >25 min / OOM-died on 2.9M rows. `duplicate_report` now uses indexed
  SQLite directly (no DuckDB attempt), reads group/waste TOTALS from the precomputed
  summary (identical definition) for `min_size==0`, and a new
  `idx_sf_scan_name_size (scan_id, file_name, file_size)` streams the paginated
  GROUP BY. SQLite proven to terminate (compute_scan_summary runs the same GROUP BY).
- Diagnosis method: on-box bridge reads (scan_runs columns, summary_json, indexes,
  config) + a faithful repro calling the real `Database`/`AnalyticsEngine` methods.
  See [[burculogo-prod-box-bridge]] for the box specs + bridge/runtime gotchas.

### What shipped 2026-06-13 (this follow-up session)
- **#287** — **pytest floor bump** `>=8.0,<9` → `>=9.0.3,<10` (dev-dep only),
  clearing GHSA-6w46-j5rx-g56g (tmpdir, MODERATE) — the last open advisory.
  Major bump validated by a differential suite run (8.4.2 vs 9.0.3: identical
  620 pass / 42 env-only fail, empty diff → no pytest-9 regressions), since CI
  can't fully vet it (Linux Docker job flakes at build; Windows job runs 2 files).
- **#288** — **Treemap "Wasted %"** (punch-list #3). Per-extension wasted % =
  stale (1+ yr unaccessed) byte share, same 365-day cutoff as the Overview
  stale KPI. `get_type_analysis` adds `stale_size`; `TypeAnalyzer` derives
  `wasted_pct`. New "Israf % (eski)" red-heatmap colour mode + tooltip row.
  `tests/test_treemap_wasted_pct.py` (4 cases). "Wasted" def = stale-only by
  operator choice (vs stale+dupes / configurable).
- **#286** — handoff doc refresh (this file).
- **#283** — **M1 list-dir auth** (closes #278). The held PR from the prior wave:
  scopes the folder-picker (`list-dir`/`open-folder`) so an UNAUTHENTICATED
  localhost caller is confined to configured source roots + their parents
  (`_path_within_source_scope`); authenticated admins keep the full picker
  (`DashboardAuth.has_valid_token` tells a real token from the localhost bypass).
  Rebased on master; the 5 pre-existing CodeQL `py/path-injection` HIGH alerts on
  the `realpath` sinks were dismissed `won't fix` with justification (proven
  pre-existing via the alert-instances endpoint), then squash-merged.
- **#285** — **R-6 wave 3**: 8 mutating endpoints now emit
  `insert_audit_event_simple` on the success path (status-guarded so no-op paths
  write no fake row): `run_scan` (scan_started), `run_archive` (archive_run),
  `drilldown_archive`, `chargeback_add/update/remove_center`,
  `chargeback_add/remove_owner`. Chargeback config is global → `source_id=None`.
  Allowlist **31→23**. Remaining 23 are all justified (approvals_* double-emit
  guard, analytics-compute, self-test, export, dry-run, no-DB-write).

### What shipped 2026-06-12 (the prior security+compliance wave)
- **#270/#271/#272** — R-5 guard series: **P-PAGE** (Rule 2), **A-AUDIT** (Rule 4),
  **S-SHAPE** (Rule 3) added to `scripts/ci_guards.py` (now 12 guards). **#273**
  hardened all three after a max-effort review live-repro'd 4 edge-case gaps
  (`.get('partial_summary_json')` bypass, dead-nested-def audit false-pass,
  `Annotated[PaginationParams]` false-positive, partial-migration false-negative).
- **#274** — docs refresh + streamlit floor bump (plotly-6 base64-JSON needs ≥1.42).
- **#276 / #280** — **R-6 audit-backlog flush waves 1 + 2**: 15 mutating endpoints
  now emit `insert_audit_event_simple` on the success path (snake_case event_type,
  status-guarded, try/except so audit failure never rolls back the mutation):
  legal_holds_*, retention_policy_*, quarantine_*, restore_*, bulk_restore,
  orphan_sid_reassign, acl_snapshot, archive_selective, archive_by_insight.
  Allowlist 46→31. **Review catch**: 3 `approvals_*` were NOT drained — the
  ApprovalRegistry._audit() already emits identical event_types; a second
  endpoint-level emit would write duplicate compliance-chain rows → kept
  allowlisted with a per-name justification.
- **#281** — **Signed Tree Head (STH) export** (Trillian/Rekor/CT pattern,
  research punch-list #1). `src/storage/audit_sth.py` + `scripts/audit_sth.py`
  (`--genkey/--emit/--verify [--check-chain]`): publishes a signed
  `{tree_size, root_hash, timestamp, signature, public_key}` checkpoint so an
  external auditor verifies the chain hasn't been rewritten with our published
  Ed25519 pubkey — turns "trust us" into "verify". Offline CLI op, no new endpoint;
  reads chain only. `cryptography>=41` now a real dep. Opt-in `audit.sth` config.
- **#282** — **value-level secret scrub** (hardening M2, issue #279). New shared
  `src/utils/secret_scrub.py`: masks secret SHAPES (PEM / `gh[pousr]_` / Slack /
  AWS AKIA / `user:pass@` URLs / conservative base64) regardless of key name, in
  BOTH the diag bundle and the error-reporter. Conservative catch-all (≥40 +
  mixed-case + digit) so SHA/MD5/UUID aren't over-masked. Composes on top of the
  existing key-name redaction.
- **#277** — **CVE floors**: `pyarrow>=23.0.1` (clears CRITICAL ACE
  GHSA-5wvp-7f3h-6wmm + 2 more) and `streamlit>=1.54.0` (clears MODERATE
  Windows-NTLM-SSRF GHSA-7p48-42j8-8846). OSV-verified clean.
- **#275** — thin `deploy/install.ps1` entry point with `-Branch` param +
  scriptblock-Create so a PR branch can be smoke-tested with one paste; `update.cmd`
  now calls it and remembers the install branch. Legacy one-liner still works.
- **Dependabot** #264 datasketch, #265 click, #266 mcp, #267 imagehash, #268 plotly
  5→6 (audited SAFE — playground px.* usage untouched by the 6.x breaks).

### What's PENDING (pick up here)
1. **Deploy #290/#292 to burculogo + verify the 3 empty-page fixes** (NEXT SESSION
   #1) — growth (#291) already live on-box via back-fill; duplicates + PII button
   need the new code. Mind the manual-python RUNTIME GOTCHA.
2. **Research punch-list 2/4/5** (Lynis index / Presidio PII / Sleuth Kit
   mactime — #3 shipped in #288; see NEXT SESSION #2).
3. **Customer on-box smoke** still owed from the prior wave (#262 CSV + Adlandırma,
   #263 PDQ option) — gated on `update.cmd` pulling current master.
4. **R-6 "later pass"** allowlist triage (NEXT SESSION #4) — optional.
5. **#114**, **#203**, **#29**. (Hardening #278/#279 both shipped: #283 + #282;
   pytest advisory cleared in #287.)

### The 8 endpoint-conventions rules — 7 of 8 auto-enforced (12 guards live)
`docs/standards/endpoint-conventions.md`. Auto: Rule 1 (R-CACHE), 2 (P-PAGE),
3 (S-SHAPE), 4 (A-AUDIT, allowlist now 23 after R-6 waves 1–3), 5 (A-AWAIT),
6 (C-CURSOR), 7 (D-CHAIN). Manual: Rule 8. New report endpoints use
`cached_report_endpoint`; pagination via `p: PaginationParams = Depends()`
(`Annotated` recognised); `async def` MUST await; reads `get_read_cursor`, writes
`get_cursor`; mutating endpoints emit audit events; summary reads via
`db.get_scan_summary` (S-SHAPE noqa = trailing comment, case-insensitive).

---

## What this project is

**Windows file-share analysis + archiving + compliance system.** Python 3.11, FastAPI dashboard on port 8085, SQLite (OLTP) + DuckDB (read-only analytics ATTACH).

Customer ships it as **source-only**: `setup-source.ps1` clones, builds venv, optionally installs as Windows service via NSSM. No EXE. `update.cmd` re-runs the installer. Data dir: `C:\FileActivity\data\`.

**Scale target**: multi-million-file shares. Customer's prod test as of 2026-04-28 was a **3.1M-file `E:\` NTFS volume** that was hammering every soft spot in the codebase.

---

## Operator workflow (the customer's hands)

```powershell
# Update path — this is the only path the operator uses day-to-day
.\update.cmd
# Service-aware as of #173 — auto-stops FileActivity service before cleanup,
# auto-restarts after. No manual Stop-Service needed anymore.

# Trigger scan
.\fa.cmd scan ortak

# OR via dashboard
http://localhost:8085 → Tara button on the source card
```

The **customer reports problems via screenshots + log paste**. Treat those as gold;
they catch issues no test suite does. Pattern: customer says "menüler boş", I trace
to a real architectural issue (long-lived DuckDB ATTACH blocking WAL truncation).

---

## Architecture map (where to look when X breaks)

| Symptom | Likely file | Why |
|---------|-------------|-----|
| Every dashboard page "waiting" / loading | `src/dashboard/api.py` (sync def, ~160 endpoints) | FastAPI event-loop starvation if a new endpoint is added as `async def` while making sync DB calls — see #215. Default to plain `def`. |
| Scan emits 0 files on real NTFS | `src/scanner/backends/_ntfs_records.py` | FRN sequence-number masking (lower 48 bits) — see #164/#165 |
| Scan aborts mid-run with `database is locked` | `src/storage/database.py::bulk_insert_scanned_files` | 5× retry with 1/2/4/8/16s backoff (#176) |
| WAL stuck at 13+ GB, won't truncate | `src/storage/analytics.py::AnalyticsEngine` | Per-query DuckDB conn (#185/#186) — long-lived ATTACH was the leak |
| Dashboard menus empty during scan | `src/storage/database.py::get_read_cursor` + `partial_summary_v2` | Read-only pool (#184) + v2 schema (#183) + frontend partial-data (#182) |
| `BOYUT: 0 B` on every file | `src/scanner/size_enricher.py` | MFT enum is path-only by design; size enrich pass runs after walk (#179) |
| Büyüme (growth) page empty / size flat-zero | `src/storage/database.py::compute_scan_summary` + `get_growth_stats` | growth reads `MAX(scan_runs.total_size)`; that COLUMN is written pre-enrich (=0). compute_scan_summary back-fills it (#293/#291). Existing scans need a one-time back-fill from `summary_json`. |
| Kopya dosyalar (duplicates) page empty at scale | `src/dashboard/api.py::duplicate_report` + `get_duplicate_groups` | DuckDB-ATTACH dup query hangs/OOMs on millions of rows; use indexed SQLite (`idx_sf_scan_name_size`) + summary totals (#295/#290). Data is usually fine — it's a timeout. |
| PII bulgular page empty | `config\config.yaml` `compliance.pii.enabled` + `src/dashboard/api.py::pii_scan` | feature defaults OFF; findings only exist after a manual `pii_scan` (content read). UI trigger = "PII Tara" button, `background=true` (#294/#292). NOTE: app uses `config\config.yaml`, NOT root `config.yaml`. |
| `update.cmd` fails on locked `bin\nssm.exe` | `deploy/setup-source.ps1` | Service-aware Stop/Start wrapper (#172/#173) |
| Dashboard menu disappears / `TypeError: null.innerHTML` | `src/dashboard/static/index.html` | Use `_setHtmlSafe(id, html)` helper, never `document.getElementById(...).innerHTML =`. Enforced by `D-CHAIN` (#216). |

---

## Concurrency model (memorize this)

This is the codebase's most consequential design. Get it wrong and the customer's scan stalls.

```
Writer connection pool (Database.get_conn / get_cursor):
  - Thread-local sqlite3.connect, busy_timeout=60s
  - Used ONLY by scanner, scheduler, write endpoints
  - bulk_insert_scanned_files retries on 'database is locked'

Read-only pool (Database.get_read_cursor):
  - Per-call sqlite3.connect(?mode=ro&cache=shared, uri=True)
  - Used by ALL dashboard read endpoints (24+ migrated in #184)
  - Independent of writer; never contends; releases between calls

DuckDB analytics (AnalyticsEngine._cursor):
  - Per-query DuckDB :memory: conn that ATTACHes SQLite, runs query, closes
  - DO NOT hold a long-lived ATTACH — it shows up to SQLite as a permanent
    reader and blocks wal_checkpoint(TRUNCATE) → WAL grows unbounded
  - Pinned by tests/test_analytics_per_query.py
```

**The trap that bit this codebase four times** (#132 / #174 / #181 / #185):
A long-lived reader (any kind — dashboard handle, DuckDB ATTACH, scheduler probe)
prevents `wal_checkpoint(TRUNCATE)` from shrinking the WAL. Symptom is always
"WAL grows to N GB and never shrinks". Fix is always "make the reader short-lived".
If you see that symptom in a future session, look for the new long-lived reader.
Background read on the failure mode: https://loke.dev/blog/sqlite-checkpoint-starvation-wal-growth

---

## Storage decision (closed)

`docs/architecture/storage-decision-2026-04-28.md` — **stay on SQLite**, do not
migrate to PostgreSQL/ClickHouse. Reopen only if customer crosses 500 GB / 200 M
rows / sub-minute RTO. ElasticsearchBackend exists as opt-in (#114 Phase 1+2);
Phase 3-5 (dashboard query layer rewrite + migration tool) is **deliberately deferred**.

---

## Test discipline

```bash
# Linux dev box (this is what runs in CI for "Pytest Linux Docker"):
python -m pytest tests/ --ignore=tests/test_elasticsearch_backend.py -q

# Expected baseline (post 2026-04-28 wave): ~654 passed, 7 skipped, ~5 pre-existing failures
# Pre-existing failures (do NOT panic, do NOT fix unless asked):
#   - tests/test_image_hash.py × 3   → imagehash/Pillow not in test container
#   - tests/test_mft_progress.py × 4 → NtfsMftBackend.__init__ kwarg drift, low priority
#   - tests/test_button_audit.py     → was duplicate _esc, fixed in #187
```

CI infrastructure: `docker/Dockerfile.test` + `docker-compose.test.yml` + `scripts/run-tests.sh`.
Run `./scripts/run-tests.sh` locally for the same environment. CI's `Pytest (Linux, Docker)`
job has `continue-on-error: true` until the master baseline is empty for 3 runs.

---

## Wave / delegation playbook

This codebase has been built mostly by waves of parallel agents. The pattern that works:

1. **Customer feedback** (screenshot or log paste) →
2. **Open an issue** with severity, root cause, fix plan (no PR-without-issue) →
3. **Decide owner**:
   - Trivial / one file → main thread
   - New module with clear interface → worktree subagent (`isolation: worktree`)
   - UI page following existing pattern → GitHub Copilot (`assign_copilot_to_issue`)
   - Architecturally significant (e.g. #114 Phase 3) → main thread, daylight, slow
4. **Subagent prompt MUST include**: branch name, exact file paths, retry/test discipline, "open PR via mcp__github__create_pull_request when done"
5. **Merge order**: foundations first (DB schema, protocols), then features. Conflicts in `index.html` are routine (multiple agents touch sidebar / loaders dict) — resolution rule: keep both sides, merge entries into the combined dict.

**Rebase-on-master before merge** if the branch is older than the latest merge — every
subagent today opens against an older base because they take 5-30 min and master moves.

---

## CI flake reality (don't chase ghosts)

Two checks flake on PRs even when master is green:

- **`Pytest (Linux, Docker)`** — pip-install timeouts on the GHA runner before pytest
  collects. Diagnosis in #91. Docker test infra (#188) addresses the chronic part.
  Job has `continue-on-error: true` until 3 master runs are green back-to-back.
- **`CodeQL` (umbrella)** — distinct from `Analyze (python)` / `Analyze (javascript)`
  which actually scan. The umbrella check inherits master-side Dependabot advisories;
  it can fail on a PR while the analysis jobs pass. Same advisory has been on master
  through #214/#215/#216/#217. Don't re-diagnose per PR.

**Don't keep diagnosing the same flake every PR**; verify locally and merge if
local + master are green.

---

## What's open (as of 2026-05-22 post-stabilization audit)

Code-tracking issues:
- **#29** — EPIC roadmap tracker (pinned, never closed). 2026-05-22 audit comment
  is the latest punch list — read it before starting any architectural work.
- **#114** — Pluggable storage Phase 3-5. Deliberately deferred. Phase 1+2 shipped
  (#121, #167); Phase 3 = dashboard query layer rewrite (~30 endpoints).
  Architecturally significant — wait for the #217 bench result before committing.

Dependabot queue: **all 10 merged 2026-05-22** (#204-#211, #9, #10). pillow→12
and elasticsearch→9 were sub-agent-audited SAFE before merge. Queue is empty.

- **#225** — Endpoint-conventions refactor EPIC. R-1..R-5 ALL shipped
  (R-5c #270 / R-5e #271 / R-5d #272 + hardening #273). **R-6 substantially done**:
  audit-backlog flush drained `A_AUDIT_ALLOWLIST` 46→23 across waves 1 (#276),
  2 (#280), 3 (#285). The remaining 23 are all justified; only an optional
  "later pass" of a few real-ish actions (create_snapshot, duplicates_*,
  notify_*) is left.
- **#203** — user's own April-30 bundle, still open. Do NOT merge as-is (D2
  DuckDB removal conflicts with keep-DuckDB + #231/#232). Triage comment posted.

Closed-this-wave issues whose context might still be referenced: #14, #20, #80, #81, #83,
#91, #112, #132, #165, #166, #172, #174, #175, #177, #181, #185, #193–#202, #212, #213,
#194 (stabilization tracker), #215–#239 (the 22-PR perf+refactor wave), #221 (drilldown table).

---

## Hard rules

- **NEVER force-push master.** PR-and-**squash**-merge only — never a plain merge
  commit, for every PR (operator preference, confirmed 2026-05-26). Squash keeps
  master a clean linear history (one commit per PR) and avoids re-introducing
  already-squashed branch commits into the graph. **Whenever a merge decision
  comes up, proactively tell the operator which method is correct and why before
  merging** — don't make them ask.
- **NEVER skip git hooks** (`--no-verify`) without explicit user permission.
- **NEVER mass-overwrite the customer's `config.yaml`.** `setup-source.ps1` preserves user
  config on update. New config keys default to safe values in code; document in `config.yaml`
  comments but don't clobber existing customer values.
- **Every write endpoint emits an audit event** via `Database.insert_audit_event_simple`
  (chain-routed when `audit.chain_enabled: true` per #160). No exceptions.
- **Read endpoints use `db.get_read_cursor()`**. Write endpoints use `db.get_cursor()`.
  If you mix in one with-block, you taint the whole call site → use `get_cursor()`.

---

## Endpoint conventions (the eight rules)

Long-form: [`docs/standards/endpoint-conventions.md`](docs/standards/endpoint-conventions.md).
Read that first if you're adding ANY new endpoint or refactoring an existing one.
The standard exists because between 2026-04-23 and 2026-05-22 about a third of
all PRs were re-fixing the same bug class in a different endpoint — shape
mismatch, missing cache, async/sync drift, null-deref. The eight rules below
make those bug classes impossible to ship.

1. **Cached reports** — every report endpoint that iterates >100k rows uses
   `cached_report_endpoint(...)` from `src/dashboard/_endpoint_helpers.py`.
   Prevents PR #224 (mit_naming uncached).
2. **Pagination** — `PaginationParams = Depends()` everywhere. No more
   `(page, limit)` / `(page, page_size)` / `(offset, limit)` drift.
3. **Summary shape** — read `summary_json` only through `db.get_scan_summary()`,
   which calls `normalize_summary()` to merge the dict and list shapes that
   `partial_summary_v2` and `compute_scan_summary` write. Prevents PR #198 / #223.
4. **Audit events** — every POST/DELETE/PUT/PATCH calls `insert_audit_event_simple`
   on success. Exceptions go to the allowlist with reviewer sign-off.
5. **async def only with await** — if the body never awaits, it's plain `def`
   so FastAPI dispatches it to the thread pool. Prevents PR #215.
6. **Connection pools** — read endpoints `get_read_cursor()`, write endpoints
   `get_cursor()`. Never mixed in one `with` block. Prevents the four-times-
   recurring WAL leak (#132 / #174 / #181 / #185).
7. **No chained innerHTML** — `_setHtmlSafe('id', html)` or stored-ref with
   explicit null-check. Enforced by `D-CHAIN` in `scripts/ci_guards.py`.
   Prevents PR #200 / #201 / #202.
8. **Config-gated features surface their gate** — show the exact
   `config.yaml` key/value in the UI when a feature is off. Prevents the
   2026-05-22 "(Bilinmiyor)" confusion. Manual review item.

CI guards `R-CACHE` / `P-PAGE` / `S-SHAPE` / `A-AUDIT` / `A-AWAIT` /
`C-CURSOR` enforce rules 1–6 mechanically (planned in the standard doc's
adoption table). `D-CHAIN` already enforces rule 7. Rule 8 stays manual.

---

## Useful incantations

```bash
# Find the FRN-mask test (the canonical example of "small fix, big consequence"):
grep -rn "_FRN_SEGMENT_MASK" src/ tests/

# Inspect WAL pressure live during a customer scan:
ls -la C:\FileActivity\data\file_activity.db-wal
# (>500 MB during scan = normal; >5 GB sustained after scan = leak; investigate readers)

# Run the SQLite-vs-DuckDB benchmark on the customer's real DB (#217):
python scripts/bench_storage.py --db C:\FileActivity\data\file_activity.db
# Settles "is DuckDB worth it?" empirically. Tested 10k synthetic rows → DuckDB 9-37x slower.

# Run every CI guard locally before pushing (12 guards: D-YAML / S-YAML / LOADERS /
# HTML-BUDGET / D-CHAIN / SVC-PARITY / R-CACHE / A-AWAIT / C-CURSOR / P-PAGE / A-AUDIT / S-SHAPE):
python scripts/ci_guards.py

# After a customer reports a problem, ALWAYS get:
#   1. The version from the dashboard footer (e.g. v1.9.0-rc1+30fd8a9)
#   2. The last 50–100 lines of C:\FileActivity\logs\file_activity.log
#   3. The dashboard screenshot (the page they were on)
# Triangulate version → commit → behaviour. Don't guess.
```

---

## Session continuity

If you're resuming a session:
1. `git log --oneline -20` to see what landed since the last reference point in this file
2. `mcp__github__list_issues state=OPEN` to see the live backlog
3. Check `~/.claude/plans/` for any plan files from a prior session that didn't ship
4. ROADMAP.md "Recent wave" section is updated at end of every major wave
