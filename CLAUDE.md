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
- **`scripts/ci_guards.py` (D-YAML / S-YAML / LOADERS / HTML-BUDGET / D-CHAIN /
  SVC-PARITY)** runs on every PR. Adds belong here, not in ad-hoc PR diffs.
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

## 🔖 SESSION HANDOFF — read this first when resuming (as of master `09c668c`, 2026-05-25)

The 2026-05-24/25 session shipped **3 PRs** (#241–#243) + a 4-area "advanced
tech" research pass, on top of the 2026-05-22 22-PR wave (kept below for
history). Current state:

### Where we are
- **master = `09c668c`**. Per-PR CI: 7/8 green; the only red is
  `Pytest (Linux, Docker)` — the documented non-blocking flake (dies in ~20s
  during docker build, `continue-on-error: true`). Do NOT chase it.
- **No open PRs from this session's *merged* work.** 2 NEW subagent PRs are
  IN FLIGHT (below). #203 still open (do NOT merge as-is — D2 DuckDB removal
  conflicts with keep-DuckDB).

### What shipped THIS session (#241–#243)
- **#241** — AI Insights **"İncele"** rerouted to the #222 Excel drilldown
  overlay (loading state + server pagination + sort + Konuma-Git column; #222
  had upgraded the *Overview* drilldown, never Insights). + **PII page** wired
  to `/api/compliance/pii/findings` + Rule-8 banner (was a stub that wiped the
  page). + `scripts/onenote_export.py` (Markdown→OneNote via Graph). Customer
  confirmed İncele works.
- **#242** — **PII checksum validators** (`src/compliance/pii/validators.py`):
  credit_card→Luhn, iban→mod-97, tckn→TC-kimlik (python-stdnum), phone→
  libphonenumber. Post-filter in `PiiEngine.scan_file`; no-ops without the
  optional deps (requirements-accel.txt). + fixed `iban_tr` regex (4→5 groups;
  the old form never matched a real 26-char TR IBAN).
- **#243** — **orphan-SID report on by default** via a `config_migrations.yaml`
  rule (false→true). update.cmd flips preserved configs; no-op vs shipped
  config. Domain-joined → SIDs resolve via pywin32 LookupAccountSid, no LDAP.

### IN FLIGHT — 2 subagent PRs (check open PRs + CI on resume)
- **SQLite FTS5 search** (`claude/fts5-search`) — trigram FTS over
  file_path/name/owner + dashboard search endpoint + box. Zero new dep.
- **USN detection signals** (`claude/usn-detection`) — blocking USN read +
  ENCRYPTION_CHANGE/DATA_TRUNCATION burst signals into ransomware_detector
  (activates the empty Anomaly page). Zero new dep.
- **Trust-but-verify:** review each diff + CI before merging (built by
  subagents, not main-thread-tested).

### What shipped this wave (the 22 PRs)
- **Perf / caching**: #224 (mit_naming), #227 (report_full + report_status),
  #228 (mit_naming_files), #231 (drilldown XLSX filter-aware), #239
  (report_export + mit_naming_export), #232 (pre-warm cache at scan complete
  → first dashboard click is instant, not a 3-30s cold-cache wait).
- **Perf / DB**: #230 — three composite indexes `(scan_id, extension)`,
  `(scan_id, owner)`, `(scan_id, file_size)`. Removes temp-B-tree GROUP/ORDER
  on the hot reports; cold compute ~30s → ~3s on the 2.89M-row DB. They
  auto-build on next `update.cmd` restart (one-time 2-5 min).
- **Refactor (EPIC #225)**: #233 R-1 `src/dashboard/_endpoint_helpers.py`
  (`cached_report_endpoint` + `PaginationParams`); #234 R-2 migrated
  types/sizes/status/mit_naming to the helper; #235 R-3
  `src/storage/_summary_compat.py::normalize_summary` wired into
  `db.get_scan_summary` (kills the dict-vs-list shape-mismatch bug class
  #198/#223); #236 R-4 stripped the dual-shape branch from report_frequency.
- **CI guards (6 → 9)**: #237 added **R-CACHE** (Rule 1) + **A-AWAIT** (Rule 5);
  #238 added **C-CURSOR** (Rule 6). Each has an allowlist with justifications
  in `scripts/ci_guards.py`. Self-tests in `tests/test_ci_guards.py` (23 tests).
- **UX**: #222 — AI Insights drilldown is now a dense Excel-style table
  (sort / multi-select / folder column / Tablo↔Kart toggle). Closed issue #221.
- **Diagnostics**: #229 — `scripts/bench_api.py` (HTTP p50/p95/p99 + cache-hit)
  and `scripts/explain_audit.py` (EXPLAIN QUERY PLAN red-flag finder).
- **Dependabot**: 10 merged (#204-#211, #9, #10) incl. major bumps pillow 12
  and elasticsearch 9 — both sub-agent-audited as SAFE before merge.

### What's PENDING (pick up here)
1. **#8/#9 — THE remaining real customer bug.** "Kopya Dosyalar" + "Adlandırma
   Uyumu" pages stick on "yükleniyor", no data. Root cause found:
   `loadDuplicates` / `loadNaming` (index.html) **swallow errors**
   (`catch{ console.error }`) → permanent spinner; backend is either just SLOW
   (cold compute on 2.89M rows) or ERRORING on the real DB. BLOCKED on the
   customer's diagnostic (rescan-test / `file_activity.log` tail / F12-Network
   status). Fix = (a) surface errors in those loaders, (b) fix the real backend
   cause — likely the Parquet-export approach in item 2.
2. **Advanced-tech research roadmap (this session — ADOPT/PILOT/SKIP):**
   - **Analytics@scale (→#8/#9):** ADOPT **Parquet export (pyarrow) at
     scan-complete + DuckDB/Polars query the Parquet** (cold GROUP BY ~30s→<1s,
     **WAL-safe**). ❌ **REJECT "persistent DuckDB ATTACH"** — it IS the
     4×-fixed WAL-leak anti-pattern (pinned by `test_analytics_per_query.py`).
     chDB=SKIP (no Windows wheel).
   - **Windows monitoring (→#2):** USN signals (in flight). PILOT ETW FileIO
     via raw ctypes; WATCH FSRM (Server-SKU only); SKIP MiniFilter (kernel sign).
   - **Near-dup:** ADOPT MinHash+LSH (`datasketch`) + PDQ (`pdqhash`).
   - **Search:** ADOPT SQLite FTS5 (in flight); Tantivy=PILOT; DuckDB-FTS=WATCH.
   - **PR order:** FTS5 (in flight) → USN (in flight) → Parquet-reports (after
     the #8/#9 diagnostic) → MinHash+PDQ.
3. **Customer activation guide** given for config-gated features (PII /
   wrong-ext+`python-magic-bin` / image-hash / AD) = their config.yaml +
   `pip install -r requirements-accel.txt` + rescan. **puremagic REJECTED for
   wrong-ext** (can't detect executables → would miss the disguise case).
4. **EPIC #225 leftovers** (R-5c P-PAGE / R-5d S-SHAPE / R-5e A-AUDIT / R-6),
   **#203** decision, **#114** storage Phase 3-5 — all still deferred.

### The 8 endpoint-conventions rules now have CI teeth
`docs/standards/endpoint-conventions.md`. Auto-enforced: Rule 1 (R-CACHE),
Rule 5 (A-AWAIT), Rule 6 (C-CURSOR), Rule 7 (D-CHAIN). Manual/pending:
Rules 2/3/4/8. New report endpoints MUST use `cached_report_endpoint`;
new `async def` MUST await; reads use `get_read_cursor`, writes `get_cursor`.

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

- **#225** — Endpoint-conventions refactor EPIC. R-1..R-4 + R-5a/b shipped;
  R-5c (`P-PAGE`), R-5d (`S-SHAPE`), R-5e (`A-AUDIT`), R-6 (audit backlog)
  still open. One PR each.
- **#203** — user's own April-30 bundle, still open. Do NOT merge as-is (D2
  DuckDB removal conflicts with keep-DuckDB + #231/#232). Triage comment posted.

Closed-this-wave issues whose context might still be referenced: #14, #20, #80, #81, #83,
#91, #112, #132, #165, #166, #172, #174, #175, #177, #181, #185, #193–#202, #212, #213,
#194 (stabilization tracker), #215–#239 (the 22-PR perf+refactor wave), #221 (drilldown table).

---

## Hard rules

- **NEVER force-push master.** PR-and-squash only.
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

# Run every CI guard locally before pushing (D-YAML / S-YAML / LOADERS / HTML-BUDGET / D-CHAIN / SVC-PARITY):
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
