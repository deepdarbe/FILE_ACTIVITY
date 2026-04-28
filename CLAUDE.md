# CLAUDE.md — FILE_ACTIVITY agent context

> **Read me first.** Compressed brief for any Claude Code session opened on this repo.
> Goal: skip the "what is this codebase" warmup. ROADMAP.md is the canonical roadmap;
> this file is the operator's mental model + agent playbook.

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
| Scan emits 0 files on real NTFS | `src/scanner/backends/_ntfs_records.py` | FRN sequence-number masking (lower 48 bits) — see #164/#165 |
| Scan aborts mid-run with `database is locked` | `src/storage/database.py::bulk_insert_scanned_files` | 5× retry with 1/2/4/8/16s backoff (#176) |
| WAL stuck at 13+ GB, won't truncate | `src/storage/analytics.py::AnalyticsEngine` | Per-query DuckDB conn (#185/#186) — long-lived ATTACH was the leak |
| Dashboard menus empty during scan | `src/storage/database.py::get_read_cursor` + `partial_summary_v2` | Read-only pool (#184) + v2 schema (#183) + frontend partial-data (#182) |
| `BOYUT: 0 B` on every file | `src/scanner/size_enricher.py` | MFT enum is path-only by design; size enrich pass runs after walk (#179) |
| `update.cmd` fails on locked `bin\nssm.exe` | `deploy/setup-source.ps1` | Service-aware Stop/Start wrapper (#172/#173) |

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

**The trap that bit this codebase three times** (#132 / #174 / #185):
A long-lived reader (any kind — dashboard handle, DuckDB ATTACH, scheduler probe)
prevents `wal_checkpoint(TRUNCATE)` from shrinking the WAL. Symptom is always
"WAL grows to N GB and never shrinks". Fix is always "make the reader short-lived".
If you see that symptom in a future session, look for the new long-lived reader.

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

`Python syntax check` and `Pytest (Linux, Docker)` flake on PRs even when master is green.
Diagnosis is in #91's body: pip-install timeouts on the GHA runner before pytest collects.
Docker test infra (#188) addresses the chronic part. Until 3 master runs are green back-to-back,
**`continue-on-error: true` stays on**. **Don't keep diagnosing the same flake every PR**;
verify locally and merge if local + master are green.

---

## What's open (as of 2026-04-28 end of wave)

Only **2 open issues**:

- **#29** — EPIC roadmap tracker (pinned, never closed)
- **#114** — Pluggable storage Phase 3-5. Deliberately deferred. Phase 1+2 shipped (#121, #167);
  Phase 3 = dashboard query layer rewrite (~30 endpoints). Architecturally significant —
  needs a fresh head, NOT a tired evening session.

Closed-this-wave issues whose context might still be referenced: #14, #20, #80, #81, #83,
#91, #112, #132, #165, #166, #172, #174, #175, #177, #181, #185.

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

## Useful incantations

```bash
# Find the FRN-mask test (the canonical example of "small fix, big consequence"):
grep -rn "_FRN_SEGMENT_MASK" src/ tests/

# Inspect WAL pressure live during a customer scan:
ls -la C:\FileActivity\data\file_activity.db-wal
# (>500 MB during scan = normal; >5 GB sustained after scan = leak; investigate readers)

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
