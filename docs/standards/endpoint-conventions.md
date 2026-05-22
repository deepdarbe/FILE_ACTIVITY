# Endpoint conventions & development standard

> **Audience**: every Claude Code session, every Copilot agent, every human dev opening a PR against this repo.
> **Status**: living document, owned by the team. Update it when a new pattern emerges or a new bug class is found.
> **Why this exists**: between 2026-04-23 and 2026-05-22 we shipped ~50 PRs, of which **~30%** were re-fixing the same bug class in a different endpoint (shape mismatch, missing cache, async/sync drift, null-deref, stale state). The pattern is structural, not incidental — there was no agreed shape for "what an endpoint looks like" so each agent invented one. This doc fixes that.

The doc is organised as **rules**. Each rule has:
- The exact bug class it prevents (with issue/PR numbers)
- The current state (what's broken, where)
- The standard (BEFORE / AFTER code samples)
- How to enforce it (CI guard if possible, code review otherwise)

The rules are listed in priority order — Rule 1 is the most-violated and most-impactful.

---

## Rule 1 — Heavy reports MUST go through `cached_report_endpoint`

**Prevents**: PR #224 (mit_naming_report iterated 2.89M rows on every click; 1–3 min latency; page looked broken). PR #223 (report_frequency had unique cache-key construction logic). Any future report that iterates `scanned_files` without an indexed `LIMIT`.

**Bug class**: every report endpoint hand-rolls cache key, fallback, and operation tracking. Three things go wrong:
1. The author forgets the cache (PR #224 mit_naming).
2. The author uses an inconsistent cache key string (`"mit_naming"` vs `"frequency:days1,days2"` — fine for now, fragile when the next agent picks a different convention).
3. The author forgets `_track_op(...)` so a 90 sec compute doesn't appear in the in-progress badge, and the user thinks the dashboard hung.

**Standard** (`src/dashboard/_endpoint_helpers.py`, new file):

```python
def cached_report_endpoint(
    db,
    *,
    scan_id: int,
    report_name: str,
    compute_fn,
    track_op_label: str,
    custom_key_suffix: str = "",
) -> dict:
    """Canonical wrapper for any report endpoint that iterates >100k rows.

    - Cache key:  f"{report_name}{':' + custom_key_suffix if custom_key_suffix else ''}"
    - Fallback:   on cache miss runs ``compute_fn()`` once per scan_id
    - Surface:    wraps in ``_track_op("analysis", track_op_label)`` so the
                  long-running computation appears in the dashboard's
                  in-progress badge.
    - Returns:    ``_attach_cache_envelope(envelope)`` so the response
                  carries ``cache_hit`` / ``computed_at`` metadata.
    """
```

BEFORE (mit_naming_report, 28 lines):
```python
@app.get("/api/reports/mit-naming/{source_id}")
def mit_naming_report(source_id: int):
    from src.scanner.file_scanner import MITNamingAnalyzer
    from src.analyzer import cache as analyzer_cache
    src = _get_source(db, source_id)
    scan_id = db.get_latest_scan_id(source_id, include_running=True)
    if not scan_id:
        raise HTTPException(404, "Tarama bulunamadi")
    def _compute() -> dict:
        analyzer = MITNamingAnalyzer()
        with db.get_read_cursor() as cur:
            cur.execute("SELECT file_path, file_name FROM scanned_files WHERE scan_id=?",
                        (scan_id,))
            for row in cur:
                analyzer.analyze(row["file_path"], row["file_name"])
        return analyzer.get_report()
    with _track_op("analysis", f"Adlandirma uyumu analizi: {src.name}", ...):
        envelope = analyzer_cache.get_or_compute(db, "mit_naming", scan_id, _compute)
        return _attach_cache_envelope(envelope)
```

AFTER (10 lines):
```python
@app.get("/api/reports/mit-naming/{source_id}")
def mit_naming_report(source_id: int):
    from src.scanner.file_scanner import MITNamingAnalyzer
    src = _get_source(db, source_id)
    scan_id = db.get_latest_scan_id(source_id, include_running=True)
    if not scan_id:
        raise HTTPException(404, "Tarama bulunamadi")
    def _compute():
        return MITNamingAnalyzer.report_from_scan(db, scan_id)
    return cached_report_endpoint(
        db, scan_id=scan_id, report_name="mit_naming",
        compute_fn=_compute,
        track_op_label=f"Adlandirma uyumu analizi: {src.name}",
    )
```

**CI guard**: `scripts/ci_guards.py` — `R-CACHE` check. Greps `src/dashboard/api.py` for direct calls to `analyzer_cache.get_or_compute(`. Fail unless inside `src/dashboard/_endpoint_helpers.py` or the deprecation allowlist.

---

## Rule 2 — Pagination MUST use `PaginationParams(Depends())`

**Prevents**: 30+ endpoints with drifting conventions: `(page, page_size)`, `(page, limit)`, `(offset, limit)`, raw `limit` only. Frontend callers must remember which endpoint uses which.

**Standard** (`src/dashboard/_endpoint_helpers.py`):

```python
from fastapi import Query

class PaginationParams:
    def __init__(self,
                 page: int = Query(1, ge=1),
                 page_size: int = Query(100, ge=1, le=500)):
        self.page = page
        self.page_size = page_size
        self.offset = (page - 1) * page_size

    def response(self, total: int, items: list) -> dict:
        return {
            "page": self.page,
            "page_size": self.page_size,
            "total": total,
            "total_pages": max(1, -(-total // self.page_size)),
            "items": items,
        }
```

Call site:
```python
@app.get("/api/reports/mit-naming/{source_id}/files")
def mit_naming_files(source_id: int, code: str = "R1",
                     p: PaginationParams = Depends()):
    rows = ...  # compute filtered list
    return p.response(len(rows), rows[p.offset : p.offset + p.page_size])
```

**CI guard**: `P-PAGE` — scan endpoint signatures, flag any endpoint that declares both `page` and `page_size`/`limit` as separate `Query(...)` params instead of via `PaginationParams = Depends()`.

---

## Rule 3 — `scan_runs.summary_json` MUST be read through a shape-normalising accessor

**Prevents**: PR #198 (frequency v2 fast-path returned dict, frontend expected list). PR #223 (frequency endpoint expected dict, `compute_scan_summary` wrote list). Any future bug where two writers of the same JSON key use different shapes.

**Bug class**: `summary_json` is written by **two** unrelated modules — `compute_scan_summary` (final, list-of-dicts shape) and `partial_summary_v2` (live, dict-of-counts shape). They overwrite the same DB column. Consumers must either know which writer ran last or hand-roll branching code.

**Standard** (`src/storage/_summary_compat.py`, new file):

```python
def normalize_summary(raw: dict | None) -> dict | None:
    """Map either writer's shape to a single canonical form.

    The canonical shape:
    - age_buckets:   list[{label, file_count, total_size}]   # 6 entries
    - size_buckets:  list[{label, file_count, total_size}]   # 5 entries
    - top_extensions: list[{extension, count, size}]
    - top_owners:     list[{owner, count, size}]
    - (everything else passes through unchanged)

    Accepts None and returns None. Idempotent: passing a canonical dict
    returns the same canonical dict.
    """
```

Then `db.get_scan_summary()` calls `normalize_summary()` before returning, and every consumer reads the canonical shape.

**CI guard**: `S-SHAPE` — grep `src/dashboard/api.py` for raw access to `summary.get("age_buckets")` / `summary["age_buckets"]` / same for `size_buckets`/`top_extensions`/`top_owners`. Fail unless the line is inside `_summary_compat.py` itself.

---

## Rule 4 — Every write endpoint MUST emit an audit event

**Prevents**: compliance audit-trail gaps. Customer auditors flag missing events; we then scramble to backfill.

**Current state**: ~24 of ~180 endpoints emit audit events. The other ~150 are silent — including write endpoints.

**Standard**: every `@app.post` / `@app.delete` / `@app.put` / `@app.patch` MUST call `db.insert_audit_event_simple(...)` (chain-routed when `audit.chain_enabled: true`) on the success path. Exceptions are explicitly listed in `CLAUDE.md` and require reviewer sign-off (e.g., `/api/health` is exempt).

```python
@app.delete("/api/sources/{source_id}")
def remove_source(source_id: int):
    src = _get_source(db, source_id)
    if not db.remove_source(src.name):
        raise HTTPException(500, "Silme basarisiz")
    db.insert_audit_event_simple(
        source_id=src.id,
        event_type="source_removed",
        username="admin",
        file_path=None,
        details=f"name={src.name}",
    )
    return {"status": "removed"}
```

**CI guard**: `A-AUDIT` — AST-parse `src/dashboard/api.py`; for every endpoint decorated with `@app.post` / `@app.delete` / `@app.put` / `@app.patch`, check that the function body contains a call to `insert_audit_event_simple(`. Fail with the endpoint name if not, unless on the allowlist.

---

## Rule 5 — `async def` only with `await`

**Prevents**: PR #215 (166/182 endpoints were `async def` with sync DB calls, blocking the event loop). Customer "every page is waiting" symptom.

**Standard**: an endpoint is `async def` if and only if its body contains at least one `await`/`async for`/`async with`. Otherwise it's plain `def` so FastAPI/Starlette dispatches it to the anyio worker thread pool.

**CI guard**: `A-AWAIT` (existing logic in PR #215 description, formalise as a guard) — AST-parse every `@app.*` endpoint; if `AsyncFunctionDef`, require at least one `Await` / `AsyncFor` / `AsyncWith` node in its OWN body (not nested in helper defs).

---

## Rule 6 — Read endpoints use `get_read_cursor()`, writes use `get_cursor()`

**Prevents**: PR #184 family (WAL contention because dashboard reads were taking writer-pool connections). Customer "menüler boş during scan" symptom hit four times (#132 / #174 / #181 / #185 — same root cause).

**Standard**: in `src/dashboard/api.py`, every read-only endpoint uses `with db.get_read_cursor() as cur:`. Write endpoints (POST/DELETE/PUT/PATCH) use `with db.get_cursor() as cur:`. **Never mix the two in the same `with` block** — if a code path can write, the whole call site uses `get_cursor()`. The current 3 known-good writer uses (api.py:879, 4819, 5387) carry an explicit comment justifying why.

**CI guard**: `C-CURSOR` — AST-parse `src/dashboard/api.py`. For every endpoint, classify by HTTP method. GET handlers calling `get_cursor()` fail unless an explicit `# noqa: C-CURSOR — UPDATE path` comment is on the line. POST/DELETE/PUT/PATCH handlers calling `get_read_cursor()` fail with no exception.

---

## Rule 7 — No chained `getElementById(...).innerHTML`

**Prevents**: PR #200 / #201 / #202 / #216 (null-deref when the element is missing from the DOM).

**Standard**: every `.innerHTML =` write either uses the helper (`_setHtmlSafe('id', html)`) or is on a stored reference with an explicit null-check above it. The chained shape `document.getElementById('foo').innerHTML = ...` is banned.

**CI guard**: `D-CHAIN` (existing in `scripts/ci_guards.py`) — already enforced. Baseline 0. Any regression fails CI.

---

## Rule 8 — Config-gated features MUST surface their config key in the UI

**Prevents**: customer 2026-05-22 confusion — "Kullanıcı Aktivitesi" showed all owners as "(Bilinmiyor)" because `scanner.read_owner: false`, with no in-UI hint that this was a config setting. User assumed bug.

**Standard**: any feature with a `config.yaml` toggle MUST render a top-of-page warning when the toggle is off:

```
⚙ Bu sayfa için 'scanner.read_owner: true' ayarı gereklidir.
   config.yaml dosyasında bu anahtarı açıp servisi yeniden başlatın.
```

Pattern: each `load*` function checks `data.feature_disabled_reason` in the response; if set, renders the warning banner instead of empty cards.

API side: each endpoint reads its own config gate and includes `"feature_disabled_reason": "scanner.read_owner=false"` in the response when applicable.

**CI guard**: not auto-enforceable — manual review checklist item. Reviewer must confirm any new config-gated feature surfaces the gate.

---

## Adopting the standard

The plan splits into **6 PRs**, ordered so each can be merged independently:

| PR | Surface | Risk | Effort |
|---|---|---|---|
| **R-1** Create `src/dashboard/_endpoint_helpers.py` + add `cached_report_endpoint` and `PaginationParams` | new file, no behavior change | Low | 1 hr |
| **R-2** Migrate 4 hot-path reports (frequency, types, sizes, mit_naming) to `cached_report_endpoint` | api.py | Low | 1 hr |
| **R-3** Create `src/storage/_summary_compat.py` + wire into `db.get_scan_summary()` | database.py + 1 call site | Med | 2 hr |
| **R-4** Remove the dual-shape branch from `report_frequency` (use canonical shape) | api.py | Low (after R-3) | 30 min |
| **R-5** Add `R-CACHE` / `P-PAGE` / `S-SHAPE` / `A-AUDIT` / `A-AWAIT` / `C-CURSOR` to `scripts/ci_guards.py` | one file, new checks | Low | 2 hr |
| **R-6** Audit endpoint inventory, allowlist known-non-conforming endpoints with justification comments | comment-only PR; flushes the backlog | Low | 1 hr |

Total: ~7-8 hours of focused work. Each PR is small (<200 lines) and independently reviewable.

The CI guards make Rules 1–7 self-enforcing. Rule 8 stays a manual review item.

---

## Living standards

When a new bug class is identified — same shape across multiple endpoints, same drift between modules, same recovery path the user does manually — add it as a new rule here. Cite the PR that exposed the class as evidence. Add a CI guard if mechanically detectable.

Don't add a rule for a one-off bug. Three instances of the same pattern is the threshold.

Don't write rules without an enforcement plan — either a CI check, a typing constraint, or "manual review checklist". A rule with no enforcement is folklore, not a standard.
