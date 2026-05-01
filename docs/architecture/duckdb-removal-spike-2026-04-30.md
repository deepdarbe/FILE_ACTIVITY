---
status: Spike findings (D2 from audit-2026-04-28.md)
date: 2026-04-30
authors: main thread, stabilization week day 3
related: issue #194 (stabilization tracker), audit-2026-04-28.md (debt D2),
  PR #186 (per-query DuckDB conn — the workaround this spike validates removing)
---

# DuckDB removal spike — measurement-backed plan

## TL;DR

Remove DuckDB. The measurement is one-sided.

On the e2e corpus (10 000 rows, sparse, default indexes), the two
DuckDB-vs-SQLite paths the dashboard exercises every page-load measure as:

| Aggregate                      | DuckDB ATTACH | Direct SQLite | Ratio       |
|--------------------------------|--------------:|--------------:|------------:|
| Duplicate groups (CTE+GROUP BY)| 72.6 ms       | 2.3 ms        | 31.8× slower|
| Extension drilldown (window)   | 69.4 ms       | 0.5 ms        | 152× slower |

The audit's working assumption — "DuckDB ~200 ms vs SQLite ~150 ms, no win"
— turns out to be conservative. At this corpus shape, DuckDB is dominated
entirely by per-query ATTACH overhead (~60–80 ms) and never reaches the
columnar phase where it would otherwise pay for itself. The ATTACH
overhead is what the per-query lifecycle from #186 introduced as a fix
for the WAL leak from #185. We replaced one structural cost with
another.

There is **no SQL feature** in the codebase's DuckDB queries that SQLite
cannot serve directly: every query is standard SQL (CTEs, window
functions, GROUP BY, HAVING). And every dashboard endpoint that calls
into DuckDB already has a `Database`-side SQLite equivalent wired up as
a fallback — DuckDB removal is overwhelmingly a deletion, not a
rewrite.

## How the measurement was taken

`scripts/spike_duckdb_vs_sqlite.py`:

1. Build the e2e corpus full mode (10 000 sparse files, deterministic).
2. Scan it via `FileScanner.scan_source(...)` against a fresh SQLite
   DB on tmpfs.
3. `PRAGMA wal_checkpoint(TRUNCATE)` once so neither engine pays
   recovery cost on the first call.
4. Run each aggregate **8 times**, take the median.
5. DuckDB path uses `AnalyticsEngine` post-#186 (per-query connection
   model — the most defensible existing shape).

Engines were run back-to-back on the same file, same OS page cache,
same Python process. `:memory:` DuckDB databases ATTACHing the SQLite
file mirror production exactly.

```
## dup-groups (CTE+GROUP BY)
DuckDB ATTACH    | median 72.61 ms | min 68.27 | max 82.07
Direct SQLite    | median  2.29 ms | min  2.25 | max  2.62
ratio: 31.8× slower

## ext-drilldown (WINDOW COUNT)
DuckDB ATTACH    | median 69.38 ms | min 63.03 | max 75.37
Direct SQLite    | median  0.46 ms | min  0.44 | max  0.82
ratio: 152× slower
```

### Why DuckDB doesn't catch up at scale

The per-query connection costs DuckDB 50–150 ms (literal comment in
`src/storage/analytics.py:151` calls this out). For DuckDB to become
faster than indexed SQLite, the column-scan portion of the query has
to pay back that overhead. The customer's heaviest aggregates touch
~3 M rows and complete in ~150 ms on indexed SQLite (per the audit's
reference numbers); DuckDB would need to come in under 80 ms on those
to break even. There is no observation suggesting it does.

The audit speculates DuckDB might pull ahead at "50 M+ rows" — that's
the **#114 Phase 1 Elasticsearch backend** territory, not the SQLite
analytics path. The DuckDB engine inside the SQLite path is firmly in
the wrong region of the curve.

## What removal looks like (scope)

Per-explore-agent inventory of the surface (file:line cited):

### Drop entirely

- `src/storage/analytics.py` — the whole `AnalyticsEngine` class
  (~480 lines) and module.
- `tests/test_analytics_per_query.py` — pins the per-query connection
  lifecycle, which only exists because of DuckDB. Goes away with the
  engine.
- `requirements.txt:29` — `duckdb>=1.0.0` line.

### Strip fallback branches (8 dashboard endpoints)

Each currently has a `try AnalyticsEngine, except / fallback to
db.get_…`. Become single-branch calls into `Database`:

| Endpoint                                 | api.py line | Current call                                    |
|------------------------------------------|------------:|-------------------------------------------------|
| `GET /api/drilldown/frequency/{src}`     | 1626        | `engine.get_files_by_frequency`                 |
| `GET /api/drilldown/type/{src}`          | 1641        | `engine.get_files_by_extension`                 |
| `GET /api/drilldown/size/{src}`          | 1657        | `engine.get_files_by_size_range`                |
| `GET /api/drilldown/owner/{src}`         | 1672        | `engine.get_files_by_owner`                     |
| `GET /api/reports/duplicates/{src}`      | 2786        | `engine.get_duplicate_groups`                   |
| `GET /api/growth/{src}`                  | 3601        | `engine.get_growth_stats`                       |
| `GET /api/db/stats`                      | 4272        | `engine.get_db_stats`                           |
| `GET /api/system/health`                 | 3991        | `engine.health()` — drop, return `{available: false}` |

### Drop from app wiring

- `src/dashboard/api.py:create_app` — the `analytics: AnalyticsEngine`
  parameter goes away. Test stubs (`_StubAnalytics` in
  `test_dashboard_smoke.py:105`, `test_integration_e2e.py`) follow.
- `main.py` / `dev_server.py` boot path — wherever the engine is
  instantiated.

### Audit-only (don't remove)

- `src/storage/staging.py` — `ParquetStager` uses DuckDB's `COPY` for
  the parquet ingest path. Separate concern from analytics; out of D2
  scope. Keep DuckDB available as `pyarrow`-side dep if needed (or
  refactor the Parquet path to use `pyarrow` directly — separate
  decision).
- `src/playground/data_access.py` — DuckDB fallback path. Verify it
  isn't on the dashboard hot path before removing.
- `src/dashboard/sql_query.py` — admin "ad-hoc SQL" panel from #29
  Phase 2. Likely advisory; verify behaviour under SQLite-only.

## Risk register

1. **Dashboard hot-path latency** — *positive*. Removal makes the
   8 endpoints **30–150× faster**. No regression risk on perf.

2. **WAL leak class of bugs** — *resolved*. The ATTACH-as-permanent-
   reader pattern (#185) goes away with the engine; per-query
   workaround (#186) loses its purpose and gets deleted with the
   tests that pin it. CLAUDE.md's "look for new long-lived readers"
   guidance simplifies (one fewer reader class to chase).

3. **Customer config flag-rot (D7)** — interacts. The customer's
   `config.yaml` on production has `analytics.enabled: true` (from
   shipped defaults). After removal, this key becomes a no-op. It's
   harmless (unused keys don't break YAML parsing), but D7's
   config-migrator should prune it on next `update.cmd` so the file
   doesn't accumulate dead keys.

4. **Hidden DuckDB-only callers** — *low*. The explore agent's grep
   covered `src/`. Re-grep before merging the removal PR to catch any
   path added between this spike and that PR.

5. **Boot-time DuckDB extension chain** (`INSTALL sqlite ; LOAD sqlite`)
   — *resolved*. One fewer failure mode at startup, particularly in
   offline / corp environments where `INSTALL sqlite` reaches out to
   duckdb.org.

## Concrete plan (own PR, after this spike lands)

1. Delete `src/storage/analytics.py` and `tests/test_analytics_per_query.py`.
2. Remove `analytics` parameter from `create_app(...)`; cascade through
   stubs in 2 test files.
3. For each of the 8 endpoints listed above: delete the
   `try AnalyticsEngine` branch, keep the existing `db.get_…` call.
4. Drop `duckdb>=1.0.0` from `requirements.txt`.
5. Audit `src/storage/staging.py`, `src/playground/data_access.py`,
   `src/dashboard/sql_query.py` — confirm no dashboard hot path
   depends on DuckDB. If `staging.py`'s Parquet ingest still wants
   DuckDB COPY, file a separate decision; otherwise rewrite via
   `pyarrow` and drop DuckDB completely.
6. Run the integration test from PR #203 (`tests/test_integration_e2e.py`)
   to confirm scan→DB→dashboard still works end-to-end.
7. Manually exercise the 8 affected endpoints in dev server.

Estimated cost: ~1 day. Aligns with the audit's "1–2 days, saves us
from one entire class of WAL leak."

## Out of scope for this spike

- 1 M / 50 M-row scaling claim. The audit says DuckDB might pull ahead
  on the upper end — but the upper end is also out of scope for the
  default backend (#114 Phase 1 Elasticsearch is the right answer
  there). The 10 K corpus measurement is sufficient to disqualify
  DuckDB as the **dashboard analytics** engine.
- Replacing DuckDB in `staging.py` (Parquet ingest). Separate decision
  with its own perf calculus (DuckDB COPY vs pyarrow).
- Closing #114. Different problem (50 M+ files).

## Recommendation

Open the removal PR. Cite this doc + the measurement script. Run
PR #203's integration test on the result to confirm end-to-end. Close
out the per-query connection workaround entirely.
