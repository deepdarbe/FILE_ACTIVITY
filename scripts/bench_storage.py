"""SQLite-vs-DuckDB micro-benchmark on the dashboard query set.

Why this exists
---------------
``docs/architecture/storage-decision-2026-04-28.md`` froze on SQLite,
and ``src/storage/analytics.py`` keeps DuckDB on a strict per-query
ATTACH lifecycle (pinned by ``tests/test_analytics_per_query.py``).
Open question, deferred from the stabilization audit: does DuckDB
actually win on the queries we run, at the row-count our customer is
running (3.1M files, growing)?

This harness times the **same logical query** through:
  1. Direct SQLite ``cursor.execute(...)`` against the customer DB.
  2. The AnalyticsEngine-style path — fresh ``duckdb.connect(":memory:")``
     → ``ATTACH 'path' AS sqlite_db (TYPE SQLITE)`` → query the attached
     SQLite via DuckDB → DETACH → ``conn.close()``.

Reports min / p50 / p95 / max over N repeats per query so that a single
cold cache outlier doesn't muddy the comparison. If DuckDB is within
~20 % on most queries, Phase 3 of #114 (dashboard query layer rewrite)
gets stronger as "drop DuckDB entirely" rather than "keep both".

Usage
-----
On the customer machine (or a copy of their DB)::

    python scripts/bench_storage.py --db C:\\FileActivity\\data\\file_activity.db \\
        --repeats 5 --queries dashboard

With no ``--db`` flag the harness builds a 100k-row synthetic DB in a
temp directory and benches against that — useful for validating the
harness itself before running on a real workload.

Output is plain text + a JSON sidecar (``bench_storage_<ts>.json``) so
the result can be diffed across runs.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import statistics
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterable


# ---------------------------------------------------------------------------
# Query set — these mirror the aggregates computed by
# ``Database.compute_scan_summary`` and the dashboard's hot endpoints.
# ---------------------------------------------------------------------------
QUERY_SET: dict[str, str] = {
    "total_files_size": (
        "SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s "
        "FROM scanned_files WHERE scan_id = :scan_id"
    ),
    "top_extensions": (
        "SELECT LOWER(SUBSTR(file_name, INSTR(file_name, '.') + 1)) ext, "
        "COUNT(*) c, COALESCE(SUM(file_size),0) s "
        "FROM scanned_files WHERE scan_id = :scan_id AND INSTR(file_name, '.') > 0 "
        "GROUP BY ext ORDER BY c DESC LIMIT 10"
    ),
    "top_owners": (
        "SELECT owner, COUNT(*) c, COALESCE(SUM(file_size),0) s "
        "FROM scanned_files "
        "WHERE scan_id = :scan_id AND owner IS NOT NULL AND owner <> '' "
        "GROUP BY owner ORDER BY s DESC LIMIT 10"
    ),
    "age_buckets": (
        "SELECT "
        "  SUM(CASE WHEN last_access_time >= date('now','-30 days') THEN 1 ELSE 0 END) b0_30, "
        "  SUM(CASE WHEN last_access_time >= date('now','-90 days') AND last_access_time < date('now','-30 days') THEN 1 ELSE 0 END) b31_90, "
        "  SUM(CASE WHEN last_access_time >= date('now','-180 days') AND last_access_time < date('now','-90 days') THEN 1 ELSE 0 END) b91_180, "
        "  SUM(CASE WHEN last_access_time >= date('now','-365 days') AND last_access_time < date('now','-180 days') THEN 1 ELSE 0 END) b181_365, "
        "  SUM(CASE WHEN last_access_time < date('now','-365 days') THEN 1 ELSE 0 END) b366_plus "
        "FROM scanned_files WHERE scan_id = :scan_id"
    ),
    "size_buckets": (
        "SELECT "
        "  SUM(CASE WHEN file_size < 102400 THEN 1 ELSE 0 END) tiny, "
        "  SUM(CASE WHEN file_size BETWEEN 102400 AND 1048576 THEN 1 ELSE 0 END) small, "
        "  SUM(CASE WHEN file_size BETWEEN 1048577 AND 104857600 THEN 1 ELSE 0 END) medium, "
        "  SUM(CASE WHEN file_size > 104857600 THEN 1 ELSE 0 END) large "
        "FROM scanned_files WHERE scan_id = :scan_id"
    ),
    "risky_count": (
        "SELECT COUNT(*) c FROM scanned_files "
        "WHERE scan_id = :scan_id AND LOWER(file_name) GLOB '*.[eE][xX][eE]' "
        "OR LOWER(file_name) GLOB '*.[bB][aA][tT]' "
        "OR LOWER(file_name) GLOB '*.[pP][sS]1' "
        "OR LOWER(file_name) GLOB '*.[vV][bB][sS]'"
    ),
    "large_files_top50": (
        "SELECT file_name, file_path, file_size FROM scanned_files "
        "WHERE scan_id = :scan_id ORDER BY file_size DESC LIMIT 50"
    ),
    "orphan_owner_count": (
        "SELECT COUNT(*) c FROM scanned_files "
        "WHERE scan_id = :scan_id AND (owner IS NULL OR owner = '')"
    ),
}


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------


@contextmanager
def _sqlite_conn(db_path: str):
    """Per-call read-only SQLite handle — matches Database.get_read_cursor."""
    uri = f"file:{db_path}?mode=ro&cache=shared"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _duckdb_attach(db_path: str):
    """Per-query DuckDB :memory: + ATTACH(SQLite) — matches AnalyticsEngine."""
    import duckdb  # type: ignore

    conn = duckdb.connect(database=":memory:")
    try:
        try:
            conn.execute("INSTALL sqlite")
        except Exception:
            pass
        conn.execute("LOAD sqlite")
        safe = db_path.replace("'", "''")
        conn.execute(f"ATTACH '{safe}' AS sqlite_db (TYPE SQLITE)")
        conn.execute("USE sqlite_db")
        yield conn
    finally:
        try:
            conn.execute("DETACH sqlite_db")
        except Exception:
            pass
        conn.close()


def _run_sqlite(db_path: str, query: str, scan_id: int) -> int:
    with _sqlite_conn(db_path) as conn:
        # Bind named parameter manually since sqlite3 wants :scan_id with dict.
        rows = conn.execute(query, {"scan_id": scan_id}).fetchall()
        return len(rows)


def _run_duckdb(db_path: str, query: str, scan_id: int) -> int:
    # DuckDB doesn't accept :scan_id binding for SQLite-attached tables in
    # all extension versions — substitute as a literal integer.
    sql = query.replace(":scan_id", str(int(scan_id)))
    with _duckdb_attach(db_path) as conn:
        rows = conn.execute(sql).fetchall()
        return len(rows)


# ---------------------------------------------------------------------------
# Synthetic DB for harness self-test (no customer data required)
# ---------------------------------------------------------------------------


def _make_synthetic_db(rows: int) -> str:
    """Build a tmp DB with ``rows`` synthetic scanned_files. Returns path."""
    tmp = Path(tempfile.mkdtemp(prefix="bench_storage_")) / "synth.db"
    conn = sqlite3.connect(tmp)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE scanned_files (
            id INTEGER PRIMARY KEY,
            scan_id INTEGER NOT NULL,
            file_name TEXT,
            file_path TEXT,
            file_size INTEGER,
            owner TEXT,
            last_access_time TEXT,
            last_modify_time TEXT
        )
        """
    )
    conn.execute("CREATE INDEX idx_scan ON scanned_files(scan_id)")
    exts = ["txt", "pdf", "exe", "bat", "ps1", "vbs", "jpg", "docx", "zip", "csv"]
    owners = [None, ""] + [f"DOM\\user{i:03d}" for i in range(50)]
    rng = random.Random(0xFA)
    rows_batch = []
    for i in range(rows):
        ext = rng.choice(exts)
        rows_batch.append(
            (
                1,  # scan_id
                f"file{i:07d}.{ext}",
                f"E:\\share\\sub{i % 1000:03d}\\file{i:07d}.{ext}",
                rng.randint(0, 10 * 1024 * 1024 * 1024),
                rng.choice(owners),
                f"2026-{rng.randint(1, 5):02d}-{rng.randint(1, 28):02d}",
                f"2026-{rng.randint(1, 5):02d}-{rng.randint(1, 28):02d}",
            )
        )
        if len(rows_batch) >= 50000:
            conn.executemany(
                "INSERT INTO scanned_files "
                "(scan_id,file_name,file_path,file_size,owner,last_access_time,last_modify_time) "
                "VALUES (?,?,?,?,?,?,?)",
                rows_batch,
            )
            rows_batch = []
    if rows_batch:
        conn.executemany(
            "INSERT INTO scanned_files "
            "(scan_id,file_name,file_path,file_size,owner,last_access_time,last_modify_time) "
            "VALUES (?,?,?,?,?,?,?)",
            rows_batch,
        )
    conn.commit()
    conn.close()
    return str(tmp)


# ---------------------------------------------------------------------------
# Bench loop
# ---------------------------------------------------------------------------


def _time_call(fn: Callable[[], object]) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def _bench_one(
    label: str, fn: Callable[[], object], repeats: int
) -> dict[str, float]:
    samples = []
    for _ in range(repeats):
        samples.append(_time_call(fn))
    s_sorted = sorted(samples)
    return {
        "label": label,
        "min": s_sorted[0],
        "p50": statistics.median(s_sorted),
        "p95": s_sorted[int(0.95 * (len(s_sorted) - 1))],
        "max": s_sorted[-1],
        "samples": samples,
    }


def _bench_pair(
    name: str, query: str, db_path: str, scan_id: int, repeats: int
) -> dict:
    sqlite_stats = _bench_one(
        "sqlite", lambda: _run_sqlite(db_path, query, scan_id), repeats
    )
    try:
        duckdb_stats = _bench_one(
            "duckdb", lambda: _run_duckdb(db_path, query, scan_id), repeats
        )
        duckdb_err = None
    except Exception as e:  # duckdb missing / extension issue
        duckdb_stats = None
        duckdb_err = repr(e)
    return {
        "query": name,
        "sqlite": sqlite_stats,
        "duckdb": duckdb_stats,
        "duckdb_error": duckdb_err,
    }


def _resolve_scan_id(db_path: str, override: int | None) -> int:
    if override is not None:
        return override
    with _sqlite_conn(db_path) as conn:
        try:
            row = conn.execute(
                "SELECT scan_id, COUNT(*) c FROM scanned_files "
                "GROUP BY scan_id ORDER BY c DESC LIMIT 1"
            ).fetchone()
            if row:
                return int(row["scan_id"])
        except sqlite3.DatabaseError:
            pass
    return 1


def _format_p50_pretty(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1e6:7.0f} us"
    if seconds < 1.0:
        return f"{seconds * 1000:7.1f} ms"
    return f"{seconds:7.2f} s "


def _print_table(results: list[dict]) -> None:
    print(f"\n{'Query':<22} {'rows':>5} {'SQLite p50':>12} {'DuckDB p50':>12} {'ratio':>7}")
    print("-" * 64)
    for r in results:
        name = r["query"]
        s = r["sqlite"]
        d = r["duckdb"]
        if d is None:
            print(f"{name:<22} {'?':>5} {_format_p50_pretty(s['p50']):>12} {'(err)':>12} {'-':>7}")
            continue
        ratio = d["p50"] / s["p50"] if s["p50"] > 0 else float("inf")
        print(
            f"{name:<22} {'?':>5} "
            f"{_format_p50_pretty(s['p50']):>12} "
            f"{_format_p50_pretty(d['p50']):>12} "
            f"{ratio:>6.2f}x"
        )


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--db",
        help="Path to the SQLite DB to bench against. If omitted, a 100k-row "
        "synthetic DB is built in a temp dir.",
    )
    ap.add_argument(
        "--scan-id",
        type=int,
        help="scan_id to query (default: pick the largest one in the DB).",
    )
    ap.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Repetitions per query (default: 5).",
    )
    ap.add_argument(
        "--queries",
        nargs="*",
        choices=list(QUERY_SET.keys()) + ["dashboard"],
        default=["dashboard"],
        help="Which queries to run (default: all dashboard hot queries).",
    )
    ap.add_argument(
        "--synthetic-rows",
        type=int,
        default=100_000,
        help="Row count for synthetic mode (default: 100k).",
    )
    args = ap.parse_args(list(argv) if argv else None)

    if args.db:
        db_path = args.db
        if not Path(db_path).exists():
            print(f"DB not found: {db_path}", file=sys.stderr)
            return 2
        print(f"Bench DB: {db_path}")
    else:
        print(f"No --db given; building synthetic DB ({args.synthetic_rows} rows)...")
        db_path = _make_synthetic_db(args.synthetic_rows)
        print(f"Synthetic DB: {db_path}")

    scan_id = _resolve_scan_id(db_path, args.scan_id)
    print(f"scan_id={scan_id}, repeats={args.repeats}")

    selected = args.queries
    if "dashboard" in selected:
        names = list(QUERY_SET.keys())
    else:
        names = list(selected)

    results = []
    for name in names:
        print(f"  running {name}...", end="", flush=True)
        r = _bench_pair(name, QUERY_SET[name], db_path, scan_id, args.repeats)
        results.append(r)
        print(" done")

    _print_table(results)

    ts = time.strftime("%Y%m%d-%H%M%S")
    out_path = Path(f"bench_storage_{ts}.json")
    out_path.write_text(json.dumps({"db": db_path, "scan_id": scan_id, "results": results}, indent=2))
    print(f"\nJSON sidecar: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
