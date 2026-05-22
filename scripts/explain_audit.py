"""EXPLAIN QUERY PLAN audit for the dashboard's hot SQL paths.

Why this exists
---------------
``scripts/bench_api.py`` measures *latency*. This script tells you *why*
a query is slow — index hit vs full table scan, B-tree vs sequential
scan, temp B-tree for sorting, etc. Together they pinpoint which
endpoints need a new index.

The query set mirrors the SQL that the dashboard's hot endpoints run
(via ``ReportGenerator`` / ``Database.compute_scan_summary`` /
``MITNamingAnalyzer``). Each query is run under ``EXPLAIN QUERY PLAN``
on the customer's real DB and the resulting plan is parsed for
red-flag patterns:

  - ``SCAN TABLE scanned_files``   ← full table scan, bad on 2.9M rows
  - ``USE TEMP B-TREE FOR ORDER``  ← unindexed ORDER BY, expensive sort
  - ``USE TEMP B-TREE FOR GROUP``  ← unindexed GROUP BY, expensive sort

Usage
-----
On the customer machine (or a copy of the DB)::

    python scripts/explain_audit.py \\
        --db C:\\FileActivity\\data\\file_activity.db

Outputs a per-query verdict and a summary of which composite indexes
*would* help (e.g. ``(scan_id, file_size)`` to remove a temp B-tree).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable

# Query set. Each entry: (name, sql, params_template).
# ``params_template`` is a dict that uses placeholder values; the real
# values come from the DB at audit time (largest scan_id by default).
QUERIES: list[tuple[str, str, dict]] = [
    (
        "compute_scan_summary.total_size",
        "SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s, "
        "COUNT(DISTINCT owner) o FROM scanned_files WHERE scan_id=?",
        {"scan_id": 1},
    ),
    (
        "compute_scan_summary.stale_count",
        "SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s FROM scanned_files "
        "WHERE scan_id=? AND COALESCE(last_access_time, last_modify_time) < ?",
        {"scan_id": 1, "cutoff": "2025-01-01"},
    ),
    (
        "compute_scan_summary.risky_count",
        "SELECT COUNT(*) c FROM scanned_files "
        "WHERE scan_id=? AND LOWER(extension) IN "
        "('exe','bat','ps1','vbs','cmd','com','scr','msi','js','wsf')",
        {"scan_id": 1},
    ),
    (
        "compute_scan_summary.top_extensions",
        "SELECT extension, COUNT(*) c, COALESCE(SUM(file_size),0) s "
        "FROM scanned_files WHERE scan_id=? "
        "GROUP BY extension ORDER BY c DESC LIMIT 10",
        {"scan_id": 1},
    ),
    (
        "compute_scan_summary.top_owners",
        "SELECT owner, COUNT(*) c, COALESCE(SUM(file_size),0) s "
        "FROM scanned_files WHERE scan_id=? AND owner IS NOT NULL "
        "GROUP BY owner ORDER BY s DESC LIMIT 10",
        {"scan_id": 1},
    ),
    (
        "compute_scan_summary.top_large_files",
        "SELECT file_name, file_path, file_size FROM scanned_files "
        "WHERE scan_id=? ORDER BY file_size DESC LIMIT 50",
        {"scan_id": 1},
    ),
    (
        "type_analyzer.analyze",
        "SELECT extension, COUNT(*) file_count, "
        "COALESCE(SUM(file_size),0) total_size, "
        "COALESCE(AVG(file_size),0) avg_size, "
        "COALESCE(MIN(file_size),0) min_size, "
        "COALESCE(MAX(file_size),0) max_size "
        "FROM scanned_files WHERE scan_id=? GROUP BY extension",
        {"scan_id": 1},
    ),
    (
        "mit_naming_files.scan",
        "SELECT id, file_path, file_name, file_size, owner, last_modify_time "
        "FROM scanned_files WHERE scan_id=?",
        {"scan_id": 1},
    ),
    (
        "duplicates.group_by_hash",
        "SELECT content_hash, COUNT(*) c, COALESCE(SUM(file_size),0) s "
        "FROM scanned_files "
        "WHERE scan_id=? AND content_hash IS NOT NULL "
        "GROUP BY content_hash HAVING c > 1 ORDER BY s DESC LIMIT 50",
        {"scan_id": 1},
    ),
]


RED_FLAGS = [
    ("SCAN TABLE scanned_files", "full_table_scan"),
    ("USE TEMP B-TREE FOR ORDER BY", "temp_btree_order"),
    ("USE TEMP B-TREE FOR GROUP BY", "temp_btree_group"),
    ("USE TEMP B-TREE FOR DISTINCT", "temp_btree_distinct"),
]


def _explain(conn: sqlite3.Connection, sql: str, params: tuple) -> list[dict]:
    """Return list of {id, parent, notused, detail} rows from EXPLAIN QUERY PLAN."""
    cur = conn.execute("EXPLAIN QUERY PLAN " + sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _list_indexes(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name='scanned_files' "
        "ORDER BY name"
    )
    return [{"name": r[0], "sql": r[1] or ""} for r in cur.fetchall()]


def _resolve_scan_id(conn: sqlite3.Connection, override: int | None) -> int:
    if override is not None:
        return override
    row = conn.execute(
        "SELECT scan_id, COUNT(*) c FROM scanned_files "
        "GROUP BY scan_id ORDER BY c DESC LIMIT 1"
    ).fetchone()
    return int(row[0]) if row else 1


def _classify(plan_rows: list[dict]) -> list[str]:
    """Walk the plan, return list of red-flag tags found."""
    flags = []
    for row in plan_rows:
        detail = row.get("detail", "") or ""
        for needle, tag in RED_FLAGS:
            if needle in detail:
                flags.append(tag)
    return flags


def _suggest_indexes(name: str, sql: str, flags: list[str]) -> list[str]:
    """Heuristic: based on WHERE + ORDER BY of the query, suggest indexes."""
    suggestions = []
    sql_upper = sql.upper()
    if "temp_btree_order" in flags and "ORDER BY FILE_SIZE" in sql_upper:
        suggestions.append("CREATE INDEX idx_scan_size ON scanned_files(scan_id, file_size DESC);")
    if "temp_btree_group" in flags and "GROUP BY EXTENSION" in sql_upper:
        suggestions.append("CREATE INDEX idx_scan_ext ON scanned_files(scan_id, extension);")
    if "temp_btree_group" in flags and "GROUP BY OWNER" in sql_upper:
        suggestions.append("CREATE INDEX idx_scan_owner ON scanned_files(scan_id, owner);")
    if "temp_btree_group" in flags and "GROUP BY CONTENT_HASH" in sql_upper:
        suggestions.append("CREATE INDEX idx_scan_hash ON scanned_files(scan_id, content_hash);")
    if "full_table_scan" in flags and "WHERE SCAN_ID=" in sql_upper.replace(" ", ""):
        suggestions.append("CREATE INDEX idx_scan_id ON scanned_files(scan_id);  -- if not present")
    return suggestions


def _run(conn: sqlite3.Connection, scan_id: int) -> dict:
    by_query = []
    all_suggestions: set[str] = set()
    for name, sql, params_template in QUERIES:
        # Fill in placeholders
        params = []
        if "scan_id" in params_template:
            params.append(scan_id)
        if "cutoff" in params_template:
            params.append("2025-01-01")
        try:
            t0 = time.perf_counter()
            plan = _explain(conn, sql, tuple(params))
            explain_time = time.perf_counter() - t0
        except sqlite3.OperationalError as e:
            by_query.append({
                "name": name,
                "error": str(e),
                "plan": [],
                "red_flags": [],
                "suggested_indexes": [],
            })
            continue
        flags = _classify(plan)
        sugg = _suggest_indexes(name, sql, flags)
        all_suggestions.update(sugg)
        by_query.append({
            "name": name,
            "sql": sql,
            "plan": [r.get("detail", "") for r in plan],
            "red_flags": flags,
            "suggested_indexes": sugg,
            "explain_time_ms": round(explain_time * 1000, 2),
        })
    return {"queries": by_query, "all_suggestions": sorted(all_suggestions)}


def _print_report(result: dict, indexes: list[dict]) -> None:
    print()
    print("=" * 80)
    print("EXISTING INDEXES ON scanned_files")
    print("=" * 80)
    if not indexes:
        print("  (none)")
    for idx in indexes:
        print(f"  {idx['name']}: {idx['sql']}")
    print()
    print("=" * 80)
    print("QUERY PLANS")
    print("=" * 80)
    for q in result["queries"]:
        print()
        print(f"## {q['name']}")
        if "error" in q:
            print(f"   ⚠ error: {q['error']}")
            continue
        flag_str = ", ".join(q["red_flags"]) if q["red_flags"] else "OK"
        print(f"   flags: {flag_str}")
        for line in q["plan"]:
            marker = "  ⚠ " if any(r[0] in line for r in RED_FLAGS) else "    "
            print(f"  {marker}{line}")
        if q["suggested_indexes"]:
            print("   Suggested:")
            for s in q["suggested_indexes"]:
                print(f"     {s}")
    print()
    print("=" * 80)
    print("CONSOLIDATED SUGGESTIONS (deduplicated)")
    print("=" * 80)
    if not result["all_suggestions"]:
        print("  No new indexes suggested — existing indexes cover the hot queries.")
    else:
        for s in result["all_suggestions"]:
            print(f"  {s}")
    print()


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite DB to audit.",
    )
    ap.add_argument(
        "--scan-id",
        type=int,
        help="scan_id to plan against (default: largest in DB).",
    )
    args = ap.parse_args(list(argv) if argv else None)

    if not Path(args.db).exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 2

    uri = f"file:{args.db}?mode=ro&cache=shared"
    conn = sqlite3.connect(uri, uri=True)
    try:
        scan_id = _resolve_scan_id(conn, args.scan_id)
        print(f"DB: {args.db}")
        print(f"scan_id: {scan_id}")
        indexes = _list_indexes(conn)
        result = _run(conn, scan_id)
        _print_report(result, indexes)

        ts = time.strftime("%Y%m%d-%H%M%S")
        out_path = f"explain_audit_{ts}.json"
        with open(out_path, "w") as f:
            json.dump({
                "db": args.db,
                "scan_id": scan_id,
                "existing_indexes": indexes,
                **result,
            }, f, indent=2)
        print(f"JSON sidecar: {out_path}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
