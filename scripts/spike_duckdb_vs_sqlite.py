"""Spike benchmark — DuckDB ATTACH vs plain SQLite for the same aggregate.

One-off measurement script for D2 (docs/architecture/audit-2026-04-28.md).
Builds the quick e2e corpus, scans it, then runs the duplicate-group
aggregate via both engines under identical input. Reports per-query
timings (median over N runs) so the audit's "no perf win" claim can be
validated against this codebase rather than asserted.

Run:
    python scripts/spike_duckdb_vs_sqlite.py
"""

from __future__ import annotations

import statistics
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.scanner.file_scanner import FileScanner
from src.storage.analytics import AnalyticsEngine
from src.storage.database import Database
from tests.fixtures.generate_corpus import generate_corpus


SCAN_CFG = {
    "batch_size": 500,
    "skip_hidden": False,
    "skip_system": False,
    "exclude_patterns": ["_owners.json"],
    "read_owner": False,
}


def _build_db(workdir: Path, *, quick: bool) -> tuple[Database, int, int]:
    corpus = workdir / "corpus"
    corpus.mkdir()
    manifest = generate_corpus(corpus, quick=quick)

    db_path = workdir / "fa.db"
    db = Database({"path": str(db_path)})
    db.connect()

    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES (?, ?)",
            ("spike", str(corpus)),
        )
        source_id = cur.lastrowid

    cfg = dict(SCAN_CFG)
    cfg["reports"] = {"output_dir": str(workdir / "reports")}
    scanner = FileScanner(db, cfg)
    result = scanner.scan_source(source_id, "spike", str(corpus))
    assert result["status"] == "completed", result

    with db.get_cursor() as cur:
        cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        scan_id = cur.execute(
            "SELECT MAX(id) FROM scan_runs WHERE source_id = ?", (source_id,)
        ).fetchone()
        scan_id = scan_id["MAX(id)"] if isinstance(scan_id, dict) else scan_id[0]

    rows = db.get_read_cursor()
    with rows as cur:
        n = cur.execute(
            "SELECT COUNT(*) AS n FROM scanned_files WHERE source_id = ?",
            (source_id,),
        ).fetchone()
    total = n["n"] if isinstance(n, dict) else n[0]
    return db, source_id, scan_id, total


def _bench(label: str, fn, runs: int) -> dict:
    samples = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return {
        "label": label,
        "runs": runs,
        "median_ms": statistics.median(samples),
        "p10_ms": min(samples),
        "p90_ms": max(samples),
        "samples_ms": [round(s, 2) for s in samples],
    }


def _duckdb_call(engine: AnalyticsEngine, scan_id: int):
    return engine.get_duplicate_groups(
        scan_id=scan_id, min_size=1024, page=1, page_size=50
    )


def _sqlite_call(db: Database, source_id: int, scan_id: int):
    return db.get_duplicate_groups(
        source_id=source_id, scan_id=scan_id, min_size=1024,
        page=1, page_size=50,
    )


def _duckdb_ext(engine: AnalyticsEngine, source_id: int, scan_id: int):
    return engine.get_files_by_extension(
        source_id=source_id, scan_id=scan_id, extension="py",
        limit=50, offset=0,
    )


def _sqlite_ext(db: Database, source_id: int, scan_id: int):
    return db.get_files_by_extension(
        source_id=source_id, scan_id=scan_id, extension="py",
        limit=50, offset=0,
    )


def main() -> int:
    runs = 8
    print(f"# DuckDB ATTACH vs SQLite — duplicate aggregate, {runs} runs each\n")

    with tempfile.TemporaryDirectory(prefix="spike_") as t:
        workdir = Path(t)
        db, source_id, scan_id, total = _build_db(workdir, quick=False)
        print(f"corpus rows: {total}, scan_id={scan_id}, source_id={source_id}\n")

        engine = AnalyticsEngine(db.db_path, {"enabled": True})
        try:
            results = [
                ("dup-groups (CTE+GROUP BY)",
                 _bench("DuckDB ATTACH",
                        lambda: _duckdb_call(engine, scan_id), runs),
                 _bench("Direct SQLite",
                        lambda: _sqlite_call(db, source_id, scan_id), runs)),
                ("ext-drilldown (WINDOW COUNT)",
                 _bench("DuckDB ATTACH",
                        lambda: _duckdb_ext(engine, source_id, scan_id), runs),
                 _bench("Direct SQLite",
                        lambda: _sqlite_ext(db, source_id, scan_id), runs)),
            ]
        finally:
            engine.close()

        for label, duck, sqlite in results:
            print(f"\n## {label}")
            print(f"{'engine':<28} | {'median':>8} | {'min':>8} | {'max':>8}")
            print(f"{'-'*28}-+-{'-'*9}-+-{'-'*9}-+-{'-'*9}")
            for r in (duck, sqlite):
                print(
                    f"{r['label']:<28} | "
                    f"{r['median_ms']:>6.2f} ms | "
                    f"{r['p10_ms']:>6.2f} ms | "
                    f"{r['p90_ms']:>6.2f} ms"
                )
            ratio = duck["median_ms"] / sqlite["median_ms"]
            print(
                f"DuckDB / SQLite = {ratio:.1f}× "
                f"({'slower' if ratio > 1 else 'faster'})"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
