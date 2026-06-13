"""Tests for the treemap per-extension "wasted %" feature (punch-list #3).

``get_type_analysis`` gained a ``stale_size`` aggregate (bytes whose
``last_access_time`` is 1+ year old, the same 365-day cutoff the Overview
"stale" KPI uses) and ``TypeAnalyzer.analyze`` derives ``wasted_pct`` from it.
The treemap colours/labels each extension node by that percentage.

These tests need no fastapi — they exercise the storage + analyzer layers
directly, so they run everywhere the suite runs.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.storage.database import Database
from src.analyzer.type_analyzer import TypeAnalyzer


def _ymd(days_ago: int) -> str:
    """A ``YYYY-MM-DD HH:MM:SS`` string ``days_ago`` days in the past."""
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


@pytest.fixture
def seeded(tmp_path):
    """DB with one source, one scan, and a hand-picked file mix.

    docx: one stale (400d) + one fresh (10d)  -> wasted is partial.
    pdf : two stale (800d, 400d), one zero-size stale -> wasted 100% of bytes.
    tmp : one with NULL last_access_time -> never counts as stale.
    """
    db = Database({"path": str(tmp_path / "wasted.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('s1', 'x')")
        scan_id = db.create_scan_run(1)
        rows = [
            # source, scan, name, path, rel, ext, size, last_access
            (1, scan_id, "a.docx", "a.docx", "a.docx", "docx", 1000, _ymd(400)),
            (1, scan_id, "b.docx", "b.docx", "b.docx", "docx", 500, _ymd(10)),
            (1, scan_id, "c.pdf", "c.pdf", "c.pdf", "pdf", 2000, _ymd(800)),
            (1, scan_id, "d.pdf", "d.pdf", "d.pdf", "pdf", 0, _ymd(400)),
            (1, scan_id, "e.tmp", "e.tmp", "e.tmp", "tmp", 100, None),
        ]
        cur.executemany(
            "INSERT INTO scanned_files"
            "(source_id, scan_id, file_name, file_path, relative_path, "
            " extension, file_size, last_access_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    yield db, scan_id
    db.close()


def test_get_type_analysis_includes_stale_size(seeded):
    db, scan_id = seeded
    by = {r["extension"]: r for r in db.get_type_analysis(1, scan_id)}
    # docx: only the 400-day file (1000 B) is stale; the 10-day one is fresh.
    assert by["docx"]["total_size"] == 1500
    assert by["docx"]["stale_size"] == 1000
    # pdf: both files are 1+ year old; bytes = 2000 (+0 for the zero-size one).
    assert by["pdf"]["total_size"] == 2000
    assert by["pdf"]["stale_size"] == 2000


def test_null_last_access_is_not_stale(seeded):
    db, scan_id = seeded
    by = {r["extension"]: r for r in db.get_type_analysis(1, scan_id)}
    # A NULL last_access_time must NOT be counted as stale (it's "unknown",
    # not "old") — mirrors compute_scan_summary's `IS NOT NULL` guard.
    assert by["tmp"]["stale_size"] == 0


def test_wasted_pct_derivation(seeded):
    db, scan_id = seeded
    by = {r["extension"]: r for r in TypeAnalyzer(db).analyze(1, scan_id)}
    assert by["docx"]["wasted_pct"] == pytest.approx(round(1000 / 1500 * 100, 1))
    assert by["pdf"]["wasted_pct"] == 100.0
    assert by["tmp"]["wasted_pct"] == 0.0
    # Formatted helper is present for the (unused-by-treemap-but-handy) UI.
    assert by["docx"]["stale_size_formatted"]


def test_wasted_pct_zero_when_no_bytes(tmp_path):
    """A type whose total_size is 0 must not divide-by-zero -> wasted 0.0."""
    db = Database({"path": str(tmp_path / "z.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('s1', 'x')")
        scan_id = db.create_scan_run(1)
        cur.execute(
            "INSERT INTO scanned_files"
            "(source_id, scan_id, file_name, file_path, relative_path, "
            " extension, file_size, last_access_time) "
            "VALUES (1, ?, 'z.log', 'z.log', 'z.log', 'log', 0, ?)",
            (scan_id, _ymd(800)),
        )
    by = {r["extension"]: r for r in TypeAnalyzer(db).analyze(1, scan_id)}
    assert by["log"]["total_size"] == 0
    assert by["log"]["wasted_pct"] == 0.0
    db.close()
