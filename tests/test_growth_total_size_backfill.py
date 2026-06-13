"""Regression test for issue #291: growth analysis empty / flat size series.

Root cause (confirmed on the customer's 31 GB / 2.9M-file box): `complete_scan_run`
writes the `scan_runs.total_files` / `total_size` COLUMNS right after the MFT walk
— which runs BEFORE the size-enrich pass — so `total_size` lands as 0 and is never
updated. `get_growth_stats` reads `MAX(total_size)` from those columns, so the
growth size series was flat-zero even though `summary_json` carried the real total
(17.8 TB on the box).

Fix: `compute_scan_summary` now back-fills the columns from the authoritative
post-enrich totals it already computes, in the same UPDATE that persists
`summary_json`. These tests pin that behaviour. No fastapi needed.
"""

from __future__ import annotations

from src.storage.database import Database


def _seed(db, sizes, *, started_at="2026-05-26 21:11:17"):
    """One source + one completed scan + scanned_files with the given sizes.

    Simulates the bug by forcing the scan_runs.total_size column to 0 (as
    complete_scan_run would when sizes are still un-enriched at walk time).
    Returns the scan_id.
    """
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")
    scan_id = db.create_scan_run(1)
    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE scan_runs SET status='completed', started_at=?, "
            "total_files=?, total_size=0 WHERE id=?",
            (started_at, len(sizes), scan_id),
        )
        cur.executemany(
            "INSERT INTO scanned_files"
            "(source_id, scan_id, file_name, file_path, relative_path, "
            " extension, file_size, last_access_time) "
            "VALUES (1, ?, ?, ?, ?, 'dat', ?, '2026-05-01 00:00:00')",
            [(scan_id, f"f{i}.dat", f"f{i}.dat", f"f{i}.dat", s)
             for i, s in enumerate(sizes)],
        )
    return scan_id


def test_compute_scan_summary_backfills_total_size(tmp_path):
    db = Database({"path": str(tmp_path / "g.db")})
    db.connect()
    scan_id = _seed(db, [1000, 2000, 3000])

    # Precondition: the column is the buggy 0 even though rows have real sizes.
    with db.get_read_cursor() as cur:
        before = cur.execute(
            "SELECT total_files, total_size FROM scan_runs WHERE id=?", (scan_id,)
        ).fetchone()
    assert before["total_size"] == 0

    summary = db.compute_scan_summary(scan_id)
    assert summary["total_size"] == 6000  # SUM of the row sizes

    # The column is now back-filled to match summary_json (the fix).
    with db.get_read_cursor() as cur:
        after = cur.execute(
            "SELECT total_files, total_size FROM scan_runs WHERE id=?", (scan_id,)
        ).fetchone()
    assert after["total_size"] == 6000
    assert after["total_files"] == 3
    db.close()


def test_growth_stats_size_series_nonzero_after_summary(tmp_path):
    """End-to-end: after compute_scan_summary, get_growth_stats reports the real
    total_size instead of 0 (the customer-visible symptom)."""
    db = Database({"path": str(tmp_path / "g2.db")})
    db.connect()
    scan_id = _seed(db, [10, 20, 70])  # total 100
    db.compute_scan_summary(scan_id)

    g = db.get_growth_stats(1)
    assert g["total_scans"] == 1
    # The daily bucket for the scan day must carry the real size, not 0.
    daily = g["daily"]
    assert daily, "expected at least one daily growth point"
    assert daily[-1]["total_size"] == 100
    assert daily[-1]["total_files"] == 3
    db.close()
