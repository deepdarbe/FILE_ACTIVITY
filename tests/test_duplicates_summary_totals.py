"""Regression tests for issue #290: duplicate report empty / too slow at scale.

On the customer's 2.9M-row / 31 GB box the duplicate report tried the DuckDB
path first, which ran for >25 min (OOM-killed) so the request never returned and
the page showed empty — even though the data had 414k duplicate groups.

Fixes pinned here (the routing/index changes are validated on-box; these tests
pin the SQLite-layer correctness):
  * get_duplicate_groups reads its TOTALS from the precomputed scan summary when
    min_size==0 (identical definition), instead of a full GROUP-BY-over-all-rows.
  * a custom min_size still computes the totals live.
  * the paginated groups + per-group files are unchanged in content.

No fastapi needed.
"""

from __future__ import annotations

from src.storage.database import Database


def _seed(db):
    """1 source, 1 completed scan, a known duplicate mix. Returns scan_id.

    a.txt×3 @100  -> group, waste 200, files 3
    b.txt×2 @50   -> group, waste 50,  files 2
    c.txt×2 @0    -> excluded (file_size>0)
    d.txt×1 @70   -> not a duplicate
    Totals (min_size=0): groups=2, files=5, waste=250.
    """
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e','x')")
    scan_id = db.create_scan_run(1)
    with db.get_cursor() as cur:
        cur.execute("UPDATE scan_runs SET status='completed' WHERE id=?", (scan_id,))
        rows = []
        def add(name, size, n):
            for i in range(n):
                rows.append((scan_id, name, f"dir{i}/{name}", f"dir{i}/{name}", size))
        add("a.txt", 100, 3)
        add("b.txt", 50, 2)
        add("c.txt", 0, 2)
        add("d.txt", 70, 1)
        cur.executemany(
            "INSERT INTO scanned_files"
            "(source_id, scan_id, file_name, file_path, relative_path, "
            " extension, file_size, last_access_time) "
            "VALUES (1, ?, ?, ?, ?, 'txt', ?, '2026-05-01 00:00:00')",
            rows,
        )
    return scan_id


def test_totals_from_summary_when_min_size_zero(tmp_path):
    db = Database({"path": str(tmp_path / "d.db")})
    db.connect()
    scan_id = _seed(db)
    summary = db.compute_scan_summary(scan_id)
    # sanity: the summary computed the duplicate totals
    assert summary["duplicate_groups"] == 2
    assert summary["duplicate_waste_size"] == 250

    res = db.get_duplicate_groups(1, min_size=0, page=1, page_size=50)
    assert res["total_groups"] == 2
    assert res["total_files"] == 5
    assert res["total_waste_size"] == 250
    # The totals must equal the precomputed summary (i.e. the summary path).
    assert res["total_groups"] == summary["duplicate_groups"]
    assert res["total_waste_size"] == summary["duplicate_waste_size"]
    db.close()


def test_paginated_groups_content(tmp_path):
    db = Database({"path": str(tmp_path / "d2.db")})
    db.connect()
    scan_id = _seed(db)
    db.compute_scan_summary(scan_id)

    res = db.get_duplicate_groups(1, min_size=0, page=1, page_size=50)
    groups = {g["file_name"]: g for g in res["groups"]}
    assert set(groups) == {"a.txt", "b.txt"}  # c.txt (0B) and d.txt (unique) excluded
    assert groups["a.txt"]["count"] == 3
    assert groups["a.txt"]["waste_size"] == 200
    assert len(groups["a.txt"]["files"]) == 3
    assert groups["b.txt"]["count"] == 2
    # Highest-waste group first (a.txt waste 200 > b.txt waste 50).
    assert res["groups"][0]["file_name"] == "a.txt"
    db.close()


def test_custom_min_size_computes_live(tmp_path):
    """min_size>0 bypasses the summary and computes totals live — b.txt (50B)
    drops out, leaving only a.txt (100B)."""
    db = Database({"path": str(tmp_path / "d3.db")})
    db.connect()
    scan_id = _seed(db)
    db.compute_scan_summary(scan_id)

    res = db.get_duplicate_groups(1, min_size=60, page=1, page_size=50)
    assert res["total_groups"] == 1
    assert res["total_files"] == 3
    assert res["total_waste_size"] == 200
    assert [g["file_name"] for g in res["groups"]] == ["a.txt"]
    db.close()


def test_no_summary_falls_back_to_live(tmp_path):
    """If compute_scan_summary was never run, totals are still correct (live)."""
    db = Database({"path": str(tmp_path / "d4.db")})
    db.connect()
    _seed(db)  # NOTE: no compute_scan_summary call
    res = db.get_duplicate_groups(1, min_size=0, page=1, page_size=50)
    assert res["total_groups"] == 2
    assert res["total_waste_size"] == 250
    db.close()
