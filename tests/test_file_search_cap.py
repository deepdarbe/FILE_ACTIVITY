"""#372: Database.search_files must return fast even for a very common term.

A broad substring (e.g. an owner name on hundreds of thousands of files) used to
make COUNT(*) + ORDER BY file_size scan every FTS match — minutes on a 9M-row /
33 GB index — and hang the search box on "Araniyor..." forever. search_files now
caps the FTS match set (_FTS_MATCH_CAP) so the work is bounded. These tests pin
correctness for normal (sub-cap) terms + owner scoping, and that the cap bounds
the total.

Pure sqlite (FTS5 trigram, which the app requires); no fastapi needed.
"""

from __future__ import annotations

import pytest

from src.storage.database import Database


@pytest.fixture
def db(tmp_path):
    d = Database({"path": str(tmp_path / "search.db"),
                  "retention": {"auto_cleanup_on_startup": False}})
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")
        cur.execute("INSERT INTO scan_runs(source_id, status) VALUES(1, 'completed')")
        rows = [
            # (file_name, owner, file_size) — all names contain 'onur'.
            ("onur_a.zip", r"BURCU\onur", 9000),
            ("onur_b.txt", r"BURCU\onur", 10),
            ("onur_c.doc", r"BURCU\ali", 50),
            ("other.log",  r"BURCU\veli", 5),   # no 'onur'
        ]
        for name, owner, size in rows:
            cur.execute(
                "INSERT INTO scanned_files(source_id, scan_id, file_path,"
                " relative_path, file_name, extension, file_size, owner) "
                "VALUES(1, 1, ?, ?, ?, 'x', ?, ?)",
                (r"E:\share\\" + name, name, name, size, owner))
    d.rebuild_fts()
    yield d
    d.close()


def test_search_matches_and_orders_by_size_desc(db):
    res = db.search_files("onur")
    assert res["total"] == 3
    assert [f["file_name"] for f in res["files"]] == [
        "onur_a.zip", "onur_c.doc", "onur_b.txt"]   # 9000 > 50 > 10


def test_no_match_returns_empty(db):
    assert db.search_files("zzzqqq")["total"] == 0


def test_owner_scope_filters_to_the_viewer(db):
    # viewer 'ali' searching 'onur' sees only the file they own.
    res = db.search_files("onur", owner_scope=("AND owner LIKE ?", ["%ali%"]))
    assert res["total"] == 1
    assert res["files"][0]["file_name"] == "onur_c.doc"


def test_cap_bounds_the_result(db, monkeypatch):
    # With a tiny cap, a 3-match term is bounded to the cap — proving a broad
    # term can never scan the whole match set (the hang fix).
    monkeypatch.setattr(Database, "_FTS_MATCH_CAP", 2)
    assert db.search_files("onur")["total"] == 2
