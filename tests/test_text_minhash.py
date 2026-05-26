"""Tests for src/analyzer/text_minhash.py (MinHash+LSH text near-dup).

The MinHash path needs the optional `datasketch` package; tests that
require it use ``pytest.importorskip``. Everything else — shingling, config
parsing, the graceful no-op when datasketch is absent, and the DB
persist/report round-trip — runs without it.
"""

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.analyzer import text_minhash as tm  # noqa: E402
from src.storage.database import Database  # noqa: E402


def _config(**over):
    cfg = {"text_near_duplicates": {"enabled": True}}
    cfg["text_near_duplicates"].update(over)
    return cfg


def _db(tmp_path):
    db = Database({"path": str(tmp_path / "tnd.db")})
    db.connect()
    return db


def _seed_scan(db):
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('s', '/s')")
        sid = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'completed')",
            (sid,),
        )
        return sid, cur.lastrowid


# ---------------------------------------------------------------------------
# shingling
# ---------------------------------------------------------------------------
def test_word_shingles_basic():
    sh = tm._word_shingles("the quick brown fox jumps over", k=3)
    assert "the quick brown" in sh
    assert "fox jumps over" in sh
    # 6 tokens, k=3 -> 4 shingles
    assert len(sh) == 4


def test_word_shingles_short_text_single_shingle():
    # fewer tokens than k -> the whole text is one shingle
    sh = tm._word_shingles("only two", k=5)
    assert sh == {"only two"}


def test_word_shingles_empty():
    assert tm._word_shingles("   ", k=3) == set()


def test_read_text_missing_file_returns_none():
    assert tm._read_text("/no/such/path.txt", 1024) is None


# ---------------------------------------------------------------------------
# config parsing
# ---------------------------------------------------------------------------
def test_engine_reads_config(tmp_path):
    db = _db(tmp_path)
    engine = tm.TextNearDuplicateEngine(
        db, _config(threshold=0.6, shingle_size=3, extensions=[".TXT", "md"]))
    assert engine.enabled is True
    assert engine.threshold == 0.6
    assert engine.shingle_size == 3
    assert "txt" in engine.extensions and "md" in engine.extensions
    db.close()


# ---------------------------------------------------------------------------
# graceful no-op when datasketch is absent
# ---------------------------------------------------------------------------
def test_compute_noop_when_unavailable(tmp_path):
    db = _db(tmp_path)
    sid, scan_id = _seed_scan(db)
    engine = tm.TextNearDuplicateEngine(db, _config())
    engine._available = False  # simulate datasketch absent, deterministically
    out = engine.compute(scan_id)
    assert out["available"] is False
    assert out["groups"] == 0
    assert out["duplicate_files"] == 0
    db.close()


# ---------------------------------------------------------------------------
# DB persist + report round-trip (no datasketch needed)
# ---------------------------------------------------------------------------
def test_persist_and_get_report(tmp_path):
    db = _db(tmp_path)
    sid, scan_id = _seed_scan(db)
    engine = tm.TextNearDuplicateEngine(db, _config())

    groups_data = [{
        "rows": [
            {"id": 1, "file_path": "/s/a.txt", "file_size": 2000},
            {"id": 2, "file_path": "/s/b.txt", "file_size": 1500},
        ],
        "file_count": 2,
        "total_size": 3500,
        "waste_size": 1500,         # total - max
        "avg_similarity": 0.91,
    }]
    engine._persist(groups_data, scan_id)

    rep = engine.get_report(scan_id, page=1, page_size=50)
    assert rep["total_groups"] == 1
    assert rep["total_waste_size"] == 1500
    assert rep["total_files"] == 2
    g = rep["groups"][0]
    assert g["file_count"] == 2
    assert g["avg_similarity"] == 0.91
    # members sorted by size desc
    assert [m["file_path"] for m in g["files"]] == ["/s/a.txt", "/s/b.txt"]

    # idempotent re-persist clears the old rows
    engine._persist([], scan_id)
    assert engine.get_report(scan_id)["total_groups"] == 0
    db.close()


# ---------------------------------------------------------------------------
# full pipeline (requires datasketch)
# ---------------------------------------------------------------------------
def test_compute_clusters_near_duplicates(tmp_path):
    pytest.importorskip("datasketch")
    db = _db(tmp_path)
    sid, scan_id = _seed_scan(db)

    base = " ".join(f"sentence number {i} about archived shared files"
                    for i in range(80))
    near = base + " with one extra trailing clause appended at the end"
    distinct = " ".join(f"completely different token {i*7} zzz"
                        for i in range(80))

    files = {"a.txt": base, "b.txt": near, "c.txt": distinct}
    rows = []
    with db.get_cursor() as cur:
        for name, content in files.items():
            p = tmp_path / name
            p.write_text(content, encoding="utf-8")
            cur.execute(
                "INSERT INTO scanned_files (source_id, scan_id, file_path, "
                "relative_path, file_name, extension, file_size, owner) "
                "VALUES (?, ?, ?, ?, ?, 'txt', ?, 'u')",
                (sid, scan_id, str(p), name, name, p.stat().st_size),
            )

    engine = tm.TextNearDuplicateEngine(
        db, _config(min_bytes=1, threshold=0.5, shingle_size=4))
    assert engine.available  # importorskip guarantees datasketch present
    stats = engine.compute(scan_id)

    # a.txt and b.txt are near-duplicates; c.txt is not.
    assert stats["groups"] == 1
    rep = engine.get_report(scan_id)
    paths = {m["file_path"] for g in rep["groups"] for m in g["files"]}
    assert any(p.endswith("a.txt") for p in paths)
    assert any(p.endswith("b.txt") for p in paths)
    assert not any(p.endswith("c.txt") for p in paths)
    db.close()
