"""Tests for #338 — top_creators served from the precomputed scan summary.

/api/growth measured 18.4s on prod because get_top_file_creators ran a live
GROUP BY owner over 2.9M rows per request (SUM(file_size) not covered by any
index -> per-row main-table fetches over a 35 GB file). compute_scan_summary
now precomputes ``top_creators`` (count-sorted, limit 20, same dict shape) and
get_top_file_creators serves unscoped default-window requests from it — the
#290/#295 summary-reuse pattern. Also: analytics.health() probe is now O(1).

No fastapi needed; the health test is duckdb-gated.
"""

from __future__ import annotations

import importlib.util
import json

import pytest

from src.storage.database import Database

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "tc.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")
    sid = d.create_scan_run(1)
    with d.get_cursor() as cur:
        cur.execute("UPDATE scan_runs SET status='completed' WHERE id=?", (sid,))
        rows = []
        # alice: 5 files, bob: 3, carol: 2 (+1 ownerless, excluded)
        for owner, n in (("alice", 5), ("bob", 3), ("carol", 2)):
            for i in range(n):
                rows.append((sid, f"{owner}{i}.txt", owner))
        rows.append((sid, "noowner.txt", None))
        cur.executemany(
            "INSERT INTO scanned_files"
            "(source_id, scan_id, file_name, file_path, relative_path,"
            " extension, file_size, owner) VALUES (1, ?, ?, ?, ?, 'txt', 100, ?)",
            [(s, n_, n_, n_, o) for (s, n_, o) in rows],
        )
    yield d, sid
    d.close()


def test_summary_contains_top_creators(db):
    d, sid = db
    summary = d.compute_scan_summary(sid)
    tc = summary["top_creators"]
    assert [c["owner"] for c in tc] == ["alice", "bob", "carol"]
    assert tc[0] == {
        "owner": "alice", "file_count": 5, "total_size": 500,
        "percentage": pytest.approx(5 / 11 * 100),
    }
    assert summary["summary_json_version"] == 2  # additive key, no bump


def test_fast_path_serves_cached_snapshot(db):
    """External/negative proof: delete rows AFTER compute — the unscoped
    default request must still return the cached values (no live query),
    while scoped and limit>20 requests reflect the delete (live path)."""
    d, sid = db
    d.compute_scan_summary(sid)
    with d.get_cursor() as cur:
        cur.execute(
            "DELETE FROM scanned_files WHERE scan_id=? AND owner='alice'",
            (sid,))

    cached = d.get_top_file_creators(1)
    assert cached[0]["owner"] == "alice"       # cache: pre-delete snapshot
    assert cached[0]["file_count"] == 5

    scoped = d.get_top_file_creators(
        1, owner_scope=("AND owner LIKE ?", ["%a%"]))
    assert all(c["owner"] != "alice" for c in scoped)  # live: post-delete

    live = d.get_top_file_creators(1, limit=25)
    assert all(c["owner"] != "alice" for c in live)    # live: limit>20


def test_limit_slices_cached_list(db):
    d, sid = db
    d.compute_scan_summary(sid)
    top1 = d.get_top_file_creators(1, limit=1)
    assert len(top1) == 1 and top1[0]["owner"] == "alice"


def test_pre_upgrade_summary_falls_back_to_live(db):
    """A v2 summary WITHOUT top_creators (pre-upgrade) must not break the
    endpoint — it falls through to the live GROUP BY."""
    d, sid = db
    d.compute_scan_summary(sid)
    with d.get_cursor() as cur:
        row = cur.execute(
            "SELECT summary_json FROM scan_runs WHERE id=?", (sid,)).fetchone()
        parsed = json.loads(row["summary_json"])
        parsed.pop("top_creators")
        cur.execute("UPDATE scan_runs SET summary_json=? WHERE id=?",
                    (json.dumps(parsed), sid))
    live = d.get_top_file_creators(1)
    assert live[0]["owner"] == "alice" and live[0]["file_count"] == 5


def test_backfill_merges_top_creators_additively(db):
    """#338 FIX 1c: a v2 summary missing the key gets ONLY the key merged
    (no full recompute), version stays 2, other keys untouched, and the
    return count includes the merge."""
    d, sid = db
    d.compute_scan_summary(sid)
    with d.get_cursor() as cur:
        row = cur.execute(
            "SELECT summary_json FROM scan_runs WHERE id=?", (sid,)).fetchone()
        parsed = json.loads(row["summary_json"])
        parsed.pop("top_creators")
        parsed["_sentinel"] = "untouched"
        cur.execute("UPDATE scan_runs SET summary_json=? WHERE id=?",
                    (json.dumps(parsed), sid))

    n = d.backfill_missing_summaries()
    assert n == 1

    merged = d.get_scan_summary(sid)
    assert merged["_sentinel"] == "untouched"          # no full recompute
    assert merged["summary_json_version"] == 2
    assert [c["owner"] for c in merged["top_creators"]] == [
        "alice", "bob", "carol"]


@pytest.mark.skipif(not HAS_DUCKDB, reason="duckdb not installed")
def test_health_probe_is_o1(db, tmp_path):
    """#338 FIX 2: the health probe proves readability without COUNTing
    the table; scanned_files_rows is gone."""
    from src.storage.analytics import AnalyticsEngine
    d, sid = db
    eng = AnalyticsEngine(d.db_path, {"enabled": True})
    if not eng.available:
        pytest.skip(f"DuckDB ATTACH unavailable: {eng._init_error!r}")
    info = eng.health()
    assert info["probe_ok"] is True
    assert info["has_rows"] is True
    assert "scanned_files_rows" not in info
    eng.close()
