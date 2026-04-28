"""Tests for partial summary v2 — issue #181 Track B1.

Covers:

* Builder unit behaviour (absorb_batch, increment_anomaly, render).
* Bucket boundary correctness (size + age).
* Top-N truncation + heap eviction.
* Memory ceiling under stress.
* DB persistence path (flush_to_db).
* Backward-compat migration helper.
* HTTP API endpoints (/api/sources/{id}/partial-summary,
  /api/scans/{id}/partial-summary).
* Cache busting via partial_updated_at.
"""

from __future__ import annotations

import json
import os
import sys
import tracemalloc
from datetime import datetime, timedelta, timezone
from typing import List

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.analyzer.partial_summary_v2 import (  # noqa: E402
    PartialSummaryV2Builder,
    _empty_v2_payload,
    _v1_to_v2,
)
from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    cfg = {"path": str(tmp_path / "v2.db")}
    database = Database(cfg)
    database.connect()
    yield database
    database.close()


def _seed_source_and_scan(database, status: str = "running"):
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("v2_src", "/tmp/v2"),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, ?)",
            (source_id, status),
        )
        scan_id = cur.lastrowid
    return source_id, scan_id


def _row(file_path: str, ext: str = "txt", size: int = 1024,
         owner: str = "alice", mtime=None) -> dict:
    return {
        "file_path": file_path,
        "extension": ext,
        "file_size": size,
        "owner": owner,
        "last_modify_time": mtime,
    }


# ---------------------------------------------------------------------------
# Builder behaviour
# ---------------------------------------------------------------------------


def test_absorb_batch_updates_by_extension(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)

    b.absorb_batch([
        _row("C:/data/a.jpg", ext="jpg", size=2_000),
        _row("C:/data/b.jpg", ext="jpg", size=3_000),
        _row("C:/data/c.png", ext="png", size=1_500),
    ])
    b.absorb_batch([
        _row("C:/data/d.jpg", ext="jpg", size=4_000),
    ])

    payload = b.render(scan_state="db_writing")
    by_ext = {e["ext"]: e for e in payload["summary"]["by_extension"]}
    assert by_ext["jpg"]["count"] == 3
    assert by_ext["jpg"]["size_bytes"] == 9_000
    assert by_ext["png"]["count"] == 1


def test_absorb_batch_size_buckets(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)

    b.absorb_batch([
        _row("C:/a", size=512),                 # <1MB
        _row("C:/b", size=5 * 1024 * 1024),     # 1-10MB
        _row("C:/c", size=50 * 1024 * 1024),    # 10-100MB
        _row("C:/d", size=500 * 1024 * 1024),   # 100-1GB
        _row("C:/e", size=2 * 1024 * 1024 * 1024),  # >1GB
    ])
    payload = b.render(scan_state="db_writing")
    sb = payload["summary"]["size_buckets"]
    assert sb["<1MB"] == 1
    assert sb["1-10MB"] == 1
    assert sb["10-100MB"] == 1
    assert sb["100-1GB"] == 1
    assert sb[">1GB"] == 1


def test_absorb_batch_skips_zero_size_in_buckets(db):
    """MFT phase: file_size=0 must not pollute size_buckets."""
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([
        _row(f"C:/m{i}", size=0) for i in range(50)
    ])
    payload = b.render(scan_state="mft_phase")
    assert all(v == 0 for v in payload["summary"]["size_buckets"].values())
    # Counts still tick up — we know the file exists, just not its size.
    assert payload["progress"]["files_so_far"] == 50


def test_absorb_batch_skips_none_mtime_in_age_buckets(db):
    """Rows without a last_modify_time leave age buckets at zero."""
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([_row(f"C:/n{i}", size=10, mtime=None) for i in range(20)])
    payload = b.render(scan_state="mft_phase")
    assert all(v == 0 for v in payload["summary"]["age_buckets"].values())


def test_age_buckets_populated_after_enrich(db):
    """When mtime is supplied we pick the right bucket."""
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        _row("C:/x1", size=100, mtime=now - timedelta(days=10)),  # <30d
        _row("C:/x2", size=100, mtime=now - timedelta(days=45)),  # 30-60d
        _row("C:/x3", size=100, mtime=now - timedelta(days=75)),  # 60-90d
        _row("C:/x4", size=100, mtime=now - timedelta(days=120)), # 90-180d
        _row("C:/x5", size=100, mtime=now - timedelta(days=300)), # 180-365d
        _row("C:/x6", size=100, mtime=now - timedelta(days=400)), # >365d
    ]
    b.absorb_batch(rows)
    ab = b.render(scan_state="enrich")["summary"]["age_buckets"]
    assert ab["<30d"] == 1
    assert ab["30-60d"] == 1
    assert ab["60-90d"] == 1
    assert ab["90-180d"] == 1
    assert ab["180-365d"] == 1
    assert ab[">365d"] == 1


def test_increment_anomaly_known_keys(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.increment_anomaly("naming", 3)
    b.increment_anomaly("extension")  # default count=1
    b.increment_anomaly("ransomware", 2)
    b.increment_anomaly("does_not_exist", 99)  # silently ignored
    a = b.render(scan_state="db_writing")["summary"]["anomalies_so_far"]
    assert a == {"naming": 3, "extension": 1, "ransomware": 2}


def test_render_round_trips_through_json(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([
        _row("C:/Şehir/türkçe.txt", ext="txt", size=500, owner="ayşe"),
    ])
    out = b.render(scan_state="db_writing", active_dir="C:/Şehir")
    blob = json.dumps(out, ensure_ascii=False)
    parsed = json.loads(blob)
    assert parsed["progress"]["active_dir"] == "C:/Şehir"
    assert parsed["summary"]["by_owner"][0]["owner"] == "ayşe"


def test_render_caps_each_dict_at_20(db):
    """Internally the builder may hold up to 1000 entries; render must
    expose at most 20.
    """
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    rows = []
    # 50 distinct extensions × 5 files each — internal dict has 50 keys.
    for i in range(50):
        for j in range(5 + (i % 3)):
            rows.append(_row(f"C:/dir{i}/f{j}.x{i}", ext=f"x{i}",
                             size=100, owner=f"u{i}"))
    b.absorb_batch(rows)
    payload = b.render(scan_state="db_writing")
    assert len(payload["summary"]["by_extension"]) == 20
    assert len(payload["summary"]["by_directory"]) == 20
    assert len(payload["summary"]["by_owner"]) == 20


def test_top_paths_keeps_largest_10(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    rows = [_row(f"C:/p{i}", size=i * 1000) for i in range(1, 25)]
    b.absorb_batch(rows)
    paths = b.render(scan_state="db_writing")["summary"]["top_paths_by_size"]
    assert len(paths) == 10
    sizes = [p["size_bytes"] for p in paths]
    # Descending, and should be the top 10 (15000..24000)
    assert sizes == sorted(sizes, reverse=True)
    assert sizes[0] == 24000
    assert sizes[-1] == 15000


def test_flush_to_db_writes_partial_summary_json(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([_row("C:/q1", size=100)])
    b.flush_to_db(scan_state="db_writing", rate_per_sec=42.5,
                  active_dir="C:/q")
    with db.get_cursor() as cur:
        row = cur.execute(
            "SELECT partial_summary_json, partial_updated_at "
            "FROM scan_runs WHERE id=?",
            (scan_id,),
        ).fetchone()
    assert row is not None
    assert row["partial_summary_json"]
    assert row["partial_updated_at"]
    parsed = json.loads(row["partial_summary_json"])
    assert parsed["schema_version"] == 2
    assert parsed["progress"]["files_so_far"] == 1
    assert parsed["progress"]["rate_per_sec"] == pytest.approx(42.5)
    assert parsed["progress"]["active_dir"] == "C:/q"


def test_flush_to_db_uses_retry_protected_path(db, monkeypatch):
    """Simulate a transient sqlite3.OperationalError on the first
    write; the v2 builder must NOT propagate the exception (it routes
    through ``save_scan_partial_summary`` which already swallows
    legacy-column errors and the writer's busy_timeout handles transient
    locks). Best-effort persistence is the contract.
    """
    import sqlite3 as _sqlite

    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([_row("C:/r1", size=100)])

    calls = {"n": 0}
    original = db.save_scan_partial_summary

    def faulty(scan_id_arg, payload):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _sqlite.OperationalError("database is locked")
        return original(scan_id_arg, payload)

    monkeypatch.setattr(db, "save_scan_partial_summary", faulty)
    # First flush hits the simulated busy lock — must not raise.
    b.flush_to_db(scan_state="db_writing", rate_per_sec=1.0)
    # Second flush should succeed via the original path.
    b.flush_to_db(scan_state="db_writing", rate_per_sec=2.0)
    with db.get_cursor() as cur:
        row = cur.execute(
            "SELECT partial_summary_json FROM scan_runs WHERE id=?",
            (scan_id,),
        ).fetchone()
    assert row["partial_summary_json"] is not None


def test_v1_to_v2_migration_produces_valid_v2_dict():
    v1 = {
        "total_files": 1234,
        "total_size": 567890,
        "unique_owners": 5,
        "top_extensions": [
            {"ext": "pdf", "count": 600, "size": 400000},
            {"ext": "docx", "count": 300, "size": 100000},
        ],
        "size_buckets": {"tiny": 100, "small": 200},
        "age_buckets": {"30d": 50},
        "is_partial": True,
        "computed_at": "2026-04-28T19:30:00",
    }
    v2 = _v1_to_v2(v1)
    assert v2["schema_version"] == 2
    assert v2["progress"]["files_so_far"] == 1234
    assert v2["progress"]["size_so_far_bytes"] == 567890
    by_ext = v2["summary"]["by_extension"]
    assert by_ext[0] == {"ext": "pdf", "count": 600, "size_bytes": 400000}
    # All v2-required keys exist.
    for key in ("by_directory", "by_owner", "size_buckets",
                "age_buckets", "anomalies_so_far", "top_paths_by_size"):
        assert key in v2["summary"]


def test_v1_to_v2_idempotent_on_v2_input():
    v2 = _empty_v2_payload()
    out = _v1_to_v2(v2)
    assert out is v2  # passthrough


def test_schema_version_is_exactly_2(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    out = b.render(scan_state="db_writing")
    assert out["schema_version"] == 2
    # Even the empty fallback payload says 2.
    assert _empty_v2_payload()["schema_version"] == 2


def test_computed_at_is_iso8601(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    out = b.render(scan_state="db_writing")
    # Parseable as ISO 8601 (no offset suffix; UTC by convention).
    parsed = datetime.fromisoformat(out["computed_at"])
    assert parsed.year >= 2026


def test_empty_absorb_batch_is_noop(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([])
    out = b.render(scan_state="db_writing")
    assert out["progress"]["files_so_far"] == 0
    assert out["summary"]["by_extension"] == []


def test_memory_ceiling_under_stress(db):
    """100-row stress test (mirrors the 100-row checkpoint mentioned in
    the spec) — builder memory should stay well under 5 MB."""
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)

    tracemalloc.start()
    snap_before = tracemalloc.take_snapshot()

    rows = []
    for i in range(100):
        rows.append(_row(
            f"C:/data/x{i % 7}/file{i}.{['jpg','png','txt','pdf'][i % 4]}",
            ext=["jpg", "png", "txt", "pdf"][i % 4],
            size=1000 + i * 100,
            owner=f"user{i % 5}",
        ))
    b.absorb_batch(rows)
    b.render(scan_state="db_writing")

    snap_after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    diffs = snap_after.compare_to(snap_before, "lineno")
    total_growth = sum(d.size_diff for d in diffs)
    assert total_growth < 5 * 1024 * 1024, (
        f"Builder grew by {total_growth/1024:.1f} KiB on 100 rows; "
        "expected <5 MiB"
    )


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


def _make_app(database):
    from fastapi.testclient import TestClient
    from src.dashboard.api import create_app
    cfg = {
        "dashboard": {
            "host": "127.0.0.1", "port": 0,
            "auth": {"enabled": False},
        },
    }
    app = create_app(database, cfg)
    return TestClient(app)


def test_endpoint_404_when_no_scan_for_source(db):
    """No scan_runs row for the given source -> 404."""
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("solo", "/tmp/solo"),
        )
        source_id = cur.lastrowid
    client = _make_app(db)
    resp = client.get(f"/api/sources/{source_id}/partial-summary")
    assert resp.status_code == 404


def test_endpoint_returns_v2_dict_when_present(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([
        _row("C:/data/a.jpg", ext="jpg", size=4096),
        _row("C:/data/b.jpg", ext="jpg", size=8192),
    ])
    b.flush_to_db(scan_state="db_writing", rate_per_sec=99.0,
                  active_dir="C:/data")
    client = _make_app(db)
    resp = client.get(f"/api/sources/{source_id}/partial-summary")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["schema_version"] == 2
    assert data["progress"]["files_so_far"] == 2
    assert data["progress"]["active_dir"] == "C:/data"
    by_ext = data["summary"]["by_extension"]
    assert by_ext[0]["ext"] == "jpg"
    assert by_ext[0]["count"] == 2


def test_endpoint_cache_busts_on_partial_updated_at_change(db):
    """Re-flushing the v2 builder updates ``partial_updated_at`` which
    is the LRU cache key — the second GET MUST return the fresh count.
    """
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([_row("C:/c1", size=100)])
    b.flush_to_db(scan_state="db_writing")
    client = _make_app(db)
    first = client.get(f"/api/sources/{source_id}/partial-summary").json()
    assert first["progress"]["files_so_far"] == 1

    # Force the partial_updated_at column to advance so the cache key
    # changes (sub-second writes on a fast box would otherwise share
    # the same timestamp string).
    import time
    time.sleep(1.05)
    b.absorb_batch([_row("C:/c2", size=200)])
    b.flush_to_db(scan_state="db_writing")
    second = client.get(f"/api/sources/{source_id}/partial-summary").json()
    assert second["progress"]["files_so_far"] == 2
    # Cache buster: the timestamps must differ.
    assert first["partial_updated_at"] != second["partial_updated_at"]


def test_scan_keyed_endpoint_returns_v2(db):
    source_id, scan_id = _seed_source_and_scan(db)
    b = PartialSummaryV2Builder(db, scan_id, source_id)
    b.absorb_batch([_row("C:/sk", size=512)])
    b.flush_to_db(scan_state="db_writing")
    client = _make_app(db)
    resp = client.get(f"/api/scans/{scan_id}/partial-summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["schema_version"] == 2
    assert data["progress"]["files_so_far"] == 1
