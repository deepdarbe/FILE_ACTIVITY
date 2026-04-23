"""Tests for issue #36: Parquet staging + DuckDB COPY ingest.

Coverage:
  * Test 1 — append + flush ingests rows into ``scanned_files``.
  * Test 2 — buffer that bypasses flush (orphan parquet) is replayed by a
    fresh ``ParquetStager.replay_orphans()`` call.
  * Test 3 — pyarrow missing simulation -> stager.available=False, append()
    falls back to ``Database.bulk_insert_scanned_files``.
"""
from __future__ import annotations

import importlib
import os
import sys

import pytest

# Repo root on sys.path (mirrors test_scan_summary_v2.py).
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.storage import staging as staging_mod  # noqa: E402


pytest.importorskip("duckdb", reason="DuckDB required for parquet ingest tests")


def _build_db(tmp_path) -> tuple[Database, int, int]:
    db_path = tmp_path / "test.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) VALUES(?, ?, ?)",
            ("src", "/tmp/src", "/tmp/arch"),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'running')",
            (source_id,),
        )
        scan_id = cur.lastrowid
    return db, source_id, scan_id


def _make_records(source_id: int, scan_id: int, n: int) -> list[dict]:
    return [
        {
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": f"/tmp/src/file_{i:06d}.dat",
            "relative_path": f"file_{i:06d}.dat",
            "file_name": f"file_{i:06d}.dat",
            "extension": "dat",
            "file_size": 1024 + i,
            "creation_time": "2026-01-01 00:00:00",
            "last_access_time": "2026-04-01 00:00:00",
            "last_modify_time": "2026-04-15 00:00:00",
            "owner": f"user{i % 5}",
            "attributes": 0,
        }
        for i in range(n)
    ]


def _staging_config(tmp_path, **overrides) -> dict:
    cfg = {
        "scanner": {
            "parquet_staging": {
                "enabled": True,
                "flush_rows": 50_000,
                "flush_seconds": 30,
                "staging_dir": str(tmp_path / "staging"),
            }
        }
    }
    cfg["scanner"]["parquet_staging"].update(overrides)
    return cfg


def test_append_and_flush_ingests_all_rows(tmp_path):
    pytest.importorskip("pyarrow")
    db, source_id, scan_id = _build_db(tmp_path)
    cfg = _staging_config(tmp_path)
    stager = staging_mod.ParquetStager(db, cfg)
    if not stager.available:
        pytest.skip(f"Stager not available: {stager._init_error}")

    records = _make_records(source_id, scan_id, 10_000)
    # Append in chunks; flush_rows=50k means no auto-flush during append.
    for chunk_start in range(0, len(records), 1000):
        stager.append(records[chunk_start:chunk_start + 1000])
    ingested = stager.flush()
    assert ingested == 10_000

    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM scanned_files WHERE scan_id = ?",
            (scan_id,),
        )
        assert cur.fetchone()["cnt"] == 10_000

    # No leftover parquet files.
    leftover = [f for f in os.listdir(tmp_path / "staging") if f.endswith(".parquet")]
    assert leftover == []


def test_replay_orphans_ingests_leftover_parquet(tmp_path):
    pytest.importorskip("pyarrow")
    db, source_id, scan_id = _build_db(tmp_path)
    cfg = _staging_config(tmp_path)

    # Write a parquet file directly (simulating a crash AFTER write but
    # BEFORE the DuckDB ingest cleared the file).
    stager = staging_mod.ParquetStager(db, cfg)
    if not stager.available:
        pytest.skip(f"Stager not available: {stager._init_error}")
    records = _make_records(source_id, scan_id, 2_500)
    orphan_path = stager._make_parquet_path()
    stager._write_parquet(records, orphan_path)
    assert os.path.exists(orphan_path)

    # Drop the original instance, simulate process restart.
    del stager

    # New instance + replay.
    fresh = staging_mod.ParquetStager(db, cfg)
    assert fresh.available
    replayed = fresh.replay_orphans()
    assert replayed == 2_500

    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM scanned_files WHERE scan_id = ?",
            (scan_id,),
        )
        assert cur.fetchone()["cnt"] == 2_500

    # Orphan parquet removed after successful ingest.
    assert not os.path.exists(orphan_path)


def test_pyarrow_missing_falls_back(tmp_path, monkeypatch):
    db, source_id, scan_id = _build_db(tmp_path)
    cfg = _staging_config(tmp_path)

    # Reload the module with pyarrow imports forced to fail. We swap the
    # module-level flags rather than re-importing, since pyarrow is already
    # in sys.modules and re-import wouldn't re-trigger the ImportError path.
    monkeypatch.setattr(staging_mod, "_HAVE_PYARROW", False, raising=True)
    monkeypatch.setattr(staging_mod, "pa", None, raising=False)
    monkeypatch.setattr(staging_mod, "pq", None, raising=False)
    # Reset one-time warning so this test exercises the WARNING branch.
    monkeypatch.setattr(staging_mod, "_warned_pyarrow_missing", False, raising=True)

    stager = staging_mod.ParquetStager(db, cfg)
    assert stager.available is False
    assert "pyarrow" in (stager._init_error or "")

    # append() without pyarrow must fall back to bulk_insert_scanned_files.
    records = _make_records(source_id, scan_id, 100)
    stager.append(records)

    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM scanned_files WHERE scan_id = ?",
            (scan_id,),
        )
        assert cur.fetchone()["cnt"] == 100

    # No parquet files written when stager is unavailable.
    staging_dir = tmp_path / "staging"
    if staging_dir.exists():
        assert [f for f in os.listdir(staging_dir) if f.endswith(".parquet")] == []
