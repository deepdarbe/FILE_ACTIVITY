"""Slow-endpoint batch 2 (part A) — O(1) probe + read-pool fixes.

Behavioural pins for two hot read paths that used the WRITER connection and a
full-scan COUNT:
* ``has_access_log_data`` — full ``COUNT(*)`` → O(1) ``SELECT 1 … LIMIT 1``.
* ``get_db_stats`` — writer cursor → read cursor (no scan-time contention).

sqlite only; no fastapi needed.
"""

from __future__ import annotations

import pytest

from src.storage.database import Database


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "b2.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")
    yield d
    d.close()


def test_has_access_log_data_empty_then_present(db):
    assert db.has_access_log_data() is False
    db.bulk_insert_access_logs([{
        "source_id": 1, "username": "u", "domain": "D",
        "file_path": r"E:\a.txt", "file_name": "a.txt", "extension": "txt",
        "access_type": "read", "access_time": "2026-07-17 10:00:00",
        "client_ip": None, "file_size": 0, "event_id": 4663,
    }])
    assert db.has_access_log_data() is True


def test_get_db_stats_shape(db):
    st = db.get_db_stats()
    assert "error" not in st
    for k in ("db_size", "wal_size", "total_disk",
              "scanned_files_count", "scan_runs_count", "sources_count"):
        assert k in st
    assert st["sources_count"] == 1
    assert st["scanned_files_count"] == 0
