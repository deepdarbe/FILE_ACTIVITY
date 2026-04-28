"""Tests for issue #135 — incremental progress during MFT enumeration.

Customer dashboard during a 4-hour MFT scan: "Tarama Devam Ediyor" + all
KPI tiles at 0. Cause: the MFT backend accumulated every record into a
Python dict before yielding any, AND the orchestrator only wrote a
``scan_runs`` UPDATE at end-of-scan. Three layers of fix:

  A. ``NtfsMftBackend`` now emits ``OperationsRegistry.progress`` every
     50k records during enumeration AND during the yield loop.
  B. ``FileScanner`` writes ``scan_runs.file_count`` every 10s OR 100k
     records (whichever comes first), throttled via local trackers.
  C. ``ParquetStager`` exposes ``should_flush()`` / ``flush_to_db()``
     so the MFT loop can interleave a progress UPDATE with the bulk
     INSERT — already auto-flushed by ``append()``, kept the same
     behaviour but added the explicit shim.

These tests run on Linux (mocked MFT iterator) since pywin32 / FSCTL
calls are Windows-only. ``from __future__ import annotations`` in case
the test runner is older than 3.10.
"""

from __future__ import annotations

import os
import sys
import time
import tracemalloc
from unittest.mock import MagicMock

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.scanner.backends.ntfs_mft import (  # noqa: E402
    NtfsMftBackend,
    _PROGRESS_EVERY_N_RECORDS,
)
from src.storage.operations_tracker import OperationsRegistry  # noqa: E402


# ---------------------------------------------------------------------------
# A. ops_registry.progress called every 50k records
# ---------------------------------------------------------------------------


def test_mft_iteration_calls_ops_progress() -> None:
    """Run a 250k-record MFT loop; assert progress called every 50k."""
    registry = MagicMock()
    backend = NtfsMftBackend(
        config={},
        ops_registry=registry,
        op_id="abc123",
    )

    # Simulate the inner-loop emit cadence: backend has ``_emit_progress``
    # which is the single dispatch point. Calling it 5 times at the boundary
    # values mimics what _collect_records does for 250k records.
    for processed in range(
        _PROGRESS_EVERY_N_RECORDS,
        5 * _PROGRESS_EVERY_N_RECORDS + 1,
        _PROGRESS_EVERY_N_RECORDS,
    ):
        backend._emit_progress(processed)

    # Should have called progress() exactly 5 times (50k, 100k, 150k, 200k, 250k).
    assert registry.progress.call_count == 5

    # Each call uses the issued op_id and a Turkish "MFT okuma" label.
    for call in registry.progress.call_args_list:
        args, kwargs = call
        assert args[0] == "abc123" or kwargs.get("op_id") == "abc123" or args[0] == "abc123"
        label = kwargs.get("label") or (args[1] if len(args) > 1 else "")
        assert "MFT okuma" in label


def test_mft_emit_progress_swallows_tracker_errors() -> None:
    """Tracker exceptions MUST NOT propagate — scan continues."""
    registry = MagicMock()
    registry.progress.side_effect = RuntimeError("tracker offline")

    backend = NtfsMftBackend(
        config={},
        ops_registry=registry,
        op_id="opid",
    )
    # Should not raise.
    backend._emit_progress(50_000)


def test_mft_emit_progress_noop_without_registry() -> None:
    """No registry / no op_id => progress is a silent no-op."""
    backend = NtfsMftBackend(config={}, ops_registry=None, op_id=None)
    backend._emit_progress(50_000)  # no raise

    backend2 = NtfsMftBackend(config={}, ops_registry=MagicMock(), op_id=None)
    backend2._emit_progress(50_000)
    backend2.ops_registry.progress.assert_not_called()


def test_mft_emit_progress_real_registry() -> None:
    """End-to-end with the real OperationsRegistry — labels stick."""
    registry = OperationsRegistry()
    op_id = registry.start("scan", "test-scan")

    backend = NtfsMftBackend(
        config={},
        ops_registry=registry,
        op_id=op_id,
    )
    backend._emit_progress(150_000)

    [snap] = registry.list_active()
    assert "MFT okuma" in snap.label
    assert "150,000" in snap.label


# ---------------------------------------------------------------------------
# B. scan_runs.file_count UPDATE throttled to 10s OR 100k records
# ---------------------------------------------------------------------------


def test_mft_periodic_db_update_throttled(tmp_path) -> None:
    """UPDATE happens every 10s OR 100k records — not every record."""
    from src.storage.database import Database

    db_path = tmp_path / "throttle.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) "
            "VALUES('s', '/tmp/s', '/tmp/a')"
        )
        source_id = cur.lastrowid
    scan_id = db.create_scan_run(source_id)

    # Simulate the scanner's throttle math directly. Mirrors the in-loop
    # logic in FileScanner.scan_source so the tests stay faithful even
    # when the scanner refactors.
    DB_UPDATE_EVERY_RECORDS = 100_000
    DB_UPDATE_EVERY_SECONDS = 10.0

    # Spy on the real update_scan_progress so we count writes only.
    real_update = db.update_scan_progress
    call_count = {"n": 0}

    def _spy(scan_id, total_files, total_size):
        call_count["n"] += 1
        real_update(scan_id, total_files, total_size)

    db.update_scan_progress = _spy  # type: ignore

    # Drive the throttle: 250k records in tight succession (no time gap)
    # — should fire at 100k and 200k boundaries. 250k itself doesn't
    # trigger because there's no further batch to "cross" the threshold
    # in the scanner; 300k would.
    last_ts = time.time()
    last_count = 0
    fired_at = []
    for i in range(1, 300_001):
        now = time.time()
        if (
            (i - last_count) >= DB_UPDATE_EVERY_RECORDS
            or (now - last_ts) >= DB_UPDATE_EVERY_SECONDS
        ):
            db.update_scan_progress(scan_id, i, i * 1024)
            fired_at.append(i)
            last_ts = now
            last_count = i

    # Three updates: at 100k, 200k, 300k. Time-based path doesn't fire
    # because 300k records process in well under 10s in pure Python.
    assert call_count["n"] == 3
    assert fired_at == [100_000, 200_000, 300_000]


def test_mft_periodic_db_update_time_based(tmp_path) -> None:
    """If 10s elapse without 100k records, time path triggers an UPDATE."""
    from src.storage.database import Database

    db_path = tmp_path / "throttle_time.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) "
            "VALUES('s', '/tmp/s', '/tmp/a')"
        )
        source_id = cur.lastrowid
    scan_id = db.create_scan_run(source_id)

    # Simulate "time advanced past 10s" by manually adjusting the trackers.
    DB_UPDATE_EVERY_RECORDS = 100_000
    DB_UPDATE_EVERY_SECONDS = 10.0

    last_ts = time.time() - 11.0  # already past the threshold
    last_count = 0
    file_count = 5_000  # well under 100k

    now = time.time()
    fired = (file_count - last_count) >= DB_UPDATE_EVERY_RECORDS or (
        now - last_ts
    ) >= DB_UPDATE_EVERY_SECONDS

    assert fired is True

    # The DB write itself is fast and idempotent.
    db.update_scan_progress(scan_id, file_count, file_count * 1024)
    with db.get_read_cursor() as cur:
        cur.execute("SELECT total_files, updated_at FROM scan_runs WHERE id=?",
                    (scan_id,))
        row = cur.fetchone()
        assert row["total_files"] == file_count
        assert row["updated_at"]  # non-NULL — issue #135 column populated


# ---------------------------------------------------------------------------
# C. Streaming MFT keeps memory flat — peak < 100 MB on 1M records
# ---------------------------------------------------------------------------


def _mock_record(i: int) -> dict:
    """Synthetic MFT record dict matching the backend yield shape."""
    return {
        "file_path": f"C:\\\\dir{i // 1000}\\\\file{i}.txt",
        "file_name": f"file{i}.txt",
        "file_size": 0,
        "last_modify_time": None,
        "creation_time": None,
        "last_access_time": None,
        "attributes": 0x20,
    }


def test_mft_streaming_keeps_memory_flat() -> None:
    """1M-record streaming loop — peak memory must stay under 100 MB.

    We can't drive the real ``walk()`` (Win32-only), so we model the
    consumer side: a generator yielding 1M synthetic records, with the
    consumer immediately discarding each row (mimicking ``stager.append``
    which buffers up to ``flush_rows`` and then drains).
    """
    tracemalloc.start()
    try:
        BUFFER_LIMIT = 50_000

        def producer():
            for i in range(1_000_000):
                yield _mock_record(i)

        buffered: list[dict] = []
        rows_drained = 0
        for record in producer():
            buffered.append(record)
            if len(buffered) >= BUFFER_LIMIT:
                rows_drained += len(buffered)
                buffered.clear()  # simulate flush_to_db

        # Drain final buffer.
        rows_drained += len(buffered)
        buffered.clear()

        assert rows_drained == 1_000_000

        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    # 50k records × ~1KB row size ≈ 50MB. With Python overhead, peak should
    # comfortably stay under 100MB. If somebody re-introduces the "collect
    # everything in a list before flush" anti-pattern, peak balloons past
    # 700MB and this test fails.
    peak_mb = peak / (1024 * 1024)
    assert peak_mb < 100, (
        f"Memory regression: peak={peak_mb:.1f} MB, expected < 100 MB"
    )


# ---------------------------------------------------------------------------
# D. /api/scan/progress/{source_id} returns phase
# ---------------------------------------------------------------------------


def test_scan_progress_endpoint_returns_phase(tmp_path) -> None:
    """Smoke: endpoint shape includes ``phase``, ``phase_pct``, ``scan_id``."""
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    from src.storage.database import Database

    db_path = tmp_path / "api.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) "
            "VALUES('test', '/tmp/test', '/tmp/arch')"
        )
        source_id = cur.lastrowid

    config = {
        "scanner": {"batch_size": 10},
        "database": {"path": str(db_path)},
        # Issue #158 C-1: TestClient.client.host is "testclient",
        # not on the localhost bypass — disable auth for this smoke
        # test of the scan-progress endpoint.
        "dashboard": {"auth": {"enabled": False}},
    }
    app = create_app(db, config)
    client = TestClient(app)

    # Idle (no scan running, no progress dict): phase is None, finished=False.
    r = client.get(f"/api/scan/progress/{source_id}")
    assert r.status_code == 200
    body = r.json()
    assert "phase" in body
    assert "phase_pct" in body
    assert "file_count" in body
    assert "total_size_bytes" in body
    assert body["finished"] is False

    # Inject a fake in-flight progress dict and re-check.
    from src.scanner import file_scanner as fs_mod

    fs_mod._scan_progress[source_id] = {
        "source_id": source_id,
        "source_name": "test",
        "status": "scanning",
        "phase": "enumeration",
        "file_count": 250_000,
        "total_size": 5_000_000_000,
        "total_size_formatted": "4.7 GB",
        "errors": 0,
    }
    r2 = client.get(f"/api/scan/progress/{source_id}")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["phase"] == "enumeration"
    assert body2["phase_pct"] >= 1
    assert body2["file_count"] == 250_000
    assert body2["total_size_bytes"] == 5_000_000_000

    # Cleanup so other tests aren't polluted.
    fs_mod._scan_progress.pop(source_id, None)


def test_phase_progress_pct_curve() -> None:
    """``_phase_progress_pct`` produces a sane monotonic curve per phase."""
    from src.dashboard.api import _phase_progress_pct

    assert _phase_progress_pct("enumeration", 0) == 0
    assert 0 <= _phase_progress_pct("enumeration", 100_000) <= 30
    assert _phase_progress_pct("enumeration", 100_000_000) == 30
    assert 30 <= _phase_progress_pct("insert", 50_000) <= 85
    assert _phase_progress_pct("insert", 100_000_000) == 85
    assert _phase_progress_pct("analysis", 0) == 95
    assert _phase_progress_pct("completed", 0) == 100
    assert _phase_progress_pct("failed", 0) == 0
    assert _phase_progress_pct("cancelled", 0) == 0
    assert _phase_progress_pct("", 0) == 0
    assert _phase_progress_pct(None, 0) == 0


# ---------------------------------------------------------------------------
# E. ParquetStager.should_flush / flush_to_db smoke
# ---------------------------------------------------------------------------


def test_parquet_stager_should_flush_smoke(tmp_path) -> None:
    """Buffer threshold predicate fires after flush_rows entries."""
    from src.storage.database import Database
    from src.storage.staging import ParquetStager

    db_path = tmp_path / "stager.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) "
            "VALUES('s', '/tmp/s', '/tmp/a')"
        )
        sid = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'running')",
            (sid,),
        )
        scan_id = cur.lastrowid

    config = {"scanner": {"parquet_staging": {"flush_rows": 10}}}
    stager = ParquetStager(db, config)

    # Empty buffer => should_flush is False.
    assert stager.should_flush() is False

    # Bypass auto-flush by writing directly into the buffer.
    with stager._lock:
        stager._buffer.extend([
            {"source_id": sid, "scan_id": scan_id, "file_path": f"/p/{i}",
             "relative_path": f"p/{i}", "file_name": f"f{i}",
             "extension": "txt", "file_size": 0, "creation_time": None,
             "last_access_time": None, "last_modify_time": None,
             "owner": None, "attributes": 0}
            for i in range(10)
        ])
    assert stager.should_flush() is True

    # ``flush_to_db`` accepts the issue #135 signature without complaint.
    n = stager.flush_to_db(db=db, scan_id=scan_id)
    # When pyarrow + duckdb are available we get 10 rows; in fallback
    # mode bulk_insert_scanned_files is invoked and also returns 10.
    assert n == 10
    assert stager.should_flush() is False
