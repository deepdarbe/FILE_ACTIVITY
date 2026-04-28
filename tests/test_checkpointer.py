"""Tests for the manual WAL checkpointer (issue #153 Lever A).

Coverage:
  * test_checkpointer_runs_truncate_on_request
  * test_checkpointer_force_truncate_above_threshold
  * test_checkpointer_stop_is_idempotent
  * test_checkpointer_does_not_fight_writers
"""

from __future__ import annotations

import os
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.checkpointer import Checkpointer  # noqa: E402


def _make_wal_db(
    path: Path, rows: int = 200, payload_bytes: int = 200
) -> sqlite3.Connection:
    """Create a WAL-mode DB and insert ``rows`` rows of ~``payload_bytes``
    each so a WAL file exists and is at least a few KB. We disable engine
    auto-checkpoint so the WAL persists for our test to inspect.

    Returns the open connection — caller is responsible for closing.
    Keeping a reader open on the WAL is what stops SQLite from
    auto-truncating it on the last close().
    """
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
    conn.executemany(
        "INSERT INTO t (payload) VALUES (?)",
        [(f"row-{i}-" + ("x" * payload_bytes),) for i in range(rows)],
    )
    conn.commit()
    return conn


def _wal_size_mb(db_path: Path) -> float:
    wal = str(db_path) + "-wal"
    if not os.path.exists(wal):
        return 0.0
    return os.path.getsize(wal) / (1024 * 1024)


def test_checkpointer_runs_truncate_on_request(tmp_path: Path) -> None:
    """request() wakes the daemon and the WAL is truncated to 0."""
    db_path = tmp_path / "live.db"
    # Big rows: 4 KB payload * 500 rows = ~2 MB WAL, well above the
    # 1 MB skip gate inside _maybe_checkpoint.
    seed_conn = _make_wal_db(db_path, rows=500, payload_bytes=4096)

    # Sanity: the WAL exists and is above 1 MB (the daemon's threshold
    # below which it skips the open() entirely).
    try:
        assert os.path.exists(str(db_path) + "-wal")
        assert _wal_size_mb(db_path) >= 1.0, (
            f"Test setup WAL too small: {_wal_size_mb(db_path)} MB"
        )

        cfg = {
            "backup": {
                # Long interval so the only thing that can wake us is request().
                "checkpoint_interval_seconds": 60,
                "checkpoint_force_threshold_mb": 1000,
            }
        }
        cp = Checkpointer(str(db_path), cfg)
        cp.start()
        try:
            cp.request()
            # Give the worker a window to wake, run TRUNCATE, and
            # update _pass_count. 5 seconds is plenty on a quiet runner.
            deadline = time.time() + 5.0
            while time.time() < deadline and cp._pass_count == 0:
                time.sleep(0.05)
            assert cp._pass_count >= 1, "Checkpointer never ran a pass"
            # WAL after TRUNCATE should be 0 bytes (file may exist but empty).
            assert _wal_size_mb(db_path) < 0.001
        finally:
            cp.stop()
            cp.join(timeout=2.0)
    finally:
        seed_conn.close()


def test_checkpointer_force_truncate_above_threshold(
    tmp_path: Path, caplog
) -> None:
    """When WAL >= force_threshold_mb the daemon logs a warning AND
    runs TRUNCATE."""
    import logging
    db_path = tmp_path / "live.db"
    seed_conn = _make_wal_db(db_path, rows=500, payload_bytes=4096)

    try:
        # Threshold below current WAL size so we definitely cross it.
        cfg = {
            "backup": {
                "checkpoint_interval_seconds": 60,
                # 1 MB threshold — our seeded WAL is well above this.
                "checkpoint_force_threshold_mb": 1,
            }
        }
        cp = Checkpointer(str(db_path), cfg)
        cp.start()
        try:
            with caplog.at_level(
                logging.WARNING, logger="file_activity.checkpointer"
            ):
                cp.request()
                deadline = time.time() + 5.0
                while time.time() < deadline and cp._pass_count == 0:
                    time.sleep(0.05)
            assert cp._pass_count >= 1
            # The "force-truncate" warning must have fired.
            force_msgs = [
                r for r in caplog.records
                if "force-truncate" in r.getMessage()
            ]
            assert force_msgs, "Expected force-truncate warning, got none"
            # And the WAL must be empty after.
            assert _wal_size_mb(db_path) < 0.001
        finally:
            cp.stop()
            cp.join(timeout=2.0)
    finally:
        seed_conn.close()


def test_checkpointer_stop_is_idempotent(tmp_path: Path) -> None:
    """stop() can be called repeatedly and on a never-started instance
    without raising."""
    db_path = tmp_path / "live.db"
    seed_conn = _make_wal_db(db_path, rows=10)
    seed_conn.close()

    cp = Checkpointer(str(db_path), {"backup": {}})

    # Stop without start — must not raise.
    cp.stop()

    # Start, then stop twice — must not raise.
    cp.start()
    cp.stop()
    cp.stop()
    cp.join(timeout=2.0)
    assert cp._thread is not None  # we did start it
    assert not cp._thread.is_alive()


def test_checkpointer_does_not_fight_writers(tmp_path: Path) -> None:
    """A concurrent INSERT loop must complete cleanly while the
    checkpointer is running."""
    db_path = tmp_path / "live.db"
    seed_conn = _make_wal_db(db_path, rows=10)
    seed_conn.close()

    cfg = {
        "backup": {
            "checkpoint_interval_seconds": 0.1,  # aggressive
            "checkpoint_force_threshold_mb": 0,  # always force-truncate
        }
    }
    cp = Checkpointer(str(db_path), cfg)
    cp.start()

    inserted = 0
    insert_errors: list[Exception] = []

    def _writer():
        nonlocal inserted
        conn = sqlite3.connect(str(db_path), timeout=10.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA wal_autocheckpoint=0")
            for i in range(500):
                try:
                    conn.execute(
                        "INSERT INTO t (payload) VALUES (?)",
                        (f"concurrent-{i}",),
                    )
                    conn.commit()
                    inserted += 1
                except sqlite3.OperationalError as e:  # pragma: no cover
                    # Write timeouts shouldn't happen with 10s; record
                    # if they do. The test still passes as long as the
                    # writer didn't crash and most rows landed.
                    insert_errors.append(e)
        finally:
            conn.close()

    t = threading.Thread(target=_writer)
    t.start()
    t.join(timeout=15.0)
    assert not t.is_alive(), "Writer thread did not finish in time"

    cp.stop()
    cp.join(timeout=2.0)

    assert inserted >= 450, (
        f"Writer made too few rows ({inserted}); checkpointer may be "
        f"starving the writer. errors={insert_errors[:3]}"
    )
    # Verify the rows are actually durable.
    conn = sqlite3.connect(str(db_path))
    try:
        n = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    finally:
        conn.close()
    # 10 from setup + the inserted count.
    assert n == 10 + inserted, f"Row count mismatch: {n} vs {10 + inserted}"


def test_checkpointer_skips_when_wal_below_1mb(tmp_path: Path) -> None:
    """Sanity: if WAL is < 1 MB the daemon skips the connection
    open entirely. We assert that pass_count stays 0 on a tiny WAL
    when only request() fires (no force threshold crossed)."""
    db_path = tmp_path / "live.db"
    # Tiny DB: 5 rows, WAL will be a few KB at most.
    seed_conn = _make_wal_db(db_path, rows=5)
    try:
        cfg = {
            "backup": {
                "checkpoint_interval_seconds": 60,
                "checkpoint_force_threshold_mb": 1000,
            }
        }
        cp = Checkpointer(str(db_path), cfg)
        cp.start()
        try:
            cp.request()
            time.sleep(0.5)  # let one iteration run
            # pass_count is incremented only when we actually run TRUNCATE.
            assert cp._pass_count == 0
        finally:
            cp.stop()
            cp.join(timeout=2.0)
    finally:
        seed_conn.close()
