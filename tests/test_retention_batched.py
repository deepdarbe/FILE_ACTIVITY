"""Tests for #337 — batched, resumable retention cleanup.

The old cleanup_old_scans deleted millions of scanned_files rows in ONE
transaction inside connect(): 5.9 GB WAL, port bind blocked ~12 min, and a
mid-delete kill rolled the whole thing back (observed on prod 2026-07-16).
The replacement (cleanup_old_scans_batched) commits per batch, excludes
running scans, drains children before the scan_runs parent (FK ON DELETE
CASCADE), and resumes cheaply after interruption.

All tests run on plain sqlite temp DBs — no fastapi needed. Fixtures disable
auto_cleanup_on_startup so no background worker races the test body.
"""

from __future__ import annotations

import sqlite3
import threading

import pytest

from src.storage.database import Database


def _mk_db(tmp_path, name="r.db"):
    db = Database({
        "path": str(tmp_path / name),
        "retention": {"auto_cleanup_on_startup": False},
    })
    db.connect()
    return db


def _seed(db, sources=2, runs_per_source=6, files_per_run=200,
          status="completed"):
    """sources × runs × files; returns {source_id: [run_ids oldest→newest]}."""
    layout = {}
    with db.get_cursor() as cur:
        for s in range(1, sources + 1):
            cur.execute(
                "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
                (f"s{s}", f"x{s}"))
    run_ids = {}
    for s in range(1, sources + 1):
        run_ids[s] = []
        for r in range(runs_per_source):
            rid = db.create_scan_run(s)
            with db.get_cursor() as cur:
                # Distinct started_at so ORDER BY started_at is stable.
                cur.execute(
                    "UPDATE scan_runs SET status=?, "
                    "started_at=datetime('2026-01-01', ?) WHERE id=?",
                    (status, f"+{r} days", rid))
                cur.executemany(
                    "INSERT INTO scanned_files"
                    "(source_id, scan_id, file_name, file_path, relative_path,"
                    " extension, file_size) VALUES (?, ?, ?, ?, ?, 'dat', 10)",
                    [(s, rid, f"f{i}.dat", f"f{i}.dat", f"f{i}.dat")
                     for i in range(files_per_run)])
            run_ids[s].append(rid)
        layout[s] = run_ids[s]
    return layout


def test_parity_shape_and_counts(tmp_path):
    """Same public contract as the old implementation: shape + semantics."""
    db = _mk_db(tmp_path)
    _seed(db, sources=2, runs_per_source=6, files_per_run=200)
    res = db.cleanup_old_scans(keep_last_n=3)
    assert set(res) >= {"deleted_runs", "deleted_files",
                        "deleted_orphans", "deleted_cache"}
    assert res["deleted_runs"] == 6          # 3 victims per source
    assert res["deleted_files"] == 6 * 200
    assert res["deleted_orphans"] == 0
    with db.get_read_cursor() as cur:
        left = cur.execute("SELECT COUNT(*) AS c FROM scan_runs").fetchone()["c"]
    assert left == 6                          # 3 kept per source
    db.close()


def test_running_scan_is_never_a_victim(tmp_path):
    """#337 amendment 1: even keep_last_n=0 must not delete a running scan
    (the /api/db/cleanup contract allows keep_last=0)."""
    db = _mk_db(tmp_path)
    _seed(db, sources=1, runs_per_source=2, files_per_run=50)
    running_id = db.create_scan_run(1)   # status defaults to 'running'
    res = db.cleanup_old_scans(keep_last_n=0)
    assert res["deleted_runs"] == 2       # only the completed ones
    with db.get_read_cursor() as cur:
        row = cur.execute(
            "SELECT status FROM scan_runs WHERE id=?", (running_id,)
        ).fetchone()
    assert row is not None and row["status"] == "running"
    db.close()


class _StopAfterBatches(threading.Event):
    """Event that sets itself after N inter-batch waits."""

    def __init__(self, n):
        super().__init__()
        self._n = n
        self.waits = 0

    def wait(self, timeout=None):
        self.waits += 1
        if self.waits >= self._n:
            self.set()
        return super().wait(0)


def test_per_batch_commit_survives_interruption_and_resumes(tmp_path):
    """The (b) guarantee: deleted batches are COMMITTED (visible from a
    fresh raw connection), the parent run still exists while children
    remain (FK-order pin), and a fresh run finishes only the remainder."""
    dbfile = tmp_path / "r2.db"
    db = _mk_db(tmp_path, "r2.db")
    _seed(db, sources=1, runs_per_source=2, files_per_run=1000)

    stopper = _StopAfterBatches(3)
    res = db.cleanup_old_scans_batched(
        keep_last_n=1, batch_size=100, sleep_between=0, stop_event=stopper)
    assert res.get("interrupted") is True
    deleted_first = res["deleted_files"]
    assert 0 < deleted_first < 1000          # partial progress, committed

    # Fresh RAW connection proves the batches were committed, not rolled
    # back, and the victim's parent row still exists (children first!).
    raw = sqlite3.connect(str(dbfile))
    left = raw.execute(
        "SELECT COUNT(*) FROM scanned_files").fetchone()[0]
    runs = raw.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0]
    raw.close()
    assert left == 2000 - deleted_first
    assert runs == 2                          # parent not deleted yet

    # Resume: completes the remainder only (no repeated work).
    res2 = db.cleanup_old_scans_batched(
        keep_last_n=1, batch_size=100, sleep_between=0)
    assert "interrupted" not in res2
    assert res2["deleted_files"] + deleted_first == 1000
    assert res2["deleted_runs"] == 1
    with db.get_read_cursor() as cur:
        assert cur.execute(
            "SELECT COUNT(*) AS c FROM scan_runs").fetchone()["c"] == 1
        assert cur.execute(
            "SELECT COUNT(*) AS c FROM scanned_files").fetchone()["c"] == 1000
    db.close()


def test_single_flight_lock(tmp_path):
    """Second concurrent caller must skip, not interleave."""
    db = _mk_db(tmp_path, "r3.db")
    _seed(db, sources=1, runs_per_source=1, files_per_run=10)
    assert db._retention_lock.acquire(blocking=False)
    try:
        res = db.cleanup_old_scans_batched(keep_last_n=0)
        assert res == {"skipped": "already_running"}
    finally:
        db._retention_lock.release()
    db.close()


def test_orphan_sweep_per_scan_id(tmp_path):
    """Orphans (rows whose scan_run vanished) are swept via the per-id
    path and counted in deleted_orphans."""
    db = _mk_db(tmp_path, "r4.db")
    _seed(db, sources=1, runs_per_source=1, files_per_run=50)
    with db.get_cursor() as cur:
        # Fabricate orphans: rows pointing at a scan_id with no scan_run.
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.executemany(
            "INSERT INTO scanned_files"
            "(source_id, scan_id, file_name, file_path, relative_path,"
            " extension, file_size) VALUES (1, 9999, ?, ?, ?, 'dat', 5)",
            [(f"o{i}", f"o{i}", f"o{i}") for i in range(30)])
    res = db.cleanup_old_scans_batched(keep_last_n=5, sleep_between=0)
    assert res["deleted_orphans"] == 30
    assert res["deleted_runs"] == 0
    db.close()


def test_write_retry_returns_rowcount(tmp_path):
    """#337: _execute_write_with_retry now reports rowcount (the batched
    deleter's termination signal)."""
    db = _mk_db(tmp_path, "r5.db")
    _seed(db, sources=1, runs_per_source=1, files_per_run=7)
    n = db._execute_write_with_retry(
        "test_delete", "DELETE FROM scanned_files WHERE source_id=?", (1,))
    assert n == 7
    db.close()
