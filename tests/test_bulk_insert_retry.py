"""Regression tests for issue #174: bulk_insert_scanned_files retry loop.

Customer prod (2026-04-28 18:30) hit `database is locked` mid-scan and the
scanner aborted after ~100k of 3.1M rows. The fix:
  * busy_timeout 5s → 60s (`Database.connect`).
  * Retry-with-backoff in `bulk_insert_scanned_files` (5 attempts, 1/2/4/8/16s).
  * `parquet_staging.enabled: false` by default (DuckDB ATTACH(READ_WRITE) is
    the noisiest neighbour; disabled by default in `config.yaml`).

This test focuses on the retry semantics: a transient `OperationalError:
database is locked` MUST NOT abort the insert; only after all retries fail
should we re-raise.

Note: `sqlite3.Connection` is an immutable C type — we can't patch its
methods. Instead we patch ``Database.get_conn`` to yield a wrapper that
fault-injects on ``executemany``.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402


class _FlakyConn:
    """Wraps a real sqlite3.Connection, fault-injecting `executemany` on
    the first N calls. Used by the retry tests below.
    """

    def __init__(self, real, fail_first_n: int, error: Exception):
        self._real = real
        self._fail_remaining = fail_first_n
        self._error = error
        self.calls = 0

    def executemany(self, sql, params):
        self.calls += 1
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise self._error
        return self._real.executemany(sql, params)

    # Pass-throughs for everything else the retry path may touch.
    def commit(self):
        return self._real.commit()

    def rollback(self):
        return self._real.rollback()

    def execute(self, *a, **kw):
        return self._real.execute(*a, **kw)

    def cursor(self):
        return self._real.cursor()


def _patch_get_conn(db: Database, fail_first_n: int, error: Exception) -> _FlakyConn:
    """Replace ``db.get_conn`` so the next call returns a flaky wrapper.
    Returns the wrapper so the caller can read ``.calls``.
    """
    real_get_conn = db.get_conn
    holder: dict = {}

    @contextlib.contextmanager
    def patched():
        with real_get_conn() as real:
            wrapper = _FlakyConn(real, fail_first_n, error)
            holder["w"] = wrapper
            try:
                yield wrapper
            finally:
                pass

    db.get_conn = patched  # type: ignore[assignment]
    return holder  # type: ignore[return-value]


def _make_db(tmp_path) -> Database:
    db = Database({"path": str(tmp_path / "retry.db")})
    db.connect()
    # Seed source + scan_run so the FK constraints don't reject our inserts.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES('s', '\\\\fs\\share')"
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'running')",
            (source_id,),
        )
        scan_id = cur.lastrowid
    db.source_id = source_id
    db.scan_id = scan_id
    return db


def _make_row(db: Database, n: int) -> dict:
    return {
        "source_id": db.source_id,
        "scan_id": db.scan_id,
        "file_path": f"E:\\f{n}.txt",
        "relative_path": f"f{n}.txt",
        "file_name": f"f{n}.txt",
        "extension": "txt",
        "file_size": n,
        "creation_time": None,
        "last_access_time": None,
        "last_modify_time": None,
        "owner": None,
        "attributes": None,
    }


def test_busy_timeout_is_60s(tmp_path):
    """Issue #174: busy_timeout bumped from 5000 to 60000."""
    db = _make_db(tmp_path)
    with db.get_cursor() as cur:
        cur.execute("PRAGMA busy_timeout")
        row = cur.fetchone()
    # Cursor uses sqlite3.Row → access by column alias "timeout".
    assert row["timeout"] == 60000, (
        f"busy_timeout regressed to {row['timeout']}, expected 60000"
    )


def test_bulk_insert_succeeds_normally(tmp_path):
    """Sanity: no contention, plain insert path succeeds first try."""
    db = _make_db(tmp_path)
    rows = [_make_row(db, i) for i in range(50)]
    db.bulk_insert_scanned_files(rows)
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM scanned_files")
        assert cur.fetchone()["c"] == 50


def test_bulk_insert_retries_on_locked_then_succeeds(tmp_path, monkeypatch):
    """Two transient `database is locked` failures, third call succeeds.
    The retry loop must absorb the first two and finish without raising.
    """
    db = _make_db(tmp_path)
    rows = [_make_row(db, i) for i in range(3)]

    monkeypatch.setattr("src.storage.database.time.sleep", lambda _s: None)

    # Each retry calls `get_conn` afresh, so each gets its own wrapper.
    # We need a counter that survives across calls. Accumulate via state
    # dict captured by closure — the wrapper class only counts within one
    # context manager block, but here we need a global call count.
    state = {"calls": 0}
    real_get_conn = db.get_conn

    @contextlib.contextmanager
    def patched():
        with real_get_conn() as real:
            class _Wrap:
                def executemany(self_inner, sql, params):
                    state["calls"] += 1
                    if state["calls"] <= 2:
                        raise sqlite3.OperationalError("database is locked")
                    return real.executemany(sql, params)
            yield _Wrap()

    db.get_conn = patched  # type: ignore[assignment]

    db.bulk_insert_scanned_files(rows)

    assert state["calls"] == 3, f"expected 3 calls (2 fail + 1 succeed), got {state['calls']}"
    # Restore for the count query.
    db.get_conn = real_get_conn  # type: ignore[assignment]
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM scanned_files")
        assert cur.fetchone()["c"] == 3


def test_bulk_insert_reraises_after_all_retries_fail(tmp_path, monkeypatch):
    """All 5 attempts hit `database is locked` — the OperationalError must
    propagate so the scanner can mark the scan failed.
    """
    db = _make_db(tmp_path)
    rows = [_make_row(db, i) for i in range(3)]

    monkeypatch.setattr("src.storage.database.time.sleep", lambda _s: None)

    state = {"calls": 0}
    real_get_conn = db.get_conn

    @contextlib.contextmanager
    def patched():
        with real_get_conn() as real:
            class _Wrap:
                def executemany(self_inner, sql, params):
                    state["calls"] += 1
                    raise sqlite3.OperationalError("database is locked")
            yield _Wrap()

    db.get_conn = patched  # type: ignore[assignment]

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        db.bulk_insert_scanned_files(rows)

    assert state["calls"] == 5, f"expected 5 attempts before giving up, got {state['calls']}"


def test_bulk_insert_does_not_retry_on_unrelated_error(tmp_path, monkeypatch):
    """A non-lock error (e.g. missing table) MUST re-raise on the first
    attempt — no point retrying a structurally-broken statement.
    """
    db = _make_db(tmp_path)
    rows = [_make_row(db, i) for i in range(3)]

    monkeypatch.setattr("src.storage.database.time.sleep", lambda _s: None)

    state = {"calls": 0}
    real_get_conn = db.get_conn

    @contextlib.contextmanager
    def patched():
        with real_get_conn() as real:
            class _Wrap:
                def executemany(self_inner, sql, params):
                    state["calls"] += 1
                    raise sqlite3.OperationalError("no such table: scanned_files")
            yield _Wrap()

    db.get_conn = patched  # type: ignore[assignment]

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        db.bulk_insert_scanned_files(rows)

    assert state["calls"] == 1, f"unrelated error must not retry; got {state['calls']} calls"


def test_bulk_insert_empty_list_is_noop(tmp_path):
    """`bulk_insert_scanned_files([])` returns without touching the DB."""
    db = _make_db(tmp_path)
    db.bulk_insert_scanned_files([])  # Must not raise
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM scanned_files")
        assert cur.fetchone()["c"] == 0
