"""Regression tests for issue #185: AnalyticsEngine per-query DuckDB connection.

The previous code held a single long-lived DuckDB connection with a permanent
SQLite ATTACH. That ATTACH shows up as an always-on SQLite reader and prevents
`PRAGMA wal_checkpoint(TRUNCATE)` from ever shrinking the WAL — customer prod
hit 13.5 GB and 74 GB WAL files at different points.

The fix moves to a per-query connection model: every `_cursor()` call opens a
fresh DuckDB connection with a fresh ATTACH, then closes it. Between calls
there is no SQLite reader, so the checkpointer can truncate.

These tests pin:
  * `_cursor()` opens AND closes a DuckDB connection per call (no reuse).
  * After a query, `wal_checkpoint(TRUNCATE)` succeeds (busy=0).
  * Concurrent queries get independent connections (no serialization bug).
  * `close()` is a safe no-op (per-query connections clean themselves up).
  * Failure during ATTACH propagates AS A FAILURE (caller must see init error).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import threading

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed")

from src.storage.analytics import AnalyticsEngine  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_db(tmp_path):
    """Real on-disk SQLite DB with a single source row so DuckDB ATTACH has
    a real schema to bind."""
    db = Database({"path": str(tmp_path / "an.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES('s1', '\\\\fs\\share')"
        )
    yield db
    db.close()


@pytest.fixture
def engine(seeded_db):
    """AnalyticsEngine pointing at the seeded DB with default config."""
    eng = AnalyticsEngine(seeded_db.db_path, {"enabled": True})
    if not eng.available:
        pytest.skip(f"DuckDB unavailable in this env: {eng._init_error!r}")
    yield eng
    eng.close()


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


def test_engine_does_not_hold_persistent_conn(engine):
    """Issue #185 fix: `self._conn` must NOT be set after init.

    The legacy implementation stashed a long-lived connection here; the
    fix removes that. If anyone re-introduces a persistent connection,
    this test fails loudly.
    """
    assert engine._conn is None, (
        "AnalyticsEngine still holds a persistent DuckDB connection — "
        "this re-introduces the WAL leak fixed by #185."
    )


def test_cursor_opens_fresh_conn_each_call(engine):
    """Two consecutive `_cursor()` calls yield two DIFFERENT connection
    objects. Proves we are not reusing a single long-lived conn."""
    seen = []
    with engine._cursor() as cur1:
        seen.append(id(cur1))
    with engine._cursor() as cur2:
        seen.append(id(cur2))
    assert seen[0] != seen[1], (
        "_cursor() reused a connection object — that means the SQLite "
        "ATTACH is held across calls, which is exactly what #185 fixed."
    )


def test_cursor_closes_conn_on_exit(engine):
    """After `_cursor()` exits, the DuckDB connection it yielded must be
    closed. Calling `.execute` on it after the with-block raises.
    """
    with engine._cursor() as cur:
        leaked = cur
    with pytest.raises(Exception):
        # Exact exception class varies across DuckDB versions; just
        # assert SOMETHING fails when the closed conn is touched.
        leaked.execute("SELECT 1")


def test_cursor_closes_conn_even_on_exception(engine):
    """If the caller raises inside the with-block, the connection is
    still closed (no FD leak across exceptions).
    """
    leaked = None
    with pytest.raises(RuntimeError, match="boom"):
        with engine._cursor() as cur:
            leaked = cur
            raise RuntimeError("boom")
    assert leaked is not None
    with pytest.raises(Exception):
        leaked.execute("SELECT 1")


# ---------------------------------------------------------------------------
# WAL checkpoint behaviour — the actual customer pain
# ---------------------------------------------------------------------------


def test_wal_checkpoint_succeeds_between_queries(engine, seeded_db):
    """The customer's 13.5 GB / 74 GB WAL leak: after the analytics engine
    runs a query the WAL must be truncatable.

    Strategy: write some data through the writer pool to grow the WAL,
    run an analytics query (opens + closes ATTACH), then PRAGMA
    wal_checkpoint(TRUNCATE) and verify it returns busy=0.
    """
    # 1. Force some WAL pages by writing something through the writer pool.
    with seeded_db.get_cursor() as cur:
        for i in range(10):
            cur.execute(
                "INSERT INTO scan_runs(source_id, status) VALUES(1, 'running')"
            )

    # 2. Run a real analytics query — this opens an ATTACH then closes it.
    with engine._cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sqlite_db.scan_runs").fetchall()

    # 3. Now TRUNCATE the WAL on a fresh writer-pool connection. busy=0
    #    indicates no reader is holding the WAL — i.e. the ATTACH from
    #    step 2 was correctly released.
    with seeded_db.get_conn() as conn:
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    busy, log_pages, checkpointed = result["busy"], result["log"], result["checkpointed"]
    assert busy == 0, (
        f"wal_checkpoint(TRUNCATE) returned busy={busy}; a reader is still "
        f"holding the WAL. log={log_pages} checkpointed={checkpointed}. "
        "If this fails, AnalyticsEngine is reintroducing the #185 leak."
    )


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_cursors_get_independent_conns(engine):
    """Two threads each open `_cursor()` simultaneously. They must NOT
    share a connection (we removed the long-lived `self._conn`).
    """
    seen = []
    barrier = threading.Barrier(2)
    err = []

    def worker():
        try:
            with engine._cursor() as cur:
                barrier.wait(timeout=10)
                seen.append(id(cur))
                cur.execute(
                    "SELECT COUNT(*) FROM sqlite_db.scan_runs"
                ).fetchall()
        except Exception as e:
            err.append(e)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start(); t2.start()
    t1.join(timeout=15); t2.join(timeout=15)
    assert not err, f"worker raised: {err}"
    assert len(seen) == 2 and seen[0] != seen[1], (
        f"concurrent _cursor() calls shared a connection: {seen}"
    )


# ---------------------------------------------------------------------------
# Init / close
# ---------------------------------------------------------------------------


def test_smoke_attach_runs_at_init(seeded_db):
    """The boot-time smoke test must succeed when the DB is reachable."""
    eng = AnalyticsEngine(seeded_db.db_path, {"enabled": True})
    if not eng.available:
        pytest.skip(f"DuckDB unavailable: {eng._init_error}")
    # `available=True` AFTER smoke test === no exception raised AND the
    # smoke-test conn was opened and closed.
    assert eng.available is True
    assert eng._conn is None
    eng.close()


def test_disabled_engine_does_not_open_anything(seeded_db):
    """`enabled: false` config must skip even the smoke test (no DuckDB
    connect, no ATTACH attempt — important for hosts that intentionally
    don't have duckdb installed).
    """
    eng = AnalyticsEngine(seeded_db.db_path, {"enabled": False})
    assert eng.available is False
    assert "enabled=false" in (eng._init_error or "")


def test_close_is_idempotent(engine):
    """After close(), available=False and a second close() doesn't crash."""
    engine.close()
    assert engine.available is False
    engine.close()  # no raise
    assert engine.available is False


def test_attach_failure_marks_engine_unavailable(tmp_path):
    """Pointing at a path that DuckDB can't ATTACH (e.g. a directory)
    leaves `available=False` with a useful `_init_error`.
    """
    bogus = str(tmp_path / "this_is_a_directory")
    os.makedirs(bogus, exist_ok=True)
    eng = AnalyticsEngine(bogus, {"enabled": True})
    assert eng.available is False
    assert eng._init_error is not None and len(eng._init_error) > 0
