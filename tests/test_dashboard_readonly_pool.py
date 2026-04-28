"""Tests for the read-only connection pool migration (issue #181 Track A).

The dashboard's read endpoints used to share the writer's thread-local
connection pool. While the scanner held the writer lock, those reads
piled up behind it and the dashboard would freeze for tens of seconds
mid-scan.

Track A swaps each read-only call site over to ``Database.get_read_cursor``,
which opens a *separate* ``mode=ro`` URI connection per call — readers
no longer contend with the writer.

These tests pin the contract so future refactors can't silently:

* Re-route a read endpoint back through the writer pool.
* Allow ``get_read_cursor`` to accept DML.
* Leak file descriptors on the per-call connection lifecycle.
* Block the writer-checkpointer with a stale read snapshot.

Test list (≥8):

1. ``get_read_cursor`` raises ``OperationalError`` on INSERT.
2. ``get_read_cursor`` succeeds on SELECT.
3. Mid-scan: writer holds a 5s transaction, reader still <1s.
4. Mid-bulk-insert: 10k-row writer, reader still <1s.
5. WAL: read cursor doesn't block ``PRAGMA wal_checkpoint(TRUNCATE)``.
6. Per-call cleanup: 100 read cursors, no FD leak (Linux only).
7. ``dict_factory`` rows look identical to the writer-pool cursor.
8. Endpoint smoke test: ``GET /api/sources`` returns 200 while a
   ``scan_runs`` row sits in 'running' state (mimics live scan).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import threading
import time

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    """Fresh on-disk SQLite Database with one source row seeded."""
    db_path = tmp_path / "ro_pool.db"
    database = Database({"path": str(db_path)})
    database.connect()
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("share1", "\\\\fs\\share1"),
        )
    yield database
    try:
        database.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Stubs for the endpoint smoke test
# ---------------------------------------------------------------------------


class _StubAnalytics:
    available = False

    def health(self):
        return {"available": False, "configured": False}

    def close(self):  # pragma: no cover - defensive
        pass


class _StubADLookup:
    available = False

    def lookup(self, name, force_refresh=False):
        return {"username": name, "email": None, "display_name": None,
                "found": False, "source": "live"}

    def health(self):
        return {"available": False, "configured": False}


class _StubEmailNotifier:
    available = False

    def send(self, *a, **kw):
        return False

    def health(self):
        return {"available": False, "configured": False}


def _make_config():
    return {
        "dashboard": {"auth": {"enabled": False}},
        "archiving": {"enabled": False, "dry_run": True},
        "audit": {"chain_enabled": False},
        "database": {},
        "analytics": {},
        "security": {
            "ransomware": {
                "enabled": False,
                "rename_velocity_threshold": 50,
                "rename_velocity_window": 60,
                "deletion_velocity_threshold": 100,
                "deletion_velocity_window": 60,
                "risky_new_extensions": [],
                "canary_file_names": [],
                "auto_kill_session": False,
                "notification_email": "",
            },
            "orphan_sid": {"enabled": False, "cache_ttl_minutes": 60},
        },
        "backup": {"enabled": False, "dir": "/tmp/_no_backups",
                   "keep_last_n": 1, "keep_weekly": 0},
        "integrations": {"syslog": {"enabled": False}},
    }


# ---------------------------------------------------------------------------
# 1) Read-only enforcement
# ---------------------------------------------------------------------------


def test_get_read_cursor_rejects_insert(db):
    """Any DML on a read cursor must raise ``OperationalError``.

    SQLite's ``mode=ro`` URI parameter is the enforcement layer; we want
    a hard failure, not a silent no-op, so the caller never thinks a
    write succeeded.
    """
    with db.get_read_cursor() as cur:
        with pytest.raises(sqlite3.OperationalError):
            cur.execute(
                "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
                ("rogue", "\\\\evil\\share"),
            )


def test_get_read_cursor_allows_select(db):
    """SELECT succeeds and returns the seeded source row."""
    with db.get_read_cursor() as cur:
        cur.execute("SELECT id, name FROM sources WHERE name=?", ("share1",))
        row = cur.fetchone()
    assert row is not None
    assert row["name"] == "share1"


# ---------------------------------------------------------------------------
# 2) No contention with the writer lock
# ---------------------------------------------------------------------------


def test_read_cursor_not_blocked_by_writer_transaction(db):
    """A 5s writer transaction must not block a parallel read.

    Simulates the scanner pattern: BEGIN IMMEDIATE on the writer
    connection, hold for 5 seconds, then commit. Meanwhile a separate
    thread runs a SELECT through ``get_read_cursor`` — that SELECT must
    return in well under a second.
    """
    writer_done = threading.Event()
    writer_started = threading.Event()

    def _hold_writer():
        # Use the thread-local writer pool the same way the scanner does.
        conn = db._get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
                ("writer_held", "\\\\fs\\hold"),
            )
            writer_started.set()
            time.sleep(5.0)
            conn.rollback()
        finally:
            writer_done.set()

    t = threading.Thread(target=_hold_writer, daemon=True)
    t.start()
    assert writer_started.wait(timeout=2.0), "writer thread didn't start"

    # Reader should NOT wait for the writer to finish.
    t0 = time.perf_counter()
    with db.get_read_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM sources")
        row = cur.fetchone()
    elapsed = time.perf_counter() - t0

    assert row is not None
    assert elapsed < 1.0, (
        f"reader was blocked by writer ({elapsed:.2f}s); "
        "the read-only pool should bypass the writer lock"
    )

    t.join(timeout=10.0)
    assert writer_done.is_set()


def test_read_cursor_not_blocked_by_bulk_insert(db):
    """While a 10k-row bulk_insert_scanned_files is running, a separate
    ``get_read_cursor`` SELECT must still return promptly.

    Repeats the pattern of the previous test but with a realistic write
    payload — proves the no-contention guarantee under the same kind of
    load the scanner generates in production.
    """
    # Need a scan_runs row first; foreign-key linkage makes the bulk
    # insert realistic.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'running')",
            (1,),
        )
        scan_id = cur.lastrowid

    payload = [
        {
            "source_id": 1,
            "scan_id": scan_id,
            "file_path": f"/test/file_{i}.bin",
            "relative_path": f"file_{i}.bin",
            "file_name": f"file_{i}.bin",
            "extension": "bin",
            "file_size": 1024 + i,
            "creation_time": "2026-01-01 00:00:00",
            "last_access_time": "2026-01-01 00:00:00",
            "last_modify_time": "2026-01-01 00:00:00",
            "owner": "tester",
            "attributes": "",
        }
        for i in range(10_000)
    ]

    insert_done = threading.Event()

    def _bulk_insert():
        try:
            db.bulk_insert_scanned_files(payload)
        finally:
            insert_done.set()

    t = threading.Thread(target=_bulk_insert, daemon=True)
    t.start()
    # Give the writer a brief head start so it's mid-flight when we read.
    time.sleep(0.05)

    t0 = time.perf_counter()
    with db.get_read_cursor() as cur:
        cur.execute("SELECT id FROM sources LIMIT 1")
        row = cur.fetchone()
    elapsed = time.perf_counter() - t0

    assert row is not None
    assert elapsed < 1.0, (
        f"reader blocked by bulk insert ({elapsed:.2f}s)"
    )

    t.join(timeout=30.0)
    assert insert_done.is_set()


# ---------------------------------------------------------------------------
# 3) WAL behaviour
# ---------------------------------------------------------------------------


def test_wal_checkpoint_truncate_after_read(db):
    """After a read cursor closes, ``PRAGMA wal_checkpoint(TRUNCATE)``
    must succeed (returns ``(0, ...)``, not ``(1, ...)``).

    A ``(1, ...)`` return means the checkpointer hit a busy reader and
    couldn't reset the WAL — which would mean ``get_read_cursor`` is
    leaking a long-lived connection. The contract is that the connection
    is closed in the ``finally`` branch of the context manager.
    """
    # Hold a read cursor briefly, then close it, then checkpoint.
    with db.get_read_cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
        time.sleep(2.0)
    # cursor + connection are now closed.

    conn = db._get_conn()
    cur = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    res = cur.fetchone()
    # SQLite returns (busy, log_pages, checkpointed_pages). Busy=0 means
    # no reader/writer was holding the WAL hostage.
    if isinstance(res, dict):
        busy = list(res.values())[0]
    else:
        busy = res[0]
    assert busy == 0, f"WAL checkpoint blocked (busy={busy}); read cursor leaked"


# ---------------------------------------------------------------------------
# 4) Per-call cleanup — no FD leak
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="num_fds() is POSIX-only; Windows uses num_handles() with very different semantics",
)
def test_no_fd_leak_across_100_calls(db):
    """Open 100 read cursors back-to-back and assert the process FD count
    is stable. Catches the regression where ``get_read_cursor`` forgets
    to close its connection.
    """
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()

    # Warmup — first call may open shared cache / other transient FDs.
    for _ in range(5):
        with db.get_read_cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

    before = proc.num_fds()
    for _ in range(100):
        with db.get_read_cursor() as cur:
            cur.execute("SELECT id FROM sources")
            cur.fetchall()
    after = proc.num_fds()

    # Allow tiny slack for unrelated FDs (logging, etc) — anything above
    # ~10 means the per-call connection isn't being closed.
    assert after - before <= 10, (
        f"FD leak: before={before} after={after} delta={after - before}"
    )


# ---------------------------------------------------------------------------
# 5) Row shape parity with the writer-pool cursor
# ---------------------------------------------------------------------------


def test_dict_factory_parity_with_writer_cursor(db):
    """Rows from ``get_read_cursor`` must look identical to rows from
    ``get_cursor`` — same dict shape, same column-name lookup. Otherwise
    every migrated endpoint silently breaks its frontend contract.
    """
    with db.get_cursor() as cur:
        cur.execute("SELECT id, name, unc_path FROM sources WHERE name=?", ("share1",))
        writer_row = cur.fetchone()

    with db.get_read_cursor() as cur:
        cur.execute("SELECT id, name, unc_path FROM sources WHERE name=?", ("share1",))
        reader_row = cur.fetchone()

    assert isinstance(writer_row, dict), (
        "writer cursor must yield dicts (dict_factory contract)"
    )
    assert isinstance(reader_row, dict), (
        "read cursor must yield dicts too"
    )
    assert reader_row["name"] == writer_row["name"]
    assert reader_row["unc_path"] == writer_row["unc_path"]
    assert reader_row["id"] == writer_row["id"]
    # Same key set, no surprise extras.
    assert set(reader_row.keys()) == set(writer_row.keys())


# ---------------------------------------------------------------------------
# 6) Endpoint smoke — GET /api/sources during a "scan in progress"
# ---------------------------------------------------------------------------


def test_endpoint_get_sources_succeeds_during_running_scan(db):
    """``GET /api/sources`` must 200 even with a 'running' scan_runs row
    — the migrated read-only path doesn't acquire the writer lock so a
    live scan can't block it.
    """
    fastapi_testclient = pytest.importorskip("fastapi.testclient")
    TestClient = fastapi_testclient.TestClient

    # Fake "scan in progress" so the read path has to skip past
    # uncommitted writer state.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'running')",
            (1,),
        )

    from src.dashboard.api import create_app
    app = create_app(
        db=db,
        config=_make_config(),
        analytics=_StubAnalytics(),
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    client = TestClient(app)

    resp = client.get("/api/sources")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body, list)
    assert any(item.get("name") == "share1" for item in body), body
