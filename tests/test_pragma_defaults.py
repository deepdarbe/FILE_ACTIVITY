"""Tests for issue #153 Lever B — tuned default pragmas.

Coverage:
  * test_pragmas_applied_after_connect — every Lever B pragma is set
  * test_existing_small_page_size_warns_only — old DB with 4096 page_size
    logs a hint, does not crash, does not silently mutate
  * test_wal_autocheckpoint_zero_when_checkpointer_alive — Lever A wires
    correctly into the new pragma fallback
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402


def _read_pragma(conn, name: str):
    """Run a PRAGMA and return the scalar value, regardless of whether
    the row_factory is dict or tuple."""
    row = conn.execute(f"PRAGMA {name}").fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        # dict_factory keys the column by its actual name, which for
        # PRAGMA statements equals the pragma name itself.
        return next(iter(row.values()))
    return row[0]


def _make_db(tmp_path: Path) -> Database:
    db_path = tmp_path / "live.db"
    config = {"path": str(db_path)}
    return Database(config)


def test_pragmas_applied_after_connect(tmp_path: Path) -> None:
    """After connect(), every Lever B pragma is at the documented value."""
    db = _make_db(tmp_path)
    db.connect()
    try:
        with db.get_conn() as conn:
            # mmap_size: requested 2 GB. SQLite rounds the actual
            # mapping down to a multiple of the system page size
            # (typically 4 KB or 64 KB), so allow any value within
            # one OS page-cluster of 2 GB.
            mmap_size = _read_pragma(conn, "mmap_size")
            assert 2_147_000_000 <= mmap_size <= 2_147_483_648, (
                f"mmap_size={mmap_size}, expected ~2147483648"
            )

            # cache_size: -262144 (negative = KB → 256 MB).
            cache_size = _read_pragma(conn, "cache_size")
            assert cache_size == -262144, (
                f"cache_size={cache_size}, expected -262144"
            )

            # synchronous: NORMAL == 1.
            sync = _read_pragma(conn, "synchronous")
            assert sync == 1, f"synchronous={sync}, expected 1 (NORMAL)"

            # temp_store: MEMORY == 2.
            temp_store = _read_pragma(conn, "temp_store")
            assert temp_store == 2, f"temp_store={temp_store}, expected 2"

            # journal_size_limit: 1 GB.
            jsl = _read_pragma(conn, "journal_size_limit")
            assert jsl == 1_073_741_824, (
                f"journal_size_limit={jsl}, expected 1073741824"
            )

            # journal_mode is still WAL (regression guard).
            jm = _read_pragma(conn, "journal_mode")
            assert (jm or "").lower() == "wal"
    finally:
        db.close()


def test_existing_small_page_size_warns_only(
    tmp_path: Path, caplog
) -> None:
    """Pre-existing DB with page_size=4096 must not crash; a hint is
    logged. We pre-create the file with 4096 then connect with our
    Database wrapper."""
    db_path = tmp_path / "live.db"

    # Pre-create with page_size=4096 and a real table so the file is
    # already paged. After this point page_size cannot be changed
    # without VACUUM.
    pre = sqlite3.connect(str(db_path))
    try:
        pre.execute("PRAGMA page_size=4096")
        pre.execute("PRAGMA journal_mode=WAL")
        pre.execute("CREATE TABLE seed (x INTEGER)")
        pre.execute("INSERT INTO seed VALUES (1)")
        pre.commit()
    finally:
        pre.close()

    db = Database({"path": str(db_path)})
    with caplog.at_level(logging.INFO, logger="file_activity.database"):
        db.connect()
    try:
        # No crash + the page_size hint message fired.
        page_hint = [
            r for r in caplog.records
            if "page_size=4096" in r.getMessage()
            or "Existing DB has page_size" in r.getMessage()
        ]
        assert page_hint, (
            "Expected page_size hint log, got: "
            f"{[r.getMessage() for r in caplog.records]}"
        )

        # And the existing page_size is still 4096 (we did not
        # silently corrupt the file).
        with db.get_conn() as conn:
            ps = _read_pragma(conn, "page_size")
            assert ps == 4096
    finally:
        db.close()


def test_wal_autocheckpoint_zero_when_checkpointer_alive(
    tmp_path: Path,
) -> None:
    """When the manual checkpointer thread starts cleanly,
    PRAGMA wal_autocheckpoint must be 0 — the engine no longer
    schedules its own checkpoints."""
    db = _make_db(tmp_path)
    db.connect()
    try:
        # Sanity: manual checkpointer is up.
        assert db.checkpointer is not None
        assert db._wal_autocheckpoint_pages == 0

        with db.get_conn() as conn:
            wac = _read_pragma(conn, "wal_autocheckpoint")
            assert wac == 0, f"wal_autocheckpoint={wac}, expected 0"
    finally:
        db.close()


def test_close_stops_checkpointer(tmp_path: Path) -> None:
    """db.close() must shut the daemon down so subsequent process
    exit / DB file rename is clean."""
    db = _make_db(tmp_path)
    db.connect()
    cp = db.checkpointer
    assert cp is not None
    assert cp._thread is not None
    assert cp._thread.is_alive()

    db.close()
    cp.join(timeout=2.0)
    assert not cp._thread.is_alive()
    # And the Database has dropped its reference.
    assert db.checkpointer is None
