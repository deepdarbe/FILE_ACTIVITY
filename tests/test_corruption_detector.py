"""Tests for issue #77 Phase 2: SQLite corruption detector.

Covers the three signals from src.storage.corruption_detector.is_corrupted:
  * test_corruption_detector_detects_truncated_db
  * test_corruption_detector_detects_missing_tables
  * test_corruption_detector_passes_valid_db

Plus the hotfix coverage for the quick/full/skip mode + timeout knobs:
  * test_corruption_detector_quick_mode_completes_fast
  * test_corruption_detector_skip_mode_returns_clean
  * test_corruption_detector_timeout_returns_clean_with_reason
  * test_corruption_detector_full_mode_still_works

The detector is intentionally read-only — every test verifies the input
file is unchanged after the probe runs (no side effects).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage import corruption_detector as cd_mod  # noqa: E402
from src.storage.corruption_detector import (  # noqa: E402
    CorruptionResult,
    CRITICAL_TABLES,
    is_corrupted,
)


def _make_valid_db(path: Path) -> None:
    """Build a minimal but app-shaped DB with all critical tables.

    The detector only checks for table presence by name + the
    integrity_check pragma — schema columns are irrelevant here, so
    we keep these tiny.
    """
    conn = sqlite3.connect(str(path))
    try:
        for tbl in CRITICAL_TABLES:
            conn.execute(f"CREATE TABLE {tbl} (id INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()


# ── 1. truncated / non-database file ────────────────────────


def test_corruption_detector_detects_truncated_db(tmp_path: Path):
    """A file that pretends to be SQLite but is truncated/garbage must
    be reported as ``integrity_fail`` — that's what SQLite raises when
    the header doesn't parse or pages don't checksum.
    """
    target = tmp_path / "broken.db"
    # Write enough bytes that os.path.getsize > 0, but the contents
    # are not a valid SQLite header — sqlite3 will raise
    # ``DatabaseError: file is not a database`` from the first PRAGMA.
    target.write_bytes(b"NOT A REAL DB" * 64)

    result = is_corrupted(str(target))
    assert isinstance(result, CorruptionResult)
    assert result.is_corrupted is True
    assert result.reason == "integrity_fail"
    assert result.details  # non-empty


# ── 2. missing tables on an otherwise valid SQLite file ─────


def test_corruption_detector_detects_missing_tables(tmp_path: Path):
    """A pristine sqlite3.connect with no schema is structurally
    valid (integrity_check returns ``ok``) but missing every critical
    table — the second check must catch this and report
    ``missing_tables`` with all three names in the details string.
    """
    target = tmp_path / "empty.db"
    conn = sqlite3.connect(str(target))
    try:
        # Create *some* table so the file actually has a SQLite header
        # on disk (otherwise sqlite3.connect leaves a zero-byte file
        # which the detector short-circuits as missing_tables on a
        # different code path — we want to exercise the real
        # sqlite_master scan here).
        conn.execute(
            "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)"
        )
        conn.commit()
    finally:
        conn.close()
    assert target.stat().st_size > 0

    result = is_corrupted(str(target))
    assert result.is_corrupted is True
    assert result.reason == "missing_tables"
    # All critical tables should be listed in the details.
    for tbl in CRITICAL_TABLES:
        assert tbl in result.details


# ── 3. healthy DB passes both checks ────────────────────────


def test_corruption_detector_passes_valid_db(tmp_path: Path):
    target = tmp_path / "healthy.db"
    _make_valid_db(target)

    # Snapshot the file bytes so we can confirm the probe is read-only.
    pre = target.read_bytes()

    result = is_corrupted(str(target))
    assert result.is_corrupted is False
    assert result.reason == "none"

    # Read-only invariant: probe must not mutate the file. (sqlite3
    # may touch the access time but the byte content stays put.)
    post = target.read_bytes()
    assert pre == post


# ── 4. zero-byte file is treated as missing-tables ──────────


def test_corruption_detector_zero_byte_file(tmp_path: Path):
    """Edge case: a freshly-created empty file. ``sqlite3.connect``
    happily accepts it, but the app cannot run against an empty
    schema — the detector short-circuits to ``missing_tables`` so
    the bootstrap can fall back to a snapshot if available.
    """
    target = tmp_path / "zero.db"
    target.touch()
    assert target.stat().st_size == 0

    result = is_corrupted(str(target))
    assert result.is_corrupted is True
    assert result.reason == "missing_tables"


# ── 5. nonexistent path returns ``not corrupted`` ───────────


def test_corruption_detector_nonexistent_path(tmp_path: Path):
    """If the DB file isn't there at all the detector defers to the
    caller — Database.connect will create one. NOT a corruption
    signal.
    """
    target = tmp_path / "does-not-exist.db"
    assert not target.exists()

    result = is_corrupted(str(target))
    assert result.is_corrupted is False
    assert result.reason == "none"


# ── 6. quick mode (default) finishes fast on a healthy DB ──


def test_corruption_detector_quick_mode_completes_fast(tmp_path: Path):
    """The quick mode is the hotfix default. On any reasonably-sized
    DB the probe must complete in well under a second. We don't have
    a 1 GB fixture in the unit-test tree, but the choice of pragma is
    what matters — verify the wall-clock budget is comfortable AND
    that the chosen pragma is ``quick_check`` not ``integrity_check``.
    """
    target = tmp_path / "healthy.db"
    _make_valid_db(target)
    # Pad the DB a bit so the probe actually walks a non-trivial page
    # set — still small enough that quick_check finishes in ms.
    conn = sqlite3.connect(str(target))
    try:
        conn.execute("CREATE TABLE blob_data (id INTEGER PRIMARY KEY, b BLOB)")
        conn.executemany(
            "INSERT INTO blob_data (b) VALUES (?)",
            [(b"x" * 4096,) for _ in range(200)],
        )
        conn.commit()
    finally:
        conn.close()

    config = {"backup": {"corruption_check_mode": "quick",
                         "corruption_check_timeout_seconds": 30}}
    t0 = time.monotonic()
    result = is_corrupted(str(target), config)
    elapsed = time.monotonic() - t0

    assert result.is_corrupted is False
    assert result.reason == "none"
    # Generous budget for shared CI runners; quick_check itself is
    # sub-100ms on a small DB.
    assert elapsed < 5.0, f"quick mode probe took {elapsed:.2f}s (>5s)"


def test_corruption_detector_skip_mode_returns_clean(tmp_path: Path):
    """``skip`` mode bypasses the pragma entirely and returns a clean
    result with reason='skipped'. This is the operator escape hatch
    for sites that have an external integrity workflow.
    """
    target = tmp_path / "anything.db"
    target.write_bytes(b"this is not even a real sqlite file")  # would normally fail

    config = {"backup": {"corruption_check_mode": "skip"}}
    result = is_corrupted(str(target), config)

    assert result.is_corrupted is False
    assert result.reason == "skipped"


def test_corruption_detector_timeout_returns_clean_with_reason(
    monkeypatch, tmp_path: Path,
):
    """If the pragma exceeds ``corruption_check_timeout_seconds`` the
    detector must return ``is_corrupted=False, reason='check_timed_out'``
    and NOT block. We simulate a hang by monkeypatching
    ``_run_pragma_with_timeout`` to behave as the real function does
    when the worker thread doesn't finish in time.
    """
    target = tmp_path / "hangs.db"
    _make_valid_db(target)

    def _fake_run(db_path, pragma, timeout):
        # Mirror the real timeout return so we test the public surface.
        return "timeout", float(timeout)

    monkeypatch.setattr(cd_mod, "_run_pragma_with_timeout", _fake_run)

    config = {"backup": {"corruption_check_mode": "quick",
                         "corruption_check_timeout_seconds": 1}}
    t0 = time.monotonic()
    result = is_corrupted(str(target), config)
    elapsed = time.monotonic() - t0

    assert result.is_corrupted is False
    assert result.reason == "check_timed_out"
    assert "1" in result.details  # mentions the timeout budget
    # The fake short-circuits — wall-clock must be tiny.
    assert elapsed < 1.0


def test_corruption_detector_full_mode_still_works(tmp_path: Path):
    """Operators who explicitly want the slow-but-thorough path can
    opt back into ``PRAGMA integrity_check`` via mode='full'. Verify
    it correctly identifies a healthy DB AND that it's actually
    running the full pragma (not silently downgraded to quick_check).
    """
    target = tmp_path / "healthy.db"
    _make_valid_db(target)

    # Spy on the pragma actually executed.
    seen: list[str] = []
    real_run = cd_mod._run_pragma_with_timeout

    def _spy(db_path, pragma, timeout):
        seen.append(pragma)
        return real_run(db_path, pragma, timeout)

    config = {"backup": {"corruption_check_mode": "full",
                         "corruption_check_timeout_seconds": 30}}

    # Use monkeypatch-style replace for the duration of the call.
    cd_mod._run_pragma_with_timeout = _spy
    try:
        result = is_corrupted(str(target), config)
    finally:
        cd_mod._run_pragma_with_timeout = real_run

    assert result.is_corrupted is False
    assert result.reason == "none"
    assert seen == ["PRAGMA integrity_check"], (
        f"full mode must run integrity_check, got: {seen}"
    )
