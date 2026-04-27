"""Tests for issue #77 Phase 1: SQLite auto-backup.

Coverage:
  * test_snapshot_creates_file_and_manifest
  * test_snapshot_sha256_matches_file_content
  * test_prune_keeps_last_n_plus_weekly
  * test_restore_refuses_on_live_connection
  * test_restore_writes_db_atomically
  * test_cli_snapshot_writes_jsonl_to_stdout
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.backup_manager import (  # noqa: E402
    BackupManager,
    RestoreResult,
    SnapshotMeta,
)


def _make_db(path: Path) -> None:
    """Create a tiny SQLite DB with a few rows so snapshots are
    non-trivial and the restored copy is byte-comparable."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        conn.executemany(
            "INSERT INTO t (payload) VALUES (?)",
            [(f"row-{i}",) for i in range(50)],
        )
        conn.commit()
    finally:
        conn.close()


def _make_mgr(tmp_path: Path, **overrides) -> BackupManager:
    db_path = tmp_path / "live.db"
    _make_db(db_path)
    cfg = {
        "backup": {
            "enabled": True,
            "dir": str(tmp_path / "backups"),
            "keep_last_n": 10,
            "keep_weekly": 4,
        }
    }
    cfg["backup"].update(overrides)
    return BackupManager(str(db_path), cfg)


# ── 1. snapshot creates file + manifest ─────────────────────


def test_snapshot_creates_file_and_manifest(tmp_path: Path):
    mgr = _make_mgr(tmp_path)
    meta = mgr.snapshot(reason="test-create")

    # File on disk
    assert os.path.exists(meta.path), "snapshot .bak file must exist"
    assert meta.size_bytes == os.path.getsize(meta.path)

    # Manifest exists, parses to JSON, contains the entry
    manifest = json.loads(Path(mgr.manifest_path).read_text("utf-8"))
    assert isinstance(manifest, list)
    assert len(manifest) == 1
    entry = manifest[0]
    assert entry["id"] == meta.id
    assert entry["reason"] == "test-create"
    assert entry["sha256"] == meta.sha256
    # The snapshot itself must be a valid SQLite DB
    conn = sqlite3.connect(meta.path)
    try:
        rows = conn.execute("SELECT COUNT(*) FROM t").fetchone()
        assert rows[0] == 50
    finally:
        conn.close()


# ── 2. SHA-256 in manifest matches file ─────────────────────


def test_snapshot_sha256_matches_file_content(tmp_path: Path):
    mgr = _make_mgr(tmp_path)
    meta = mgr.snapshot(reason="checksum-test")

    h = hashlib.sha256()
    with open(meta.path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    assert meta.sha256 == h.hexdigest()

    # And that's also what's in the manifest
    manifest = json.loads(Path(mgr.manifest_path).read_text("utf-8"))
    assert manifest[0]["sha256"] == h.hexdigest()


# ── 3. prune keeps last N + weekly anchors ──────────────────


def test_prune_keeps_last_n_plus_weekly(tmp_path: Path, monkeypatch):
    """30 snapshots across 4 weeks -> prune to keep_last_n=5 +
    keep_weekly=4. Survivors must be the 5 newest snapshots PLUS the
    earliest snapshot of each of the 4 most recent ISO weeks (those
    not already in the daily window)."""
    mgr = _make_mgr(
        tmp_path,
        keep_last_n=5,
        keep_weekly=4,
    )

    # Build 30 deterministic snapshot timestamps spanning 30 days, one
    # per day at 03:00, starting 2026-03-15. We monkeypatch
    # BackupManager._now to return them in turn.
    from datetime import timedelta
    base = datetime(2026, 3, 15, 3, 0, 0)
    times: list[datetime] = [base + timedelta(days=d) for d in range(30)]

    counter = {"i": 0}

    def fake_now(self=None):
        # bound or unbound — we monkeypatch as staticmethod
        i = counter["i"]
        counter["i"] += 1
        return times[i]

    monkeypatch.setattr(BackupManager, "_now", staticmethod(fake_now))

    created: list[SnapshotMeta] = []
    for _ in range(30):
        created.append(mgr.snapshot(reason="seed"))
    assert len(created) == 30

    deleted = mgr.prune()

    survivors = mgr.list_snapshots()
    survivor_ids = {m.id for m in survivors}

    # Daily survivors: the 5 newest by id
    expected_daily = {m.id for m in sorted(created, key=lambda m: m.id)[-5:]}

    # Weekly survivors: for each of the 4 most recent ISO weeks
    # represented in `created`, take the earliest snapshot.
    by_week: dict[tuple[int, int], list[SnapshotMeta]] = {}
    for m in created:
        dt = datetime.strptime(m.id[:15], "%Y%m%d_%H%M%S")
        wk = (dt.isocalendar()[0], dt.isocalendar()[1])
        by_week.setdefault(wk, []).append(m)
    # Most recent 4 weeks
    recent_weeks = sorted(by_week.keys(), reverse=True)[:4]
    expected_weekly = {
        sorted(by_week[wk], key=lambda m: m.id)[0].id for wk in recent_weeks
    }

    expected_survivors = expected_daily | expected_weekly
    assert survivor_ids == expected_survivors, (
        f"Mismatch.\nGot: {sorted(survivor_ids)}\n"
        f"Expected: {sorted(expected_survivors)}"
    )
    assert deleted == 30 - len(expected_survivors)

    # And every survivor still has a real file on disk
    for m in survivors:
        assert os.path.exists(m.path)


# ── 4. restore refuses while a live connection holds the DB ─


def test_restore_refuses_on_live_connection(tmp_path: Path):
    mgr = _make_mgr(tmp_path)
    meta = mgr.snapshot(reason="for-restore")

    # Open + actively hold a write lock so VACUUM-INTO-style locks fail
    conn = sqlite3.connect(mgr.db_path, isolation_level=None)
    try:
        conn.execute("BEGIN EXCLUSIVE")
        with pytest.raises(RuntimeError, match="live connection"):
            mgr.restore(meta.id)
    finally:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        conn.close()


# ── 5. restore writes db atomically (byte-equal) ────────────


def test_restore_writes_db_atomically(tmp_path: Path):
    mgr = _make_mgr(tmp_path)
    meta = mgr.snapshot(reason="for-roundtrip")

    # Mutate the live DB after the snapshot
    conn = sqlite3.connect(mgr.db_path)
    try:
        conn.execute("INSERT INTO t (payload) VALUES ('post-snapshot')")
        conn.commit()
    finally:
        conn.close()

    # Force-close any WAL by checkpointing then deleting siblings —
    # restore() does that internally too, but the test wants a clean
    # baseline.
    conn = sqlite3.connect(mgr.db_path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()

    mgr.restore(meta.id)

    # The live DB should now be byte-equal to the snapshot
    with open(mgr.db_path, "rb") as f:
        live_bytes = f.read()
    with open(meta.path, "rb") as f:
        snap_bytes = f.read()
    assert live_bytes == snap_bytes

    # And the post-snapshot row must be gone
    conn = sqlite3.connect(mgr.db_path)
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM t WHERE payload='post-snapshot'"
        ).fetchone()
        assert rows[0] == 0
    finally:
        conn.close()


# ── 6. CLI snapshot writes JSONL to stdout ──────────────────


def test_cli_snapshot_writes_jsonl_to_stdout(tmp_path: Path):
    """Run `python -m src.storage.backup_manager snapshot` against a
    fixture DB + config, verify exit 0 and that stdout contains the
    SnapshotMeta as JSON.
    """
    db_path = tmp_path / "fixture.db"
    _make_db(db_path)

    # Minimal config.yaml the CLI can find via load_config
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "database:\n"
        f"  path: \"{db_path}\"\n"
        "backup:\n"
        "  enabled: true\n"
        f"  dir: \"{tmp_path / 'backups'}\"\n"
        "  keep_last_n: 5\n"
        "  keep_weekly: 2\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")

    proc = subprocess.run(
        [
            sys.executable, "-m", "src.storage.backup_manager",
            "--config", str(cfg_path),
            "snapshot", "--reason", "test",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )

    assert proc.returncode == 0, (
        f"CLI exited non-zero.\nstdout={proc.stdout!r}\n"
        f"stderr={proc.stderr!r}"
    )
    # Stdout must be parseable JSON describing the snapshot
    last_line = proc.stdout.strip().splitlines()[-1]
    payload = json.loads(last_line)
    assert payload["ok"] is True
    assert payload["reason"] == "test"
    assert payload["sha256"]
    assert payload["size_bytes"] > 0
    # And the .bak file actually landed
    assert os.path.exists(payload["path"])


# ─────────────────────────────────────────────────────────────
# Phase 2 (#77) — auto_restore_if_needed
# ─────────────────────────────────────────────────────────────

def _corrupt_db(path: Path) -> None:
    """Replace the DB file with garbage so PRAGMA integrity_check
    will raise ``file is not a database``.
    """
    path.write_bytes(b"NOT A REAL DB" * 128)


def _make_app_shaped_db(path: Path) -> None:
    """Like _make_db but also creates the three critical tables the
    corruption detector probes, so the freshly-restored DB passes the
    detector and Database.connect() boots cleanly.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE scan_runs (id INTEGER PRIMARY KEY)"
        )
        conn.execute(
            "CREATE TABLE scanned_files (id INTEGER PRIMARY KEY)"
        )
        conn.execute(
            "CREATE TABLE file_audit_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "source_id INTEGER,"
            "event_time TEXT,"
            "event_type TEXT,"
            "username TEXT,"
            "file_path TEXT,"
            "file_name TEXT,"
            "details TEXT,"
            "detected_by TEXT)"
        )
        conn.commit()
    finally:
        conn.close()


def _make_app_shaped_mgr(tmp_path: Path, **overrides) -> BackupManager:
    db_path = tmp_path / "live.db"
    _make_app_shaped_db(db_path)
    cfg = {
        "backup": {
            "enabled": True,
            "dir": str(tmp_path / "backups"),
            "keep_last_n": 10,
            "keep_weekly": 4,
            "auto_restore_on_corruption": False,
        },
        "smtp": {"enabled": False},
    }
    cfg["backup"].update(overrides)
    return BackupManager(str(db_path), cfg)


def test_auto_restore_disabled_in_config_no_restore(tmp_path: Path):
    """Corruption is detected but the config flag is false. No
    restore, no rename — the broken DB stays exactly where it is so
    the operator can investigate. Result must report
    ``reason='disabled_in_config'``.
    """
    mgr = _make_app_shaped_mgr(tmp_path)
    # Take a snapshot first so a viable target exists — we want to
    # prove the gate is config, not snapshot availability.
    mgr.snapshot(reason="seed")

    # Corrupt the live DB
    _corrupt_db(Path(mgr.db_path))
    pre_size = os.path.getsize(mgr.db_path)
    pre_bytes = Path(mgr.db_path).read_bytes()

    assert mgr.auto_restore_on_corruption is False
    result = mgr.auto_restore_if_needed()

    assert isinstance(result, RestoreResult)
    assert result.restored is False
    assert result.reason == "disabled_in_config"

    # Live DB untouched — same size, same bytes, no broken-* sibling.
    assert os.path.getsize(mgr.db_path) == pre_size
    assert Path(mgr.db_path).read_bytes() == pre_bytes
    siblings = [p.name for p in Path(mgr.db_path).parent.iterdir()
                if "broken" in p.name]
    assert siblings == []


def test_auto_restore_enabled_performs_salvage(tmp_path: Path):
    """Flag true + corrupted DB + valid snapshot:
       * the broken file is preserved at <db>.broken-<ts>
       * the snapshot is copied to db_path
       * the restored db_path passes integrity_check
       * an audit event is written into the restored DB
    """
    mgr = _make_app_shaped_mgr(
        tmp_path,
        auto_restore_on_corruption=True,
    )
    snap = mgr.snapshot(reason="pre-corrupt")

    # Now corrupt the live DB.
    _corrupt_db(Path(mgr.db_path))

    result = mgr.auto_restore_if_needed()
    assert result is not None
    assert result.restored is True
    assert result.reason == "auto_restored"
    assert result.snapshot_id == snap.id

    # Broken file preserved
    assert result.broken_path is not None
    assert os.path.exists(result.broken_path), "broken DB must be kept for forensics"
    # Forensic name format: <db_path>.broken-YYYYMMDD_HHMMSS
    assert result.broken_path.startswith(mgr.db_path + ".broken-")

    # The restored DB passes integrity_check (we don't compare
    # byte-for-byte against the snapshot because the auto_restore
    # audit insert intentionally mutates the live file).
    assert os.path.exists(mgr.db_path)
    assert os.path.getsize(mgr.db_path) > 0

    # And it passes integrity_check now.
    conn = sqlite3.connect(mgr.db_path)
    try:
        rows = conn.execute("PRAGMA integrity_check").fetchall()
        assert [r[0] for r in rows] == ["ok"]
        # The audit event the manager wrote during salvage must be
        # visible on the restored DB.
        cur = conn.execute(
            "SELECT event_type, detected_by FROM file_audit_events "
            "WHERE event_type='auto_restore'"
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "auto_restore"
        assert rows[0][1] == "backup_manager"
    finally:
        conn.close()
    # And the manager reports the audit_event_id it inserted.
    assert result.audit_event_id is not None


def test_auto_restore_no_snapshot_available(tmp_path: Path):
    """Flag true + corrupted DB + zero snapshots → no salvage,
    broken file untouched, ``reason='no_snapshot'``.
    """
    mgr = _make_app_shaped_mgr(
        tmp_path,
        auto_restore_on_corruption=True,
    )
    # Don't take any snapshots
    assert mgr.list_snapshots() == []

    _corrupt_db(Path(mgr.db_path))
    pre_bytes = Path(mgr.db_path).read_bytes()

    result = mgr.auto_restore_if_needed()
    assert result is not None
    assert result.restored is False
    assert result.reason == "no_snapshot"

    # Broken file is left in place — the application should refuse to
    # boot rather than silently delete the only copy.
    assert Path(mgr.db_path).read_bytes() == pre_bytes
    siblings = [p.name for p in Path(mgr.db_path).parent.iterdir()
                if ".broken-" in p.name]
    assert siblings == []


def test_auto_restore_passes_when_db_is_healthy(tmp_path: Path):
    """No corruption, no action — result must say not_corrupted and
    leave the DB completely alone.
    """
    mgr = _make_app_shaped_mgr(
        tmp_path,
        auto_restore_on_corruption=True,
    )
    mgr.snapshot(reason="seed")

    pre_bytes = Path(mgr.db_path).read_bytes()
    result = mgr.auto_restore_if_needed()
    assert result is not None
    assert result.restored is False
    assert result.reason == "not_corrupted"
    assert Path(mgr.db_path).read_bytes() == pre_bytes
