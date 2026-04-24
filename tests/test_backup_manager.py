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

from src.storage.backup_manager import BackupManager, SnapshotMeta  # noqa: E402


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
