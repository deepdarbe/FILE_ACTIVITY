"""SQLite auto-backup manager (issue #77 Phase 1).

Provides safe, lock-free SQLite snapshots using ``VACUUM INTO`` (works under
WAL without exclusive locks), SHA-256 verification, an atomic JSON manifest,
and a retention policy combining last-N + weekly survivors.

Design notes
------------
* ``VACUUM INTO 'path'`` is the single supported snapshot primitive — it
  produces a defragmented, internally-consistent copy without holding an
  exclusive lock on the live DB. We deliberately do NOT use ``shutil.copy``
  (would race with WAL writes) or ``sqlite3.Connection.backup`` (extra
  complexity, no upside here).
* ``manifest.json`` is rewritten atomically (tempfile + ``os.replace``).
* ``restore()`` is provided as a Phase-1 utility but never auto-called this
  round. Phase 2 (``auto_restore_on_corruption``) will wire it from the
  startup probe — the config field exists today but the value must stay
  ``False`` until that work lands.
* Snapshot failures must NEVER abort the calling operation — callers wrap
  ``snapshot()`` defensively and continue.

CLI
---
``python -m src.storage.backup_manager {snapshot|list|restore|prune}``

Reuses :func:`src.utils.config_loader.load_config` so behaviour matches
``main.py``.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger("file_activity.storage.backup_manager")


@dataclass
class SnapshotMeta:
    """Metadata for one backup snapshot.

    ``id`` is the timestamp string used in the file name
    (``YYYYMMDD_HHMMSS``). It is also the primary key in the manifest.
    """

    id: str
    path: str
    size_bytes: int
    sha256: str
    created_at: str  # ISO 8601
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SnapshotMeta":
        # Be liberal in what we accept — older manifest rows may be
        # missing fields; treat absent values as empty/zero.
        return cls(
            id=str(data.get("id", "")),
            path=str(data.get("path", "")),
            size_bytes=int(data.get("size_bytes", 0) or 0),
            sha256=str(data.get("sha256", "")),
            created_at=str(data.get("created_at", "")),
            reason=str(data.get("reason", "")),
        )


class BackupManager:
    """Owns the snapshot/manifest/prune lifecycle for one SQLite DB."""

    MANIFEST_NAME = "manifest.json"

    def __init__(self, db_path: str, config: dict):
        """``config`` is the **whole** loaded config dict, the same shape
        ``main.py`` passes around. Backup-specific options live under
        ``config['backup']``.
        """
        self.db_path = str(db_path)
        self.config = config or {}
        backup_cfg = (self.config.get("backup") or {}) if isinstance(self.config, dict) else {}

        self.enabled: bool = bool(backup_cfg.get("enabled", True))
        self.backup_dir: str = str(backup_cfg.get("dir", "data/backups"))
        self.keep_last_n: int = int(backup_cfg.get("keep_last_n", 10) or 0)
        self.keep_weekly: int = int(backup_cfg.get("keep_weekly", 4) or 0)
        # Phase 2 placeholder — read but never acted on this round.
        self.auto_restore_on_corruption: bool = bool(
            backup_cfg.get("auto_restore_on_corruption", False)
        )

    # ──────────────────────────────────────────────
    # Paths
    # ──────────────────────────────────────────────

    @property
    def manifest_path(self) -> str:
        return os.path.join(self.backup_dir, self.MANIFEST_NAME)

    def _snapshot_filename(self, snap_id: str) -> str:
        base = os.path.basename(self.db_path) or "file_activity.db"
        return f"{base}.{snap_id}.bak"

    def _ensure_dir(self) -> None:
        os.makedirs(self.backup_dir, exist_ok=True)

    # ──────────────────────────────────────────────
    # Manifest IO
    # ──────────────────────────────────────────────

    def _read_manifest(self) -> list[dict]:
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return []
        except (OSError, ValueError) as e:
            # Corrupt manifest is recoverable — we log + start fresh
            # rather than crash the snapshot run. The .bak files on disk
            # are still valid; they just lose their metadata until next
            # snapshot rebuilds the manifest.
            logger.error("manifest unreadable (%s) — starting fresh", e)
            return []
        if not isinstance(data, list):
            logger.error("manifest is not a list — starting fresh")
            return []
        return data

    def _write_manifest_atomic(self, entries: list[dict]) -> None:
        """Write manifest via tempfile + os.replace so a crash mid-write
        cannot leave a half-written file.
        """
        self._ensure_dir()
        fd, tmp_path = tempfile.mkstemp(
            prefix=".manifest.", suffix=".tmp", dir=self.backup_dir,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2, sort_keys=True)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    # Some filesystems (tmpfs in CI) don't support fsync;
                    # the replace below is still atomic.
                    pass
            os.replace(tmp_path, self.manifest_path)
        except Exception:
            # Best-effort cleanup of the tempfile if we never replaced.
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise

    # ──────────────────────────────────────────────
    # Snapshot
    # ──────────────────────────────────────────────

    @staticmethod
    def _sha256_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _now() -> datetime:
        # Indirection so tests can monkeypatch ``BackupManager._now``.
        return datetime.now()

    def snapshot(self, reason: str) -> SnapshotMeta:
        """Take a snapshot via ``VACUUM INTO``.

        Returns the SnapshotMeta on success. Raises on failure — callers
        that want best-effort behaviour must wrap this in try/except (the
        archive + retention hooks do).
        """
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(
                f"source DB not found: {self.db_path}"
            )

        self._ensure_dir()
        now = self._now()
        snap_id = now.strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(
            self.backup_dir, self._snapshot_filename(snap_id)
        )

        # If a snapshot in the same second exists (rare in tests), suffix
        # a counter so VACUUM INTO doesn't fail with "file exists".
        if os.path.exists(out_path):
            i = 1
            while True:
                cand = os.path.join(
                    self.backup_dir,
                    self._snapshot_filename(f"{snap_id}_{i}"),
                )
                if not os.path.exists(cand):
                    out_path = cand
                    snap_id = f"{snap_id}_{i}"
                    break
                i += 1

        logger.info(
            "snapshot starting: db=%s -> %s (reason=%s)",
            self.db_path, out_path, reason,
        )

        # VACUUM INTO requires a fresh connection (no open transaction)
        # and refuses to overwrite an existing path — both already
        # guaranteed above.
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            # Quote the path safely — VACUUM INTO uses string literal
            # syntax; double single-quotes inside the literal.
            quoted = out_path.replace("'", "''")
            conn.execute(f"VACUUM INTO '{quoted}'")
        finally:
            conn.close()

        size = os.path.getsize(out_path)
        sha = self._sha256_file(out_path)
        meta = SnapshotMeta(
            id=snap_id,
            path=out_path,
            size_bytes=size,
            sha256=sha,
            created_at=now.isoformat(timespec="seconds"),
            reason=str(reason or ""),
        )

        # Append to manifest (atomic).
        entries = self._read_manifest()
        entries.append(meta.to_dict())
        self._write_manifest_atomic(entries)

        logger.info(
            "snapshot ok: id=%s size=%d sha256=%s reason=%s",
            meta.id, meta.size_bytes, meta.sha256[:12], meta.reason,
        )
        return meta

    # ──────────────────────────────────────────────
    # List / Prune / Restore
    # ──────────────────────────────────────────────

    def list_snapshots(self) -> list[SnapshotMeta]:
        """Return snapshot metas sorted by id ascending (oldest first)."""
        entries = self._read_manifest()
        metas = [SnapshotMeta.from_dict(e) for e in entries]
        metas.sort(key=lambda m: m.id)
        return metas

    def _iso_week_key(self, snap_id: str) -> Optional[tuple[int, int]]:
        """Parse ``YYYYMMDD_HHMMSS`` -> (iso_year, iso_week). Returns
        ``None`` for unparsable ids (which are then treated as not
        contributing a weekly survivor).
        """
        try:
            dt = datetime.strptime(snap_id[:15], "%Y%m%d_%H%M%S")
        except ValueError:
            return None
        iso = dt.isocalendar()
        return (iso[0], iso[1])

    def prune(self) -> int:
        """Delete snapshots beyond the retention budget. Returns the
        count removed.

        Retention rules:
          * Always keep the ``keep_last_n`` newest snapshots.
          * In addition, keep the oldest snapshot of each of the
            ``keep_weekly`` most recent ISO weeks (the "weekly anchor").
            A weekly anchor is preserved even if the daily window would
            otherwise drop it.
          * Anything not in either survivor set is deleted.
        """
        metas = self.list_snapshots()
        if not metas:
            return 0

        # Keep the last N (newest) — list is sorted ascending, so take
        # the tail.
        keep_ids: set[str] = set()
        if self.keep_last_n > 0:
            for m in metas[-self.keep_last_n:]:
                keep_ids.add(m.id)

        # Weekly anchors: walk newest-first, pick the *first* (= oldest
        # we encounter when iterating reversed) snapshot per ISO week,
        # but only for the most recent ``keep_weekly`` distinct weeks.
        # Spec: "Weekly = first snapshot of each ISO week (kept;
        # daily-rotation doesn't delete it)." We implement that as the
        # earliest snapshot in chronological order whose ISO week falls
        # in the most recent ``keep_weekly`` weeks.
        if self.keep_weekly > 0:
            # Collect distinct weeks newest -> oldest
            seen_weeks: list[tuple[int, int]] = []
            for m in reversed(metas):
                wk = self._iso_week_key(m.id)
                if wk is None:
                    continue
                if wk not in seen_weeks:
                    seen_weeks.append(wk)
                if len(seen_weeks) >= self.keep_weekly:
                    break
            target_weeks = set(seen_weeks)

            # For each target week, pick the earliest snapshot (chrono
            # order) — that's the "first snapshot of the week".
            week_first: dict[tuple[int, int], str] = {}
            for m in metas:  # ascending
                wk = self._iso_week_key(m.id)
                if wk is None or wk not in target_weeks:
                    continue
                if wk not in week_first:
                    week_first[wk] = m.id
            keep_ids.update(week_first.values())

        deleted = 0
        survivors: list[dict] = []
        for m in metas:
            if m.id in keep_ids:
                survivors.append(m.to_dict())
                continue
            try:
                if m.path and os.path.exists(m.path):
                    os.remove(m.path)
                deleted += 1
                logger.info("pruned snapshot: id=%s path=%s", m.id, m.path)
            except OSError as e:
                # File deletion failed — keep the manifest entry so we
                # can retry next prune. Log but don't crash.
                logger.error(
                    "prune failed to delete %s: %s", m.path, e,
                )
                survivors.append(m.to_dict())

        if deleted:
            self._write_manifest_atomic(survivors)
        return deleted

    def _detect_live_connection(self) -> bool:
        """Best-effort check that no other process holds the DB open.

        We open a fresh connection and run ``PRAGMA database_list`` just
        to confirm the DB is reachable; the real signal is whether we
        can acquire an exclusive write lock immediately. If we cannot,
        someone else (likely the dashboard) is connected.
        """
        if not os.path.exists(self.db_path):
            return False
        conn = sqlite3.connect(self.db_path, timeout=0.5)
        try:
            conn.execute("PRAGMA database_list").fetchall()
            try:
                # BEGIN EXCLUSIVE will fail fast if any other connection
                # holds a lock on the WAL. We immediately roll back —
                # we never want to mutate during a "is it safe to
                # restore?" probe.
                conn.execute("BEGIN EXCLUSIVE")
                conn.execute("ROLLBACK")
                return False
            except sqlite3.OperationalError:
                return True
        finally:
            conn.close()

    def restore(self, snapshot_id: str) -> None:
        """Copy a snapshot file over ``db_path``. Caller is responsible
        for stopping the dashboard / scheduler first; this method
        refuses to run if it detects a live connection.

        NOTE (Phase 1): never auto-called. Phase 2 will wire this from
        the startup corruption probe when
        ``backup.auto_restore_on_corruption`` is True.
        """
        metas = {m.id: m for m in self.list_snapshots()}
        meta = metas.get(snapshot_id)
        if meta is None:
            raise KeyError(f"unknown snapshot id: {snapshot_id}")
        if not os.path.exists(meta.path):
            raise FileNotFoundError(
                f"snapshot file missing on disk: {meta.path}"
            )

        if self._detect_live_connection():
            raise RuntimeError(
                "refusing to restore: a live connection holds the "
                f"database lock on {self.db_path}. Stop the dashboard / "
                "service first."
            )

        # Verify integrity of the snapshot file before clobbering the
        # live DB. SHA-256 from manifest must match disk.
        actual = self._sha256_file(meta.path)
        if meta.sha256 and actual != meta.sha256:
            raise RuntimeError(
                f"snapshot {snapshot_id} sha256 mismatch — refusing "
                f"to restore (manifest={meta.sha256[:12]}, "
                f"disk={actual[:12]})"
            )

        # Atomic-ish swap: copy to a sibling tempfile, then rename over
        # the live DB. Also remove WAL/SHM siblings — they belong to the
        # *old* DB and would corrupt the restored copy if SQLite
        # re-attached them.
        db_dir = os.path.dirname(self.db_path) or "."
        os.makedirs(db_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=".restore.", suffix=".db", dir=db_dir,
        )
        os.close(fd)
        try:
            shutil.copy2(meta.path, tmp)
            os.replace(tmp, self.db_path)
        except Exception:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass
            raise

        for sibling in (self.db_path + "-wal", self.db_path + "-shm"):
            try:
                if os.path.exists(sibling):
                    os.remove(sibling)
            except OSError as e:
                logger.warning(
                    "restore: failed to remove %s: %s", sibling, e
                )

        logger.info(
            "restore ok: id=%s -> %s (size=%d)",
            meta.id, self.db_path, meta.size_bytes,
        )


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def _resolve_db_path_and_manager(config_path: str) -> BackupManager:
    """Load config the same way main.py does and build a manager."""
    # Local import so importing this module never pulls yaml in.
    from src.utils.config_loader import load_config

    cfg = load_config(config_path)
    db_path = ((cfg.get("database") or {}).get("path")
               or "data/file_activity.db")
    return BackupManager(db_path, cfg)


def _cmd_snapshot(mgr: BackupManager, reason: str) -> int:
    try:
        meta = mgr.snapshot(reason=reason)
    except Exception as e:
        logger.error("snapshot failed: %s\n%s", e, traceback.format_exc())
        print(json.dumps({"ok": False, "error": str(e)}))
        return 1
    print(json.dumps({"ok": True, **meta.to_dict()}))
    return 0


def _cmd_list(mgr: BackupManager) -> int:
    metas = mgr.list_snapshots()
    for m in metas:
        print(json.dumps(m.to_dict()))
    return 0


def _cmd_restore(mgr: BackupManager, snap_id: str) -> int:
    try:
        mgr.restore(snap_id)
    except Exception as e:
        logger.error("restore failed: %s\n%s", e, traceback.format_exc())
        print(json.dumps({"ok": False, "error": str(e)}))
        return 1
    print(json.dumps({"ok": True, "restored": snap_id}))
    return 0


def _cmd_prune(mgr: BackupManager) -> int:
    try:
        deleted = mgr.prune()
    except Exception as e:
        logger.error("prune failed: %s\n%s", e, traceback.format_exc())
        print(json.dumps({"ok": False, "error": str(e)}))
        return 1
    print(json.dumps({"ok": True, "deleted": deleted}))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    # Make sure the CLI emits at least INFO logs to stderr so the
    # operator sees what happened even if the JSON line on stdout is
    # piped away.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

    parser = argparse.ArgumentParser(
        prog="python -m src.storage.backup_manager",
        description="FILE ACTIVITY SQLite backup manager (issue #77).",
    )
    parser.add_argument(
        "--config", "-c", default="config.yaml",
        help="Path to config.yaml (same default as main.py).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_snap = sub.add_parser("snapshot", help="Take a snapshot now.")
    p_snap.add_argument("--reason", default="manual",
                        help="Free-text reason for the manifest entry.")

    sub.add_parser("list", help="List known snapshots from the manifest.")

    p_rest = sub.add_parser("restore", help="Restore a snapshot over the live DB.")
    p_rest.add_argument("--id", required=True, help="Snapshot id (YYYYMMDD_HHMMSS).")

    sub.add_parser("prune", help="Apply retention: keep_last_n + keep_weekly.")

    args = parser.parse_args(argv)
    mgr = _resolve_db_path_and_manager(args.config)

    if args.cmd == "snapshot":
        return _cmd_snapshot(mgr, args.reason)
    if args.cmd == "list":
        return _cmd_list(mgr)
    if args.cmd == "restore":
        return _cmd_restore(mgr, args.id)
    if args.cmd == "prune":
        return _cmd_prune(mgr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
