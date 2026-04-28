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
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger("file_activity.storage.backup_manager")


@dataclass
class RestoreResult:
    """Outcome of an :meth:`BackupManager.auto_restore_if_needed` call.

    ``restored=True`` means the live DB was replaced from a snapshot;
    the broken file lives on at ``broken_path`` for forensics. When
    ``restored=False`` the ``reason`` carries one of:

      * ``"not_corrupted"`` — probe said the DB is fine, no action taken.
      * ``"disabled_in_config"`` — corruption detected but
        ``backup.auto_restore_on_corruption`` is False. Caller should
        log CRITICAL.
      * ``"corruption_detected"`` — alias used at the API layer for the
        disabled-in-config path so the dashboard can surface a banner.
      * ``"no_snapshot"`` — corruption detected, config flag true, but
        no snapshot is available to restore from.
      * ``"restore_failed"`` — corruption detected, attempted restore,
        but the salvage step itself raised. Broken DB is preserved.
    """

    restored: bool
    reason: str
    snapshot_id: Optional[str] = None
    broken_path: Optional[str] = None
    audit_event_id: Optional[int] = None
    ts: Optional[str] = None
    details: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


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
        # Phase 2 (#77): when True and the dashboard bootstrap probe
        # finds the live DB corrupted, ``auto_restore_if_needed`` will
        # forensic-rename the broken file and copy the newest snapshot
        # over ``db_path``. Default False — opt-in only.
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
            # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
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

        NOTE: this is the manual-restore path. Phase 2 (#77) auto-restore
        on corruption goes through :meth:`auto_restore_if_needed`, which
        runs *before* any live connection is opened so the live-lock
        check below is automatically satisfied.
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
        # live DB. SHA-256 from manifest must match disk. An empty
        # manifest sha256 is treated as a hard failure (audit M-1):
        # historically we silently skipped the check, which means a
        # tampered manifest or a pre-#77 snapshot could be restored
        # without integrity proof. Operators must verify by hand.
        actual = self._sha256_file(meta.path)
        if not meta.sha256:
            raise RuntimeError(
                f"Snapshot {meta.id} has no sha256 sidecar — refusing to restore "
                "(manifest tampering or pre-#77 snapshot). Manually verify integrity first."
            )
        if actual != meta.sha256:
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
    # Phase 2 — auto-restore on corruption (#77)
    # ──────────────────────────────────────────────

    def auto_restore_if_needed(self) -> Optional["RestoreResult"]:
        """Probe the live DB; if corrupted, salvage from latest snapshot.

        Called once at dashboard bootstrap, BEFORE the main ``Database``
        instance opens its long-lived connection. By then the DB is
        unowned (no other connection holds the WAL lock), so we can
        safely move the broken file aside and drop the snapshot in its
        place.

        Behaviour matrix:

          * Not corrupted → returns ``RestoreResult(restored=False,
            reason="not_corrupted")``. No I/O.
          * Corrupted + ``auto_restore_on_corruption=False`` → returns
            ``RestoreResult(restored=False, reason="disabled_in_config",
            details=<probe details>)``. Caller logs CRITICAL.
          * Corrupted + flag true + no snapshot → returns
            ``RestoreResult(restored=False, reason="no_snapshot")``.
            The broken DB is left untouched.
          * Corrupted + flag true + snapshot found → forensic-renames
            the broken DB, copies the snapshot over, wipes -wal/-shm,
            writes an ``auto_restore`` audit event and (if SMTP is on)
            emails the admin. Returns ``RestoreResult(restored=True,
            ...)``.

        Never raises on the corruption-detection path; restore-step
        failures bubble out as ``reason="restore_failed"`` with the
        traceback summarised in ``details``.
        """
        # Local import to avoid circular import at module load time.
        from src.storage.corruption_detector import (
            DEFAULT_CHECK_MODE,
            DEFAULT_CHECK_TIMEOUT_SECONDS,
            is_corrupted,
        )

        # Pull the same knobs the detector reads — purely so we can log
        # them BEFORE the probe runs. Next time someone hits a startup
        # hang the log shows exactly which pragma+timeout was active.
        backup_cfg = (self.config.get("backup") or {}) if isinstance(self.config, dict) else {}
        log_mode = str(
            backup_cfg.get("corruption_check_mode", DEFAULT_CHECK_MODE)
            or DEFAULT_CHECK_MODE
        )
        try:
            log_timeout = float(backup_cfg.get(
                "corruption_check_timeout_seconds",
                DEFAULT_CHECK_TIMEOUT_SECONDS,
            ))
        except (TypeError, ValueError):
            log_timeout = float(DEFAULT_CHECK_TIMEOUT_SECONDS)

        logger.info(
            "auto-restore probe: starting corruption check (mode=%s, "
            "timeout=%ds)",
            log_mode, int(log_timeout),
        )
        t0 = time.monotonic()
        probe = is_corrupted(self.db_path, self.config)
        elapsed = time.monotonic() - t0
        logger.info(
            "auto-restore probe: corruption check completed "
            "(result=%s, took=%.2fs)",
            probe.reason, elapsed,
        )
        if not probe.is_corrupted:
            return RestoreResult(
                restored=False,
                reason="not_corrupted",
                details=probe.details,
            )

        logger.critical(
            "DB CORRUPTION DETECTED: db=%s reason=%s details=%s",
            self.db_path, probe.reason, probe.details,
        )

        if not self.auto_restore_on_corruption:
            return RestoreResult(
                restored=False,
                reason="disabled_in_config",
                details=f"{probe.reason}: {probe.details}",
            )

        # Locate the newest snapshot. ``list_snapshots`` returns
        # ascending-by-id so the tail is freshest.
        try:
            metas = self.list_snapshots()
        except Exception as e:
            logger.error("auto-restore: cannot read manifest: %s", e)
            return RestoreResult(
                restored=False,
                reason="restore_failed",
                details=f"manifest read failed: {e}",
            )

        # Walk newest-first, skip any whose .bak file is gone from disk.
        chosen: Optional[SnapshotMeta] = None
        for meta in reversed(metas):
            if meta.path and os.path.exists(meta.path):
                chosen = meta
                break
        if chosen is None:
            logger.error(
                "auto-restore: no usable snapshot (manifest=%d entries)",
                len(metas),
            )
            return RestoreResult(
                restored=False,
                reason="no_snapshot",
                details=f"manifest_entries={len(metas)}",
            )

        # 1. Forensic-rename the broken DB. NEVER delete it.
        ts = self._now().strftime("%Y%m%d_%H%M%S")
        broken_path = f"{self.db_path}.broken-{ts}"
        try:
            os.replace(self.db_path, broken_path)
        except OSError as e:
            logger.error(
                "auto-restore: cannot rename broken DB %s -> %s: %s",
                self.db_path, broken_path, e,
            )
            return RestoreResult(
                restored=False,
                reason="restore_failed",
                details=f"rename broken DB failed: {e}",
            )

        # 2. Copy the snapshot over db_path. ``shutil.copy2`` preserves
        # mtime so the file looks freshly minted to ops tooling.
        try:
            shutil.copy2(chosen.path, self.db_path)
        except OSError as e:
            # Try to restore the broken file so we don't leave the
            # caller with no DB at all.
            try:
                os.replace(broken_path, self.db_path)
            except OSError:
                pass
            logger.error(
                "auto-restore: snapshot copy failed: %s", e,
            )
            return RestoreResult(
                restored=False,
                reason="restore_failed",
                details=f"copy snapshot failed: {e}",
            )

        # 3. Wipe -wal/-shm siblings — they belong to the broken DB.
        for sibling in (self.db_path + "-wal", self.db_path + "-shm"):
            try:
                if os.path.exists(sibling):
                    os.remove(sibling)
            except OSError as e:
                logger.warning(
                    "auto-restore: failed to remove %s: %s", sibling, e,
                )

        logger.critical(
            "auto-restore OK: snapshot=%s -> %s (broken preserved at %s)",
            chosen.id, self.db_path, broken_path,
        )

        # 4. Write audit event + 5. send admin email. Both best-effort —
        # neither failure should mask the successful restore.
        audit_event_id = self._write_auto_restore_audit(
            snapshot_id=chosen.id,
            broken_path=broken_path,
            corruption_reason=probe.reason,
            corruption_details=probe.details,
        )
        self._send_auto_restore_email(
            snapshot_id=chosen.id,
            broken_path=broken_path,
            corruption_reason=probe.reason,
            corruption_details=probe.details,
        )

        return RestoreResult(
            restored=True,
            reason="auto_restored",
            snapshot_id=chosen.id,
            broken_path=broken_path,
            audit_event_id=audit_event_id,
            ts=self._now().isoformat(timespec="seconds"),
            details=f"{probe.reason}: {probe.details}",
        )

    def _write_auto_restore_audit(
        self,
        snapshot_id: str,
        broken_path: str,
        corruption_reason: str,
        corruption_details: str,
    ) -> Optional[int]:
        """Append an ``auto_restore`` row to ``file_audit_events`` in the
        freshly-restored DB.

        We deliberately bypass the full ``Database`` constructor here:
        the dashboard's ``create_app`` will call ``db.connect()`` right
        after we return, and that path runs the whole table+index
        initialisation. Doing it twice (once here, once there) is both
        wasteful and brittle — instead we open a raw stdlib sqlite3
        connection, perform a single INSERT against ``file_audit_events``
        (which is one of the critical tables the corruption detector
        verified to exist on the snapshot we just restored from), and
        close. Mirrors ``insert_audit_event_simple`` row-shape.
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            try:
                now = self._now().strftime("%Y-%m-%d %H:%M:%S")
                file_name = os.path.basename(self.db_path)
                details = json.dumps({
                    "snapshot_id": snapshot_id,
                    "broken_path": broken_path,
                    "corruption_reason": corruption_reason,
                    "corruption_details": corruption_details[:500],
                }, sort_keys=True)
                cur = conn.execute(
                    "INSERT INTO file_audit_events "
                    "(source_id, event_time, event_type, username, "
                    " file_path, file_name, details, detected_by) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (None, now, "auto_restore", "system",
                     self.db_path, file_name, details,
                     "backup_manager"),
                )
                event_id = cur.lastrowid
                conn.commit()
                logger.info(
                    "auto-restore: audit event %s written", event_id,
                )
                return event_id
            finally:
                conn.close()
        except Exception as e:
            logger.warning(
                "auto-restore: audit event write failed: %s", e,
            )
            return None

    def _send_auto_restore_email(
        self,
        snapshot_id: str,
        broken_path: str,
        corruption_reason: str,
        corruption_details: str,
    ) -> None:
        """Best-effort admin notification. Skipped silently when SMTP
        isn't configured (``smtp.enabled=false`` or fields missing).
        """
        smtp_cfg = (self.config or {}).get("smtp") or {}
        if not smtp_cfg.get("enabled", False):
            return
        notif_cfg = (self.config or {}).get("notifications") or {}
        admin = (notif_cfg.get("admin_cc_email") or "").strip()
        if not admin:
            return

        try:  # pragma: no cover - SMTP path is integration-tested elsewhere
            import smtplib
            import ssl
            from email.mime.text import MIMEText

            host = smtp_cfg.get("host", "").strip()
            port = int(smtp_cfg.get("port", 587))
            from_addr = smtp_cfg.get("from_address", "")
            if not host or not from_addr:
                return

            subject_prefix = (
                notif_cfg.get("subject_prefix") or "[File Activity]"
            ).strip()
            subject = (
                f"{subject_prefix} DB auto-restored from snapshot "
                f"{snapshot_id}"
            )
            body = (
                f"FILE ACTIVITY detected SQLite corruption at startup "
                f"and auto-restored the database.\n\n"
                f"  db_path:     {self.db_path}\n"
                f"  snapshot:    {snapshot_id}\n"
                f"  broken_path: {broken_path}\n"
                f"  reason:      {corruption_reason}\n"
                f"  details:     {corruption_details[:500]}\n\n"
                f"The broken DB has been preserved for forensics. "
                f"Please investigate."
            )
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = admin

            timeout = int(smtp_cfg.get("timeout_seconds", 10))
            if smtp_cfg.get("use_ssl", False):
                ctx = ssl.create_default_context()
                client = smtplib.SMTP_SSL(host, port,
                                          timeout=timeout, context=ctx)
            else:
                client = smtplib.SMTP(host, port, timeout=timeout)
                client.ehlo()
                if smtp_cfg.get("use_tls", True):
                    ctx = ssl.create_default_context()
                    client.starttls(context=ctx)
                    client.ehlo()
            try:
                username = smtp_cfg.get("username", "")
                password = (
                    os.environ.get("SMTP_PASSWORD")
                    or smtp_cfg.get("password", "")
                )
                if username and password:
                    client.login(username, password)
                client.sendmail(from_addr, [admin], msg.as_string())
            finally:
                try:
                    client.quit()
                except Exception:
                    pass
            logger.info("auto-restore: admin notification sent to %s", admin)
        except Exception as e:
            logger.warning(
                "auto-restore: admin email failed (non-fatal): %s", e,
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
