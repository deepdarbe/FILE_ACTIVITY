"""Real-time file change monitoring using polling.

After initial scan, monitors the source directory for changes
(new files, modified files, deleted files) at configurable intervals.
"""

import os
import time
import logging
import threading
from datetime import datetime

from src.scanner.win_attributes import get_file_times, _long_path
from src.scanner.share_resolver import get_relative_path
from src.storage.database import Database
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.watcher")

_watchers = {}  # source_id -> FileWatcher
_watchers_lock = threading.Lock()


def get_watcher_status(source_id: int = None) -> dict:
    with _watchers_lock:
        if source_id and source_id in _watchers:
            return _watchers[source_id].get_status()
        if not source_id:
            return {sid: w.get_status() for sid, w in _watchers.items()}
        return {"status": "inactive"}


DEFAULT_POLL_INTERVAL = 60  # Polling aralığı — config.yaml ile override edilebilir
MIN_POLL_INTERVAL = 10      # Daha düşük değerler DoS etkisi yaratır, reddedilir


class FileWatcher:
    def __init__(self, db: Database, source_id: int, path: str,
                 interval: int = DEFAULT_POLL_INTERVAL,
                 ransomware_detector=None,
                 config: dict = None):
        self.db = db
        self.source_id = source_id
        self.path = path
        # Minimum sinir uygula — cok dusuk degerler FS'i sarsitir
        self.interval = max(MIN_POLL_INTERVAL, interval)
        # Optional RansomwareDetector — when set, every audit event is
        # forwarded via consume_event(...). Wired by the dashboard during
        # /api/watcher/{source_id}/start (or by the service container).
        self.ransomware_detector = ransomware_detector
        # Issue #33: opt-in USN journal tail. When enabled and supported
        # (local NTFS + admin), polling loop is replaced by event-driven
        # tail with sub-second latency. Falls back to polling on any error.
        self.config = config or {}
        self._usn_tailer = None
        self._usn_stop_event = None
        self._usn_thread = None
        self._running = False
        self._thread = None
        self._stats_lock = threading.Lock()
        self.stats = {
            "status": "idle",
            "last_check": None,
            "new_files": 0,
            "modified_files": 0,
            "deleted_files": 0,
            "total_changes": 0,
            "checks_completed": 0,
            "last_changes": [],  # Rolling buffer of last 50 changes
        }

    def start(self):
        if self._running:
            return
        self._running = True
        # Issue #33: prefer event-driven USN tail when enabled + supported.
        # On success we skip the polling thread entirely. On any failure we
        # fall back to the legacy polling loop — never block the watcher.
        if self._try_start_usn_tail():
            with _watchers_lock:
                _watchers[self.source_id] = self
            logger.info(
                "File watcher started (USN tail mode) for source %d",
                self.source_id,
            )
            return

        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
        with _watchers_lock:
            _watchers[self.source_id] = self
        logger.info("File watcher started for source %d (interval: %ds)", self.source_id, self.interval)

    def stop(self):
        self._running = False
        if self._usn_stop_event is not None:
            self._usn_stop_event.set()
        if self._usn_tailer is not None:
            try:
                self._usn_tailer.close()
            except Exception:
                pass
        with _watchers_lock:
            _watchers.pop(self.source_id, None)
        logger.info("File watcher stopped for source %d", self.source_id)

    # ── USN tail integration (issue #33) ─────────────────────────────

    def _try_start_usn_tail(self) -> bool:
        """If config.scanner.usn_tail_enabled and the volume supports it,
        spin up a background USN tailer thread and return True. Otherwise
        return False — caller falls back to polling."""
        if not self.config.get("scanner", {}).get("usn_tail_enabled", False):
            return False
        try:
            from src.scanner.backends.ntfs_usn_tail import NtfsUsnTailer
            if not NtfsUsnTailer.is_supported(self.path):
                return False
            volume_letter = os.path.splitdrive(os.path.abspath(self.path))[0].rstrip(":")
            if not volume_letter:
                return False
            tailer = NtfsUsnTailer(self.db, self.config, self.source_id, volume_letter)
            init_result = tailer.initialize()
            if init_result.get("gap_detected"):
                logger.warning(
                    "USN tail kaynak %d icin gap tespit edildi (%s) — full rescan onerilir",
                    self.source_id, init_result.get("reason"),
                )
            self._usn_tailer = tailer
            self._usn_stop_event = threading.Event()
            self._usn_thread = threading.Thread(
                target=tailer.run_loop,
                args=(self._on_usn_event,),
                kwargs={
                    "poll_interval_seconds": float(
                        self.config.get("scanner", {}).get("usn_poll_interval_seconds", 1.0)
                    ),
                    "stop_event": self._usn_stop_event,
                },
                daemon=True,
            )
            self._usn_thread.start()
            return True
        except NotImplementedError:
            return False
        except Exception as e:
            logger.warning("USN tail baslatilamadi (kaynak %d), polling'e dusuluyor: %s",
                           self.source_id, e)
            return False

    def _on_usn_event(self, event: dict) -> None:
        """Bridge USN reasons to existing _record_audit semantics."""
        reasons = set(event.get("reason") or [])
        # Choose ONE event_type per record (ordered by priority)
        if "FILE_DELETE" in reasons:
            etype = "delete"
        elif "FILE_CREATE" in reasons:
            etype = "create"
        elif "RENAME_NEW_NAME" in reasons:
            etype = "rename"
        elif reasons & {"DATA_OVERWRITE", "DATA_EXTEND", "DATA_TRUNCATION"}:
            etype = "modify"
        else:
            # Skip BASIC_INFO_CHANGE / SECURITY_CHANGE / CLOSE noise
            return
        # USN gives us the file name, not full path. Best effort path is
        # the volume root + name; the watcher doesn't reconstruct full
        # parent path here (would require an MFT lookup).
        fname = event.get("file_name", "")
        with self._stats_lock:
            if etype == "create":
                self.stats["new_files"] += 1
            elif etype == "modify":
                self.stats["modified_files"] += 1
            elif etype == "delete":
                self.stats["deleted_files"] += 1
            self.stats["total_changes"] += 1
            self.stats["last_check"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self._record_audit(etype, fname, owner=None)
        except Exception as e:
            logger.debug("USN _on_usn_event audit hata: %s", e)

    def get_status(self):
        with self._stats_lock:
            snapshot = dict(self.stats)
            # last_changes liste referansi kopyalanir (okuyucu modify etmesin)
            snapshot["last_changes"] = list(snapshot.get("last_changes", []))
        snapshot["running"] = self._running
        snapshot["interval"] = self.interval
        return snapshot

    def _watch_loop(self):
        while self._running:
            try:
                with self._stats_lock:
                    self.stats["status"] = "checking"
                self._check_changes()
                with self._stats_lock:
                    self.stats["status"] = "waiting"
                    self.stats["last_check"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.stats["checks_completed"] += 1
            except Exception as e:
                logger.error("Watcher error source %d: %s", self.source_id, e)
                with self._stats_lock:
                    self.stats["status"] = "error"

            # Sleep in small increments so stop() is responsive
            for _ in range(self.interval):
                if not self._running:
                    break
                time.sleep(1)

    def _build_file_record(self, fpath: str) -> dict:
        """Build a file record dict from filesystem using win_attributes."""
        info = get_file_times(fpath, read_owner=True)
        fname = os.path.basename(fpath)
        ext_parts = os.path.splitext(fname)
        extension = ext_parts[1].lstrip(".").lower() if ext_parts[1] else None
        rel_path = get_relative_path(fpath, self.path)

        return {
            "source_id": self.source_id,
            "file_path": fpath,
            "relative_path": rel_path,
            "file_name": fname,
            "extension": extension,
            "file_size": info.file_size,
            "creation_time": info.creation_time,
            "last_access_time": info.last_access_time,
            "last_modify_time": info.last_modify_time,
            "owner": info.owner,
            "attributes": str(info.win32_attributes) if info.win32_attributes else None,
        }

    def _add_change(self, change_type: str, fpath: str, size: int = 0):
        """Add a change to the rolling buffer (last 50)."""
        entry = {
            "type": change_type,
            "path": fpath,
            "file_name": os.path.basename(fpath),
            "size": size,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with self._stats_lock:
            changes = self.stats["last_changes"]
            changes.append(entry)
            if len(changes) > 50:
                self.stats["last_changes"] = changes[-50:]

    def _record_audit(self, event_type: str, fpath: str, owner: str = None):
        """Record file audit event in database AND fan out to the
        ransomware detector if one is wired in.

        When ``audit.chain_enabled`` is True (issue #38), the chained
        variant also appends a tamper-evident hash-chain row. With the
        flag off this is identical to the original direct insert.
        """
        now = datetime.now()
        event = {
            "source_id": self.source_id,
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "username": owner,
            "file_path": fpath,
            "file_name": os.path.basename(fpath),
            "detected_by": "watcher",
        }
        try:
            if hasattr(self.db, "insert_audit_event_chained"):
                self.db.insert_audit_event_chained(event)
            else:
                self.db.insert_audit_event(**event)
        except Exception as e:
            logger.debug("Audit event error %s: %s", fpath[:60], e)

        # Forward to ransomware detector — issue #37. Failures here are
        # never fatal; security checks must not break basic auditing.
        det = self.ransomware_detector
        if det is not None:
            try:
                det.consume_event({
                    "timestamp": now,
                    "source_id": self.source_id,
                    "username": owner,
                    "file_path": fpath,
                    "event_type": event_type,
                })
            except Exception as e:
                logger.debug("Ransomware detector error %s: %s", fpath[:60], e)

    def _check_changes(self):
        scan_id = self.db.get_latest_scan_id(self.source_id)
        if not scan_id:
            return

        # Get known files from DB
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT file_path, file_size, last_modify_time
                FROM scanned_files WHERE source_id = ? AND scan_id = ?
            """, (self.source_id, scan_id))
            known = {r["file_path"]: (r["file_size"], r["last_modify_time"]) for r in cur.fetchall()}

        new_count = 0
        modified_count = 0
        current_paths = set()

        # Walk current filesystem
        scan_path = _long_path(self.path) if len(self.path) >= 240 else self.path
        for root, dirs, files in os.walk(scan_path):
            for fname in files:
                fpath = os.path.join(root, fname)
                current_paths.add(fpath)

                try:
                    st = os.stat(fpath)
                    mtime = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")

                    if fpath not in known:
                        # NEW file: build record and upsert to DB
                        new_count += 1
                        try:
                            record = self._build_file_record(fpath)
                            self.db.upsert_scanned_file(self.source_id, scan_id, record)
                            self._add_change("new", fpath, st.st_size)
                            self._record_audit("create", fpath, record.get("owner"))
                        except Exception as e:
                            logger.debug("Watcher upsert error (new) %s: %s", fpath[:80], e)

                    elif known[fpath][0] != st.st_size or known[fpath][1] != mtime:
                        # MODIFIED file: update record in DB
                        modified_count += 1
                        try:
                            record = self._build_file_record(fpath)
                            self.db.upsert_scanned_file(self.source_id, scan_id, record)
                            self._add_change("modified", fpath, st.st_size)
                            self._record_audit("modify", fpath, record.get("owner"))
                        except Exception as e:
                            logger.debug("Watcher upsert error (mod) %s: %s", fpath[:80], e)

                except (OSError, PermissionError):
                    pass

        # Deleted files: remove from DB
        deleted_paths = set(known.keys()) - current_paths
        deleted_count = len(deleted_paths)
        for dpath in deleted_paths:
            try:
                self.db.delete_scanned_file(self.source_id, scan_id, dpath)
                self._add_change("deleted", dpath, known[dpath][0] if dpath in known else 0)
                self._record_audit("delete", dpath)
            except Exception as e:
                logger.debug("Watcher delete error %s: %s", dpath[:80], e)

        with self._stats_lock:
            self.stats["new_files"] += new_count
            self.stats["modified_files"] += modified_count
            self.stats["deleted_files"] += deleted_count
            self.stats["total_changes"] += new_count + modified_count + deleted_count

        if new_count + modified_count + deleted_count > 0:
            logger.info("Watcher source %d: +%d new, ~%d modified, -%d deleted",
                       self.source_id, new_count, modified_count, deleted_count)
