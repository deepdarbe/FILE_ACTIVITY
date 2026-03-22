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


def get_watcher_status(source_id: int = None) -> dict:
    if source_id and source_id in _watchers:
        return _watchers[source_id].get_status()
    return {sid: w.get_status() for sid, w in _watchers.items()} if not source_id else {"status": "inactive"}


class FileWatcher:
    def __init__(self, db: Database, source_id: int, path: str, interval: int = 300):
        self.db = db
        self.source_id = source_id
        self.path = path
        self.interval = interval  # seconds between checks
        self._running = False
        self._thread = None
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
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
        _watchers[self.source_id] = self
        logger.info("File watcher started for source %d (interval: %ds)", self.source_id, self.interval)

    def stop(self):
        self._running = False
        if self.source_id in _watchers:
            del _watchers[self.source_id]
        logger.info("File watcher stopped for source %d", self.source_id)

    def get_status(self):
        return {**self.stats, "running": self._running, "interval": self.interval}

    def _watch_loop(self):
        while self._running:
            try:
                self.stats["status"] = "checking"
                self._check_changes()
                self.stats["status"] = "waiting"
                self.stats["last_check"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.stats["checks_completed"] += 1
            except Exception as e:
                logger.error("Watcher error source %d: %s", self.source_id, e)
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
        changes = self.stats["last_changes"]
        changes.append(entry)
        if len(changes) > 50:
            self.stats["last_changes"] = changes[-50:]

    def _record_audit(self, event_type: str, fpath: str, owner: str = None):
        """Record file audit event in database."""
        try:
            self.db.insert_audit_event(
                source_id=self.source_id,
                event_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                event_type=event_type,
                username=owner,
                file_path=fpath,
                file_name=os.path.basename(fpath),
                detected_by='watcher'
            )
        except Exception as e:
            logger.debug("Audit event error %s: %s", fpath[:60], e)

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

        self.stats["new_files"] += new_count
        self.stats["modified_files"] += modified_count
        self.stats["deleted_files"] += deleted_count
        self.stats["total_changes"] += new_count + modified_count + deleted_count

        if new_count + modified_count + deleted_count > 0:
            logger.info("Watcher source %d: +%d new, ~%d modified, -%d deleted",
                       self.source_id, new_count, modified_count, deleted_count)
