"""Event-driven file change monitoring backed by the ``watchdog`` package.

This module wraps watchdog's ``Observer`` + ``FileSystemEventHandler`` to
deliver create / modify / delete notifications without polling. It is the
default backend selected by :class:`WatcherFactory` whenever the host
supports it; on hosts without ``watchdog`` installed (or when the watched
path is a UNC network share where ``ReadDirectoryChangesW`` is unreliable)
the factory transparently falls back to the polling
:class:`src.scanner.file_watcher.FileWatcher`.

Issue #14: replace the polling-based file watcher with event-driven
monitoring. The polling watcher is preserved as a fallback path — both
this class and ``FileWatcher`` expose the same ``start() / stop() /
is_running() / get_status()`` shape so callers (the dashboard API, the
service container) do not need to know which backend they hold.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("file_activity.watchdog_watcher")

# Module-level registry mirrors ``file_watcher._watchers`` so the
# dashboard `/api/watcher/status` endpoint can introspect both backends
# uniformly via ``WatcherFactory.get_status``.
_watchers: Dict[int, "WatchdogWatcher"] = {}
_watchers_lock = threading.Lock()


# ─── Public defaults ──────────────────────────────────────────────────

DEFAULT_DEBOUNCE_MS = 250  # coalesce bursts within this window
MAX_LAST_EVENTS = 50       # rolling buffer matches polling watcher


def _is_unc_path(path: str) -> bool:
    """Return True if ``path`` is a Windows UNC share (``\\\\server\\share``).

    ``ReadDirectoryChangesW`` (the Win32 API watchdog uses on Windows)
    drops or duplicates events on most network redirectors, so we always
    fall back to polling for UNC paths. Forward-slash UNC variants
    (``//server/share``) are also detected.
    """
    if not path:
        return False
    p = str(path)
    return p.startswith("\\\\") or p.startswith("//")


def _import_watchdog():
    """Lazy import so the module loads on hosts without ``watchdog``.

    Returns the ``(Observer, FileSystemEventHandler)`` tuple, or raises
    ``ImportError`` on failure. Centralised so tests can monkey-patch one
    function instead of poking ``sys.modules``.
    """
    from watchdog.observers import Observer  # noqa: WPS433 (deliberate lazy)
    from watchdog.events import FileSystemEventHandler  # noqa: WPS433
    return Observer, FileSystemEventHandler


# ─── Watcher implementation ───────────────────────────────────────────


class WatchdogWatcher:
    """Event-driven file watcher.

    Parameters mirror :class:`FileWatcher` so :class:`WatcherFactory` can
    swap them transparently. ``callback`` is optional — when provided it
    receives a serialisable event dict for every coalesced change. When
    ``db`` is provided the watcher records audit events using the same
    semantics as the polling watcher.
    """

    def __init__(
        self,
        db=None,
        source_id: int = 0,
        path: str = "",
        callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        ransomware_detector=None,
        config: Optional[dict] = None,
        debounce_ms: int = DEFAULT_DEBOUNCE_MS,
    ) -> None:
        self.db = db
        self.source_id = source_id
        self.path = path
        self.callback = callback
        self.ransomware_detector = ransomware_detector
        self.config = config or {}
        self.debounce_ms = max(0, int(debounce_ms))
        # Polling watcher exposes ``interval``; some callers read it for
        # the status endpoint. For event-driven mode we report 0.
        self.interval = 0

        self._observer = None
        self._handler = None
        self._running = False
        self._lock = threading.Lock()
        # debounce: path -> (event_type, last_seen_monotonic)
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._pending_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self.stats: Dict[str, Any] = {
            "status": "idle",
            "backend": "watchdog",
            "last_check": None,
            "new_files": 0,
            "modified_files": 0,
            "deleted_files": 0,
            "total_changes": 0,
            "checks_completed": 0,
            "last_changes": [],
        }

    # ── Capability probe ────────────────────────────────────────────

    @classmethod
    def available(cls, path: Optional[str] = None) -> bool:
        """Return True if this backend can actually watch ``path``.

        The check is two-fold: (a) the ``watchdog`` package must import
        cleanly, and (b) ``path`` (when supplied) must not be a UNC share
        where ``ReadDirectoryChangesW`` is unreliable.
        """
        if path is not None and _is_unc_path(path):
            return False
        try:
            _import_watchdog()
        except ImportError:
            return False
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("watchdog probe failed: %s", exc)
            return False
        return True

    # ── Lifecycle ───────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            Observer, FileSystemEventHandler = _import_watchdog()

            watcher = self

            class _Handler(FileSystemEventHandler):
                def on_created(self, event):  # type: ignore[override]
                    if event.is_directory:
                        return
                    watcher._handle_event("create", event.src_path)

                def on_modified(self, event):  # type: ignore[override]
                    if event.is_directory:
                        return
                    watcher._handle_event("modify", event.src_path)

                def on_deleted(self, event):  # type: ignore[override]
                    if event.is_directory:
                        return
                    watcher._handle_event("delete", event.src_path)

                def on_moved(self, event):  # type: ignore[override]
                    if event.is_directory:
                        return
                    # Treat a move as delete(src) + create(dest)
                    watcher._handle_event("delete", event.src_path)
                    dest = getattr(event, "dest_path", None)
                    if dest:
                        watcher._handle_event("create", dest)

            self._handler = _Handler()
            self._observer = Observer()
            self._observer.schedule(self._handler, self.path, recursive=True)
            try:
                self._observer.start()
            except Exception as exc:
                logger.error(
                    "Watchdog observer failed to start for %s: %s",
                    self.path, exc,
                )
                self._observer = None
                self._handler = None
                raise
            self._running = True
            with self._stats_lock:
                self.stats["status"] = "running"

        with _watchers_lock:
            _watchers[self.source_id] = self
        logger.info(
            "Watchdog watcher started for source %d (%s)",
            self.source_id, self.path,
        )

    def stop(self) -> None:
        # Stop is idempotent — calling it twice (or before start) must
        # not raise. Useful when the dashboard hits /stop on a watcher
        # that already crashed out.
        with self._lock:
            if not self._running and self._observer is None:
                return
            obs = self._observer
            self._observer = None
            self._handler = None
            self._running = False

        if obs is not None:
            try:
                obs.stop()
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Observer stop error: %s", exc)
            try:
                obs.join(timeout=2.0)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Observer join error: %s", exc)

        with _watchers_lock:
            _watchers.pop(self.source_id, None)
        with self._stats_lock:
            self.stats["status"] = "stopped"
        logger.info("Watchdog watcher stopped for source %d", self.source_id)

    def is_running(self) -> bool:
        return self._running

    # ── Event handling ──────────────────────────────────────────────

    def _handle_event(self, event_type: str, fpath: str) -> None:
        """Coalesce a raw watchdog event then dispatch to callback / DB.

        Debouncing is per-path: bursts of N modify-events on the same
        file collapse into a single callback invocation per debounce
        window, matching the behaviour users expect from a "change
        notification" stream rather than a raw FS event firehose.
        """
        if not fpath:
            return
        if self.debounce_ms <= 0:
            self._dispatch(event_type, fpath)
            return

        import time
        now = time.monotonic()
        deadline = now + (self.debounce_ms / 1000.0)
        fire = False
        with self._pending_lock:
            existing = self._pending.get(fpath)
            if existing is None:
                # First touch — schedule a flush. We use a one-shot timer
                # rather than a background sweeper so idle watchers cost
                # nothing.
                self._pending[fpath] = {
                    "event_type": event_type,
                    "deadline": deadline,
                }
                fire = True
            else:
                # Promote priority: delete > create > modify
                priority = {"modify": 0, "create": 1, "delete": 2}
                if priority.get(event_type, 0) > priority.get(existing["event_type"], 0):
                    existing["event_type"] = event_type
                existing["deadline"] = deadline

        if fire:
            t = threading.Timer(self.debounce_ms / 1000.0, self._flush_path, args=(fpath,))
            t.daemon = True
            t.start()

    def _flush_path(self, fpath: str) -> None:
        with self._pending_lock:
            entry = self._pending.pop(fpath, None)
        if entry is None:
            return
        self._dispatch(entry["event_type"], fpath)

    def _dispatch(self, event_type: str, fpath: str) -> None:
        evt = self._build_event(event_type, fpath)
        # Stats / rolling buffer
        with self._stats_lock:
            if event_type == "create":
                self.stats["new_files"] += 1
            elif event_type == "modify":
                self.stats["modified_files"] += 1
            elif event_type == "delete":
                self.stats["deleted_files"] += 1
            self.stats["total_changes"] += 1
            self.stats["checks_completed"] += 1
            self.stats["last_check"] = evt["time"]
            buf: List[dict] = self.stats["last_changes"]
            buf.append({
                "type": ("new" if event_type == "create"
                         else "modified" if event_type == "modify"
                         else "deleted"),
                "path": fpath,
                "file_name": os.path.basename(fpath),
                "size": evt.get("size", 0),
                "time": evt["time"],
            })
            if len(buf) > MAX_LAST_EVENTS:
                self.stats["last_changes"] = buf[-MAX_LAST_EVENTS:]

        # Audit DB (best-effort) — mirrors polling watcher's _record_audit
        if self.db is not None:
            try:
                self._record_audit(event_type, fpath)
            except Exception as exc:
                logger.debug("audit record failed for %s: %s", fpath[:80], exc)

        # User callback (best-effort)
        if self.callback is not None:
            try:
                self.callback(evt)
            except Exception as exc:
                logger.debug("watcher callback raised for %s: %s", fpath[:80], exc)

    def _build_event(self, event_type: str, fpath: str) -> Dict[str, Any]:
        size = 0
        try:
            if event_type != "delete" and os.path.exists(fpath):
                size = os.path.getsize(fpath)
        except OSError:
            size = 0
        return {
            "source_id": self.source_id,
            "event_type": event_type,
            "file_path": fpath,
            "file_name": os.path.basename(fpath),
            "size": size,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "backend": "watchdog",
        }

    def _record_audit(self, event_type: str, fpath: str) -> None:
        now = datetime.now()
        event = {
            "source_id": self.source_id,
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "username": None,
            "file_path": fpath,
            "file_name": os.path.basename(fpath),
            "detected_by": "watcher",
        }
        if hasattr(self.db, "insert_audit_event_chained"):
            self.db.insert_audit_event_chained(event)
        elif hasattr(self.db, "insert_audit_event"):
            self.db.insert_audit_event(**event)

        det = self.ransomware_detector
        if det is not None:
            try:
                det.consume_event({
                    "timestamp": now,
                    "source_id": self.source_id,
                    "username": None,
                    "file_path": fpath,
                    "event_type": event_type,
                })
            except Exception as exc:
                logger.debug("ransomware detector error: %s", exc)

    # ── Status / serialisation ──────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        with self._stats_lock:
            snapshot = dict(self.stats)
            snapshot["last_changes"] = list(snapshot.get("last_changes", []))
        snapshot["running"] = self._running
        snapshot["interval"] = self.interval
        snapshot["backend"] = "watchdog"
        return snapshot

    @staticmethod
    def serialise_event(evt: Dict[str, Any]) -> str:
        """JSON-encode an event dict for the dashboard event log.

        Ensures any non-serialisable values (e.g. ``datetime``) are
        coerced to strings so the log endpoint never raises.
        """

        def _default(o):
            if isinstance(o, datetime):
                return o.strftime("%Y-%m-%d %H:%M:%S")
            return str(o)

        return json.dumps(evt, default=_default, sort_keys=True)


# ─── Factory ──────────────────────────────────────────────────────────


class WatcherFactory:
    """Selector that returns the right watcher implementation.

    Selection rules (first match wins):

    1. ``config.watcher.backend == "polling"`` → polling watcher.
    2. UNC path (``\\\\server\\share``)         → polling watcher.
    3. ``watchdog`` package not importable     → polling watcher.
    4. otherwise                               → :class:`WatchdogWatcher`.
    """

    @staticmethod
    def create(
        config: Optional[dict],
        source,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        *,
        db=None,
        source_id: Optional[int] = None,
        path: Optional[str] = None,
        interval: Optional[int] = None,
        ransomware_detector=None,
    ):
        """Return a watcher instance ready to be ``start()``ed.

        ``source`` may be either a source object (with ``.id`` and
        ``.unc_path`` attributes — what ``api.py`` already passes) or a
        plain string path. For maximum flexibility callers can also pass
        ``db / source_id / path / interval`` as kwargs; when supplied
        they win over the values teased out of ``source``.
        """
        cfg = config or {}
        watcher_cfg = cfg.get("watcher", {}) if isinstance(cfg, dict) else {}
        backend = (watcher_cfg.get("backend") or "watchdog").lower()

        # Resolve source-derived defaults.
        if path is None:
            path = getattr(source, "unc_path", None) or (source if isinstance(source, str) else "")
        if source_id is None:
            source_id = getattr(source, "id", 0) or 0

        from src.scanner.file_watcher import FileWatcher, DEFAULT_POLL_INTERVAL
        if interval is None:
            try:
                interval = int(watcher_cfg.get("poll_interval_seconds",
                                                DEFAULT_POLL_INTERVAL))
            except (TypeError, ValueError):
                interval = DEFAULT_POLL_INTERVAL

        # Rule 1 — explicit override
        if backend == "polling":
            logger.debug("WatcherFactory: polling forced via config")
            return FileWatcher(db, source_id, path, interval,
                               ransomware_detector=ransomware_detector,
                               config=cfg)

        # Rules 2 + 3 — capability probe
        if not WatchdogWatcher.available(path):
            reason = "UNC path" if _is_unc_path(path) else "watchdog unavailable"
            logger.info("WatcherFactory: falling back to polling (%s)", reason)
            return FileWatcher(db, source_id, path, interval,
                               ransomware_detector=ransomware_detector,
                               config=cfg)

        # Rule 4 — default
        logger.debug("WatcherFactory: using watchdog backend")
        return WatchdogWatcher(
            db=db,
            source_id=source_id,
            path=path,
            callback=callback,
            ransomware_detector=ransomware_detector,
            config=cfg,
        )

    @staticmethod
    def get_status(source_id: Optional[int] = None) -> dict:
        """Aggregate status across both backends.

        The dashboard `/api/watcher/status` endpoint ultimately calls
        this; it merges the polling registry with the watchdog one so
        the UI does not need to care which backend is active.
        """
        from src.scanner.file_watcher import _watchers as polling_watchers
        merged: Dict[int, dict] = {}
        with _watchers_lock:
            for sid, w in _watchers.items():
                merged[sid] = w.get_status()
        for sid, w in polling_watchers.items():
            if sid not in merged:
                merged[sid] = w.get_status()
        if source_id is not None:
            return merged.get(source_id, {"status": "inactive"})
        return merged
