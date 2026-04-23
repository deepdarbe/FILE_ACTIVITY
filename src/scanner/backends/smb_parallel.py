"""Parallel ``os.scandir`` backend.

Primary use-case is SMB shares where round-trip latency dominates. We fan the
walk out across a thread pool, one subtree per worker, so multiple metadata
requests are in flight against the server at once. On local NTFS it's also a
decent default — per-call overhead is tiny compared to a single-threaded walk.

Notes
-----
* ``os.scandir`` is used everywhere; ``DirEntry.stat()`` on Windows already
  contains the Win32 attribute bitmask (``st_file_attributes``) so we don't
  need an extra API call per file for skip_hidden / skip_system.
* Long paths (>= 240 chars) are transparently prefixed with ``\\\\?\\`` via
  ``win_attributes._long_path`` so 260+ character UNC trees still scan.
* Owner lookup is intentionally gated on ``read_owner`` — it requires a
  per-file SECURITY_DESCRIPTOR round-trip which is ~50x slower than a stat.
"""

from __future__ import annotations

import fnmatch
import logging
import os
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional

from src.scanner.win_attributes import _long_path, get_file_times

logger = logging.getLogger("file_activity.scanner.smb_parallel")


# Win32 file attribute bit flags — duplicated here so we don't need pywin32 on
# non-Windows platforms (e.g. CI, tests). Values come from winnt.h.
_FILE_ATTRIBUTE_HIDDEN = 0x00000002
_FILE_ATTRIBUTE_SYSTEM = 0x00000004


class SmbParallelBackend:
    """Thread-pool parallel ``os.scandir`` walker.

    Parameters
    ----------
    config:
        Either the full application config (``{"scanner": {...}, ...}``) or
        just the ``scanner`` subsection. Accepted keys:

        * ``smb_workers`` (int, default 32)  — thread count
        * ``exclude_patterns`` (list[str])   — fnmatch globs applied to names
        * ``skip_hidden`` (bool, default True)
        * ``skip_system`` (bool, default True)
        * ``read_owner`` (bool, default False)
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        scanner_cfg = config.get("scanner", config) if isinstance(config, dict) else {}
        self._workers: int = int(scanner_cfg.get("smb_workers", 32))
        self._exclude_patterns: List[str] = list(scanner_cfg.get("exclude_patterns", []))
        self._skip_hidden: bool = bool(scanner_cfg.get("skip_hidden", True))
        self._skip_system: bool = bool(scanner_cfg.get("skip_system", True))
        self._read_owner: bool = bool(scanner_cfg.get("read_owner", False))

        # Sentinel used to signal end-of-stream on the result queue.
        self._SENTINEL = object()

    # ------------------------------------------------------------------ API

    def walk(self, root: str) -> Iterator[Dict[str, Any]]:
        """Walk ``root`` and yield one metadata dict per file.

        The implementation seeds a thread pool with the root's immediate
        subdirectories (plus a task for the root's own files). Each worker
        recurses its assigned subtree sequentially but multiple subtrees run
        concurrently, which is the sweet spot for SMB: server-side directory
        enumeration is cheap, the cost is round-trip latency.
        """
        if not root:
            return

        # Results are pushed onto a bounded queue so the consumer (caller)
        # applies natural back-pressure — workers block on ``put`` when the
        # consumer lags, keeping memory bounded even on huge trees.
        results: "queue.Queue[Any]" = queue.Queue(maxsize=10_000)
        pending = threading.Semaphore(0)  # unused; kept for readability
        pending_tasks = [0]
        pending_lock = threading.Lock()

        def _submit(executor: ThreadPoolExecutor, path: str) -> None:
            with pending_lock:
                pending_tasks[0] += 1
            executor.submit(_safe_worker, path)

        def _safe_worker(path: str) -> None:
            try:
                self._walk_subtree(path, results, executor, _submit)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Worker hatasi %s: %s", path, exc)
            finally:
                with pending_lock:
                    pending_tasks[0] -= 1
                    if pending_tasks[0] == 0:
                        results.put(self._SENTINEL)

        with ThreadPoolExecutor(max_workers=max(1, self._workers)) as executor:
            # Seed: enumerate immediate children of root. Files are emitted
            # directly, directories become worker tasks.
            seeded_any = False
            for child in self._iter_entries(root):
                try:
                    is_dir = child.is_dir(follow_symlinks=False)
                except OSError:
                    continue

                if is_dir:
                    _submit(executor, child.path)
                    seeded_any = True
                else:
                    row = self._build_row(child)
                    if row is not None:
                        results.put(row)

            if not seeded_any:
                # Root had no subdirs — emit the terminator so we exit cleanly.
                results.put(self._SENTINEL)

            # Drain the queue until we see the sentinel. Workers push the
            # sentinel exactly once (when the last outstanding task finishes).
            while True:
                item = results.get()
                if item is self._SENTINEL:
                    break
                yield item

    # ---------------------------------------------------------- internals

    def _walk_subtree(
        self,
        path: str,
        results: "queue.Queue[Any]",
        executor: ThreadPoolExecutor,
        submit_fn,
    ) -> None:
        """Depth-first walk of a single subtree, emitting rows into ``results``.

        Subdirectories encountered here are handled inline (not re-submitted)
        which keeps queue churn low. The fan-out happens only at the root.
        """
        stack: List[str] = [path]
        while stack:
            current = stack.pop()
            for entry in self._iter_entries(current):
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                except OSError:
                    continue

                if is_dir:
                    stack.append(entry.path)
                else:
                    row = self._build_row(entry)
                    if row is not None:
                        results.put(row)

    def _iter_entries(self, path: str):
        """``os.scandir`` iterator that swallows per-directory errors."""
        scan_path = _long_path(path) if len(path) >= 240 else path
        try:
            with os.scandir(scan_path) as it:
                for entry in it:
                    yield entry
        except PermissionError:
            logger.debug("Dizin erisim reddedildi: %s", path)
        except OSError as exc:
            logger.debug("Dizin hatasi: %s - %s", path, exc)

    # ------------------------------------------------------------ filters

    def _should_skip(self, entry: os.DirEntry, attrs: int) -> bool:
        """Return True if ``entry`` is filtered out by config rules."""
        name = entry.name

        for pattern in self._exclude_patterns:
            if fnmatch.fnmatch(name, pattern):
                return True

        if sys.platform == "win32":
            if self._skip_hidden and (attrs & _FILE_ATTRIBUTE_HIDDEN):
                return True
            if self._skip_system and (attrs & _FILE_ATTRIBUTE_SYSTEM):
                return True
        else:
            # POSIX: dotfiles are the conventional "hidden" signal.
            if self._skip_hidden and name.startswith("."):
                return True

        return False

    # --------------------------------------------------------- row builder

    def _build_row(self, entry: os.DirEntry) -> Optional[Dict[str, Any]]:
        """Convert a ``DirEntry`` to the dict contract documented in
        ``backends/__init__.py``. Returns None if the file should be skipped
        or stat fails."""
        try:
            st = entry.stat(follow_symlinks=False)
        except (PermissionError, OSError) as exc:
            logger.debug("stat hatasi: %s - %s", entry.path, exc)
            return None

        attrs = getattr(st, "st_file_attributes", 0)
        if self._should_skip(entry, attrs):
            return None

        # Cheap path: DirEntry.stat() already has size + POSIX times.
        from datetime import datetime

        try:
            ct = datetime.fromtimestamp(st.st_ctime).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            ct = None
        try:
            at = datetime.fromtimestamp(st.st_atime).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            at = None
        try:
            mt = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            mt = None

        owner: Optional[str] = None

        # Expensive path: owner lookup requires a Win32 SECURITY_DESCRIPTOR
        # query. Only do it when explicitly requested. We reuse
        # get_file_times() so the pywin32 code lives in exactly one place.
        if self._read_owner and sys.platform == "win32":
            try:
                info = get_file_times(entry.path, read_owner=True)
                owner = info.owner
                # Win32 timestamps are more accurate than POSIX ones from
                # DirEntry.stat() on SMB, so prefer them when available.
                if info.creation_time:
                    ct = info.creation_time
                if info.last_access_time:
                    at = info.last_access_time
                if info.last_modify_time:
                    mt = info.last_modify_time
                if info.win32_attributes:
                    attrs = info.win32_attributes
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("owner lookup hatasi: %s - %s", entry.path, exc)

        return {
            "file_path": entry.path,
            "file_name": entry.name,
            "file_size": st.st_size,
            "creation_time": ct,
            "last_access_time": at,
            "last_modify_time": mt,
            "attributes": int(attrs or 0),
            "owner": owner,
        }


__all__ = ["SmbParallelBackend"]
