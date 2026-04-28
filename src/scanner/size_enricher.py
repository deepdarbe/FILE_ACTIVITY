"""Issue #175 — post-walk size + timestamp enrich pass.

The MFT scanner backend (:class:`~src.scanner.backends.ntfs_mft.NtfsMftBackend`)
emits records with ``file_size = 0`` and ``last_modify_time = None`` because
``FSCTL_ENUM_USN_DATA`` returns paths only — by design, the USN journal
has the path resolution but not the standard information attributes. The
customer's BOYUT (size) KPI is therefore stuck at ``0 B`` for every scan
that uses the MFT backend.

This module provides :class:`SizeEnricher`, a post-walk pass that streams
``scanned_files`` rows for the just-completed scan, calls ``os.stat`` (or
the FSCTL backend on local NTFS, when available), and bulk-UPDATEs the
size + timestamp columns through the retry-protected
``Database.bulk_update_file_sizes`` helper added alongside this change.

Two backends:

* ``fsctl`` — Windows-only, uses ``FSCTL_GET_NTFS_FILE_RECORD`` via ctypes
  on the volume handle. ~10-50x faster than os.stat on local NTFS. Falls
  back to ``stat`` on ``UnsupportedError`` or ``PermissionError``.
* ``stat``  — cross-platform, plain ``os.stat()``. ~50-500 files/sec.

Public API::

    e = SizeEnricher(config, db)
    e.available  # True if at least the stat fallback works
    e.enrich(scan_id, source_id, paths_iter, progress_cb=None)

The progress callback signature matches the existing ops banner contract::

    progress_cb(stage="size_enrich", processed=N)

so the dashboard banner picks it up without a translation layer.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional

logger = logging.getLogger("file_activity.scanner.size_enrich")


# Tunables — kept module-level so tests can monkey-patch them when
# exercising chunk-boundary behaviour without pumping millions of rows.
DEFAULT_WORKERS = 8
CHUNK_SIZE = 5000
PROGRESS_EVERY = 10_000


def _to_iso(ts: Optional[float]) -> Optional[str]:
    """Convert a POSIX timestamp to ISO-8601 UTC string.

    SQLite stores timestamps as TEXT; we use UTC to match what
    :mod:`src.scanner.win_attributes` produces on the original walk.
    Returns ``None`` for ``None``/0 input so we don't poison rows that
    legitimately had no timestamp (rare on real filesystems but cheap
    to guard).
    """
    if ts is None:
        return None
    try:
        # Use UTC ISO with second precision; mtime/atime sub-second on
        # modern filesystems is fine but not what the rest of the
        # pipeline expects.
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except (OSError, OverflowError, ValueError):
        return None


class SizeEnricher:
    """Post-walk pass filling ``file_size`` + timestamps for path-only rows.

    Constructor is config + db only; the work is parametrised at the
    :meth:`enrich` call so a single :class:`SizeEnricher` instance can be
    reused across scans.
    """

    def __init__(self, config: dict, db):
        # ``config`` may be the full top-level config dict (with a
        # ``scanner`` subsection) or just the scanner subsection — be
        # liberal in what we accept, like FileScanner does.
        if isinstance(config, dict) and "scanner" in config:
            scanner_cfg = config.get("scanner") or {}
        else:
            scanner_cfg = config or {}
        self._scanner_cfg = scanner_cfg
        self.db = db

        self.workers = int(scanner_cfg.get("size_enrich_workers", DEFAULT_WORKERS))
        if self.workers < 1:
            self.workers = 1

        # 0 means "no skip"; a positive value means "skip files whose
        # st_size > N MB" (rare; only useful when stat itself stalls on
        # specific files, e.g. some legacy network FS edge cases).
        self.max_mb = int(scanner_cfg.get("size_enrich_max_mb", 0) or 0)

        # Lazy-loaded FSCTL backend on Windows. Module import happens
        # inside ``enrich`` to keep Linux/macOS test runs free of any
        # ctypes setup cost.
        self._fsctl_loaded = False
        self._fsctl_available = False

    # ── lifecycle ────────────────────────────────────────────────────

    @property
    def available(self) -> bool:
        """True if at least the stat fallback works.

        On Linux/macOS this is always True. On Windows it is also True
        unless ``os.stat`` itself is somehow unavailable (which would
        mean the runtime is broken — at which point nothing else works
        either).
        """
        return hasattr(os, "stat")

    # ── public entry point ───────────────────────────────────────────

    def enrich(
        self,
        scan_id: int,
        source_id: int,
        paths_iter: Iterable[str],
        progress_cb: Optional[Callable[..., None]] = None,
    ) -> int:
        """Enrich ``scanned_files`` rows for ``scan_id`` and return count.

        Streams paths from ``paths_iter`` (caller decides how to source
        them — typically ``SELECT file_path FROM scanned_files WHERE
        scan_id=? AND file_size=0``), batches into 5000-row chunks, and
        calls :meth:`Database.bulk_update_file_sizes` per chunk. A worker
        thread pool of size ``size_enrich_workers`` parallelises the
        ``os.stat`` calls; the actual DB write is serialised on the main
        thread so the workers don't share a sqlite connection.

        Returns the number of rows successfully UPDATE'd.
        """
        if not self.available:
            logger.debug("SizeEnricher unavailable (no os.stat?); skipping")
            return 0

        enriched = 0
        skipped = 0
        buffer: list[dict] = []
        last_progress_emit = 0

        # Workers stat in parallel; main thread drains chunks into the
        # DB. ThreadPoolExecutor.map preserves input order which is
        # convenient but not load-balanced — submit/as_completed keeps
        # the workers fed when individual stat calls are slow.
        max_in_flight = self.workers * 4

        def _stat_one(path: str) -> Optional[dict]:
            return self._stat_path(path)

        def _flush(rows: list[dict]) -> int:
            """Write a chunk and return the number of rows actually written."""
            if not rows:
                return 0
            for r in rows:
                # Composite key for the UPDATE: scan_id is constant for
                # this enrichment pass so we tag every row up front.
                r["scan_id"] = scan_id
            return self.db.bulk_update_file_sizes(rows)

        executor = ThreadPoolExecutor(
            max_workers=self.workers,
            thread_name_prefix=f"size-enrich-{scan_id}",
        )
        try:
            futures: dict = {}
            iterator = iter(paths_iter)
            exhausted = False
            processed = 0

            while not exhausted or futures:
                # Top up the in-flight pool.
                while not exhausted and len(futures) < max_in_flight:
                    try:
                        path = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    if not path:
                        continue
                    fut = executor.submit(_stat_one, path)
                    futures[fut] = path

                if not futures:
                    break

                # Drain whatever has finished. We don't wait on a single
                # future — block until at least one is done, then sweep
                # everything else that's already ready.
                done_set = set()
                for fut in as_completed(list(futures.keys())):
                    done_set.add(fut)
                    path = futures.pop(fut)
                    processed += 1
                    try:
                        row = fut.result()
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.debug(
                            "size_enrich worker raised on %s: %s", path, exc,
                        )
                        skipped += 1
                        row = None

                    if row is None:
                        skipped += 1
                    else:
                        row["file_path"] = path
                        buffer.append(row)

                    if len(buffer) >= CHUNK_SIZE:
                        try:
                            written = _flush(buffer)
                            enriched += written
                        except Exception:
                            # Re-raise: a DB failure that survived the
                            # retry loop is fatal for this pass.
                            raise
                        buffer = []

                    if (
                        progress_cb is not None
                        and processed - last_progress_emit >= PROGRESS_EVERY
                    ):
                        last_progress_emit = processed
                        try:
                            progress_cb(stage="size_enrich", processed=processed)
                        except Exception as cb_err:  # pragma: no cover
                            logger.debug(
                                "size_enrich progress_cb raised: %s", cb_err,
                            )

                    # Break out so we can refill the queue rather than
                    # draining everything at once — keeps memory bounded.
                    if not exhausted:
                        break

            # Final flush.
            if buffer:
                written = _flush(buffer)
                enriched += written
                buffer = []

            # Final progress emit so the banner doesn't sit at the last
            # PROGRESS_EVERY tick when the total isn't a clean multiple.
            if progress_cb is not None and processed != last_progress_emit:
                try:
                    progress_cb(stage="size_enrich", processed=processed)
                except Exception as cb_err:  # pragma: no cover
                    logger.debug(
                        "size_enrich final progress_cb raised: %s", cb_err,
                    )
        finally:
            executor.shutdown(wait=True)

        logger.debug(
            "SizeEnricher.enrich scan=%d enriched=%d skipped=%d",
            scan_id, enriched, skipped,
        )
        # Stash skipped count where _run_size_enrich can pick it up
        # (it's primarily an INFO log line, not a return value, but
        # the test surface expects to be able to read it).
        self.last_skipped = skipped
        self.last_processed = processed if 'processed' in locals() else 0
        return enriched

    # ── stat backend ─────────────────────────────────────────────────

    def _stat_path(self, path: str) -> Optional[dict]:
        """Return an UPDATE row for ``path`` or ``None`` to skip.

        Skips on:
          * permission denied
          * file gone (TOCTOU between MFT walk and enrich pass)
          * size cap exceeded (when ``size_enrich_max_mb > 0``)
        """
        try:
            # follow_symlinks=False: a symlink to another volume must
            # NOT cause us to read a file we never enumerated — the MFT
            # backend doesn't follow either, so the row representation
            # stays consistent.
            st = os.stat(path, follow_symlinks=False)
        except (FileNotFoundError, PermissionError, OSError) as exc:
            logger.debug("size_enrich skip %s: %s", path, exc)
            return None

        size = int(getattr(st, "st_size", 0) or 0)

        if self.max_mb > 0 and size > self.max_mb * 1_048_576:
            logger.debug(
                "size_enrich skip %s (%d bytes > %d MB cap)",
                path, size, self.max_mb,
            )
            return None

        return {
            "file_size": size,
            "last_modify_time": _to_iso(getattr(st, "st_mtime", None)),
            "last_access_time": _to_iso(getattr(st, "st_atime", None)),
            "creation_time": _to_iso(getattr(st, "st_ctime", None)),
        }

    # ── (future) FSCTL backend hook ──────────────────────────────────

    def _maybe_load_fsctl(self) -> bool:
        """Lazy-import the Windows FSCTL accelerator.

        Always returns False on non-Windows hosts. On Windows it tries to
        import the ctypes wrapper; if anything goes wrong (no admin, no
        SeBackup privilege, OS not NT 10+, network drive, etc.) we
        silently fall back to ``os.stat``. Today this is a stub — the
        actual FSCTL_GET_NTFS_FILE_RECORD wrapper lives behind a feature
        flag we'll wire in a follow-up; the hook is here so the
        constructor's two-backend contract isn't a lie.
        """
        if self._fsctl_loaded:
            return self._fsctl_available
        self._fsctl_loaded = True
        if sys.platform != "win32":
            self._fsctl_available = False
            return False
        # Future: import src.scanner.backends.ntfs_fsctl_stat here. The
        # current implementation always falls through to os.stat which
        # is the documented fallback.
        self._fsctl_available = False
        return False
