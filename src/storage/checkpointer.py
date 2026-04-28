"""Manual WAL checkpointer (issue #153, Lever A).

A daemon thread that runs ``PRAGMA wal_checkpoint(TRUNCATE)`` against the
SQLite WAL on idle moments. Three goals, in order of importance:

  1. **Prevent WAL bloat.** A customer hit a 25 GB WAL before the #119
     hotfix; with ``wal_autocheckpoint=0`` the engine never truncates
     unless something asks. This thread *is* the something.
  2. **Run only when the scanner is between batches.** ``request()`` is
     a non-blocking signal the scanner calls right after each
     ``stager.append(batch)``; the loop wakes, checks WAL size, runs
     the pragma. If the scanner isn't running, the timer-driven path
     keeps the WAL groomed during quiet hours.
  3. **Force-truncate above ``checkpoint_force_threshold_mb``.** Safety
     net: even if the scanner is hammering, an explicit truncate
     attempt fires when the WAL grows past the cap, so worst-case WAL
     size stays bounded.

The thread is daemon-mode (auto-stops on process exit), holds no
references to the writer connection, and opens its own short-lived
connection per checkpoint pass. The dedicated connection is what lets
us call ``TRUNCATE`` without fighting the writer's ``check_same_thread``
guarantees: SQLite serialises checkpoints internally, and a missed
checkpoint is harmless — the next iteration retries.

If init fails (sqlite version too old, OS path issues, etc.) callers
should catch and fall back to ``PRAGMA wal_autocheckpoint=1000``; see
``Database.connect`` for the wiring.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from typing import Optional

logger = logging.getLogger("file_activity.checkpointer")


class Checkpointer:
    """Daemon thread that runs ``PRAGMA wal_checkpoint(TRUNCATE)`` on idle.

    Lifecycle:
      * ``start()`` spawns the daemon thread. Idempotent: a second call
        on an already-running checkpointer is a no-op.
      * ``request()`` signals "now is a good time" — non-blocking, safe
        to call from the scanner's hot batch-flush path.
      * ``stop()`` flips the stop event and wakes the thread; ``join``-able
        but daemon-mode means we don't have to wait at process exit.

    Thread safety:
      * ``_stop`` and ``_wakeup`` are ``threading.Event`` (lock-free reads).
      * Each iteration opens a fresh ``sqlite3.connect`` so the worker
        thread never shares a handle with anything else (no
        ``check_same_thread`` minefield).
    """

    def __init__(self, db_path: str, config: dict) -> None:
        self.db_path = db_path
        backup_cfg = (config or {}).get("backup") or {}
        # Both knobs live under ``backup`` so ops sees them next to the
        # snapshot/restore controls — the WAL is part of the same story.
        # ``interval_seconds`` is float so tests / tuning can use sub-
        # second values; force-threshold is a whole-MB integer.
        self.interval_seconds = float(
            backup_cfg.get("checkpoint_interval_seconds", 30)
        )
        self.force_threshold_mb = int(
            backup_cfg.get("checkpoint_force_threshold_mb", 500)
        )
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Health: last error string, last successful checkpoint timestamp,
        # pass counter. The dashboard can surface these later if we decide
        # to add a /api/system/checkpointer endpoint.
        self._last_error: Optional[str] = None
        self._last_run_ts: float = 0.0
        self._pass_count: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spawn the daemon thread. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._wakeup.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="file_activity.checkpointer",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Checkpointer baslatildi: interval=%.2fs, force_threshold=%dMB",
            self.interval_seconds, self.force_threshold_mb,
        )

    def request(self) -> None:
        """Non-blocking signal: "now is a good time to checkpoint".

        Called from the scanner immediately after a batch flush. If the
        thread is in its timer wait, this wakes it; if it's already
        running a checkpoint, the wakeup is coalesced with the next
        iteration. Either way, the caller never blocks.
        """
        self._wakeup.set()

    def stop(self) -> None:
        """Graceful shutdown. Idempotent — safe to call on a stopped
        checkpointer or one that never started."""
        self._stop.set()
        self._wakeup.set()

    def join(self, timeout: Optional[float] = None) -> None:
        """Wait for the worker thread to exit. Mainly for tests."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        """Wait → checkpoint → repeat. Exits on ``_stop``.

        Wait blocks on ``_wakeup`` with a timeout; this means three
        events end the wait:
          * ``request()`` was called (scanner finished a batch)
          * ``interval_seconds`` elapsed (timer-driven groom)
          * ``stop()`` was called (we exit)
        """
        while not self._stop.is_set():
            self._wakeup.wait(timeout=self.interval_seconds)
            self._wakeup.clear()
            if self._stop.is_set():
                return
            try:
                self._maybe_checkpoint()
            except Exception as e:  # pragma: no cover - defensive
                # Per-iteration failures should never kill the daemon.
                # Capture the message for diagnostics and keep going.
                self._last_error = str(e)
                logger.warning("Checkpointer iteration basarisiz: %s", e)

    def _maybe_checkpoint(self) -> None:
        """Inspect WAL size; run TRUNCATE if appropriate.

        Logic:
          * No WAL file or WAL < 1 MB → nothing to do (avoids opening a
            connection just to no-op; SQLite handles tiny WALs natively).
          * WAL ≥ ``force_threshold_mb`` → log a warning *and* run
            TRUNCATE; this is the safety-net path when the scanner is
            hammering us.
          * Otherwise → run TRUNCATE; it's idempotent and cheap.
        """
        wal_path = self.db_path + "-wal"
        if not os.path.exists(wal_path):
            return
        try:
            wal_bytes = os.path.getsize(wal_path)
        except OSError:
            # WAL may have been truncated under us between the exists()
            # check and the stat — harmless, retry on next pass.
            return
        wal_mb = wal_bytes / (1024 * 1024)
        if wal_mb < 1:
            return

        forced = wal_mb >= self.force_threshold_mb
        if forced:
            logger.warning(
                "Checkpointer force-truncate: WAL=%.1f MB >= threshold=%dMB",
                wal_mb, self.force_threshold_mb,
            )

        # Open a short-lived dedicated connection. Doing so per pass
        # avoids: (a) ``check_same_thread`` issues if we shared the
        # writer's handle, (b) keeping a stale handle through DB
        # restore/swap (issue #77 auto-restore renames the file out
        # from under us). The 5-second timeout lets us back off
        # cleanly when the writer holds the lock.
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()

        # WAL size *after* TRUNCATE — useful for distinguishing a
        # successful truncate from a reader-blocked one.
        try:
            wal_after_mb = (
                os.path.getsize(wal_path) / (1024 * 1024)
                if os.path.exists(wal_path)
                else 0
            )
        except OSError:
            wal_after_mb = 0

        import time as _time
        self._last_run_ts = _time.time()
        self._pass_count += 1
        self._last_error = None

        # Keep this at debug level on the happy path so the log stays
        # quiet during a long scan; bump to info when we forced.
        log_fn = logger.info if forced else logger.debug
        log_fn(
            "Checkpointer pass: WAL %.1f MB -> %.1f MB%s",
            wal_mb, wal_after_mb,
            " (forced)" if forced else "",
        )
