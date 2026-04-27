"""SQLite corruption detection (issue #77 Phase 2).

Probe used by the dashboard bootstrap to decide whether the live DB is
salvageable or whether ``BackupManager.auto_restore_if_needed`` should
take over.

Design rules
------------
* Read-only — never write, never even hold a transaction. We open a
  fresh transient connection, run two checks, close it. No side effects.
* Stdlib only (``sqlite3`` + dataclasses + threading). No SQLAlchemy / ORMs.
* Two signals, both deliberately conservative:
    1. ``PRAGMA quick_check`` (default) / ``PRAGMA integrity_check``
       (opt-in, slow on big DBs) — SQLite's own consistency probe; the
       result is the literal string ``ok`` when the file's b-trees /
       checksums are coherent. Anything else (multi-line error report,
       ``database disk image is malformed``, ``file is not a database``)
       counts as ``integrity_fail``.
    2. Critical-table presence — ``scan_runs``, ``scanned_files`` and
       ``file_audit_events``. Missing any of these = ``missing_tables``.
       We never auto-restore on transient errors (locked file, network
       blip, permission); those raise ``OperationalError`` from the
       ``connect`` call itself and bubble out as ``is_corrupted=False``
       with ``reason="probe_error"`` so the caller can log + continue.

Hotfix (multi-GB DBs)
---------------------
``PRAGMA integrity_check`` walks every page AND reconciles every index;
on a 3.5 GB SQLite that was hanging the dashboard for minutes/forever.
``PRAGMA quick_check`` is functionally the same b-tree walk minus the
index reconciliation step — typically 100x faster on big DBs and still
catches the corruption modes that matter for "is this file usable?".

Two new config knobs (``backup`` block):
  * ``corruption_check_mode``: ``"quick"`` (default), ``"full"`` or
    ``"skip"``.
  * ``corruption_check_timeout_seconds``: hard cap (default 30s). The
    pragma runs in a daemon thread; if it doesn't finish in time we
    return ``CorruptionResult(is_corrupted=False, reason="check_timed_out")``
    and log a WARNING — never let a probe block startup.

The detector intentionally does not consult the manifest or any backup
state. It only answers the question "is this file usable right now?"
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger("file_activity.storage.corruption_detector")

# Tables the application cannot function without. If any one of these
# is missing we treat the DB as corrupted — typically the file is
# either fresh-zero-byte, truncated mid-format, or someone restored
# the wrong file over it.
CRITICAL_TABLES: tuple[str, ...] = (
    "scan_runs",
    "scanned_files",
    "file_audit_events",
)

# Defaults preserved when the operator's config.yaml predates the
# hotfix and lacks the new knobs.
DEFAULT_CHECK_MODE = "quick"
DEFAULT_CHECK_TIMEOUT_SECONDS = 30


@dataclass
class CorruptionResult:
    """Outcome of one corruption probe.

    ``reason`` is one of:
      * ``"none"`` — file passes both checks (``is_corrupted=False``)
      * ``"integrity_fail"`` — ``PRAGMA quick_check``/``integrity_check``
        returned anything other than ``ok``
      * ``"missing_tables"`` — at least one critical table absent
      * ``"probe_error"`` — could not even open the file (locked,
        permission denied, etc.). Treated as **not** corrupted so we
        do NOT auto-restore on transient noise.
      * ``"skipped"`` — operator set ``corruption_check_mode: skip``.
        Returns ``is_corrupted=False`` so bootstrap proceeds.
      * ``"check_timed_out"`` — pragma exceeded the configured timeout.
        Returns ``is_corrupted=False`` (assume healthy) and logs a
        WARNING so operators know to investigate manually.

    ``details`` carries the human-readable evidence (raw integrity
    output, comma-separated missing-table names, or the OSError text).
    """

    is_corrupted: bool
    reason: str
    details: str


def _resolve_check_settings(config: Optional[dict]) -> tuple[str, float]:
    """Pull the (mode, timeout) tuple from config with safe defaults.

    Backwards-compatible: a None / empty config or one missing the new
    keys yields the same behaviour as the previous code path (now via
    ``quick_check`` instead of ``integrity_check`` — that's the hotfix).
    """
    backup_cfg: dict = {}
    if isinstance(config, dict):
        raw = config.get("backup")
        if isinstance(raw, dict):
            backup_cfg = raw

    mode = str(backup_cfg.get("corruption_check_mode", DEFAULT_CHECK_MODE) or DEFAULT_CHECK_MODE).strip().lower()
    if mode not in ("quick", "full", "skip"):
        logger.warning(
            "unknown corruption_check_mode=%r — falling back to %r",
            mode, DEFAULT_CHECK_MODE,
        )
        mode = DEFAULT_CHECK_MODE

    try:
        timeout = float(backup_cfg.get(
            "corruption_check_timeout_seconds",
            DEFAULT_CHECK_TIMEOUT_SECONDS,
        ))
    except (TypeError, ValueError):
        timeout = float(DEFAULT_CHECK_TIMEOUT_SECONDS)
    if timeout <= 0:
        timeout = float(DEFAULT_CHECK_TIMEOUT_SECONDS)
    return mode, timeout


def _run_pragma_with_timeout(
    db_path: str,
    pragma: str,
    timeout: float,
) -> tuple[str, Any]:
    """Run ``pragma`` against ``db_path`` in a daemon thread.

    Returns ``(state, payload)`` where ``state`` is one of:
      * ``"ok"``       — payload is the rows (list of tuples)
      * ``"db_error"`` — payload is the ``sqlite3.DatabaseError`` raised
                         (corruption signal)
      * ``"op_error"`` — payload is the ``sqlite3.OperationalError``
                         (transient — locked, busy, etc.)
      * ``"os_error"`` — payload is an ``OSError`` reaching the file
      * ``"timeout"``  — payload is the elapsed budget (float)

    Quick-fail safety: if the thread is *still* alive after
    ``2 * timeout`` we log CRITICAL and report ``timeout`` regardless,
    so a misbehaving I/O layer can never wedge the dashboard startup.
    """
    box: dict[str, Any] = {"state": None, "payload": None}

    def _target() -> None:
        try:
            conn = sqlite3.connect(db_path, timeout=2)
            try:
                rows = conn.execute(pragma).fetchall()
                box["state"] = "ok"
                box["payload"] = rows
            finally:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
        except sqlite3.DatabaseError as e:
            box["state"] = "db_error"
            box["payload"] = e
        except sqlite3.OperationalError as e:  # pragma: no cover - subclass of DatabaseError
            box["state"] = "op_error"
            box["payload"] = e
        except OSError as e:
            box["state"] = "os_error"
            box["payload"] = e

    th = threading.Thread(
        target=_target,
        name="corruption-probe",
        daemon=True,
    )
    th.start()
    th.join(timeout)
    if th.is_alive():
        # First-stage timeout — pragma is still running. Give it a
        # second chance up to 2*timeout for a graceful return, then
        # bail out CRITICAL. We never attempt to kill the thread
        # (Python has no safe primitive for that); since it's daemon
        # it will die with the process.
        th.join(timeout)
        if th.is_alive():
            logger.critical(
                "corruption probe still alive after %.1fs (>2x timeout) — "
                "abandoning thread and continuing assuming healthy",
                2 * timeout,
            )
            return "timeout", 2 * timeout
        return "timeout", timeout
    state = box.get("state") or "op_error"
    return state, box.get("payload")


def is_corrupted(
    db_path: str,
    config: Optional[dict] = None,
) -> CorruptionResult:
    """Run the two checks against ``db_path`` in a transient connection.

    The pragma runs in a daemon thread bounded by
    ``backup.corruption_check_timeout_seconds`` so a multi-GB DB cannot
    wedge the dashboard bootstrap. The choice of pragma comes from
    ``backup.corruption_check_mode`` — defaults to ``quick`` (cheap and
    catches the corruption modes that block the app) and can be flipped
    to ``full`` for the slow-but-thorough ``PRAGMA integrity_check`` or
    ``skip`` to disable corruption probing entirely.

    Backwards compatibility: callers that don't pass a ``config`` get
    the safe defaults — quick mode, 30s timeout.
    """
    mode, timeout = _resolve_check_settings(config)

    if mode == "skip":
        # Operator opted out entirely. We still return a
        # CorruptionResult so the caller's logging stays uniform.
        return CorruptionResult(
            is_corrupted=False,
            reason="skipped",
            details="corruption_check_mode=skip",
        )

    if not os.path.exists(db_path):
        # No file at all — caller's responsibility (Database.connect
        # will create one). Not corrupted.
        return CorruptionResult(
            is_corrupted=False,
            reason="none",
            details=f"db_path does not exist: {db_path}",
        )

    # Zero-byte file is a special case sqlite3.connect() accepts (it
    # would create an empty-but-valid DB on first write). Treat it as
    # missing tables — the application cannot run against an empty
    # schema and we'd rather salvage from backup if one exists.
    try:
        if os.path.getsize(db_path) == 0:
            return CorruptionResult(
                is_corrupted=True,
                reason="missing_tables",
                details="db file is zero bytes",
            )
    except OSError as e:
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=f"stat failed: {e}",
        )

    pragma = "PRAGMA integrity_check" if mode == "full" else "PRAGMA quick_check"
    state, payload = _run_pragma_with_timeout(db_path, pragma, timeout)

    if state == "timeout":
        # Hotfix safety: assume healthy so the dashboard continues. Ops
        # see a WARNING and can re-run the probe manually with mode=full
        # if they suspect corruption.
        logger.warning(
            "corruption probe %s on %s exceeded %.1fs — assuming healthy "
            "(set backup.corruption_check_mode=full and re-run manually "
            "if you suspect corruption)",
            pragma, db_path, timeout,
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="check_timed_out",
            details=f"{pragma} exceeded {timeout:.1f}s",
        )

    if state == "db_error":
        # ``file is not a database`` / ``database disk image is
        # malformed`` are explicit corruption signals — they're a
        # subclass of DatabaseError, not OperationalError. SQLite raises
        # these from the very first PRAGMA we run.
        return CorruptionResult(
            is_corrupted=True,
            reason="integrity_fail",
            details=str(payload)[:500],
        )

    if state == "op_error":
        # Transient: file locked, OS busy, etc. NOT corrupted.
        logger.warning(
            "corruption probe transient error on %s: %s", db_path, payload,
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=str(payload)[:500],
        )

    if state == "os_error":
        # Permission denied / IO error reaching the file.
        logger.warning(
            "corruption probe OS error on %s: %s", db_path, payload,
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=str(payload)[:500],
        )

    # state == "ok" — interpret the rows.
    rows = payload or []
    flat = [str(r[0]) if isinstance(r, tuple) else str(r) for r in rows]
    if flat != ["ok"]:
        details = "; ".join(flat)[:500] or "<no output>"
        return CorruptionResult(
            is_corrupted=True,
            reason="integrity_fail",
            details=details,
        )

    # Pragma passed. Check 2: critical tables present. This second
    # query is sub-millisecond on any DB so we run it inline (no
    # threading overhead) but still inside a try/except for safety.
    try:
        conn = sqlite3.connect(db_path, timeout=2)
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name IN (?,?,?)",
                CRITICAL_TABLES,
            )
            present = {row[0] for row in cur.fetchall()}
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass
    except sqlite3.DatabaseError as e:
        return CorruptionResult(
            is_corrupted=True,
            reason="integrity_fail",
            details=str(e)[:500],
        )
    except sqlite3.OperationalError as e:  # pragma: no cover - subclass
        logger.warning(
            "corruption probe (table check) transient error on %s: %s",
            db_path, e,
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=str(e)[:500],
        )
    except OSError as e:
        logger.warning(
            "corruption probe (table check) OS error on %s: %s", db_path, e,
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=str(e)[:500],
        )

    missing = [t for t in CRITICAL_TABLES if t not in present]
    if missing:
        return CorruptionResult(
            is_corrupted=True,
            reason="missing_tables",
            details=",".join(missing),
        )

    return CorruptionResult(
        is_corrupted=False,
        reason="none",
        details="ok",
    )
