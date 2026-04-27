"""SQLite corruption detection (issue #77 Phase 2).

Probe used by the dashboard bootstrap to decide whether the live DB is
salvageable or whether ``BackupManager.auto_restore_if_needed`` should
take over.

Design rules
------------
* Read-only — never write, never even hold a transaction. We open a
  fresh transient connection, run two checks, close it. No side effects.
* Stdlib only (``sqlite3`` + dataclasses). No SQLAlchemy / ORMs.
* Two signals, both deliberately conservative:
    1. ``PRAGMA integrity_check`` — SQLite's own consistency probe; the
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

The detector intentionally does not consult the manifest or any backup
state. It only answers the question "is this file usable right now?"
"""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass

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


@dataclass
class CorruptionResult:
    """Outcome of one corruption probe.

    ``reason`` is one of:
      * ``"none"`` — file passes both checks (``is_corrupted=False``)
      * ``"integrity_fail"`` — ``PRAGMA integrity_check`` returned
        anything other than ``ok``
      * ``"missing_tables"`` — at least one critical table absent
      * ``"probe_error"`` — could not even open the file (locked,
        permission denied, etc.). Treated as **not** corrupted so we
        do NOT auto-restore on transient noise.

    ``details`` carries the human-readable evidence (raw integrity
    output, comma-separated missing-table names, or the OSError text).
    """

    is_corrupted: bool
    reason: str
    details: str


def is_corrupted(db_path: str) -> CorruptionResult:
    """Run the two checks against ``db_path`` in a transient connection.

    The connection is opened with a short ``timeout`` so a busy WAL
    doesn't make us wait — corruption probes must be fast and never
    block the dashboard bootstrap. The connection is always closed,
    even on exception.
    """
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

    conn: sqlite3.Connection | None = None
    try:
        # ``timeout=2`` keeps us from sitting on a busy WAL during
        # startup. Read-only-ish: we never BEGIN a write transaction.
        conn = sqlite3.connect(db_path, timeout=2)
        # Check 1: integrity_check. Returns one row per problem; a
        # healthy DB returns exactly one row whose only column is the
        # literal string "ok".
        rows = conn.execute("PRAGMA integrity_check").fetchall()
        flat = [str(r[0]) if isinstance(r, tuple) else str(r) for r in rows]
        if flat != ["ok"]:
            details = "; ".join(flat)[:500] or "<no output>"
            return CorruptionResult(
                is_corrupted=True,
                reason="integrity_fail",
                details=details,
            )

        # Check 2: critical tables present.
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name IN (?,?,?)",
            CRITICAL_TABLES,
        )
        present = {row[0] for row in cur.fetchall()}
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
    except sqlite3.DatabaseError as e:
        # ``file is not a database`` / ``database disk image is
        # malformed`` are explicit corruption signals — they're a
        # subclass of DatabaseError, not OperationalError. SQLite raises
        # these from the very first PRAGMA we run.
        return CorruptionResult(
            is_corrupted=True,
            reason="integrity_fail",
            details=str(e)[:500],
        )
    except sqlite3.OperationalError as e:
        # Transient: file locked, OS busy, etc. NOT corrupted.
        logger.warning(
            "corruption probe transient error on %s: %s", db_path, e
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=str(e)[:500],
        )
    except OSError as e:
        # Permission denied / IO error reaching the file.
        logger.warning(
            "corruption probe OS error on %s: %s", db_path, e
        )
        return CorruptionResult(
            is_corrupted=False,
            reason="probe_error",
            details=str(e)[:500],
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
