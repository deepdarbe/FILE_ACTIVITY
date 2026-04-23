"""Legal hold registry — freeze paths from archive/retention/cleanup.

Issue #59. Implements a glob-based path freeze registry that gates
destructive operations on ``scanned_files`` rows.

A *hold* is an ``fnmatch`` pattern (e.g. ``/share/finance/*``) plus a
human-readable reason and an optional case reference. While a hold
is *active* (``released_at IS NULL``), every path that matches the
pattern is blocked from:

* ``ArchiveEngine.archive_files`` — files are skipped + audited.
* Retention purge / scan-retention cleanup (callers consult
  :meth:`LegalHoldRegistry.is_held` before deleting).

Holds cannot be ``DELETE``'d by application code — only released
(``released_at`` stamped, ``released_by`` recorded). Every add /
release is written to ``file_audit_events`` via
``insert_audit_event_chained`` when available (so it participates
in the tamper-evident chain from issue #38), falling back to
``insert_audit_event`` otherwise.

The class is intentionally cheap to construct: a single ``Database``
reference plus the application config dict. No background threads,
no caching.
"""

from __future__ import annotations

import fnmatch
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger("file_activity.compliance.legal_hold")


class LegalHoldRegistry:
    """Glob-based path freeze registry for legal/compliance holds.

    Once a hold is added, matching paths are blocked from archive,
    retention purge, and scan retention cleanup. Holds cannot be
    deleted — only released. Every add/release is audited via the
    chained audit log when enabled (#38).
    """

    def __init__(self, db, config: dict):
        self.db = db
        self.config = config or {}
        # Allow callers to disable enforcement at runtime via config.
        # When disabled, ``is_held`` always returns None (no blocks)
        # but the registry still records add/release for audit trail.
        cfg = (self.config.get("compliance") or {}).get("legal_hold") or {}
        self.enabled = bool(cfg.get("enabled", True))

    # ──────────────────────────────────────────────
    # Audit helper
    # ──────────────────────────────────────────────

    def _audit(self, event_type: str, file_path: str, details: str,
               username: str) -> None:
        """Write an audit event for a hold mutation.

        Prefers ``insert_audit_event_chained`` when the database
        exposes it (issue #38). Falls back to ``insert_audit_event``
        and finally to ``insert_audit_event_simple``. Failures are
        logged and swallowed — audit must never break the primary
        operation, but operators should see the warning.
        """
        try:
            if hasattr(self.db, "insert_audit_event_chained"):
                self.db.insert_audit_event_chained({
                    "source_id": None,
                    "event_type": event_type,
                    "username": username,
                    "file_path": file_path,
                    "details": details,
                    "detected_by": "legal_hold",
                })
                return
        except Exception as e:
            logger.warning("legal_hold chained audit failed: %s", e)

        try:
            if hasattr(self.db, "insert_audit_event"):
                self.db.insert_audit_event(
                    source_id=None,
                    event_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    event_type=event_type,
                    username=username,
                    file_path=file_path,
                    file_name=None,
                    details=details,
                    detected_by="legal_hold",
                )
                return
        except Exception as e:
            logger.warning("legal_hold audit fallback failed: %s", e)

        try:
            if hasattr(self.db, "insert_audit_event_simple"):
                self.db.insert_audit_event_simple(
                    source_id=None,
                    event_type=event_type,
                    username=username,
                    file_path=file_path,
                    details=details,
                    detected_by="legal_hold",
                )
        except Exception as e:
            logger.warning("legal_hold audit_simple fallback failed: %s", e)

    # ──────────────────────────────────────────────
    # Queries
    # ──────────────────────────────────────────────

    def is_held(self, file_path: str) -> Optional[dict]:
        """Returns first matching active hold record, or None.

        Active = ``released_at IS NULL``. Uses ``fnmatch.fnmatch``
        for pattern matching, evaluated in Python (not in SQL) so
        operators can use full glob syntax (``*``, ``?``, ``[seq]``)
        without worrying about SQLite's ``GLOB`` quirks.

        When the registry is disabled via config, always returns
        ``None`` — the gate is open.
        """
        if not self.enabled or not file_path:
            return None
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM legal_holds WHERE released_at IS NULL "
                "ORDER BY id ASC"
            )
            rows = cur.fetchall()
        for row in rows:
            pattern = row["path_pattern"]
            if fnmatch.fnmatch(file_path, pattern):
                return dict(row)
        return None

    def list_active(self) -> list[dict]:
        """Return every hold whose ``released_at`` is NULL."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM legal_holds WHERE released_at IS NULL "
                "ORDER BY created_at DESC, id DESC"
            )
            return [dict(r) for r in cur.fetchall()]

    def list_history(self, page: int = 1, page_size: int = 50) -> dict:
        """Paginated audit-style listing of *all* holds (active + released)."""
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 50), 500))
        offset = (page - 1) * page_size
        with self.db.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM legal_holds")
            total = cur.fetchone()["cnt"]
            cur.execute(
                "SELECT * FROM legal_holds "
                "ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                (page_size, offset),
            )
            rows = [dict(r) for r in cur.fetchall()]
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "holds": rows,
        }

    def count_held_paths(self, source_id: int = None) -> int:
        """Cheap count for sidebar badge: number of scanned_files
        rows currently matched by any active hold. Used by dashboard.

        Iterates active holds (typically a handful) and accumulates
        ``COUNT(DISTINCT id)`` per pattern using SQLite ``GLOB``.
        For the sidebar badge we only need a ballpark — we therefore
        sum per-pattern counts and dedupe via a subquery union when
        more than one pattern is active.
        """
        actives = self.list_active()
        if not actives:
            return 0

        with self.db.get_cursor() as cur:
            # Build a UNION across patterns so we count distinct file
            # paths even when several holds overlap. SQLite's GLOB
            # operator matches the same syntax as fnmatch for our
            # purposes (``*`` / ``?`` / character classes), which is
            # what operators type into the pattern field.
            params: list = []
            selects = []
            for row in actives:
                clause = (
                    "SELECT id FROM scanned_files "
                    "WHERE file_path GLOB ?"
                )
                params.append(row["path_pattern"])
                if source_id is not None:
                    clause += " AND source_id = ?"
                    params.append(int(source_id))
                selects.append(clause)
            sql = (
                "SELECT COUNT(*) AS cnt FROM ("
                + " UNION ".join(selects)
                + ")"
            )
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["cnt"] or 0)

    # ──────────────────────────────────────────────
    # Mutations
    # ──────────────────────────────────────────────

    def add_hold(self, pattern: str, reason: str, case_ref: str,
                 created_by: str) -> int:
        """Insert a new active hold and return its row id.

        ``pattern`` and ``reason`` are required (non-empty). Audits
        the action via the chained log when available.
        """
        if not pattern or not str(pattern).strip():
            raise ValueError("pattern is required")
        if not reason or not str(reason).strip():
            raise ValueError("reason is required")
        if not created_by or not str(created_by).strip():
            raise ValueError("created_by is required")

        with self.db.get_cursor() as cur:
            cur.execute(
                "INSERT INTO legal_holds "
                "(path_pattern, reason, case_reference, created_by) "
                "VALUES (?, ?, ?, ?)",
                (pattern, reason, case_ref or None, created_by),
            )
            hold_id = cur.lastrowid

        details = (
            f"Legal hold #{hold_id} added: pattern={pattern!r} "
            f"case={case_ref or '-'} reason={reason}"
        )
        self._audit(
            event_type="legal_hold_added",
            file_path=pattern,
            details=details,
            username=created_by,
        )
        return int(hold_id)

    def release_hold(self, hold_id: int, released_by: str) -> bool:
        """Stamp ``released_at`` on the hold.

        Returns True when the hold was active and got released.
        Returns False when the hold does not exist or is already
        released (idempotent — no double-audit, no error).
        """
        if not released_by or not str(released_by).strip():
            raise ValueError("released_by is required")

        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT id, path_pattern, reason, released_at "
                "FROM legal_holds WHERE id = ?",
                (int(hold_id),),
            )
            row = cur.fetchone()
            if row is None or row["released_at"] is not None:
                return False
            cur.execute(
                "UPDATE legal_holds "
                "SET released_at = ?, released_by = ? "
                "WHERE id = ? AND released_at IS NULL",
                (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    released_by,
                    int(hold_id),
                ),
            )
            updated = cur.rowcount

        if not updated:
            return False

        details = (
            f"Legal hold #{hold_id} released: pattern={row['path_pattern']!r} "
            f"reason={row['reason']}"
        )
        self._audit(
            event_type="legal_hold_released",
            file_path=row["path_pattern"],
            details=details,
            username=released_by,
        )
        return True
