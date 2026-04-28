"""Gain reporter — captures before / after / delta metrics around write ops.

Issue #83 Phase 1. Persists operator-visible "neydik / ne olduk / kazanim"
snapshots to the ``gain_reports`` table. Schema-only this round; the
duplicate quarantine flow is the first user. Phase 3 will plug archive
and retention purge into the same reporter.

The ``capture_before`` / ``capture_after`` helpers compute storage-level
metrics (total file count, total size in GB, duplicate group count,
distinct duplicate-content size) for a given ``scope`` dict. The shape
of ``scope`` is intentionally open — callers pass at minimum
``source_id``, optionally ``scan_id``. Both helpers tolerate missing
keys: a stub snapshot is returned when the scope is empty so that the
report still renders cleanly in the UI.

``save`` is the only mutator. Failures are NEVER swallowed — a gain
report write that fails would mask data loss in the operation it
describes, so callers must see the exception and decide what to do.

All metric-collection queries use the public ``Database.get_cursor``
context manager so we participate in the same connection pooling /
transaction story as the rest of storage. We do NOT open our own
sqlite3 handles.

Stdlib only.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger("file_activity.storage.gain_reporter")


_GB = 1024 * 1024 * 1024


def _bytes_to_gb(n: int) -> float:
    """Round to 4 decimal places — enough precision for tiny corpora."""
    if not n:
        return 0.0
    return round(float(n) / _GB, 4)


class GainReporter:
    """Captures 'before' / 'after' / 'delta' metrics around any write op.

    Schema-only this round; integration with archive/retention is Phase 3.
    Duplicate cleaner is the first user.
    """

    def __init__(self, db, config: Optional[dict] = None):
        self.db = db
        self.config = config or {}

    # ──────────────────────────────────────────────
    # Snapshots
    # ──────────────────────────────────────────────

    def _snapshot(self, scope: dict) -> dict:
        """Compute storage-level metrics for ``scope``.

        Returns a dict with the following keys (all numeric, never None):
          * total_files            int   scanned_files row count
          * total_size_bytes       int   sum(file_size)
          * total_size_gb          float helper for UI display
          * duplicate_groups       int   COUNT of (file_name,file_size) groups
                                          with > 1 row
          * duplicate_files        int   sum of group counts
          * duplicate_waste_bytes  int   sum((cnt-1)*file_size)
          * duplicate_waste_gb     float helper for UI display
          * captured_at            str   timestamp string

        ``scope`` may include:
          * source_id (int)        scopes the metrics to one source
          * scan_id   (int)        scopes to a specific scan
          * (when both omitted, the snapshot is global)
        """
        source_id = (scope or {}).get("source_id")
        scan_id = (scope or {}).get("scan_id")

        # Resolve scan_id from source_id if needed — gives the latest
        # *completed* scan; we explicitly do not use a running scan
        # because mid-scan numbers move under our feet.
        if source_id and not scan_id:
            try:
                scan_id = self.db.get_latest_scan_id(
                    int(source_id), include_running=False
                )
            except Exception as e:
                logger.warning(
                    "gain_reporter: get_latest_scan_id(%s) failed: %s",
                    source_id, e,
                )
                scan_id = None

        snap = {
            "total_files": 0,
            "total_size_bytes": 0,
            "total_size_gb": 0.0,
            "duplicate_groups": 0,
            "duplicate_files": 0,
            "duplicate_waste_bytes": 0,
            "duplicate_waste_gb": 0.0,
            "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if source_id is not None:
            snap["source_id"] = int(source_id)
        if scan_id is not None:
            snap["scan_id"] = int(scan_id)

        if not scan_id:
            # No completed scan = empty / unknown corpus. Return the stub
            # so the UI can still render the panel.
            return snap

        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS c, COALESCE(SUM(file_size), 0) AS s "
                    "FROM scanned_files WHERE scan_id = ?",
                    (int(scan_id),),
                )
                row = cur.fetchone()
                snap["total_files"] = int(row["c"] or 0)
                snap["total_size_bytes"] = int(row["s"] or 0)
                snap["total_size_gb"] = _bytes_to_gb(snap["total_size_bytes"])

                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS g,
                        COALESCE(SUM(cnt), 0) AS f,
                        COALESCE(SUM((cnt - 1) * file_size), 0) AS w
                    FROM (
                        SELECT file_name, file_size, COUNT(*) AS cnt
                        FROM scanned_files
                        WHERE scan_id = ? AND file_size > 0
                        GROUP BY file_name, file_size
                        HAVING COUNT(*) > 1
                    )
                    """,
                    (int(scan_id),),
                )
                d = cur.fetchone()
                snap["duplicate_groups"] = int(d["g"] or 0)
                snap["duplicate_files"] = int(d["f"] or 0)
                snap["duplicate_waste_bytes"] = int(d["w"] or 0)
                snap["duplicate_waste_gb"] = _bytes_to_gb(
                    snap["duplicate_waste_bytes"]
                )
        except Exception as e:
            # Metric capture must never raise — logging is enough so the
            # caller's primary write op can proceed (it has its own
            # error path). The persisted snapshot will be the stub.
            logger.warning(
                "gain_reporter: snapshot failed for scope=%s: %s",
                scope, e,
            )

        return snap

    def capture_before(self, scope: dict) -> dict:
        """Snapshot before the operation runs."""
        snap = self._snapshot(scope)
        snap["phase"] = "before"
        return snap

    def capture_after(self, scope: dict) -> dict:
        """Snapshot after the operation completes."""
        snap = self._snapshot(scope)
        snap["phase"] = "after"
        return snap

    # ──────────────────────────────────────────────
    # Delta + persistence
    # ──────────────────────────────────────────────

    @staticmethod
    def compute_delta(before: dict, after: dict) -> dict:
        """before - after for every numeric key the two share.

        Positive numbers mean "we shed N units" (good for waste/size),
        which is the operator-friendly framing used in the UI.
        Non-numeric or asymmetric keys are ignored. Always returns a
        dict — never raises.
        """
        delta: dict = {}
        before = before or {}
        after = after or {}
        for k, b_val in before.items():
            if k not in after:
                continue
            a_val = after[k]
            if isinstance(b_val, (int, float)) and isinstance(a_val, (int, float)) \
                    and not isinstance(b_val, bool) and not isinstance(a_val, bool):
                d = b_val - a_val
                # Round float deltas to 4 places to match capture precision.
                if isinstance(d, float):
                    d = round(d, 4)
                delta[k] = d
        return delta

    def save(self, operation: str, before: dict, after: dict,
             delta: Optional[dict] = None,
             scan_id: Optional[int] = None,
             source_id: Optional[int] = None,
             audit_event_id: Optional[int] = None,
             quarantine_path: Optional[str] = None) -> int:
        """Persist a row to ``gain_reports``. Returns the row id.

        ``operation`` is a short machine-readable label
        (e.g. ``duplicate_quarantine``). ``before`` / ``after`` are the
        full snapshot dicts; ``delta`` is computed automatically when
        omitted via :meth:`compute_delta`.

        ``scan_id`` / ``source_id`` are denormalised onto the row for
        cheap operation-history filtering — pass them through when
        known. ``audit_event_id`` and ``quarantine_path`` link the row
        back to the per-file audit event and quarantine destination so
        the operator can drill from the gain report into the forensic
        evidence in one click.

        Wrapped in a small retry loop (mirrors
        :meth:`Database.bulk_insert_scanned_files` from #176) so a
        transient ``database is locked`` mid-scan does not abort the
        entire op. Five attempts, exponential backoff (1s, 2s, 4s, 8s,
        16s) — same envelope as the bulk inserter.

        Raises on DB error. Callers should NOT swallow.
        """
        if not operation or not str(operation).strip():
            raise ValueError("operation is required")
        if delta is None:
            delta = self.compute_delta(before or {}, after or {})

        before_json = json.dumps(before or {}, sort_keys=True, default=str)
        after_json = json.dumps(after or {}, sort_keys=True, default=str)
        delta_json = json.dumps(delta or {}, sort_keys=True, default=str)
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Late import — sqlite3 is the only error type we discriminate
        # on, and we don't want to pull it in at module import time.
        import sqlite3 as _sqlite3
        import time as _time

        sql = (
            "INSERT INTO gain_reports "
            "(operation, source_id, scan_id, completed_at, before_json, "
            "after_json, delta_json, audit_event_id, quarantine_path) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            str(operation),
            int(source_id) if source_id is not None else None,
            int(scan_id) if scan_id is not None else None,
            completed_at,
            before_json,
            after_json,
            delta_json,
            int(audit_event_id) if audit_event_id is not None else None,
            str(quarantine_path) if quarantine_path else None,
        )

        backoff = 1.0
        last_err: Optional[Exception] = None
        for attempt in range(1, 6):
            try:
                with self.db.get_cursor() as cur:
                    cur.execute(sql, params)
                    return int(cur.lastrowid)
            except _sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "database is locked" not in msg and "busy" not in msg:
                    raise
                last_err = e
                if attempt < 5:
                    logger.warning(
                        "gain_reports save locked (attempt %d/5), "
                        "retry in %.1fs: %s", attempt, backoff, e,
                    )
                    _time.sleep(backoff)
                    backoff *= 2
        logger.error(
            "gain_reports save exhausted retries (5x): %s", last_err,
        )
        if last_err is not None:
            raise last_err
        # Unreachable — for static checkers.
        raise RuntimeError("gain_reports save failed without error")

    # ──────────────────────────────────────────────
    # Queries (used by /api/operations/* endpoints)
    # ──────────────────────────────────────────────

    def list_reports(self, limit: int = 50,
                     operation: Optional[str] = None,
                     source_id: Optional[int] = None,
                     page: int = 1) -> list[dict]:
        """Recent gain_reports rows, newest first.

        ``operation`` filters on the operation label (exact match).
        ``source_id`` filters by the source the operation was scoped to.
        ``page`` is 1-based (page=1 returns the first ``limit`` rows).
        ``limit`` is clamped to [1, 500] so a buggy caller cannot
        exhaust the API. JSON columns are pre-decoded so frontends
        don't need to double-parse.

        Read-side query — uses :meth:`Database.get_read_cursor` so we
        share the read pool from #181 Track A.
        """
        limit = max(1, min(int(limit or 50), 500))
        try:
            page = max(1, int(page or 1))
        except (TypeError, ValueError):
            page = 1
        offset = (page - 1) * limit
        sql = "SELECT * FROM gain_reports"
        params: list = []
        where: list[str] = []
        if operation:
            where.append("operation = ?")
            params.append(str(operation))
        if source_id is not None:
            where.append("source_id = ?")
            params.append(int(source_id))
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY started_at DESC, id DESC LIMIT ? OFFSET ?"
        params.append(limit)
        params.append(int(offset))
        cursor_ctx = (
            self.db.get_read_cursor()
            if hasattr(self.db, "get_read_cursor")
            else self.db.get_cursor()
        )
        with cursor_ctx as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            row["before"] = self._safe_loads(row.pop("before_json", "{}"))
            row["after"] = self._safe_loads(row.pop("after_json", "{}"))
            row["delta"] = self._safe_loads(row.pop("delta_json", "{}"))
        return rows

    def count_reports(self, operation: Optional[str] = None,
                      source_id: Optional[int] = None) -> int:
        """Total row count matching the same filters as
        :meth:`list_reports`. Used by the UI pager.
        """
        sql = "SELECT COUNT(*) AS c FROM gain_reports"
        params: list = []
        where: list[str] = []
        if operation:
            where.append("operation = ?")
            params.append(str(operation))
        if source_id is not None:
            where.append("source_id = ?")
            params.append(int(source_id))
        if where:
            sql += " WHERE " + " AND ".join(where)
        cursor_ctx = (
            self.db.get_read_cursor()
            if hasattr(self.db, "get_read_cursor")
            else self.db.get_cursor()
        )
        with cursor_ctx as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["c"] if row and "c" in row.keys() else (row[0] if row else 0))

    def get_report(self, report_id: int) -> Optional[dict]:
        """Single row by id; None when missing."""
        cursor_ctx = (
            self.db.get_read_cursor()
            if hasattr(self.db, "get_read_cursor")
            else self.db.get_cursor()
        )
        with cursor_ctx as cur:
            cur.execute(
                "SELECT * FROM gain_reports WHERE id = ?", (int(report_id),)
            )
            row = cur.fetchone()
        if row is None:
            return None
        row = dict(row)
        row["before"] = self._safe_loads(row.pop("before_json", "{}"))
        row["after"] = self._safe_loads(row.pop("after_json", "{}"))
        row["delta"] = self._safe_loads(row.pop("delta_json", "{}"))
        return row

    @staticmethod
    def _safe_loads(blob) -> dict:
        if not blob:
            return {}
        if isinstance(blob, (dict, list)):
            return blob
        try:
            return json.loads(blob)
        except Exception:
            return {"_raw": str(blob)}
