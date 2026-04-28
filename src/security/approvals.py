"""Two-person approval framework — Phase 1 (issue #112).

High-impact admin operations (snapshot restore, bulk archive/purge,
retention apply) can be gated behind a two-person rule so a single
compromised or careless account can't wipe a database. The framework
itself is generic: each callsite pushes a payload onto the
``pending_approvals`` queue, a *different* operator approves it, and a
final ``execute`` step runs the operation by invoking a callable
registered for the operation type.

Phase 1 wires only ``snapshot_restore`` as a proof of concept. Other
operations follow in subsequent PRs.

Design notes
------------

* **Default OFF.** ``approvals.enabled=false`` makes ``is_required``
  return False unconditionally — every existing endpoint behaves
  exactly as before. Backwards compat is critical.
* **Self-approval refused server-side.** ``approve()`` raises
  ``SelfApprovalError`` when the approving user matches
  ``requested_by``. The frontend must also disable the button, but the
  rule lives on the server.
* **Idempotent transitions.** ``approve``/``reject``/``execute`` reject
  rows that aren't in the expected source state.
* **Audit trail.** Every transition writes a ``file_audit_events`` row
  via ``insert_audit_event_chained`` when available (issue #38), with
  ``insert_audit_event`` / ``insert_audit_event_simple`` fallbacks. A
  failure to audit is logged but doesn't break the operation.
* **Expiry.** ``expires_at`` defaults to now + ``expiry_hours``. The
  scheduler runs ``expire_stale`` hourly to flip stale rows to
  ``expired``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

logger = logging.getLogger("file_activity.security.approvals")


# ── Exceptions ─────────────────────────────────────────────


class ApprovalError(Exception):
    """Base class for approval framework errors."""


class ApprovalNotFound(ApprovalError):
    """No row with the given id."""


class SelfApprovalError(ApprovalError):
    """Refused: the approving user is the same as the requester."""


class InvalidStateError(ApprovalError):
    """Row not in the expected source state for this transition."""


class ApprovalExpiredError(ApprovalError):
    """Row expired before the requested transition could run."""


# ── DTO ────────────────────────────────────────────────────


@dataclass
class ApprovalRequest:
    """Snapshot of a ``pending_approvals`` row.

    ``payload`` is the parsed JSON dict; the raw column is
    ``payload_json``. Time fields are ISO strings as returned by SQLite
    so the dataclass is JSON-serialisable for API responses.
    """

    id: int
    operation_type: str
    payload: dict
    requested_by: str
    requested_at: str
    expires_at: str
    status: str
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    rejected_by: Optional[str] = None
    rejected_at: Optional[str] = None
    rejection_reason: Optional[str] = None
    executed_at: Optional[str] = None
    executed_result: Optional[dict] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


def _row_to_request(row: dict) -> ApprovalRequest:
    payload = {}
    raw = row.get("payload_json")
    if raw:
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {"_raw": raw}
    result = None
    raw_r = row.get("executed_result_json")
    if raw_r:
        try:
            result = json.loads(raw_r)
        except Exception:
            result = {"_raw": raw_r}
    return ApprovalRequest(
        id=int(row["id"]),
        operation_type=row["operation_type"],
        payload=payload,
        requested_by=row["requested_by"],
        requested_at=str(row.get("requested_at") or ""),
        expires_at=str(row.get("expires_at") or ""),
        status=row["status"],
        approved_by=row.get("approved_by"),
        approved_at=str(row["approved_at"]) if row.get("approved_at") else None,
        rejected_by=row.get("rejected_by"),
        rejected_at=str(row["rejected_at"]) if row.get("rejected_at") else None,
        rejection_reason=row.get("rejection_reason"),
        executed_at=str(row["executed_at"]) if row.get("executed_at") else None,
        executed_result=result,
    )


# ── Registry ───────────────────────────────────────────────


class ApprovalRegistry:
    """SQLite-backed pending approval queue.

    Cheap to construct; stores no state outside the DB. Safe to share
    across threads — every method acquires its own cursor.
    """

    def __init__(self, db, config: Any):
        self.db = db
        self.config = config or {}
        cfg = (self.config.get("approvals") or {}) if isinstance(self.config, dict) else {}
        self.enabled = bool(cfg.get("enabled", False))
        # Issue #158 (H-2) — refuse the structurally-unsafe combo of
        # ``approvals.enabled=true`` with ``identity_source='client_supplied'``.
        # In that combo any caller can claim the requester's username on the
        # second leg of the two-person rule, defeating the gate. We raise
        # at construction time so the dashboard refuses to boot rather than
        # silently shipping a bypass.
        identity_source = (cfg.get("identity_source") or "client_supplied").strip().lower()
        if self.enabled and identity_source == "client_supplied":
            raise RuntimeError(
                "approvals.enabled=true is incompatible with "
                "identity_source='client_supplied' - this combination allows "
                "trivial self-approval bypass. Set identity_source to "
                "'windows' or 'header' before enabling approvals."
            )
        try:
            self.expiry_hours = int(cfg.get("expiry_hours", 24))
        except (TypeError, ValueError):
            self.expiry_hours = 24
        if self.expiry_hours <= 0:
            self.expiry_hours = 24
        raw_list = cfg.get("require_for") or []
        self.require_for: list[str] = [
            str(x) for x in raw_list if isinstance(x, str)
        ]

    # ── Audit helper ───────────────────────────────────────

    def _audit(self, event_type: str, username: str, details: dict) -> None:
        """Write an audit event. Never raises — audit is best-effort."""
        try:
            payload = json.dumps(details, default=str, sort_keys=True)
        except Exception:
            payload = str(details)
        try:
            if hasattr(self.db, "insert_audit_event_chained"):
                self.db.insert_audit_event_chained({
                    "source_id": None,
                    "event_type": event_type,
                    "username": username,
                    "file_path": f"approval:{details.get('approval_id', '?')}",
                    "details": payload,
                    "detected_by": "approvals",
                })
                return
        except Exception as e:
            logger.warning("approvals chained audit failed: %s", e)
        try:
            if hasattr(self.db, "insert_audit_event"):
                self.db.insert_audit_event(
                    source_id=None,
                    event_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    event_type=event_type,
                    username=username,
                    file_path=f"approval:{details.get('approval_id', '?')}",
                    file_name=None,
                    details=payload,
                    detected_by="approvals",
                )
                return
        except Exception as e:
            logger.warning("approvals audit fallback failed: %s", e)
        try:
            if hasattr(self.db, "insert_audit_event_simple"):
                self.db.insert_audit_event_simple(
                    source_id=None,
                    event_type=event_type,
                    username=username,
                    file_path=f"approval:{details.get('approval_id', '?')}",
                    details=payload,
                    detected_by="approvals",
                )
        except Exception as e:
            logger.warning("approvals audit_simple fallback failed: %s", e)

    # ── Public API ─────────────────────────────────────────

    def is_required(self, operation_type: str) -> bool:
        """True if approvals are enabled AND the op is in
        ``require_for``."""
        if not self.enabled:
            return False
        return operation_type in self.require_for

    def request(
        self,
        operation_type: str,
        payload: dict,
        requested_by: str,
    ) -> ApprovalRequest:
        """Insert a pending row, return the populated DTO."""
        if not isinstance(operation_type, str) or not operation_type.strip():
            raise ApprovalError("operation_type required")
        if not isinstance(requested_by, str) or not requested_by.strip():
            requested_by = "unknown"
        payload_json = json.dumps(payload or {}, default=str, sort_keys=True)
        expires_at = (datetime.now() + timedelta(hours=self.expiry_hours)) \
            .strftime("%Y-%m-%d %H:%M:%S")

        with self.db.get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_approvals
                  (operation_type, payload_json, requested_by, expires_at, status)
                VALUES (?, ?, ?, ?, 'pending')
                """,
                (operation_type, payload_json, requested_by, expires_at),
            )
            new_id = cur.lastrowid
            cur.execute(
                "SELECT * FROM pending_approvals WHERE id = ?", (new_id,)
            )
            row = cur.fetchone()

        req = _row_to_request(dict(row))
        self._audit(
            "approval_requested",
            requested_by,
            {
                "approval_id": req.id,
                "operation_type": req.operation_type,
                "expires_at": req.expires_at,
            },
        )
        logger.info(
            "approval requested id=%s op=%s by=%s expires=%s",
            req.id, req.operation_type, req.requested_by, req.expires_at,
        )
        return req

    def get(self, approval_id: int) -> ApprovalRequest:
        """Fetch a single approval row or raise ``ApprovalNotFound``."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM pending_approvals WHERE id = ?",
                (int(approval_id),),
            )
            row = cur.fetchone()
        if row is None:
            raise ApprovalNotFound(f"approval id {approval_id} not found")
        return _row_to_request(dict(row))

    def approve(
        self,
        approval_id: int,
        approved_by: str,
    ) -> ApprovalRequest:
        """Mark the row approved. Refuses self-approval (B == A)."""
        if not isinstance(approved_by, str) or not approved_by.strip():
            approved_by = "unknown"
        req = self.get(approval_id)
        if req.status != "pending":
            raise InvalidStateError(
                f"cannot approve: status={req.status!r} (need 'pending')"
            )
        if self._is_expired(req.expires_at):
            # Auto-flip to expired and refuse the approve.
            self._mark_expired(approval_id)
            raise ApprovalExpiredError(
                f"approval {approval_id} expired at {req.expires_at}"
            )
        if approved_by == req.requested_by:
            raise SelfApprovalError(
                "self-approval refused: approver must differ from requester"
            )
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                UPDATE pending_approvals
                SET status='approved', approved_by=?, approved_at=?
                WHERE id=? AND status='pending'
                """,
                (approved_by, now, approval_id),
            )
            if cur.rowcount == 0:
                raise InvalidStateError(
                    "approval row changed mid-flight (lost race)"
                )
        self._audit(
            "approval_approved",
            approved_by,
            {
                "approval_id": approval_id,
                "operation_type": req.operation_type,
                "requested_by": req.requested_by,
            },
        )
        logger.info(
            "approval approved id=%s by=%s (requested_by=%s)",
            approval_id, approved_by, req.requested_by,
        )
        return self.get(approval_id)

    def reject(
        self,
        approval_id: int,
        rejected_by: str,
        reason: str,
    ) -> ApprovalRequest:
        """Mark the row rejected. Anyone (including the requester) can
        reject — useful for the requester to cancel their own request."""
        if not isinstance(rejected_by, str) or not rejected_by.strip():
            rejected_by = "unknown"
        if not isinstance(reason, str):
            reason = ""
        reason = reason.strip()
        req = self.get(approval_id)
        if req.status != "pending":
            raise InvalidStateError(
                f"cannot reject: status={req.status!r} (need 'pending')"
            )
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                UPDATE pending_approvals
                SET status='rejected', rejected_by=?, rejected_at=?,
                    rejection_reason=?
                WHERE id=? AND status='pending'
                """,
                (rejected_by, now, reason, approval_id),
            )
            if cur.rowcount == 0:
                raise InvalidStateError(
                    "approval row changed mid-flight (lost race)"
                )
        self._audit(
            "approval_rejected",
            rejected_by,
            {
                "approval_id": approval_id,
                "operation_type": req.operation_type,
                "requested_by": req.requested_by,
                "reason": reason,
            },
        )
        logger.info(
            "approval rejected id=%s by=%s reason=%r",
            approval_id, rejected_by, reason,
        )
        return self.get(approval_id)

    def execute(
        self,
        approval_id: int,
        executor: Callable[[dict], dict],
    ) -> dict:
        """Run ``executor(payload)`` on an approved row. Records the
        result, transitions to ``executed``. Re-raises whatever the
        executor raises *after* recording the failure in audit log.
        """
        req = self.get(approval_id)
        if req.status != "approved":
            raise InvalidStateError(
                f"cannot execute: status={req.status!r} (need 'approved')"
            )

        # We don't gate execute on expiry because operators may
        # legitimately approve an op late in the window and execute past
        # it — the approve step has already happened, expiry was for the
        # approval gate, not the execution gate.

        try:
            result = executor(dict(req.payload))
        except Exception as e:
            logger.exception(
                "approval execute failed id=%s op=%s err=%s",
                approval_id, req.operation_type, e,
            )
            self._audit(
                "approval_execute_failed",
                req.approved_by or "unknown",
                {
                    "approval_id": approval_id,
                    "operation_type": req.operation_type,
                    "error": str(e),
                },
            )
            raise
        if not isinstance(result, dict):
            result = {"result": result}
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                UPDATE pending_approvals
                SET status='executed', executed_at=?, executed_result_json=?
                WHERE id=? AND status='approved'
                """,
                (now, json.dumps(result, default=str, sort_keys=True),
                 approval_id),
            )
            if cur.rowcount == 0:
                raise InvalidStateError(
                    "approval row changed mid-flight (lost race)"
                )
        self._audit(
            "approval_executed",
            req.approved_by or "unknown",
            {
                "approval_id": approval_id,
                "operation_type": req.operation_type,
                "requested_by": req.requested_by,
            },
        )
        logger.info(
            "approval executed id=%s op=%s",
            approval_id, req.operation_type,
        )
        return result

    def list_pending(self) -> list[ApprovalRequest]:
        """All rows still in ``pending`` (not yet expired by clock time
        — the scheduler flips those to ``expired`` separately)."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM pending_approvals "
                "WHERE status='pending' ORDER BY requested_at DESC, id DESC"
            )
            rows = cur.fetchall()
        return [_row_to_request(dict(r)) for r in rows]

    def list_history(self, limit: int = 50) -> list[ApprovalRequest]:
        """All approvals (any status), most recent first."""
        try:
            limit = max(1, min(int(limit), 1000))
        except (TypeError, ValueError):
            limit = 50
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM pending_approvals "
                "ORDER BY requested_at DESC, id DESC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
        return [_row_to_request(dict(r)) for r in rows]

    def expire_stale(self) -> int:
        """Flip any pending rows whose ``expires_at`` is in the past to
        ``expired``. Returns the number of rows flipped. Intended to be
        called from an APScheduler hourly job."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT id, operation_type, requested_by FROM pending_approvals "
                "WHERE status='pending' AND expires_at < ?",
                (now,),
            )
            stale = cur.fetchall()
            if not stale:
                return 0
            cur.execute(
                "UPDATE pending_approvals SET status='expired' "
                "WHERE status='pending' AND expires_at < ?",
                (now,),
            )
            n = cur.rowcount
        for row in stale:
            self._audit(
                "approval_expired",
                "system",
                {
                    "approval_id": row["id"],
                    "operation_type": row["operation_type"],
                    "requested_by": row["requested_by"],
                },
            )
        if n:
            logger.info("expired %s stale approval rows", n)
        return n

    # ── Internals ─────────────────────────────────────────

    @staticmethod
    def _is_expired(expires_at: str) -> bool:
        if not expires_at:
            return False
        try:
            ts = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                ts = datetime.fromisoformat(expires_at)
            except ValueError:
                return False
        return ts < datetime.now()

    def _mark_expired(self, approval_id: int) -> None:
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    "UPDATE pending_approvals SET status='expired' "
                    "WHERE id=? AND status='pending'",
                    (approval_id,),
                )
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("mark_expired failed for %s: %s", approval_id, e)
        self._audit(
            "approval_expired",
            "system",
            {"approval_id": approval_id},
        )
