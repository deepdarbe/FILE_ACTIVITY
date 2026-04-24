"""GDPR retention engine (issue #58).

Operator-defined "files matching <pattern> older than <N> days ->
archive | delete" rules persisted in ``retention_policies``. Each
:meth:`RetentionEngine.apply` call walks ``scanned_files`` for matches,
optionally hands them to an archiver, and writes one
``retention_archive`` / ``retention_delete`` row per processed file
into ``file_audit_events``. The audit-event trail is what the
:meth:`attestation_report` reads back so an external auditor can prove
the engine actually did what the policy declared.

``apply`` defaults to ``dry_run=True`` — the operator must opt in to
mutation. We use :func:`fnmatch.fnmatch` so the pattern_match column
behaves like a familiar shell glob (``*.log``,
``\\\\share\\projects\\closed\\*``).

External dependencies: stdlib ``fnmatch`` only. ``archive_engine`` is
duck-typed — anything exposing ``archive_files(files, archive_dest,
operation_id, source_id)`` works.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("file_activity.compliance.retention")

VALID_ACTIONS = ("archive", "delete")


class RetentionEngine:
    """CRUD + apply + attestation for retention_policies rows."""

    def __init__(self, db, config: dict, archive_engine=None):
        self.db = db
        self.archive_engine = archive_engine
        # Issue #77: keep the full config so we can drive the backup
        # snapshot hook from ``apply()``.
        self._full_config = config or {}
        cfg = ((config or {}).get("compliance", {}) or {}).get("retention", {}) or {}
        self.enabled = bool(cfg.get("enabled", False))
        # Default destination if a policy doesn't carry its own (rare).
        self.default_archive_dest = cfg.get("default_archive_dest")

    # ──────────────────────────────────────────────
    # CRUD
    # ──────────────────────────────────────────────

    def add_policy(self, name: str, pattern_match: str,
                   retain_days: int, action: str) -> int:
        """Insert a new policy row. Returns the inserted ID.

        Raises ``ValueError`` for invalid action or non-positive
        retain_days, ``sqlite3.IntegrityError`` if the name is taken.
        """
        if action not in VALID_ACTIONS:
            raise ValueError(
                f"action must be one of {VALID_ACTIONS!r}, got {action!r}"
            )
        retain_days = int(retain_days)
        if retain_days <= 0:
            raise ValueError("retain_days must be positive")
        with self.db.get_cursor() as cur:
            cur.execute(
                """INSERT INTO retention_policies
                   (name, pattern_match, retain_days, action, enabled)
                   VALUES (?, ?, ?, ?, 1)""",
                (str(name), str(pattern_match or ""),
                 retain_days, str(action)),
            )
            return int(cur.lastrowid)

    def list_policies(self) -> list[dict]:
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT id, name, pattern_match, retain_days, action, "
                "enabled, created_at "
                "FROM retention_policies ORDER BY name ASC"
            )
            return [dict(r) for r in cur.fetchall()]

    def remove_policy(self, name: str) -> bool:
        """Delete by name. Returns True if a row was actually removed."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "DELETE FROM retention_policies WHERE name=?",
                (str(name),),
            )
            return cur.rowcount > 0

    def _get_policy(self, name: str) -> Optional[dict]:
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM retention_policies WHERE name=?",
                (str(name),),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ──────────────────────────────────────────────
    # Apply
    # ──────────────────────────────────────────────

    def _matches_pattern(self, file_path: str, pattern: str) -> bool:
        """``fnmatch.fnmatch`` on the full file_path. Empty pattern
        matches every row (operator can use that for blanket retention).
        """
        if not pattern:
            return True
        return fnmatch.fnmatch(file_path, pattern)

    def _candidate_files(self, pattern: str, retain_days: int) -> list[dict]:
        """Find scanned_files rows whose last_modify_time is older than
        cutoff and whose path matches the pattern.

        We deduplicate by file_path (latest scan wins) so re-scans
        don't re-process the same file twice.
        """
        cutoff = datetime.now() - timedelta(days=int(retain_days))
        cutoff_iso = cutoff.strftime("%Y-%m-%d %H:%M:%S")

        with self.db.get_cursor() as cur:
            # MAX(last_modify_time) per file_path so the most recent
            # mtime governs the retain decision. Pre-filter on cutoff
            # at the SQL layer to keep memory bounded.
            cur.execute(
                """
                SELECT file_path,
                       MAX(last_modify_time) AS last_modify_time,
                       MAX(file_size)        AS file_size,
                       MAX(source_id)        AS source_id,
                       MAX(scan_id)          AS scan_id,
                       MAX(owner)            AS owner
                FROM scanned_files
                WHERE last_modify_time IS NOT NULL
                  AND last_modify_time < ?
                GROUP BY file_path
                """,
                (cutoff_iso,),
            )
            rows = [dict(r) for r in cur.fetchall()]

        out: list[dict] = []
        for r in rows:
            if self._matches_pattern(r["file_path"], pattern):
                out.append(r)
        return out

    def _write_audit(self, event_type: str, file_path: str,
                     source_id: Optional[int], policy_name: str,
                     action: str, dry_run: bool) -> None:
        """Append one ``retention_archive`` or ``retention_delete`` row
        to ``file_audit_events`` so the attestation report can find it.

        Best-effort: failures are logged but never raised — we don't
        want a disabled audit table to block real retention work.
        """
        details = json.dumps({
            "policy": policy_name,
            "action": action,
            "dry_run": bool(dry_run),
        })
        event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    """INSERT INTO file_audit_events
                       (source_id, event_time, event_type, username,
                        file_path, file_name, details, detected_by)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (source_id, event_time, event_type, "retention_engine",
                     file_path, os.path.basename(file_path),
                     details, "retention"),
                )
        except sqlite3.Error as e:  # pragma: no cover - defensive
            logger.warning("retention audit write failed for %s: %s",
                           file_path, e)

    def apply(self, policy_name: str, dry_run: bool = True) -> dict:
        """Apply a policy. Returns ``{policy, matched, processed,
        errors, dry_run, elapsed_seconds}``.

        ``matched`` = number of files that matched pattern + age.
        ``processed`` = number successfully archived/deleted (==
        matched in dry-run).
        """
        started = time.time()
        policy = self._get_policy(policy_name)
        if policy is None:
            raise ValueError(f"policy not found: {policy_name}")
        if not policy.get("enabled", 1):
            return {
                "policy": policy_name,
                "matched": 0,
                "processed": 0,
                "errors": ["policy_disabled"],
                "dry_run": bool(dry_run),
                "elapsed_seconds": 0.0,
            }

        action = policy["action"]
        pattern = policy["pattern_match"] or ""
        retain_days = int(policy["retain_days"])

        candidates = self._candidate_files(pattern, retain_days)
        matched = len(candidates)
        processed = 0
        errors: list[str] = []
        event_type = (
            "retention_archive" if action == "archive" else "retention_delete"
        )

        # Dry-run: just count + audit "would do".
        if dry_run:
            for c in candidates:
                self._write_audit(
                    event_type, c["file_path"], c.get("source_id"),
                    policy_name, action, dry_run=True,
                )
                processed += 1
            elapsed = time.time() - started
            logger.info(
                "retention apply (dry_run): policy=%s matched=%d",
                policy_name, matched,
            )
            return {
                "policy": policy_name,
                "matched": matched,
                "processed": processed,
                "errors": errors,
                "dry_run": True,
                "elapsed_seconds": round(elapsed, 3),
            }

        # Issue #77: pre-apply SQLite snapshot (real run only). Failure
        # logs ERROR but never aborts the retention sweep — we never
        # want backup-system trouble to block GDPR-driven purges.
        self._maybe_pre_apply_snapshot(reason=f"pre-retention:{policy_name}")

        # Real run.
        if action == "archive":
            if self.archive_engine is None:
                raise RuntimeError(
                    "archive_engine not wired — retention apply with "
                    "action='archive' requires an ArchiveEngine "
                    "instance at construction time"
                )
            # ArchiveEngine.archive_files expects a list of file dicts
            # with at least file_path; we group by source_id since
            # archive_dest is stored on the source row.
            try:
                self.archive_engine.archive_files(
                    candidates,
                    self.default_archive_dest or "",
                    operation_id=None,
                    source_id=(
                        candidates[0].get("source_id") if candidates else None
                    ),
                )
                for c in candidates:
                    self._write_audit(
                        event_type, c["file_path"], c.get("source_id"),
                        policy_name, action, dry_run=False,
                    )
                processed = matched
            except Exception as e:
                logger.exception("retention archive failed for policy=%s",
                                 policy_name)
                errors.append(str(e))
        elif action == "delete":
            for c in candidates:
                fp = c["file_path"]
                try:
                    if os.path.exists(fp):
                        os.remove(fp)
                    self._write_audit(
                        event_type, fp, c.get("source_id"),
                        policy_name, action, dry_run=False,
                    )
                    processed += 1
                except OSError as e:
                    logger.warning("retention delete failed for %s: %s",
                                   fp, e)
                    errors.append(f"{fp}: {e}")

        elapsed = time.time() - started
        logger.info(
            "retention apply: policy=%s matched=%d processed=%d errors=%d",
            policy_name, matched, processed, len(errors),
        )
        return {
            "policy": policy_name,
            "matched": matched,
            "processed": processed,
            "errors": errors,
            "dry_run": False,
            "elapsed_seconds": round(elapsed, 3),
        }

    # ──────────────────────────────────────────────
    # Issue #77: pre-apply SQLite backup hook
    # ──────────────────────────────────────────────

    def _maybe_pre_apply_snapshot(self, reason: str) -> None:
        """Take a SQLite snapshot before mutating retention runs.

        Honours ``config.backup.snapshot_on_apply`` (default True). All
        failures are caught + logged so retention proceeds on best
        effort.
        """
        backup_cfg = (self._full_config or {}).get("backup") or {}
        if not backup_cfg.get("snapshot_on_apply", True):
            return
        if not backup_cfg.get("enabled", True):
            return
        try:
            db_path = (
                (self._full_config or {}).get("database", {}).get("path")
                or "data/file_activity.db"
            )
            from src.storage.backup_manager import BackupManager
            BackupManager(db_path, self._full_config).snapshot(reason=reason)
        except Exception as e:
            logger.error(
                "pre-retention snapshot failed (%s) — continuing: %s",
                reason, e, exc_info=True,
            )

    # ──────────────────────────────────────────────
    # Attestation
    # ──────────────────────────────────────────────

    def attestation_report(self, since_days: int = 30) -> dict:
        """Aggregate past purges from ``file_audit_events`` for an
        external compliance auditor.

        Returns::

            {
              "since_days": int,
              "generated_at": iso8601,
              "totals": {"archive": int, "delete": int},
              "by_policy": [
                {"policy": str, "action": str, "count": int,
                 "first_event": iso8601, "last_event": iso8601},
                ...
              ],
              "events": [...up to 1000 individual events...],
            }
        """
        since = max(1, int(since_days or 30))
        cutoff_iso = (
            datetime.now() - timedelta(days=since)
        ).strftime("%Y-%m-%d %H:%M:%S")

        totals = {"archive": 0, "delete": 0}
        by_policy: dict[tuple, dict] = {}
        events: list[dict] = []

        with self.db.get_cursor() as cur:
            cur.execute(
                """SELECT id, event_time, event_type, file_path, details
                   FROM file_audit_events
                   WHERE event_type IN ('retention_archive', 'retention_delete')
                     AND event_time >= ?
                   ORDER BY event_time ASC
                   LIMIT 5000""",
                (cutoff_iso,),
            )
            rows = [dict(r) for r in cur.fetchall()]

        for r in rows:
            try:
                meta = json.loads(r.get("details") or "{}")
            except (ValueError, TypeError):
                meta = {}
            policy = meta.get("policy") or "unknown"
            action = meta.get("action") or (
                "archive" if r["event_type"] == "retention_archive" else "delete"
            )
            if action in totals:
                totals[action] += 1
            key = (policy, action)
            agg = by_policy.setdefault(key, {
                "policy": policy,
                "action": action,
                "count": 0,
                "first_event": r["event_time"],
                "last_event": r["event_time"],
            })
            agg["count"] += 1
            agg["last_event"] = r["event_time"]
            if len(events) < 1000:
                events.append({
                    "id": r["id"],
                    "event_time": r["event_time"],
                    "event_type": r["event_type"],
                    "file_path": r["file_path"],
                    "policy": policy,
                    "dry_run": bool(meta.get("dry_run")),
                })

        return {
            "since_days": since,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "totals": totals,
            "by_policy": sorted(
                by_policy.values(),
                key=lambda x: (-x["count"], x["policy"]),
            ),
            "events": events,
        }
