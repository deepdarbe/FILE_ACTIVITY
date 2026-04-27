"""Duplicate cleaner — quarantine-only delete (issue #83 Phase 1).

SAFETY-CRITICAL. This module ships the destructive-side of the duplicate
report. We intentionally limit Phase 1 to **quarantine** (move to a
holding directory under ``data/quarantine/<YYYYMMDD>/<hash>/``) and never
``os.remove`` any source file. Phase 2 will add the auto-purge job.

Hard rules (every request must satisfy ALL of them):

1. Caller passes ``confirm=True`` AND ``safety_token == "QUARANTINE"``.
   Either missing → ``ValueError``. ``require_safety_token`` config
   flag must remain true in production.
2. Each candidate file is checked against
   :meth:`LegalHoldRegistry.is_held`. Held files are skipped, audited,
   counted in ``skipped_held`` — never moved.
3. Each candidate's duplicate-group membership is checked. The LAST
   remaining member of any duplicate group is refused — moving it
   would mean we lose the only copy. Counted in ``skipped_last_copy``.
4. Per-request file count cannot exceed
   ``duplicates.quarantine.bulk_delete_max_files`` (default 500).
5. Every move is recorded as a ``file_audit_events`` row (chained when
   available) AND a ``quarantine_log`` row pointing back to the
   ``gain_reports`` row id. The audit trail is the only way to recover
   if something goes wrong.

The quarantine directory layout is ``<root>/<YYYYMMDD>/<sha1(orig)>/``.
We hash the original parent path so two files with the same name from
different folders don't collide. A SHA-256 sidecar (``<file>.sha256``)
is written next to each quarantined file for forensic verification.

NEVER ``os.remove`` here — only ``shutil.move``. ``shutil.move`` falls
back to copy+remove when source / dest are on different filesystems,
which on Windows means a temp copy + DeleteFile. That's fine for the
unit-test fixture (``tmp_path``) but operators must keep the
quarantine root on the SAME volume as the source share in production
(see config docstring).

Stdlib only. ``from __future__ import annotations`` for forward refs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("file_activity.archiver.duplicate_cleaner")


SAFETY_TOKEN_VALUE = "QUARANTINE"
PURGE_SAFETY_TOKEN_VALUE = "PURGE"


# ──────────────────────────────────────────────
# Result dataclasses
# ──────────────────────────────────────────────


@dataclass
class PreviewResult:
    """Dry-run result. ``files`` carries one entry per requested id."""

    would_move: int = 0
    skipped_held: int = 0
    skipped_last_copy: int = 0
    skipped_missing: int = 0
    total_size_bytes: int = 0
    total_size_freed_gb: float = 0.0
    errors: list[dict] = field(default_factory=list)
    files: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class QuarantineResult:
    """Full move result, including before/after snapshots and audit ids."""

    moved: int = 0
    skipped_held: int = 0
    skipped_last_copy: int = 0
    skipped_missing: int = 0
    total_size_bytes: int = 0
    total_size_freed_gb: float = 0.0
    errors: list[dict] = field(default_factory=list)
    files: list[dict] = field(default_factory=list)
    before: dict = field(default_factory=dict)
    after: dict = field(default_factory=dict)
    delta: dict = field(default_factory=dict)
    gain_report_id: Optional[int] = None
    confirm: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


# ──────────────────────────────────────────────
# Phase 2 (issue #110) — hard delete + restore
# ──────────────────────────────────────────────


@dataclass
class PurgeResult:
    """Per-file outcome for hard-delete (``purge_one``).

    ``status`` is one of:
      * ``"purged"``        — file removed AND ``quarantine_log.purged_at`` set
      * ``"skipped_missing"`` — file already gone from disk; row stamped
      * ``"skipped_already_purged"`` — row already has ``purged_at``
      * ``"skipped_restored"`` — row was restored, must not be purged
      * ``"skipped_not_found"`` — no quarantine_log row with that id
      * ``"abort_sha_mismatch"`` — SHA-256 differs from sidecar; FORENSIC,
        NO DELETE — operator review required
      * ``"error"``         — anything else (filesystem, db); details in
        ``reason``
    """

    quarantine_log_id: Optional[int] = None
    status: str = "error"
    reason: Optional[str] = None
    quarantine_path: Optional[str] = None
    original_path: Optional[str] = None
    sha256_expected: Optional[str] = None
    sha256_actual: Optional[str] = None
    audit_event_id: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RestoreResult:
    """Per-file outcome for restore from quarantine.

    ``status`` is one of:
      * ``"restored"``           — file moved back to ``original_path``
      * ``"skipped_collision"``  — original_path already has a file
      * ``"skipped_already_restored"`` — row.restored_at already set
      * ``"skipped_already_purged"``   — row.purged_at already set
      * ``"skipped_not_found"``  — no quarantine_log row with that id
      * ``"skipped_missing"``    — quarantine_path already gone
      * ``"error"``              — anything else; details in ``reason``
    """

    quarantine_log_id: Optional[int] = None
    status: str = "error"
    reason: Optional[str] = None
    quarantine_path: Optional[str] = None
    original_path: Optional[str] = None
    audit_event_id: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────


def _hash_parent(original_path: str) -> str:
    """Stable short hash of the *parent* directory of the original path.

    Using the parent (not the full path) means two files with the same
    name from the same folder still go into the same hashed bucket —
    the filename itself preserves uniqueness within that bucket. Falls
    back to hashing the whole path when ``os.path.dirname`` is empty.
    """
    parent = os.path.dirname(original_path or "") or original_path or ""
    return hashlib.sha1(parent.encode("utf-8", errors="replace")).hexdigest()[:16]


def _sha256_file(path: str, chunk_size: int = 1024 * 1024) -> Optional[str]:
    """Streamed SHA-256 of a file. Returns None on read failure."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.warning("sha256 read failed for %s: %s", path, e)
        return None


# ──────────────────────────────────────────────
# Cleaner
# ──────────────────────────────────────────────


class DuplicateCleaner:
    """Quarantine-only delete for duplicate files (#83 Phase 1)."""

    def __init__(self, db, config: Optional[dict] = None):
        self.db = db
        self.config = config or {}
        cfg = (self.config.get("duplicates") or {}).get("quarantine") or {}
        self.enabled = bool(cfg.get("enabled", True))
        self.quarantine_root = Path(cfg.get("dir") or "data/quarantine")
        self.bulk_max = int(cfg.get("bulk_delete_max_files") or 500)
        self.require_token = bool(cfg.get("require_safety_token", True))
        # Phase 2 retention horizon — files older than this are eligible
        # for hard delete by the daily purge job. Operators can extend
        # in config.yaml; we never go below 1 (sanity floor) or accept
        # nonsense (negatives, strings).
        try:
            self.quarantine_days = max(1, int(cfg.get("quarantine_days") or 30))
        except (TypeError, ValueError):
            self.quarantine_days = 30
        try:
            self.purge_hour = int(cfg.get("purge_hour", 3))
        except (TypeError, ValueError):
            self.purge_hour = 3
        if self.purge_hour < 0 or self.purge_hour > 23:
            self.purge_hour = 3
        # Idempotent — no error when the dir already exists.
        try:
            self.quarantine_root.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            # Don't crash construction; the move call will surface it.
            logger.warning(
                "quarantine root mkdir failed (%s): %s",
                self.quarantine_root, e,
            )
        # Lazy-built once per process; ``is_held`` re-queries DB on
        # every call so newly-added holds are picked up mid-process.
        self._hold_registry = None

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def preview(self, file_ids: list[int]) -> PreviewResult:
        """Dry-run. NEVER moves anything. ``would_move`` is the count
        of files that *would* be moved if :meth:`quarantine` were called
        with the same ids and a valid confirm/token.

        Held files contribute to ``skipped_held``. Last-copy refusals
        contribute to ``skipped_last_copy``. Missing-on-disk contribute
        to ``skipped_missing`` and an entry in ``errors``.
        """
        result = PreviewResult()
        ids = self._validate_ids(file_ids, raise_on_cap=False)
        if not ids:
            return result

        # Load all candidate file rows in one query.
        rows = self._fetch_files(ids)
        # Build a per-(name,size) lookup of remaining members for the
        # last-copy check. We scope by scan_id-per-file because
        # duplicate groups are scan-bound. Cache to avoid N queries.
        group_index = self._build_group_remaining_index(rows)

        registry = self._registry()

        # Determine which ids would actually be moved (subset of ``rows``).
        for row in rows:
            entry = {
                "id": row["id"],
                "file_path": row["file_path"],
                "file_size": row.get("file_size", 0) or 0,
                "outcome": "would_move",
                "reason": None,
            }

            # Held files are skipped first — even if missing on disk.
            held = self._safe_is_held(registry, row["file_path"])
            if held:
                entry["outcome"] = "skipped_held"
                entry["reason"] = (
                    f"Hold #{held['id']}: {held.get('reason', '')}"
                )
                result.skipped_held += 1
                result.files.append(entry)
                continue

            # Last-copy guard: if removing this file would leave 0
            # remaining duplicates in the (name,size) group, refuse.
            key = (row["scan_id"], row["file_name"], row["file_size"])
            remaining = group_index.get(key)
            if remaining is not None and remaining <= 1:
                entry["outcome"] = "skipped_last_copy"
                entry["reason"] = (
                    "Son kopya — silinirse veri kaybi olur"
                )
                result.skipped_last_copy += 1
                result.files.append(entry)
                continue

            # Source file missing on disk = surface but don't blow up.
            if not os.path.exists(row["file_path"]):
                entry["outcome"] = "skipped_missing"
                entry["reason"] = "Kaynak dosya disk uzerinde bulunamadi"
                result.skipped_missing += 1
                result.errors.append({
                    "id": row["id"], "file_path": row["file_path"],
                    "error": "missing",
                })
                result.files.append(entry)
                continue

            # Decrement the in-memory remaining counter so the *next*
            # file in the same group sees the correct future state.
            if remaining is not None:
                group_index[key] = remaining - 1

            result.would_move += 1
            result.total_size_bytes += int(entry["file_size"])
            result.files.append(entry)

        # ID-level errors that didn't turn into a row (unknown ids).
        unknown = set(ids) - {r["id"] for r in rows}
        for uid in unknown:
            result.errors.append(
                {"id": uid, "error": "unknown_or_other_source"}
            )

        # Cap notice — preview tolerates over-cap, but quarantine() refuses.
        if len(ids) > self.bulk_max:
            result.errors.append({
                "error": (
                    f"requested {len(ids)} > cap {self.bulk_max}; "
                    "quarantine call will refuse"
                ),
            })

        result.total_size_freed_gb = round(
            result.total_size_bytes / (1024 * 1024 * 1024), 4
        )
        return result

    def quarantine(self, file_ids: list[int], confirm: bool,
                   safety_token: str,
                   moved_by: str = "system",
                   source_id: Optional[int] = None) -> QuarantineResult:
        """Move files to quarantine. The destructive entry point.

        Args:
            file_ids: scanned_files row ids to move.
            confirm: must be ``True``. ``False`` raises.
            safety_token: must equal ``"QUARANTINE"`` when
                ``duplicates.quarantine.require_safety_token`` is true.
            moved_by: free-text actor (audit trail).
            source_id: optional, used to scope the gain-report scope.

        Returns:
            :class:`QuarantineResult` with per-file outcomes,
            before/after snapshots, delta, and ``gain_report_id``.

        Raises:
            ValueError: when confirm/token gates fail or count > cap.
            RuntimeError: when the global kill-switch is off.
        """
        # Global kill-switch — disable in case of emergency.
        if not self.enabled:
            raise RuntimeError(
                "duplicate quarantine is disabled "
                "(duplicates.quarantine.enabled=false)"
            )

        if not confirm:
            raise ValueError(
                "confirm=True required to perform quarantine"
            )
        if self.require_token and safety_token != SAFETY_TOKEN_VALUE:
            raise ValueError(
                "safety_token must equal "
                f"{SAFETY_TOKEN_VALUE!r} (got {safety_token!r})"
            )

        ids = self._validate_ids(file_ids, raise_on_cap=True)
        result = QuarantineResult(confirm=True)

        # Capture BEFORE snapshot via the gain reporter. We import here
        # to avoid a hard cycle at module import time.
        from src.storage.gain_reporter import GainReporter
        reporter = GainReporter(self.db, self.config)
        scope = {"source_id": source_id} if source_id else {}
        result.before = reporter.capture_before(scope)
        scan_id_for_report = result.before.get("scan_id")

        rows = self._fetch_files(ids)
        group_index = self._build_group_remaining_index(rows)
        registry = self._registry()

        # Today's date bucket — used for every file in this batch so
        # the operator can quickly find "all files quarantined on D".
        date_bucket = datetime.now().strftime("%Y%m%d")

        # Per-file move loop. We deliberately do NOT wrap this in a
        # single SQL transaction: the audit + quarantine_log writes are
        # independent per file, and partial failure must remain
        # auditable. The gain_report row is written ONCE at the end and
        # captures the post-batch state — so a half-completed batch is
        # still recoverable from the audit trail + quarantine_log rows.
        for row in rows:
            outcome = self._move_one(
                row, registry, group_index, date_bucket,
                moved_by=moved_by,
            )
            result.files.append(outcome)

            kind = outcome["outcome"]
            if kind == "moved":
                result.moved += 1
                result.total_size_bytes += int(row.get("file_size") or 0)
            elif kind == "skipped_held":
                result.skipped_held += 1
            elif kind == "skipped_last_copy":
                result.skipped_last_copy += 1
            elif kind == "skipped_missing":
                result.skipped_missing += 1
                result.errors.append({
                    "id": row["id"], "file_path": row["file_path"],
                    "error": "missing",
                })
            elif kind == "error":
                result.errors.append({
                    "id": row["id"], "file_path": row["file_path"],
                    "error": outcome.get("reason") or "unknown",
                })

        # Unknown ids
        unknown = set(ids) - {r["id"] for r in rows}
        for uid in unknown:
            result.errors.append(
                {"id": uid, "error": "unknown_or_other_source"}
            )

        result.total_size_freed_gb = round(
            result.total_size_bytes / (1024 * 1024 * 1024), 4
        )

        # Capture AFTER snapshot + persist gain report. We pass through
        # ``scan_id_for_report`` so the row is filterable by scan.
        result.after = reporter.capture_after(scope)
        result.delta = reporter.compute_delta(result.before, result.after)
        try:
            result.gain_report_id = reporter.save(
                operation="duplicate_quarantine",
                before=result.before,
                after=result.after,
                delta=result.delta,
                scan_id=scan_id_for_report,
            )
        except Exception as e:
            # If the gain report itself fails we still want operators
            # to see the moved files — log loudly but do not raise.
            logger.error(
                "gain_reports save failed for duplicate_quarantine: %s", e
            )

        # Backfill quarantine_log rows with the gain_report_id now that
        # we have it. Anchored by audit_event_id we wrote per-file.
        if result.gain_report_id is not None:
            self._link_quarantine_to_report(
                [f for f in result.files if f.get("outcome") == "moved"],
                result.gain_report_id,
            )

        return result

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    def _validate_ids(self, file_ids, raise_on_cap: bool) -> list[int]:
        if not file_ids:
            return []
        if not isinstance(file_ids, (list, tuple, set)):
            raise ValueError("file_ids must be a list of integers")
        ids: list[int] = []
        for v in file_ids:
            try:
                iv = int(v)
            except Exception:
                raise ValueError(f"file_ids must be integers, got {v!r}")
            if iv > 0:
                ids.append(iv)
        # De-dup but preserve order (first wins).
        seen: set = set()
        deduped: list[int] = []
        for v in ids:
            if v in seen:
                continue
            seen.add(v)
            deduped.append(v)
        if raise_on_cap and len(deduped) > self.bulk_max:
            raise ValueError(
                f"file_ids count {len(deduped)} exceeds cap {self.bulk_max}"
            )
        return deduped

    def _registry(self):
        """Build the legal-hold registry on first use, cache it."""
        if self._hold_registry is not None:
            return self._hold_registry
        try:
            from src.compliance.legal_hold import LegalHoldRegistry
            self._hold_registry = LegalHoldRegistry(self.db, self.config)
        except Exception as e:
            logger.warning("LegalHoldRegistry init failed: %s", e)
            self._hold_registry = None
        return self._hold_registry

    @staticmethod
    def _safe_is_held(registry, file_path: str):
        if registry is None:
            return None
        try:
            return registry.is_held(file_path)
        except Exception as e:
            logger.warning("legal_hold check failed for %s: %s",
                           file_path, e)
            return None

    def _fetch_files(self, ids: list[int]) -> list[dict]:
        """Load scanned_files rows for the given ids in a single query."""
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        sql = (
            f"SELECT id, source_id, scan_id, file_path, relative_path, "
            f"file_name, file_size FROM scanned_files "
            f"WHERE id IN ({placeholders})"
        )
        with self.db.get_cursor() as cur:
            cur.execute(sql, list(ids))
            return [dict(r) for r in cur.fetchall()]

    def _build_group_remaining_index(self, rows: list[dict]) -> dict:
        """For every (scan_id, file_name, file_size) touched by ``rows``,
        compute the number of *currently-existing* duplicate members.

        Last-copy detection uses this counter: when remaining <= 1 the
        candidate is the only copy left and we refuse to move it.
        """
        index: dict = {}
        if not rows:
            return index
        # Group keys we need.
        keys = {(r["scan_id"], r["file_name"], r["file_size"]) for r in rows}
        with self.db.get_cursor() as cur:
            for scan_id, name, size in keys:
                cur.execute(
                    "SELECT COUNT(*) AS c FROM scanned_files "
                    "WHERE scan_id = ? AND file_name = ? AND file_size = ?",
                    (scan_id, name, size),
                )
                row = cur.fetchone()
                index[(scan_id, name, size)] = int(row["c"] or 0)
        return index

    def _quarantine_target(self, original_path: str, date_bucket: str,
                           file_name: str) -> Path:
        """Compute the destination path under the quarantine root."""
        bucket = self.quarantine_root / date_bucket / _hash_parent(original_path)
        bucket.mkdir(parents=True, exist_ok=True)
        # Collision-safe within the same bucket.
        target = bucket / file_name
        if target.exists():
            stem, ext = os.path.splitext(file_name)
            suffix = datetime.now().strftime("%H%M%S%f")
            target = bucket / f"{stem}.{suffix}{ext}"
        return target

    def _move_one(self, row: dict, registry, group_index: dict,
                  date_bucket: str, moved_by: str) -> dict:
        """Move a single file into quarantine.

        Returns a per-file outcome dict (subset of QuarantineResult.files).
        """
        outcome = {
            "id": row["id"],
            "file_path": row["file_path"],
            "file_size": int(row.get("file_size") or 0),
            "outcome": "error",
            "reason": None,
            "quarantine_path": None,
            "sha256": None,
            "audit_event_id": None,
        }

        original_path = row["file_path"]

        # Legal-hold gate.
        held = self._safe_is_held(registry, original_path)
        if held:
            outcome["outcome"] = "skipped_held"
            outcome["reason"] = (
                f"Hold #{held['id']}: {held.get('reason', '')}"
            )
            self._audit(
                event_type="duplicate_quarantine_skipped_legal_hold",
                source_id=row.get("source_id"),
                username=moved_by,
                file_path=original_path,
                details=outcome["reason"],
            )
            return outcome

        # Last-copy gate. Decrement the remaining count when we accept
        # so subsequent siblings see the *future* state.
        key = (row["scan_id"], row["file_name"], row["file_size"])
        remaining = group_index.get(key)
        if remaining is not None and remaining <= 1:
            outcome["outcome"] = "skipped_last_copy"
            outcome["reason"] = "Son kopya — silinirse veri kaybi olur"
            self._audit(
                event_type="duplicate_quarantine_skipped_last_copy",
                source_id=row.get("source_id"),
                username=moved_by,
                file_path=original_path,
                details=outcome["reason"],
            )
            return outcome

        # Disk-presence gate.
        if not os.path.exists(original_path):
            outcome["outcome"] = "skipped_missing"
            outcome["reason"] = "Kaynak dosya disk uzerinde bulunamadi"
            return outcome

        # Compute SHA-256 BEFORE move so a forensic compare is possible
        # even after the move corrupts mtime.
        sha = _sha256_file(original_path)
        outcome["sha256"] = sha

        try:
            target = self._quarantine_target(
                original_path, date_bucket, row["file_name"]
            )
            # NEVER os.remove — only shutil.move.
            shutil.move(original_path, str(target))
            outcome["quarantine_path"] = str(target)
            outcome["outcome"] = "moved"

            # Forensic sidecars: SHA-256 file + JSON manifest entry.
            self._write_sidecars(target, original_path, sha, row, moved_by)

            # Decrement the in-memory remaining count.
            if remaining is not None:
                group_index[key] = remaining - 1

            # Audit trail (chained when enabled).
            evt_id = self._audit(
                event_type="duplicate_quarantine_moved",
                source_id=row.get("source_id"),
                username=moved_by,
                file_path=original_path,
                details=(
                    f"Quarantined to {target} sha256={sha or '-'} "
                    f"size={outcome['file_size']}"
                ),
            )
            outcome["audit_event_id"] = evt_id

            # Persist quarantine_log row (gain_report_id filled later).
            try:
                with self.db.get_cursor() as cur:
                    cur.execute(
                        "INSERT INTO quarantine_log "
                        "(file_id, original_path, quarantine_path, sha256, "
                        "file_size, moved_by, gain_report_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, NULL)",
                        (row["id"], original_path, str(target), sha,
                         int(outcome["file_size"]), moved_by),
                    )
                    outcome["quarantine_log_id"] = cur.lastrowid
            except Exception as e:
                logger.error(
                    "quarantine_log insert failed for %s: %s",
                    original_path, e,
                )
        except Exception as e:
            outcome["outcome"] = "error"
            outcome["reason"] = str(e)
            logger.error(
                "quarantine move failed for %s: %s", original_path, e
            )
        return outcome

    def _write_sidecars(self, target: Path, original_path: str,
                        sha: Optional[str], row: dict, moved_by: str) -> None:
        """Drop a ``.sha256`` and a ``.manifest.json`` next to the file.

        Sidecar failures are logged + tolerated — the move itself is
        already committed and audited.
        """
        try:
            if sha:
                with open(str(target) + ".sha256", "w", encoding="utf-8") as f:
                    f.write(f"{sha}  {target.name}\n")
        except Exception as e:
            logger.warning("sha256 sidecar write failed: %s", e)

        try:
            manifest = {
                "schema": "file_activity.quarantine.v1",
                "moved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "moved_by": moved_by,
                "original_path": original_path,
                "original_size": int(row.get("file_size") or 0),
                "sha256": sha,
                "file_id": row["id"],
                "scan_id": row.get("scan_id"),
                "source_id": row.get("source_id"),
            }
            with open(str(target) + ".manifest.json", "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, default=str)
        except Exception as e:
            logger.warning("manifest sidecar write failed: %s", e)

    def _audit(self, event_type: str, source_id, username: str,
               file_path: str, details: str) -> Optional[int]:
        """Emit an audit event. Prefers the chained variant when present."""
        try:
            if hasattr(self.db, "insert_audit_event_chained"):
                return self.db.insert_audit_event_chained({
                    "source_id": source_id,
                    "event_type": event_type,
                    "username": username,
                    "file_path": file_path,
                    "details": details,
                    "detected_by": "duplicate_cleaner",
                })
        except Exception as e:
            logger.warning("chained audit failed (%s): %s", event_type, e)

        try:
            if hasattr(self.db, "insert_audit_event_simple"):
                return self.db.insert_audit_event_simple(
                    source_id=source_id,
                    event_type=event_type,
                    username=username,
                    file_path=file_path,
                    details=details,
                    detected_by="duplicate_cleaner",
                )
        except Exception as e:
            logger.warning("audit_simple fallback failed (%s): %s",
                           event_type, e)
        return None

    # ──────────────────────────────────────────────
    # Phase 2 (issue #110) — hard delete + restore
    # ──────────────────────────────────────────────

    def purge_one(self, quarantine_log_id: int,
                  purged_by: str = "system") -> PurgeResult:
        """Hard-delete a single quarantined file.

        SAFETY-CRITICAL contract:
          1. Read the ``quarantine_log`` row. If missing → ``skipped_not_found``.
          2. If row already has ``purged_at`` → ``skipped_already_purged``.
          3. If row has ``restored_at`` → ``skipped_restored`` (don't purge
             a file the operator pulled back).
          4. If quarantine_path missing → stamp ``purged_at`` and return
             ``skipped_missing`` (so the row reflects reality, doesn't get
             retried daily forever).
          5. **VERIFY SHA-256** of the on-disk file against the sidecar.
             Any mismatch → ``abort_sha_mismatch`` and we DO NOT call
             ``os.remove``. The row stays untouched so the operator can
             investigate. This is the forensic-preserve rule: corruption
             mid-quarantine = preserve, never silent delete.
          6. ``os.remove`` the file + sidecars. Stamp ``purged_at``.
             Write audit event.

        Per-file errors never raise — they return a ``PurgeResult`` with
        ``status='error'`` so batch callers can keep going.
        """
        result = PurgeResult(quarantine_log_id=int(quarantine_log_id))
        try:
            row = self._fetch_quarantine_row(int(quarantine_log_id))
        except Exception as e:
            result.status = "error"
            result.reason = f"db read failed: {e}"
            return result
        if row is None:
            result.status = "skipped_not_found"
            result.reason = "quarantine_log id not found"
            return result

        result.quarantine_path = row.get("quarantine_path")
        result.original_path = row.get("original_path")
        result.sha256_expected = row.get("sha256")

        if row.get("purged_at"):
            result.status = "skipped_already_purged"
            result.reason = "row already has purged_at"
            return result
        if row.get("restored_at"):
            result.status = "skipped_restored"
            result.reason = "row was restored — refusing to purge"
            return result

        qpath = row.get("quarantine_path") or ""

        # Pre-existing physical absence: stamp the row so we don't keep
        # retrying — but log it as an audit event for forensics.
        if not qpath or not os.path.exists(qpath):
            self._stamp_purged(int(quarantine_log_id))
            result.audit_event_id = self._audit(
                event_type="duplicate_quarantine_purge_skipped_missing",
                source_id=None,
                username=purged_by,
                file_path=qpath or row.get("original_path") or "",
                details=(
                    f"Quarantined file already missing on disk; row "
                    f"#{quarantine_log_id} stamped purged_at without delete."
                ),
            )
            result.status = "skipped_missing"
            result.reason = "quarantine_path missing on disk"
            return result

        # Defensive SHA-256 verify against sidecar / row.
        actual = _sha256_file(qpath)
        result.sha256_actual = actual
        expected = self._read_sidecar_sha(qpath) or row.get("sha256")
        if expected and actual and expected != actual:
            # FORENSIC: never delete a corrupted-or-tampered file. Audit
            # loudly so the SOC can investigate.
            result.status = "abort_sha_mismatch"
            result.reason = (
                f"sha256 mismatch: expected={expected} actual={actual}"
            )
            result.audit_event_id = self._audit(
                event_type="duplicate_quarantine_purge_sha_mismatch",
                source_id=None,
                username=purged_by,
                file_path=qpath,
                details=(
                    f"Refused hard delete of quarantine_log #"
                    f"{quarantine_log_id}: sha256 mismatch "
                    f"(expected={expected} actual={actual}). "
                    f"File preserved for forensic review."
                ),
            )
            logger.warning(
                "purge_one aborted (sha mismatch) for log #%s path=%s",
                quarantine_log_id, qpath,
            )
            return result

        # If we have neither expected nor actual, that's a degraded case:
        # we still proceed (operators may have wiped sidecars), but we
        # record a softer audit reason.
        if not expected:
            logger.info(
                "purge_one: no sidecar/row sha for log #%s — proceeding",
                quarantine_log_id,
            )

        # Hard delete: remove the file + sidecars (best-effort).
        try:
            os.remove(qpath)
        except FileNotFoundError:
            # Race with manual rm — treat as missing.
            self._stamp_purged(int(quarantine_log_id))
            result.audit_event_id = self._audit(
                event_type="duplicate_quarantine_purge_skipped_missing",
                source_id=None,
                username=purged_by,
                file_path=qpath,
                details=(
                    f"Race: file disappeared between sha verify and "
                    f"remove for row #{quarantine_log_id}."
                ),
            )
            result.status = "skipped_missing"
            result.reason = "file disappeared mid-purge"
            return result
        except Exception as e:
            result.status = "error"
            result.reason = f"os.remove failed: {e}"
            logger.error("purge_one os.remove failed for %s: %s", qpath, e)
            return result

        # Best-effort sidecar cleanup (failures only logged).
        for suffix in (".sha256", ".manifest.json"):
            sidecar = qpath + suffix
            try:
                if os.path.exists(sidecar):
                    os.remove(sidecar)
            except Exception as e:
                logger.warning("sidecar remove failed for %s: %s", sidecar, e)

        # Stamp row + audit.
        self._stamp_purged(int(quarantine_log_id))
        result.audit_event_id = self._audit(
            event_type="duplicate_quarantine_purged",
            source_id=None,
            username=purged_by,
            file_path=qpath,
            details=(
                f"Hard-deleted quarantine_log #{quarantine_log_id} "
                f"sha256={actual or '-'} original_path="
                f"{row.get('original_path')}"
            ),
        )
        result.status = "purged"
        return result

    def purge_expired(self, now: Optional[datetime] = None,
                      purged_by: str = "system") -> list[PurgeResult]:
        """Find every quarantine_log row older than ``quarantine_days``
        and call :meth:`purge_one` on each.

        Returns a list of :class:`PurgeResult`. Per-file errors never
        abort the batch — every row gets a result entry so the scheduler
        run-log can show a comprehensive summary.
        """
        if now is None:
            now = datetime.now()
        cutoff = now - timedelta(days=self.quarantine_days)
        results: list[PurgeResult] = []
        try:
            rows = self._fetch_purge_candidates(cutoff)
        except Exception as e:
            logger.error("purge_expired candidate query failed: %s", e)
            return results

        for r in rows:
            try:
                results.append(self.purge_one(
                    int(r["id"]), purged_by=purged_by,
                ))
            except Exception as e:
                # Defence in depth — purge_one is supposed to never raise,
                # but if a defect slips through we still record a per-row
                # error and keep going.
                logger.error(
                    "purge_one raised unexpectedly for log #%s: %s",
                    r.get("id"), e,
                )
                results.append(PurgeResult(
                    quarantine_log_id=int(r["id"]),
                    status="error",
                    reason=f"purge_one raised: {e}",
                    quarantine_path=r.get("quarantine_path"),
                    original_path=r.get("original_path"),
                ))
        return results

    def restore(self, quarantine_log_id: int,
                restored_by: str = "system") -> RestoreResult:
        """Move a quarantined file back to ``original_path``.

        Refuses if:
          * row not found → ``skipped_not_found``
          * row already restored or purged
          * ``original_path`` already exists on disk (collision)
          * quarantine file missing on disk

        On success: ``shutil.move(quarantine_path, original_path)``,
        stamps ``restored_at``, writes audit event, and best-effort
        cleans up the orphan sidecars.
        """
        result = RestoreResult(quarantine_log_id=int(quarantine_log_id))
        try:
            row = self._fetch_quarantine_row(int(quarantine_log_id))
        except Exception as e:
            result.status = "error"
            result.reason = f"db read failed: {e}"
            return result
        if row is None:
            result.status = "skipped_not_found"
            result.reason = "quarantine_log id not found"
            return result

        result.quarantine_path = row.get("quarantine_path")
        result.original_path = row.get("original_path")

        if row.get("purged_at"):
            result.status = "skipped_already_purged"
            result.reason = "row already purged — file no longer exists"
            return result
        if row.get("restored_at"):
            result.status = "skipped_already_restored"
            result.reason = "row already restored"
            return result

        qpath = row.get("quarantine_path") or ""
        opath = row.get("original_path") or ""

        if not qpath or not os.path.exists(qpath):
            result.status = "skipped_missing"
            result.reason = "quarantine_path missing on disk"
            return result
        if not opath:
            result.status = "error"
            result.reason = "original_path is empty"
            return result
        if os.path.exists(opath):
            result.status = "skipped_collision"
            result.reason = (
                f"original_path already exists ({opath}) — refusing to "
                f"overwrite"
            )
            result.audit_event_id = self._audit(
                event_type="duplicate_quarantine_restore_collision",
                source_id=None,
                username=restored_by,
                file_path=opath,
                details=(
                    f"Refused restore of #{quarantine_log_id}: "
                    f"original_path already exists."
                ),
            )
            return result

        # Make sure the parent of original_path exists.
        try:
            parent = os.path.dirname(opath)
            if parent and not os.path.isdir(parent):
                os.makedirs(parent, exist_ok=True)
        except Exception as e:
            result.status = "error"
            result.reason = f"mkdir parent failed: {e}"
            return result

        try:
            shutil.move(qpath, opath)
        except Exception as e:
            result.status = "error"
            result.reason = f"shutil.move failed: {e}"
            logger.error("restore move failed for %s: %s", qpath, e)
            return result

        # Best-effort sidecar cleanup (orphans now).
        for suffix in (".sha256", ".manifest.json"):
            sidecar = qpath + suffix
            try:
                if os.path.exists(sidecar):
                    os.remove(sidecar)
            except Exception as e:
                logger.warning(
                    "restore sidecar remove failed for %s: %s", sidecar, e
                )

        self._stamp_restored(int(quarantine_log_id))
        result.audit_event_id = self._audit(
            event_type="duplicate_quarantine_restored",
            source_id=None,
            username=restored_by,
            file_path=opath,
            details=(
                f"Restored quarantine_log #{quarantine_log_id} from "
                f"{qpath} to {opath}"
            ),
        )
        result.status = "restored"
        return result

    # ──────────────────────────────────────────────
    # Phase 2 internal helpers
    # ──────────────────────────────────────────────

    def _fetch_quarantine_row(self, qlog_id: int) -> Optional[dict]:
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT id, file_id, original_path, quarantine_path, "
                "sha256, file_size, moved_at, moved_by, gain_report_id, "
                "purged_at, restored_at "
                "FROM quarantine_log WHERE id = ?",
                (int(qlog_id),),
            )
            r = cur.fetchone()
            return dict(r) if r else None

    def _fetch_purge_candidates(self, cutoff: datetime) -> list[dict]:
        """Rows with moved_at < cutoff and not yet purged/restored."""
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT id, original_path, quarantine_path, moved_at "
                "FROM quarantine_log "
                "WHERE moved_at < ? "
                "  AND purged_at IS NULL "
                "  AND restored_at IS NULL "
                "ORDER BY moved_at ASC",
                (cutoff_str,),
            )
            return [dict(r) for r in cur.fetchall()]

    def _stamp_purged(self, qlog_id: int) -> None:
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    "UPDATE quarantine_log SET purged_at = "
                    "CURRENT_TIMESTAMP WHERE id = ? AND purged_at IS NULL",
                    (int(qlog_id),),
                )
        except Exception as e:
            logger.error("purged_at stamp failed for #%s: %s", qlog_id, e)

    def _stamp_restored(self, qlog_id: int) -> None:
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    "UPDATE quarantine_log SET restored_at = "
                    "CURRENT_TIMESTAMP WHERE id = ? AND restored_at IS NULL",
                    (int(qlog_id),),
                )
        except Exception as e:
            logger.error("restored_at stamp failed for #%s: %s", qlog_id, e)

    @staticmethod
    def _read_sidecar_sha(qpath: str) -> Optional[str]:
        """Read the ``<qpath>.sha256`` sidecar and return the hex digest.

        The sidecar format is ``<digest>  <filename>\\n`` (sha256sum-style).
        Tolerates trailing whitespace, missing file, malformed content;
        returns ``None`` on any failure.
        """
        sidecar = qpath + ".sha256"
        try:
            with open(sidecar, "r", encoding="utf-8") as f:
                line = f.readline().strip()
            if not line:
                return None
            token = line.split()[0]
            # Hex digests are 64 chars for sha256.
            if len(token) >= 32 and all(
                c in "0123456789abcdefABCDEF" for c in token
            ):
                return token.lower()
            return None
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.warning("sidecar read failed for %s: %s", sidecar, e)
            return None

    def _link_quarantine_to_report(self, moved_files: list[dict],
                                    report_id: int) -> None:
        """UPDATE quarantine_log rows to reference the gain_report_id.

        Identifies rows by the auto-incremented log ids we captured
        on insert. Failure is logged and tolerated.
        """
        log_ids = [
            f.get("quarantine_log_id") for f in moved_files
            if f.get("quarantine_log_id") is not None
        ]
        if not log_ids:
            return
        placeholders = ",".join("?" for _ in log_ids)
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    f"UPDATE quarantine_log SET gain_report_id = ? "
                    f"WHERE id IN ({placeholders})",
                    [int(report_id), *log_ids],
                )
        except Exception as e:
            logger.warning(
                "quarantine_log gain_report_id backfill failed: %s", e
            )
