"""Orphaned-SID report and bulk reassignment (issue #56).

Walks the most recent ``scanned_files`` rows for a source/scan, groups
them by their owner identifier (SID or DOMAIN\\Name string), then asks
``ADLookup`` whether each owner still resolves. Owners that don't
resolve are surfaced as "orphan SIDs" — files that nobody owns anymore
because the principal has been deleted from Active Directory (typical
for departed staff, decommissioned service accounts, etc.).

The ``orphan_sid_cache`` table memoises the AD lookup result so a
re-run of ``detect_orphans`` doesn't re-query AD for SIDs we just
checked. The TTL is configurable
(``security.orphan_sid.cache_ttl_minutes``, default 1440 = 24h).

Bulk reassignment uses ``win32security.SetNamedSecurityInfo`` with
``OWNER_SECURITY_INFORMATION`` and is Windows-only. Everything else
(``detect_orphans``, ``get_orphan_files``, ``export_csv``) is DB-only
and works fine on Linux + CI. ``pywin32`` is lazily imported so the
module is import-safe on every platform.

Defaults are intentionally conservative: ``reassign_owner(dry_run=True)``
is the default, per-file failures are logged at DEBUG and never abort
the batch, and an opt-in ``require_dual_approval_for_reassign`` config
flag is honoured by the ``/reassign`` endpoint.
"""

from __future__ import annotations

import csv
import logging
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("file_activity.security.orphan_sid")


_DEFAULT_CACHE_TTL_MIN = 1440  # 24 hours
_DEFAULT_MAX_UNIQUE_SIDS = 1000
_SAMPLE_PATHS_PER_SID = 5


class OrphanSidAnalyzer:
    """Detect file-owner SIDs that no longer resolve via AD, support
    bulk reassignment to a manager / archive owner.

    Cross-platform safe — pywin32 lazily imported. Reassignment
    methods raise NotImplementedError on non-Windows.
    """

    def __init__(self, db, config: dict, ad_lookup=None):
        self.db = db
        self.ad_lookup = ad_lookup
        cfg = ((config or {}).get("security", {}) or {}).get("orphan_sid", {}) or {}
        self.enabled = bool(cfg.get("enabled", True))
        self.cache_ttl_minutes = int(cfg.get("cache_ttl_minutes", _DEFAULT_CACHE_TTL_MIN))
        self.max_unique_sids_default = int(cfg.get("max_unique_sids", _DEFAULT_MAX_UNIQUE_SIDS))
        self.require_dual_approval_for_reassign = bool(
            cfg.get("require_dual_approval_for_reassign", False)
        )

    # ──────────────────────────────────────────────
    # Capability probe
    # ──────────────────────────────────────────────

    def is_supported(self) -> bool:
        """True on Windows with pywin32 importable. False otherwise.

        ``detect_orphans``/``get_orphan_files``/``export_csv`` work on
        Linux too (DB-only). Only ``reassign_owner`` requires Windows.
        """
        if sys.platform != "win32":
            return False
        try:
            import win32security  # noqa: F401
            return True
        except Exception:  # pragma: no cover - import probe only
            return False

    # ──────────────────────────────────────────────
    # Detection
    # ──────────────────────────────────────────────

    def detect_orphans(self, scan_id: int,
                       max_unique_sids: int = _DEFAULT_MAX_UNIQUE_SIDS) -> dict:
        """Walk ``scanned_files`` for the scan, group by owner SID,
        check each via ``ad_lookup`` (cached in ``orphan_sid_cache``).

        Returns::

            {scan_id, total_files, total_orphan_files, orphan_sids: [
                {sid, file_count, total_size, sample_paths: [up to 5]}
            ], elapsed_seconds}

        Caching: ``orphan_sid_cache.resolved=0`` means the SID didn't
        resolve on the last check; we re-check after
        ``cache_ttl_minutes`` (default 1440 = 24h).
        """
        started = time.time()
        cap = int(max_unique_sids or self.max_unique_sids_default)

        # Group by owner up-front so we only ever do ONE AD lookup per
        # distinct owner string per scan, no matter how many files
        # share an orphaned principal.
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                SELECT owner,
                       COUNT(*)        AS file_count,
                       SUM(file_size)  AS total_size
                FROM scanned_files
                WHERE scan_id = ?
                  AND owner IS NOT NULL
                  AND owner <> ''
                GROUP BY owner
                ORDER BY file_count DESC
                LIMIT ?
                """,
                (int(scan_id), cap),
            )
            owner_rows = [dict(r) for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*) AS c FROM scanned_files WHERE scan_id = ?",
                (int(scan_id),),
            )
            total_files = int(cur.fetchone()["c"])

        orphan_sids: list[dict] = []
        total_orphan_files = 0

        for row in owner_rows:
            owner = row["owner"]
            file_count = int(row["file_count"] or 0)
            total_size = int(row["total_size"] or 0)

            resolved = self._check_owner_cached(owner)
            if resolved:
                continue

            sample_paths = self._sample_paths_for_owner(scan_id, owner)
            orphan_sids.append({
                "sid": owner,
                "file_count": file_count,
                "total_size": total_size,
                "sample_paths": sample_paths,
            })
            total_orphan_files += file_count

        elapsed = time.time() - started
        logger.info(
            "Orphan-SID scan: scan_id=%s total_files=%d orphan_sids=%d "
            "orphan_files=%d in %.2fs",
            scan_id, total_files, len(orphan_sids), total_orphan_files, elapsed,
        )
        return {
            "scan_id": int(scan_id),
            "total_files": total_files,
            "total_orphan_files": total_orphan_files,
            "orphan_sids": orphan_sids,
            "elapsed_seconds": round(elapsed, 3),
        }

    # ──────────────────────────────────────────────
    # Drill-down
    # ──────────────────────────────────────────────

    def get_orphan_files(self, source_id: int, sid: str,
                         page: int = 1, page_size: int = 100) -> dict:
        """Paginated list of files owned by an orphan SID."""
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 100), 1000))
        offset = (page - 1) * page_size

        with self.db.get_cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) AS c FROM scanned_files
                   WHERE source_id = ? AND owner = ?""",
                (int(source_id), sid),
            )
            total = int(cur.fetchone()["c"])

            cur.execute(
                """SELECT file_path, file_name, file_size,
                          last_access_time, last_modify_time, owner
                   FROM scanned_files
                   WHERE source_id = ? AND owner = ?
                   ORDER BY file_path
                   LIMIT ? OFFSET ?""",
                (int(source_id), sid, page_size, offset),
            )
            files = [dict(r) for r in cur.fetchall()]

        return {
            "source_id": int(source_id),
            "sid": sid,
            "page": page,
            "page_size": page_size,
            "total": total,
            "files": files,
        }

    # ──────────────────────────────────────────────
    # Bulk reassignment (Windows-only)
    # ──────────────────────────────────────────────

    def reassign_owner(self, source_id: int, old_sid: str,
                       new_owner: str, dry_run: bool = True,
                       max_files: Optional[int] = None) -> dict:
        """Set new owner on every matching file. Windows-only.

        Returns ``{scanned, changed, errors, dry_run, elapsed_seconds}``.
        Logs per-file failures at DEBUG and never aborts the batch on
        a single bad ACL — the operator should still get a summary so
        they can re-run on the failures.
        """
        if not new_owner or not str(new_owner).strip():
            raise ValueError("new_owner is required")

        if not dry_run and sys.platform != "win32":
            raise NotImplementedError(
                "OrphanSidAnalyzer.reassign_owner requires Windows + pywin32"
            )

        started = time.time()
        scanned = 0
        changed = 0
        errors = 0

        with self.db.get_cursor() as cur:
            sql = (
                "SELECT file_path FROM scanned_files "
                "WHERE source_id = ? AND owner = ? "
                "ORDER BY file_path"
            )
            params: list = [int(source_id), old_sid]
            if max_files is not None:
                sql += " LIMIT ?"
                params.append(int(max_files))
            cur.execute(sql, tuple(params))
            paths = [r["file_path"] for r in cur.fetchall()]

        if dry_run:
            elapsed = time.time() - started
            return {
                "scanned": len(paths),
                "changed": 0,
                "errors": 0,
                "dry_run": True,
                "elapsed_seconds": round(elapsed, 3),
            }

        # Real run — Windows path. Resolve the new owner string once,
        # then SetNamedSecurityInfo per file.
        try:
            import win32security  # type: ignore
        except Exception as e:  # pragma: no cover - import-time on Windows
            raise NotImplementedError(
                f"pywin32 unavailable on this host: {e}"
            )

        try:
            new_sid, _domain, _typ = win32security.LookupAccountName(None, new_owner)
        except Exception as e:
            raise ValueError(f"Cannot resolve new_owner {new_owner!r}: {e}")

        for path in paths:
            scanned += 1
            try:
                win32security.SetNamedSecurityInfo(
                    path,
                    win32security.SE_FILE_OBJECT,
                    win32security.OWNER_SECURITY_INFORMATION,
                    new_sid,
                    None,
                    None,
                    None,
                )
                changed += 1
            except Exception as e:
                errors += 1
                logger.debug("Reassign failed for %s: %s", path, e)
                continue

        elapsed = time.time() - started
        logger.info(
            "Orphan-SID reassign done: source=%s old_sid=%s new_owner=%s "
            "scanned=%d changed=%d errors=%d in %.2fs",
            source_id, old_sid, new_owner, scanned, changed, errors, elapsed,
        )
        return {
            "scanned": scanned,
            "changed": changed,
            "errors": errors,
            "dry_run": False,
            "elapsed_seconds": round(elapsed, 3),
        }

    # ──────────────────────────────────────────────
    # CSV export
    # ──────────────────────────────────────────────

    def export_csv(self, source_id: int, scan_id: int,
                   output_path: str) -> int:
        """CSV of orphan files for offline review.

        Cols: ``path, owner_sid, file_size, last_modify_time, owner_resolved``.
        Returns the row count.
        """
        # Re-use detect_orphans to get the orphan SID set; that also
        # populates the cache so the per-file check below is cheap.
        report = self.detect_orphans(int(scan_id))
        orphan_sids = {row["sid"] for row in report.get("orphan_sids", [])}

        rows_written = 0
        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow([
                "path", "owner_sid", "file_size",
                "last_modify_time", "owner_resolved",
            ])
            if not orphan_sids:
                return 0

            placeholders = ",".join(["?"] * len(orphan_sids))
            params: list = [int(source_id), int(scan_id), *orphan_sids]
            with self.db.get_cursor() as cur:
                cur.execute(
                    f"""SELECT file_path, owner, file_size, last_modify_time
                        FROM scanned_files
                        WHERE source_id = ? AND scan_id = ?
                          AND owner IN ({placeholders})
                        ORDER BY file_path""",
                    tuple(params),
                )
                for r in cur.fetchall():
                    writer.writerow([
                        r["file_path"],
                        r["owner"],
                        r["file_size"],
                        r["last_modify_time"] or "",
                        "false",
                    ])
                    rows_written += 1
        logger.info(
            "Orphan-SID CSV export: source=%s scan=%s rows=%d -> %s",
            source_id, scan_id, rows_written, output_path,
        )
        return rows_written

    # ──────────────────────────────────────────────
    # Internals
    # ──────────────────────────────────────────────

    def _check_owner_cached(self, owner: str) -> bool:
        """Return True if the owner currently resolves, False if it
        looks orphaned. Consults ``orphan_sid_cache`` first; only
        falls through to ``ad_lookup`` if the cached entry is missing
        or stale.
        """
        cached = self._cache_get(owner)
        if cached is not None and not cached["stale"]:
            return bool(cached["resolved"])

        # Either no cache row or it's past the TTL — re-check via AD.
        resolved, name = self._lookup_owner(owner)
        self._cache_put(owner, resolved, name)
        return resolved

    def _lookup_owner(self, owner: str) -> tuple[bool, Optional[str]]:
        """Ask ADLookup whether ``owner`` still exists. Treats any
        result with ``found=False`` (or a None/empty result) as orphaned.
        Never raises — failure to query AD => assume unresolved so the
        operator gets a finding rather than a silent skip.
        """
        if self.ad_lookup is None:
            return False, None
        try:
            res = self.ad_lookup.lookup(owner)
        except Exception as e:
            logger.debug("ad_lookup raised on %s: %s", owner, e)
            return False, None
        if not res:
            return False, None
        if not res.get("found"):
            return False, res.get("display_name")
        return True, res.get("display_name")

    def _cache_get(self, sid: str) -> Optional[dict]:
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT resolved, resolved_name, checked_at "
                "FROM orphan_sid_cache WHERE sid = ?",
                (sid,),
            )
            row = cur.fetchone()
        if not row:
            return None
        checked_at = self._parse_ts(row["checked_at"])
        ttl = timedelta(minutes=self.cache_ttl_minutes)
        stale = (datetime.utcnow() - checked_at) > ttl if checked_at else True
        return {
            "resolved": bool(row["resolved"]),
            "resolved_name": row["resolved_name"],
            "checked_at": checked_at,
            "stale": stale,
        }

    def _cache_put(self, sid: str, resolved: bool,
                   resolved_name: Optional[str]) -> None:
        with self.db.get_cursor() as cur:
            cur.execute(
                """INSERT INTO orphan_sid_cache (sid, resolved, resolved_name, checked_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(sid) DO UPDATE SET
                       resolved = excluded.resolved,
                       resolved_name = excluded.resolved_name,
                       checked_at = CURRENT_TIMESTAMP""",
                (sid, 1 if resolved else 0, resolved_name),
            )

    def _sample_paths_for_owner(self, scan_id: int, owner: str) -> list[str]:
        with self.db.get_cursor() as cur:
            cur.execute(
                """SELECT file_path FROM scanned_files
                   WHERE scan_id = ? AND owner = ?
                   ORDER BY file_path
                   LIMIT ?""",
                (int(scan_id), owner, _SAMPLE_PATHS_PER_SID),
            )
            return [r["file_path"] for r in cur.fetchall()]

    @staticmethod
    def _parse_ts(value) -> Optional[datetime]:
        """SQLite TIMESTAMP DEFAULT CURRENT_TIMESTAMP returns a string
        like ``2026-04-23 12:34:56`` — turn it into a UTC ``datetime``.
        Returns None if the value is missing or unparseable.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        s = str(value)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None
