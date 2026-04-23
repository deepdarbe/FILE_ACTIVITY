"""NTFS ACL / effective-permissions analyzer (issue #49).

Reads on-disk Windows DACLs via pywin32 and persists a normalized snapshot
of the ACEs to ``file_acl_snapshots`` (created idempotently in
:meth:`Database._create_tables`). The dashboard then surfaces three
operator-facing queries on top of that table:

* ``get_effective_acl(path)``       -- live read for a single path.
* ``find_paths_for_trustee(sid)``   -- "where does this user/group have
  access?" for the most recent snapshot.
* ``detect_sprawl(scan_id)``        -- top over-permissioned trustees
  (e.g. ``Everyone`` / ``Domain Users`` granted Modify+).

pywin32 is lazily imported so the module is import-safe on Linux and CI.
``is_supported()`` is the single source of truth for callers that want to
short-circuit on non-Windows hosts. ``snapshot_source()`` walks the
``scanned_files`` rows for a given scan, calls ``GetFileSecurity`` per
path, and inserts the resulting rows in batches; per-file failures are
logged at DEBUG and never abort the run.

Wiring into the scanner is intentionally NOT done here -- that lives
behind ``security.acl_analyzer.snapshot_during_scan`` and is a follow-up
PR. The point of this module is to make the data + queries available.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Optional

logger = logging.getLogger("file_activity.security.acl_analyzer")


# ACE flag bit indicating the ACE is inherited from a parent container.
# https://learn.microsoft.com/windows/win32/secauthz/ace-header
INHERITED_ACE_FLAG = 0x10

# Known generic ACE types (we also accept anything non-zero as DENY for
# readability — only ALLOW (0) and DENY (1) appear on a typical DACL).
ACE_TYPE_ALLOW = 0
ACE_TYPE_DENY = 1

# Default top-N cap for sprawl + trustee queries. Capped to keep the
# dashboard responsive on million-file shares.
_DEFAULT_TOP_N = 50
_INSERT_BATCH = 500


class AclAnalyzer:
    """NTFS ACL analyzer using pywin32 (lazy loaded — Linux-safe)."""

    # Common standard rights bundles. Anything that does not match exactly
    # falls back to ``Custom (0x...)`` so the operator can still see the
    # raw mask in the UI without having to learn the bit layout.
    PERMISSION_MASK_NAMES = {
        0x001F01FF: "FullControl",
        0x001301BF: "Modify",
        0x001200A9: "Read+Execute",
        0x00120089: "Read",
        0x00120116: "Write",
    }

    def __init__(self, db, config: dict, ad_lookup=None):
        self.db = db
        self.ad_lookup = ad_lookup
        cfg = ((config or {}).get("security", {}) or {}).get("acl_analyzer", {}) or {}
        self.enabled = bool(cfg.get("enabled", True))
        self.snapshot_during_scan = bool(cfg.get("snapshot_during_scan", False))
        # Default sprawl threshold = Modify; anything granting Modify or
        # higher to a broad principal counts as a finding.
        self.sprawl_threshold_mask = int(cfg.get("sprawl_threshold_mask", 0x001301BF))

    # ──────────────────────────────────────────────
    # Capability probes
    # ──────────────────────────────────────────────

    def is_supported(self) -> bool:
        """True on Windows with pywin32 importable. False otherwise."""
        if sys.platform != "win32":
            return False
        try:
            import win32security  # noqa: F401
            return True
        except Exception:  # pragma: no cover - import probe only
            return False

    # ──────────────────────────────────────────────
    # Live read (Windows-only)
    # ──────────────────────────────────────────────

    def get_effective_acl(self, path: str) -> dict:
        """Effective DACL for one path. Windows-only.

        Returns a dict shaped like::

            {path, owner_sid, owner_name,
             entries: [{trustee_sid, trustee_name, permission_name, mask,
                        ace_type, is_inherited, source}],
             errors: [...]}

        Per-ACE failures (e.g. an unresolvable SID) populate ``errors``
        but never raise — the operator should still see the rest of the
        DACL.
        """
        if sys.platform != "win32":
            raise NotImplementedError("AclAnalyzer.get_effective_acl requires Windows")

        import win32security  # type: ignore

        sd = win32security.GetFileSecurity(
            path,
            win32security.OWNER_SECURITY_INFORMATION
            | win32security.DACL_SECURITY_INFORMATION,
        )
        owner_sid = sd.GetSecurityDescriptorOwner()
        owner_sid_str = win32security.ConvertSidToStringSid(owner_sid)
        owner_name = self._sid_to_name(owner_sid)
        dacl = sd.GetSecurityDescriptorDacl()

        entries: list[dict] = []
        errors: list[str] = []
        if dacl is not None:
            for i in range(dacl.GetAceCount()):
                try:
                    ace = dacl.GetAce(i)
                    # Object ACEs are 6-tuples; standard ACEs are 3-tuples.
                    # Either way the first element is (ace_type, ace_flags),
                    # the second is the access mask, and the SID is at the
                    # tail. Pull from the end so both shapes work.
                    header = ace[0]
                    ace_type, ace_flags = header[0], header[1]
                    mask = ace[1]
                    sid = ace[-1]
                    is_inherited = bool(ace_flags & INHERITED_ACE_FLAG)
                    entries.append({
                        "trustee_sid": str(win32security.ConvertSidToStringSid(sid)),
                        "trustee_name": self._sid_to_name(sid),
                        "permission_name": self._mask_to_name(mask),
                        "mask": int(mask),
                        "ace_type": "ALLOW" if ace_type == ACE_TYPE_ALLOW else "DENY",
                        "is_inherited": is_inherited,
                        "source": "inherited" if is_inherited else "direct",
                    })
                except Exception as e:
                    logger.debug("ACE %d on %s failed: %s", i, path, e)
                    errors.append(f"ace_{i}: {e}")

        return {
            "path": path,
            "owner_sid": str(owner_sid_str),
            "owner_name": owner_name,
            "entries": entries,
            "errors": errors,
        }

    # ──────────────────────────────────────────────
    # DB-backed queries (cross-platform)
    # ──────────────────────────────────────────────

    def find_paths_for_trustee(self, trustee_sid: str,
                               limit: int = 100) -> list[dict]:
        """Where does this SID have access? Returns rows from the most
        recent snapshot per file_path so re-scans don't double-count.
        """
        limit = max(1, min(int(limit or 100), 10_000))
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.file_path,
                    s.trustee_sid,
                    s.trustee_name,
                    s.permissions_mask,
                    s.permission_name,
                    s.is_inherited,
                    s.ace_type,
                    s.scan_id,
                    s.recorded_at
                FROM file_acl_snapshots s
                JOIN (
                    SELECT file_path, trustee_sid, MAX(id) AS max_id
                    FROM file_acl_snapshots
                    WHERE trustee_sid = ?
                    GROUP BY file_path, trustee_sid
                ) latest
                  ON latest.max_id = s.id
                WHERE s.ace_type = 'ALLOW'
                ORDER BY s.permissions_mask DESC, s.file_path ASC
                LIMIT ?
                """,
                (trustee_sid, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def detect_sprawl(self, scan_id: Optional[int] = None,
                      severity_threshold: int = 0x001301BF) -> list[dict]:
        """Top over-permissioned trustees.

        Groups ``file_acl_snapshots`` by ``trustee_sid`` and returns the
        50 worst offenders (most files with >= threshold). Only ALLOW
        ACEs count; DENY ACEs reduce, not increase, exposure.

        ``scan_id`` is optional — if omitted, ALL snapshots are scanned.
        """
        threshold = int(severity_threshold)
        params: list = [threshold]
        clause = ""
        if scan_id is not None:
            clause = " AND scan_id = ?"
            params.append(int(scan_id))

        sql = (
            "SELECT trustee_sid, trustee_name, "
            "       COUNT(DISTINCT file_path) AS file_count, "
            "       MAX(permissions_mask)     AS max_mask, "
            "       MAX(permission_name)      AS sample_permission_name "
            "FROM file_acl_snapshots "
            "WHERE ace_type = 'ALLOW' "
            "  AND permissions_mask >= ? "
            f" {clause} "
            "GROUP BY trustee_sid, trustee_name "
            "ORDER BY file_count DESC "
            "LIMIT ?"
        )
        params.append(_DEFAULT_TOP_N)

        with self.db.get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return [dict(r) for r in cur.fetchall()]

    # ──────────────────────────────────────────────
    # Snapshot writer (Windows-only path walk)
    # ──────────────────────────────────────────────

    def snapshot_source(self, source_id: int, scan_id: int,
                        max_files: Optional[int] = None) -> dict:
        """Walk ``scanned_files`` for the scan, persist DACLs.

        Heavy operation — emits a progress log every 1000 files. Per-file
        ``GetFileSecurity`` failures are tallied in the ``errors`` count
        and logged at DEBUG. Returns
        ``{scanned, errors, elapsed_seconds, written}``.
        """
        if not self.is_supported():
            raise NotImplementedError("AclAnalyzer.snapshot_source requires Windows + pywin32")

        started = time.time()
        scanned = 0
        errors = 0
        written = 0
        batch: list[tuple] = []

        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT file_path FROM scanned_files WHERE source_id=? AND scan_id=?",
                (int(source_id), int(scan_id)),
            )
            rows = cur.fetchall()

        paths = [r["file_path"] for r in rows]
        if max_files is not None:
            paths = paths[: int(max_files)]

        for path in paths:
            scanned += 1
            try:
                acl = self.get_effective_acl(path)
            except Exception as e:
                errors += 1
                logger.debug("ACL read failed for %s: %s", path, e)
                continue

            for entry in acl.get("entries", []):
                batch.append((
                    int(scan_id),
                    path,
                    entry["trustee_sid"],
                    entry.get("trustee_name"),
                    int(entry["mask"]),
                    entry.get("permission_name"),
                    1 if entry.get("is_inherited") else 0,
                    entry.get("ace_type"),
                ))
                if len(batch) >= _INSERT_BATCH:
                    written += self._flush_batch(batch)
                    batch = []

            if scanned % 1000 == 0:
                logger.info(
                    "ACL snapshot progress: scanned=%d errors=%d written=%d",
                    scanned, errors, written,
                )

        if batch:
            written += self._flush_batch(batch)

        elapsed = time.time() - started
        logger.info(
            "ACL snapshot done: scanned=%d errors=%d written=%d in %.1fs",
            scanned, errors, written, elapsed,
        )
        return {
            "scanned": scanned,
            "errors": errors,
            "written": written,
            "elapsed_seconds": round(elapsed, 3),
        }

    # ──────────────────────────────────────────────
    # Internals
    # ──────────────────────────────────────────────

    def _flush_batch(self, batch: list[tuple]) -> int:
        with self.db.get_cursor() as cur:
            cur.executemany(
                """
                INSERT INTO file_acl_snapshots (
                    scan_id, file_path, trustee_sid, trustee_name,
                    permissions_mask, permission_name, is_inherited, ace_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            return cur.rowcount or len(batch)

    def _mask_to_name(self, mask: int) -> str:
        """Resolve an access mask to a friendly bundle name.

        Exact match wins; anything else falls through to ``Custom (0x..)``
        so the operator still sees the raw bits without learning the
        layout. Matches are *exact* on purpose — fuzzy matching produces
        misleading labels (e.g. Modify+Delete looks like FullControl).
        """
        try:
            mask_i = int(mask)
        except Exception:
            return f"Custom ({mask!r})"
        name = self.PERMISSION_MASK_NAMES.get(mask_i)
        if name:
            return name
        return f"Custom (0x{mask_i:08X})"

    def _sid_to_name(self, sid) -> Optional[str]:
        """Resolve a SID -> ``DOMAIN\\Name``. Falls back to the string
        SID on failure. Best-effort only; never raises.
        """
        if sid is None:
            return None
        try:
            import win32security  # type: ignore
            name, domain, _typ = win32security.LookupAccountSid(None, sid)
            if domain:
                return f"{domain}\\{name}"
            return name
        except Exception:
            try:
                import win32security  # type: ignore
                return str(win32security.ConvertSidToStringSid(sid))
            except Exception:
                return str(sid)
