"""SQLite-backed implementation of :class:`StorageBackend`.

Phase 1 of issue #114. This module is a thin shim over the existing
``Database`` class — it does not introduce new SQL when an equivalent
``Database`` method already exists. The point of this layer is to lock
in the protocol shape so Phase 2 (Elasticsearch) can drop in without
churn in the dashboard query layer.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

logger = logging.getLogger("file_activity.storage.sqlite_backend")


# Whitelisted filter_dsl keys. Defined module-level so Phase 2 ES
# backend can import and share the exact same set — same DSL, same
# validation. Anything outside this set is rejected with ValueError.
_ALLOWED_FILTER_KEYS: frozenset[str] = frozenset(
    {
        "extension",
        "owner",
        "min_size",
        "max_size",
        "min_mtime",
        "max_mtime",
        "directory_prefix",
    }
)

# group_by / metric whitelists — same defensive posture: never
# interpolate user-supplied identifiers into SQL without checking
# against an allowlist first.
_ALLOWED_GROUP_BY: frozenset[str] = frozenset(
    {"extension", "owner", "directory_path"}
)
_ALLOWED_METRICS: frozenset[str] = frozenset({"count", "sum_size"})


class SqliteBackend:
    """Thin wrapper around :class:`src.storage.database.Database`.

    Implements :class:`StorageBackend` (structural — Protocol). All
    methods scope by ``scan_id`` so callers don't have to thread
    ``source_id`` through the abstraction.
    """

    name = "sqlite"

    def __init__(self, db: Any, config: dict) -> None:
        self.db = db
        self.config = config or {}

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def insert_scanned_files(self, scan_id: int, rows: list[dict]) -> int:
        """Bulk insert. Returns inserted count.

        Rows must already carry ``source_id`` (scanned_files has a NOT
        NULL FK to sources). The backend does not infer source_id from
        scan_id — that's the caller's job, same as the existing
        ``Database.bulk_insert_scanned_files`` contract.
        """
        if not rows:
            return 0
        # Stamp scan_id consistently — protect against rows that omit
        # it or carry a stale value.
        for r in rows:
            r["scan_id"] = scan_id
        # Reuse the existing optimised executemany path.
        self.db.bulk_insert_scanned_files(rows)
        return len(rows)

    def delete_scan(self, scan_id: int) -> int:
        """Delete all scanned_files rows for a scan. Returns deleted count."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "DELETE FROM scanned_files WHERE scan_id = ?",
                (scan_id,),
            )
            return cur.rowcount or 0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def count_scanned_files(self, scan_id: int) -> int:
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM scanned_files WHERE scan_id = ?",
                (scan_id,),
            )
            return cur.fetchone()["cnt"]

    def query_files(
        self,
        scan_id: int,
        filter_dsl: dict,
        limit: int = 1000,
    ) -> list[dict]:
        """Filter on scan; filter_dsl uses only whitelisted keys.

        Translation table:
          extension         -> WHERE extension = ?
          owner             -> WHERE owner = ?
          min_size          -> WHERE file_size >= ?
          max_size          -> WHERE file_size <= ?
          min_mtime         -> WHERE last_modify_time >= ?
          max_mtime         -> WHERE last_modify_time <= ?
          directory_prefix  -> WHERE file_path LIKE ? || '%'

        Anything else raises ``ValueError`` — same whitelist Phase 2 ES
        will share.
        """
        if filter_dsl is None:
            filter_dsl = {}
        unknown = set(filter_dsl) - _ALLOWED_FILTER_KEYS
        if unknown:
            raise ValueError(
                f"query_files: unsupported filter keys: {sorted(unknown)}. "
                f"Allowed: {sorted(_ALLOWED_FILTER_KEYS)}"
            )

        conditions = ["scan_id = ?"]
        params: list[Any] = [scan_id]

        if "extension" in filter_dsl:
            conditions.append("extension = ?")
            params.append(filter_dsl["extension"])
        if "owner" in filter_dsl:
            conditions.append("owner = ?")
            params.append(filter_dsl["owner"])
        if "min_size" in filter_dsl:
            conditions.append("file_size >= ?")
            params.append(filter_dsl["min_size"])
        if "max_size" in filter_dsl:
            conditions.append("file_size <= ?")
            params.append(filter_dsl["max_size"])
        if "min_mtime" in filter_dsl:
            conditions.append("last_modify_time >= ?")
            params.append(filter_dsl["min_mtime"])
        if "max_mtime" in filter_dsl:
            conditions.append("last_modify_time <= ?")
            params.append(filter_dsl["max_mtime"])
        if "directory_prefix" in filter_dsl:
            conditions.append("file_path LIKE ?")
            # Append %; caller passes the prefix, we manage the wildcard
            # so they can't smuggle one in.
            prefix = str(filter_dsl["directory_prefix"]).replace("%", r"\%")
            params.append(prefix + "%")

        where = " AND ".join(conditions)
        sql = (
            f"SELECT * FROM scanned_files WHERE {where} "
            f"ORDER BY id LIMIT ?"
        )
        params.append(int(limit))

        with self.db.get_cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def aggregate(
        self,
        scan_id: int,
        group_by: str,
        metric: str = "count",
    ) -> list[dict]:
        """Aggregation by extension/owner/directory_path."""
        if group_by not in _ALLOWED_GROUP_BY:
            raise ValueError(
                f"aggregate: unsupported group_by={group_by!r}. "
                f"Allowed: {sorted(_ALLOWED_GROUP_BY)}"
            )
        if metric not in _ALLOWED_METRICS:
            raise ValueError(
                f"aggregate: unsupported metric={metric!r}. "
                f"Allowed: {sorted(_ALLOWED_METRICS)}"
            )

        # directory_path isn't a column — synthesise it via a SQLite
        # rtrim of the file_name from file_path. We do this in SQL so
        # the GROUP BY doesn't have to materialise every row.
        if group_by == "directory_path":
            group_expr = (
                "substr(file_path, 1, length(file_path) - length(file_name))"
            )
            select_alias = "directory_path"
        else:
            group_expr = group_by
            select_alias = group_by

        metric_expr = "COUNT(*)" if metric == "count" else "SUM(file_size)"
        metric_alias = "count" if metric == "count" else "sum_size"

        sql = (
            f"SELECT {group_expr} AS {select_alias}, "
            f"{metric_expr} AS {metric_alias} "
            f"FROM scanned_files WHERE scan_id = ? "
            f"GROUP BY {group_expr} "
            f"ORDER BY {metric_alias} DESC"
        )
        with self.db.get_cursor() as cur:
            cur.execute(sql, (scan_id,))
            return [dict(r) for r in cur.fetchall()]

    def search_text(
        self,
        scan_id: int,
        query: str,
        limit: int = 100,
    ) -> list[dict]:
        """LIKE-based full-text search on file_path. Phase 2 ES will
        replace this with a proper analyzed text query."""
        # Escape user-supplied LIKE wildcards so they only match
        # literally; we control the surrounding %.
        q = (query or "").replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
        pattern = f"%{q}%"
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT * FROM scanned_files "
                "WHERE scan_id = ? AND file_path LIKE ? ESCAPE '\\' "
                "ORDER BY id LIMIT ?",
                (scan_id, pattern, int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]

    def iterate_scan(
        self,
        scan_id: int,
        batch_size: int = 1000,
    ) -> Iterator[list[dict]]:
        """Yield batches of scanned_files rows for a scan.

        Used by migrations (Phase 5: SQLite -> ES export) and any heavy
        report that doesn't want to load the whole scan into RAM.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        offset = 0
        while True:
            with self.db.get_cursor() as cur:
                cur.execute(
                    "SELECT * FROM scanned_files WHERE scan_id = ? "
                    "ORDER BY id LIMIT ? OFFSET ?",
                    (scan_id, batch_size, offset),
                )
                batch = [dict(r) for r in cur.fetchall()]
            if not batch:
                return
            yield batch
            if len(batch) < batch_size:
                return
            offset += batch_size

    def health_check(self) -> dict:
        """Returns ``{'name', 'available', 'details'}``.

        ``available`` is a hard boolean — Phase 3 will use it to decide
        whether to surface a banner in the dashboard. ``details``
        carries the underlying ``Database.health_check`` payload for
        diagnostics.
        """
        try:
            details = self.db.health_check()
            available = bool(details.get("status") == "ok")
        except Exception as e:  # pragma: no cover - defensive
            details = {"status": "error", "message": str(e)}
            available = False
        return {
            "name": self.name,
            "available": available,
            "details": details,
        }
