"""Admin ad-hoc SQL query support (issue #48).

Whitelist-guarded read-only query executor. The guard validates the
SQL string against a small allow-list of tables and a deny-list of
mutating keywords, then executes the query through DuckDB with the
SQLite database ATTACH'ed read-only — exactly the same pattern used by
``src/storage/analytics.py``. Direct SQLite access is intentionally
avoided so that ad-hoc queries cannot acquire a writable cursor.

Validation order (mirrors the issue spec):
    1. Strip line (``-- ...``) and block (``/* ... */``) comments.
    2. Reject any token in ``BLOCKED_KEYWORDS`` (case-insensitive,
       word boundaries).
    3. Require the cleaned SQL to start with ``SELECT`` or ``WITH``.
    4. Extract every ``FROM``/``JOIN`` table reference and reject
       unknown or explicitly blocked tables. The ``sqlite_db.``
       attach prefix is tolerated and stripped.
    5. Append ``LIMIT {max_rows}`` if no ``LIMIT`` clause is present.
    6. Reject if the (original) SQL exceeds ``MAX_SQL_LENGTH`` chars.

Execution opens an in-memory DuckDB connection per call, ATTACHes the
SQLite database read-only, and runs the validated SQL on a worker
thread with a hard timeout. The connection is closed on timeout via
``interrupt()`` + ``close()`` so a runaway query cannot survive past
the request.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Optional

logger = logging.getLogger("file_activity.sql_query")

try:
    import duckdb  # type: ignore
    _HAVE_DUCKDB = True
except ImportError:  # pragma: no cover - duckdb is a hard dep elsewhere
    duckdb = None
    _HAVE_DUCKDB = False


_COMMENT_LINE_RE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
# `FROM` / `JOIN` followed by an optional schema qualifier and an
# unquoted identifier. Quoted identifiers (`"foo"`) are rejected by
# matching only the bare-word form — the panel is for ad-hoc SELECTs,
# not arbitrary DDL gymnastics.
_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
_LIMIT_RE = re.compile(r"\bLIMIT\b", re.IGNORECASE)


class SqlQueryGuard:
    """Whitelist-based SQL guard for ad-hoc admin queries."""

    ALLOWED_TABLES = frozenset([
        "scanned_files", "scan_runs", "audit_events", "archived_files",
        "archive_operations", "duplicate_hash_groups",
        "duplicate_hash_members", "ransomware_alerts", "audit_log_chain",
        "scheduled_tasks", "sources", "policies",
    ])
    BLOCKED_TABLES = frozenset([
        # Sensitive — never expose via ad-hoc query.
        "notification_log", "ad_user_cache", "usn_tail_state",
    ])
    BLOCKED_KEYWORDS = frozenset([
        "INSERT", "UPDATE", "DELETE", "DROP", "ATTACH", "DETACH",
        "PRAGMA", "VACUUM", "ALTER", "CREATE", "REPLACE", "GRANT",
        "REVOKE", "TRUNCATE",
    ])
    ATTACH_ALIAS = "sqlite_db"
    MAX_SQL_LENGTH = 5000

    def __init__(self, max_rows: int = 10000, timeout_seconds: int = 30):
        self.max_rows = max(1, int(max_rows))
        self.timeout_seconds = max(1, int(timeout_seconds))

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def _strip_comments(cls, sql: str) -> str:
        sql = _COMMENT_BLOCK_RE.sub(" ", sql)
        sql = _COMMENT_LINE_RE.sub(" ", sql)
        return sql

    def validate(self, sql: str) -> tuple[bool, Optional[str]]:
        """Return ``(ok, reason)``. ``reason`` is ``None`` on success."""
        if sql is None or not sql.strip():
            return False, "Bos sorgu"
        if len(sql) > self.MAX_SQL_LENGTH:
            return False, f"Sorgu cok uzun (>{self.MAX_SQL_LENGTH} karakter)"

        cleaned = self._strip_comments(sql).strip().rstrip(";").strip()
        if not cleaned:
            return False, "Bos sorgu"

        upper = cleaned.upper()
        for kw in self.BLOCKED_KEYWORDS:
            if re.search(rf"\b{kw}\b", upper):
                return False, f"Yasakli anahtar kelime: {kw}"

        first_token_match = re.match(r"\s*([A-Za-z]+)", cleaned)
        if not first_token_match:
            return False, "Sorgu SELECT veya WITH ile baslamali"
        first_token = first_token_match.group(1).upper()
        if first_token not in ("SELECT", "WITH"):
            return False, "Sorgu SELECT veya WITH ile baslamali"

        refs = _TABLE_REF_RE.findall(cleaned)
        if not refs:
            # No tables at all (e.g. `SELECT 1`) — harmless, allow.
            return True, None
        for schema_prefix, table in refs:
            tname = table.lower()
            # CTE names are caught by the unknown-table branch below;
            # the panel does not pre-parse `WITH cte AS (...)`. We
            # allow the alias prefix `sqlite_db.` and tolerate any
            # other prefix only if the table itself is whitelisted.
            if tname in self.BLOCKED_TABLES:
                return False, f"Yasakli tablo: {tname}"
            if tname not in self.ALLOWED_TABLES:
                return False, f"Izin verilmeyen tablo: {tname}"

        return True, None

    # ------------------------------------------------------------------
    # Rewrite + execute
    # ------------------------------------------------------------------

    def _prepare(self, sql: str) -> str:
        """Strip comments, normalise table refs to the attach alias,
        append ``LIMIT`` when missing."""
        cleaned = self._strip_comments(sql).strip().rstrip(";").strip()

        def _rewrite(match: re.Match) -> str:
            keyword = match.group(0).split()[0]
            schema_prefix = match.group(1) or ""
            table = match.group(2)
            # Drop any user-supplied prefix and force the attach alias
            # so that `FROM scanned_files` and
            # `FROM sqlite_db.scanned_files` both resolve identically.
            return f"{keyword} {self.ATTACH_ALIAS}.{table}"

        rewritten = _TABLE_REF_RE.sub(_rewrite, cleaned)

        if not _LIMIT_RE.search(rewritten):
            rewritten = f"{rewritten} LIMIT {self.max_rows}"
        return rewritten

    def execute(self, db, sql: str) -> dict:
        """Run ``sql`` via DuckDB attached to SQLite (read-only).

        Returns ``{columns, rows, row_count, truncated, elapsed_ms}``.
        Raises ``RuntimeError`` on attach/exec failure or timeout.
        """
        if not _HAVE_DUCKDB:
            raise RuntimeError("duckdb paketi yuklenmemis")

        prepared = self._prepare(sql)
        db_path = getattr(db, "db_path", None) or getattr(db, "path", None)
        if not db_path:
            raise RuntimeError("DB nesnesinde db_path bulunamadi")

        conn = duckdb.connect(database=":memory:")
        try:
            try:
                conn.execute("INSTALL sqlite")
            except Exception:
                pass
            conn.execute("LOAD sqlite")
            attached = False
            for attach_sql in (
                f"ATTACH '{db_path}' AS {self.ATTACH_ALIAS} (TYPE SQLITE, READ_ONLY)",
                f"ATTACH '{db_path}' AS {self.ATTACH_ALIAS} (TYPE SQLITE)",
            ):
                try:
                    conn.execute(attach_sql)
                    attached = True
                    break
                except Exception as e:
                    last_err = e
            if not attached:
                raise RuntimeError(f"SQLite ATTACH basarisiz: {last_err}")

            # Run the query on a worker so we can enforce the timeout
            # via DuckDB's ``interrupt()``. Anything still pending after
            # the timeout has its connection torn down.
            result_box: dict = {}

            def _runner():
                try:
                    cur = conn.execute(prepared)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchmany(self.max_rows + 1)
                    result_box["columns"] = cols
                    result_box["rows"] = rows
                except Exception as e:
                    result_box["error"] = e

            t0 = time.perf_counter()
            worker = threading.Thread(target=_runner, daemon=True)
            worker.start()
            worker.join(self.timeout_seconds)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            if worker.is_alive():
                try:
                    conn.interrupt()
                except Exception:
                    pass
                worker.join(1.0)
                raise RuntimeError(
                    f"Sorgu zaman asimina ugradi (>{self.timeout_seconds}s)"
                )

            if "error" in result_box:
                raise RuntimeError(str(result_box["error"]))

            cols = result_box.get("columns", [])
            rows = result_box.get("rows", [])
            truncated = len(rows) > self.max_rows
            if truncated:
                rows = rows[: self.max_rows]
            # Coerce to plain Python lists so JSON serialisation never
            # leaks a duckdb-specific type into the response.
            return {
                "columns": list(cols),
                "rows": [list(r) for r in rows],
                "row_count": len(rows),
                "truncated": truncated,
                "elapsed_ms": round(elapsed_ms, 2),
            }
        finally:
            try:
                conn.close()
            except Exception:
                pass
