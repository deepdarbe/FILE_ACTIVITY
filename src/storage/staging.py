"""Parquet staging + DuckDB COPY ingest for bulk scanned-file rows.

Replaces per-batch SQLite ``executemany`` calls with a Parquet-staged path
that DuckDB ingests in a single ``INSERT ... SELECT * FROM read_parquet(...)``
through the ``sqlite_scanner`` extension. On a 100k-row scan this yields a
10-50x throughput improvement vs row-by-row INSERTs because:

  * pyarrow writes a columnar Parquet file in one shot (no per-row Python
    type marshalling per column)
  * DuckDB streams the Parquet file into SQLite using a single transaction
    rather than ``executemany`` round-trips

Behaviour summary:

  * ``append(records)`` accumulates rows in a buffer; auto-flushes when the
    buffer hits ``flush_rows`` or the last flush is older than
    ``flush_seconds``.
  * ``flush()`` writes the buffer to ``data/staging/scan-<ts>-<uuid>.parquet``,
    opens DuckDB, ATTACHes SQLite read-write, runs the bulk INSERT, DETACHes,
    closes DuckDB and deletes the parquet file.
  * ``replay_orphans()`` scans the staging directory at startup, ingests any
    leftover ``.parquet`` files (crash-recovered) and deletes them.
  * If ``pyarrow`` is missing OR DuckDB ATTACH(READ_WRITE) fails, the stager
    silently falls back to ``Database.bulk_insert_scanned_files``.

Concurrency safety: DuckDB ATTACH(READ_WRITE) while the dashboard's SQLite
connection is open is contentious. We mitigate by keeping the DuckDB
connection short-lived (open -> attach -> insert -> detach -> close inside
``flush()``) and SQLite WAL mode tolerates a short writer window.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Optional

logger = logging.getLogger("file_activity.staging")

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
    _HAVE_PYARROW = True
except ImportError:
    pa = None
    pq = None
    _HAVE_PYARROW = False

try:
    import duckdb  # type: ignore
    _HAVE_DUCKDB = True
except ImportError:
    duckdb = None
    _HAVE_DUCKDB = False


# Columns written in scanned_files INSERT order. Must match
# Database.bulk_insert_scanned_files signature so DuckDB SELECT * lines up.
_COLUMNS = (
    "source_id", "scan_id", "file_path", "relative_path", "file_name",
    "extension", "file_size", "creation_time", "last_access_time",
    "last_modify_time", "owner", "attributes",
)

_DEFAULT_FLUSH_ROWS = 50_000
_DEFAULT_FLUSH_SECONDS = 30
_DEFAULT_STAGING_DIR = "data/staging"

# One-time fallback warning flag (module level so we don't log per-instance).
_warned_pyarrow_missing = False


def _emit_pyarrow_warning_once() -> None:
    global _warned_pyarrow_missing
    if not _warned_pyarrow_missing:
        _warned_pyarrow_missing = True
        logger.warning(
            "pyarrow yuklu degil; ParquetStager devre disi, "
            "klasik bulk_insert_scanned_files kullanilacak. "
            "Kurulum: pip install 'pyarrow>=14.0'"
        )


class ParquetStager:
    """Stage scanned-file rows to rotating Parquet, periodically COPY into SQLite."""

    def __init__(self, db, config: dict):
        self.db = db
        full_cfg = config or {}
        scanner_cfg = full_cfg.get("scanner", full_cfg) if isinstance(full_cfg, dict) else {}
        ps_cfg = (scanner_cfg.get("parquet_staging") or {}) if isinstance(scanner_cfg, dict) else {}

        self.enabled = bool(ps_cfg.get("enabled", True))
        self.flush_rows = int(ps_cfg.get("flush_rows", _DEFAULT_FLUSH_ROWS))
        self.flush_seconds = float(ps_cfg.get("flush_seconds", _DEFAULT_FLUSH_SECONDS))
        self.staging_dir = ps_cfg.get("staging_dir", _DEFAULT_STAGING_DIR)
        self.db_path = getattr(db, "db_path", None)

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

        # available=True means "use the parquet path"; False means "fall back".
        self.available = False
        self._init_error: Optional[str] = None

        if not self.enabled:
            self._init_error = "config.scanner.parquet_staging.enabled=false"
            logger.info("ParquetStager devre disi: %s", self._init_error)
            return
        if not _HAVE_PYARROW:
            _emit_pyarrow_warning_once()
            self._init_error = "pyarrow yuklu degil"
            return
        if not _HAVE_DUCKDB:
            self._init_error = "duckdb yuklu degil"
            logger.warning("ParquetStager devre disi: %s", self._init_error)
            return
        if not self.db_path:
            self._init_error = "db.db_path bos"
            logger.warning("ParquetStager devre disi: %s", self._init_error)
            return

        try:
            os.makedirs(self.staging_dir, exist_ok=True)
        except Exception as e:
            self._init_error = f"staging_dir olusturulamadi: {e}"
            logger.warning("ParquetStager devre disi: %s", self._init_error)
            return

        self.available = True
        logger.info(
            "ParquetStager hazir (flush_rows=%d, flush_seconds=%.0f, dir=%s)",
            self.flush_rows, self.flush_seconds, self.staging_dir,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, records: list) -> None:
        """Buffer rows; auto-flush when threshold or age exceeded.

        If the stager isn't available, falls back to
        ``Database.bulk_insert_scanned_files`` so callers can use a single
        code path.
        """
        if not records:
            return
        if not self.available:
            self._fallback_insert(records)
            return

        do_flush = False
        with self._lock:
            self._buffer.extend(records)
            buffered = len(self._buffer)
            age = time.monotonic() - self._last_flush
            if buffered >= self.flush_rows or age >= self.flush_seconds:
                do_flush = True
        if do_flush:
            self.flush()

    def flush(self) -> int:
        """Drain the buffer to a Parquet file then INSERT ... SELECT into SQLite.

        Returns the number of rows ingested. On any failure (parquet write,
        DuckDB ATTACH, INSERT) falls back to ``bulk_insert_scanned_files``
        and returns the row count anyway, so the caller doesn't lose data.
        """
        with self._lock:
            if not self._buffer:
                self._last_flush = time.monotonic()
                return 0
            buffer = self._buffer
            self._buffer = []
            self._last_flush = time.monotonic()

        if not self.available:
            self._fallback_insert(buffer)
            return len(buffer)

        path = self._make_parquet_path()
        try:
            self._write_parquet(buffer, path)
        except Exception as e:
            logger.warning("Parquet yazma basarisiz, SQLite fallback: %s", e)
            self._safe_unlink(path)
            self._fallback_insert(buffer)
            return len(buffer)

        try:
            ingested = self._ingest_parquet(path)
            self._safe_unlink(path)
            return ingested
        except Exception as e:
            logger.warning(
                "DuckDB ingest basarisiz (%s), SQLite fallback ile devam", e
            )
            # Fall back to executemany so we don't lose rows; remove the
            # parquet so it isn't replayed later as a duplicate.
            try:
                self._fallback_insert(buffer)
            finally:
                self._safe_unlink(path)
            return len(buffer)

    # ------------------------------------------------------------------
    # Issue #135 — thin shims for callers that want to stream MFT records
    # one at a time. ``append`` already auto-flushes when the buffer hits
    # ``flush_rows`` (default 50k), but the streaming MFT loop in
    # NtfsMftBackend wants a "did we just cross the threshold" predicate
    # so it can emit a progress UPDATE in lockstep with the flush. These
    # methods exist purely so the call site reads naturally; both delegate
    # to existing behaviour.
    # ------------------------------------------------------------------

    def should_flush(self) -> bool:
        """Return True if the buffer has hit ``flush_rows`` or ``flush_seconds``.

        Cheap O(1) check — does not lock more than necessary. Callers can
        use this to interleave a progress UPDATE before draining the
        buffer to disk.
        """
        with self._lock:
            buffered = len(self._buffer)
            age = time.monotonic() - self._last_flush
            return buffered >= self.flush_rows or (
                buffered > 0 and age >= self.flush_seconds
            )

    def flush_to_db(self, db=None, scan_id: int = None) -> int:
        """Force-flush the buffer to SQLite. Alias of :meth:`flush`.

        ``db`` and ``scan_id`` are accepted for callsite ergonomics
        (matches the issue #135 streaming pattern) but are ignored
        because the stager already holds a reference to the database
        and the rows carry their own scan_id field.
        """
        # Touch the unused params so static analysers don't flag dead
        # arguments — and so we silently accept the issue #135 signature
        # without breaking older callers.
        del db, scan_id
        return self.flush()

    def replay_orphans(self) -> int:
        """Ingest leftover ``.parquet`` files in the staging dir (crash recovery)."""
        if not self.available:
            return 0
        try:
            entries = [
                e for e in os.listdir(self.staging_dir)
                if e.endswith(".parquet")
            ]
        except FileNotFoundError:
            return 0
        except Exception as e:
            logger.warning("Staging dizini okunamadi: %s", e)
            return 0
        if not entries:
            return 0

        total_rows = 0
        total_bytes = 0
        for name in sorted(entries):
            path = os.path.join(self.staging_dir, name)
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
            try:
                rows = self._ingest_parquet(path)
                total_rows += rows
                total_bytes += size
                self._safe_unlink(path)
                logger.info(
                    "Orphan parquet replay: %s -> %d satir (%.1f KB)",
                    name, rows, size / 1024.0,
                )
            except Exception as e:
                logger.warning("Orphan parquet ingest basarisiz (%s): %s", name, e)
        if total_rows:
            logger.info(
                "ParquetStager replay tamamlandi: %d dosya, %d satir, %.1f KB",
                len(entries), total_rows, total_bytes / 1024.0,
            )
        return total_rows

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _make_parquet_path(self) -> str:
        ts = time.strftime("%Y%m%dT%H%M%S")
        name = f"scan-{ts}-{uuid.uuid4().hex[:8]}.parquet"
        return os.path.join(self.staging_dir, name)

    def _write_parquet(self, records: list, path: str) -> None:
        # Build columnar arrays. We let pyarrow infer types from Python
        # objects (mixed int/None for owner/attributes works fine).
        cols: dict = {c: [r.get(c) for r in records] for c in _COLUMNS}
        # source_id/scan_id/file_size/attributes are integers; explicit schema
        # makes DuckDB INSERT type-stable when SQLite expects INTEGER.
        schema = pa.schema([
            ("source_id", pa.int64()),
            ("scan_id", pa.int64()),
            ("file_path", pa.string()),
            ("relative_path", pa.string()),
            ("file_name", pa.string()),
            ("extension", pa.string()),
            ("file_size", pa.int64()),
            ("creation_time", pa.string()),
            ("last_access_time", pa.string()),
            ("last_modify_time", pa.string()),
            ("owner", pa.string()),
            ("attributes", pa.int64()),
        ])
        table = pa.table(cols, schema=schema)
        pq.write_table(table, path, compression="snappy")

    def _ingest_parquet(self, path: str) -> int:
        """Open DuckDB, ATTACH SQLite RW, INSERT, DETACH, close. Short window."""
        if not _HAVE_DUCKDB:
            raise RuntimeError("duckdb yok")
        # Quote single quotes inside paths defensively.
        sqlite_path = (self.db_path or "").replace("'", "''")
        parquet_path = path.replace("'", "''")

        conn = duckdb.connect(database=":memory:")
        try:
            try:
                conn.execute("INSTALL sqlite")
            except Exception:
                pass
            conn.execute("LOAD sqlite")

            attach_variants = [
                f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE SQLITE, READ_WRITE)",
                f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE SQLITE)",
            ]
            attached = False
            last_err: Optional[Exception] = None
            for sql in attach_variants:
                try:
                    conn.execute(sql)
                    attached = True
                    break
                except Exception as e:
                    last_err = e
            if not attached:
                raise RuntimeError(
                    f"SQLite ATTACH(READ_WRITE) basarisiz: {last_err}"
                )

            # DuckDB transaction wraps the INSERT; the underlying SQLite
            # writer lock is acquired by the sqlite_scanner extension and
            # serialises against concurrent readers/writers. DuckDB's
            # SQL dialect uses BEGIN TRANSACTION (not BEGIN IMMEDIATE).
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute(
                    f"INSERT INTO sqlite_db.scanned_files "
                    f"({', '.join(_COLUMNS)}) "
                    f"SELECT {', '.join(_COLUMNS)} "
                    f"FROM read_parquet('{parquet_path}')"
                )
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

            # Row count from the parquet file (avoids extra SQLite COUNT).
            row = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
            ).fetchone()
            ingested = int((row[0] if row else 0) or 0)

            try:
                conn.execute("DETACH sqlite_db")
            except Exception:
                pass
            return ingested
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fallback_insert(self, records: list) -> None:
        try:
            self.db.bulk_insert_scanned_files(records)
        except Exception as e:
            logger.error("Fallback bulk_insert_scanned_files basarisiz: %s", e)
            raise

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            if os.path.exists(path):
                os.unlink(path)
        except Exception as e:
            logger.warning("Parquet temizleme basarisiz (%s): %s", path, e)
