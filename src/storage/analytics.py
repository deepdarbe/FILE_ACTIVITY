"""DuckDB tabanli analitik motor.

SQLite veritabanini salt-okunur olarak ATTACH eder ve agir aggregate
sorgulari (duplicate grup tespiti, buyume istatistikleri, db ozet) icin
kolon tabanli motor saglar. SQLite kaynak-gercek; DuckDB sadece okur.

Kullanim:
    engine = AnalyticsEngine(db_path, config.get("analytics", {}))
    if engine.available:
        groups = engine.get_duplicate_groups(scan_id, min_size, page, page_size)

DuckDB kurulu degilse veya ATTACH basarisiz olursa `available=False`
doner ve cagiran kod SQLite fallback'ine dusebilir.
"""

import logging
import threading
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger("file_activity.analytics")

try:
    import duckdb  # type: ignore
    _HAVE_DUCKDB = True
except ImportError:
    duckdb = None
    _HAVE_DUCKDB = False


class AnalyticsEngine:
    """DuckDB uzerinden SQLite'a salt-okunur analitik koprusu."""

    def __init__(self, db_path: str, config: dict):
        self.db_path = db_path
        self.enabled = bool(config.get("enabled", True))
        self.memory_limit = config.get("memory_limit", "512MB")
        self.threads = int(config.get("threads", 4))
        # Issue #185 — `_lock` retained for API compat; the per-query
        # connection model below means concurrent queries don't actually
        # need to serialize, but a few callers still pass through it.
        self._lock = threading.Lock()
        # Issue #185 — DuckDB connection is no longer kept alive between
        # queries. A long-lived ATTACH on a SQLite file shows up as a
        # permanent SQLite reader and prevents `wal_checkpoint(TRUNCATE)`
        # from ever shrinking the WAL. Customer prod observed WAL stuck
        # at 13 GB / 74 GB at multiple points. Each query now gets its
        # own DuckDB connection that ATTACHes, runs, and is closed —
        # the SQLite reader is released between queries so the
        # checkpointer can truncate.
        self._conn = None  # kept for `close()` API compat; not used at runtime
        self.available = False
        self._init_error: Optional[str] = None

        if not _HAVE_DUCKDB:
            self._init_error = "duckdb paketi yuklenmemis"
            logger.info("AnalyticsEngine devre disi: %s", self._init_error)
            return
        if not self.enabled:
            self._init_error = "config.analytics.enabled=false"
            logger.info("AnalyticsEngine devre disi: %s", self._init_error)
            return

        try:
            # Validate that DuckDB can ATTACH the SQLite file at boot
            # (catches missing extension / bad path early). Connection
            # is closed immediately — no lingering reader.
            self._smoke_test_attach()
            self.available = True
            logger.info(
                "AnalyticsEngine hazir (DuckDB %s, memory=%s, threads=%d, "
                "per-query connection)",
                duckdb.__version__, self.memory_limit, self.threads
            )
        except Exception as e:
            self._init_error = str(e)
            logger.warning("AnalyticsEngine baslatilamadi, SQLite fallback kullanilacak: %s", e)

    def _smoke_test_attach(self):
        """Open + ATTACH + close once to verify the runtime is healthy.

        Run at __init__ so `available=True` only when DuckDB + sqlite
        extension + the actual DB file are all reachable. The connection
        opened here is closed before this method returns.
        """
        conn = self._make_attached_conn()
        try:
            conn.execute("SELECT 1 FROM sqlite_db.scan_runs LIMIT 0")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _make_attached_conn(self):
        """Open a fresh DuckDB :memory: connection and ATTACH the SQLite
        file as ``sqlite_db`` (read-only). Caller MUST close it.

        Issue #185 — Per-query lifecycle means the SQLite reader window
        opened by the sqlite_scanner extension is short — between calls
        the checkpointer can run TRUNCATE.
        """
        conn = duckdb.connect(database=":memory:")
        # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
        conn.execute(f"SET threads={self.threads}")

        try:
            conn.execute("INSTALL sqlite")
        except Exception:
            # Bundled in offline / corp env — ignore.
            pass
        conn.execute("LOAD sqlite")

        # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
        attach_sql_variants = [
            f"ATTACH '{self.db_path}' AS sqlite_db (TYPE SQLITE, READ_ONLY)",
            f"ATTACH '{self.db_path}' AS sqlite_db (TYPE SQLITE)",
        ]
        last_err = None
        for sql in attach_sql_variants:
            try:
                conn.execute(sql)
                last_err = None
                break
            except Exception as e:
                last_err = e
        if last_err is not None:
            conn.close()
            raise last_err
        return conn

    @contextmanager
    def _cursor(self):
        """Yield a fresh, short-lived DuckDB connection with SQLite ATTACHed.

        Issue #185 — Each call opens its OWN DuckDB connection, ATTACHes
        the SQLite DB, yields, then closes. This releases the SQLite
        reader between queries so `wal_checkpoint(TRUNCATE)` can shrink
        the WAL. The previous long-lived `self._conn` model was the root
        cause of the 13–74 GB WAL leak in customer prod.

        Concurrency: each call gets an independent connection — multiple
        queries run in parallel without serialization. The legacy
        `self._lock` is no longer used at the cursor layer (there is no
        shared mutable engine state to protect; each connection isolates
        its own ATTACH state).

        Cost: DuckDB :memory: connect + ATTACH on a SQLite file is
        ~50–150 ms in our observation — invisible on dashboard pages
        that issue at most a couple of queries per request.
        """
        if not self.available:
            raise RuntimeError("AnalyticsEngine kullanilamaz durumda")
        conn = self._make_attached_conn()
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def close(self):
        """No-op now (kept for API compat). Per-query connections clean
        themselves up in `_cursor`'s `finally`.
        """
        self.available = False
        self._conn = None

    # ──────────────────────────────────────────────
    # Duplicate raporu (DuckDB tek-gecis CTE)
    # ──────────────────────────────────────────────

    def get_duplicate_groups(self, scan_id: int, min_size: int,
                              page: int, page_size: int) -> dict:
        """Kopya dosya gruplarini tek CTE ile hesapla.

        SQLite uygulamasinda 3 ayri GROUP BY + her grup icin ekstra sorgu
        calisirken, burada tek agregasyon + tek detay sorgusu kullanilir.
        """
        offset = (page - 1) * page_size

        with self._cursor() as cur:
            # Tek CTE ile totaller ve sayfa birlikte alinir
            summary = cur.execute(
                """
                WITH dup AS (
                    SELECT file_name, file_size, COUNT(*) AS cnt,
                           (COUNT(*) - 1) * file_size AS waste_size
                    FROM sqlite_db.scanned_files
                    WHERE scan_id = ? AND file_size > ?
                    GROUP BY file_name, file_size
                    HAVING COUNT(*) > 1
                )
                SELECT
                    (SELECT COUNT(*) FROM dup) AS total_groups,
                    (SELECT COALESCE(SUM(cnt), 0) FROM dup) AS total_files,
                    (SELECT COALESCE(SUM(waste_size), 0) FROM dup) AS total_waste
                """,
                [scan_id, min_size],
            ).fetchone()
            total_groups = int(summary[0] or 0)
            total_files = int(summary[1] or 0)
            total_waste = int(summary[2] or 0)

            groups_rows = cur.execute(
                """
                SELECT file_name, file_size, COUNT(*) AS cnt,
                       (COUNT(*) - 1) * file_size AS waste_size
                FROM sqlite_db.scanned_files
                WHERE scan_id = ? AND file_size > ?
                GROUP BY file_name, file_size
                HAVING COUNT(*) > 1
                ORDER BY waste_size DESC
                LIMIT ? OFFSET ?
                """,
                [scan_id, min_size, page_size, offset],
            ).fetchall()

            groups = []
            for g in groups_rows:
                file_name, file_size, cnt, waste_size = g
                files_rows = cur.execute(
                    """
                    SELECT id, file_path, relative_path, owner,
                           last_access_time, last_modify_time
                    FROM sqlite_db.scanned_files
                    WHERE scan_id = ? AND file_name = ? AND file_size = ?
                    ORDER BY last_modify_time DESC
                    """,
                    [scan_id, file_name, file_size],
                ).fetchall()
                files = [
                    {
                        "id": r[0], "file_path": r[1], "relative_path": r[2],
                        "owner": r[3], "last_access_time": r[4],
                        "last_modify_time": r[5],
                    }
                    for r in files_rows
                ]
                groups.append({
                    "file_name": file_name,
                    "file_size": int(file_size or 0),
                    "count": int(cnt),
                    "waste_size": int(waste_size or 0),
                    "files": files,
                })

        total_pages = max(1, -(-total_groups // page_size))
        return {
            "total_groups": total_groups,
            "total_waste_size": total_waste,
            "total_files": total_files,
            "groups": groups,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "scan_id": scan_id,
            "engine": "duckdb",
        }

    # ──────────────────────────────────────────────
    # Drill-down sorgulari (tek-gecis total + sayfa)
    # ──────────────────────────────────────────────

    def _drilldown(self, where_sql: str, params: list, order_by: str,
                    limit: int, offset: int) -> dict:
        """Tek WINDOW-COUNT sorgusuyla hem toplam hem sayfayi doner.

        SQLite yolunda iki ayri COUNT + SELECT kullanilirken burada
        `COUNT(*) OVER ()` ile columnar tarama tek seferde yeterli olur.
        """
        sql = f"""
            SELECT *, COUNT(*) OVER () AS _total
            FROM sqlite_db.scanned_files
            WHERE {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """
        with self._cursor() as cur:
            res = cur.execute(sql, list(params) + [limit, offset])
            cols = [d[0] for d in res.description]
            rows = res.fetchall()

        if not rows:
            # Dosya yok; toplami yine de ayri kucuk bir COUNT ile al
            count_sql = f"SELECT COUNT(*) FROM sqlite_db.scanned_files WHERE {where_sql}"
            with self._cursor() as cur:
                total = int(cur.execute(count_sql, list(params)).fetchone()[0] or 0)
            return {"total": total, "files": [], "engine": "duckdb"}

        total_idx = cols.index("_total")
        total = int(rows[0][total_idx] or 0)
        keep_cols = [c for c in cols if c != "_total"]
        files = []
        for row in rows:
            rec = {cols[i]: row[i] for i in range(len(cols)) if cols[i] != "_total"}
            files.append(rec)
        return {"total": total, "files": files, "engine": "duckdb"}

    def get_files_by_owner(self, source_id: int, scan_id: int, owner: str,
                            limit: int, offset: int) -> dict:
        is_unknown = owner in ("Bilinmiyor", None, "")
        if is_unknown:
            where = "source_id = ? AND scan_id = ? AND owner IS NULL"
            params = [source_id, scan_id]
        else:
            where = "source_id = ? AND scan_id = ? AND owner = ?"
            params = [source_id, scan_id, owner]
        return self._drilldown(where, params, "file_size DESC", limit, offset)

    def get_files_by_extension(self, source_id: int, scan_id: int,
                                extension: str, limit: int, offset: int) -> dict:
        ext = (extension or "").lower().lstrip(".")
        is_none = ext in ("uzantisiz", "")
        if is_none:
            where = "source_id = ? AND scan_id = ? AND extension IS NULL"
            params = [source_id, scan_id]
        else:
            where = "source_id = ? AND scan_id = ? AND extension = ?"
            params = [source_id, scan_id, ext]
        return self._drilldown(where, params, "file_size DESC", limit, offset)

    def get_files_by_size_range(self, source_id: int, scan_id: int,
                                 min_bytes: int, max_bytes: Optional[int],
                                 limit: int, offset: int) -> dict:
        where = "source_id = ? AND scan_id = ? AND file_size >= ?"
        params: list = [source_id, scan_id, min_bytes]
        if max_bytes is not None:
            where += " AND file_size < ?"
            params.append(max_bytes)
        return self._drilldown(where, params, "file_size DESC", limit, offset)

    def get_files_by_frequency(self, source_id: int, scan_id: int,
                                min_days: int, max_days: Optional[int],
                                limit: int, offset: int) -> dict:
        from datetime import datetime, timedelta
        where = ("source_id = ? AND scan_id = ? "
                 "AND last_access_time IS NOT NULL AND last_access_time <= ?")
        params: list = [source_id, scan_id,
                        (datetime.now() - timedelta(days=min_days)).strftime('%Y-%m-%d')]
        if max_days is not None:
            where += " AND last_access_time > ?"
            params.append((datetime.now() - timedelta(days=max_days)).strftime('%Y-%m-%d'))
        return self._drilldown(where, params, "last_access_time ASC", limit, offset)

    # ──────────────────────────────────────────────
    # Buyume ve db istatistikleri
    # ──────────────────────────────────────────────

    def get_growth_stats(self, source_id: int) -> dict:
        """Yillik/aylik/gunluk buyume + toplam tarama sayisi.

        `started_at` SQLite attach'inda VARCHAR olarak gelir, bu yuzden
        SQLite'in `strftime('%Y',...)` davranisi yerine ISO string
        substring kullaniyoruz (ayni sonuc, tip donusumu gerekmez).
        """
        base_where = "source_id = ? AND status = 'completed'"
        with self._cursor() as cur:
            yearly = cur.execute(
                f"""
                SELECT substr(started_at, 1, 4) AS year,
                       MAX(total_files) AS total_files,
                       MAX(total_size) AS total_size
                FROM sqlite_db.scan_runs
                WHERE {base_where}
                GROUP BY year
                ORDER BY year
                """,
                [source_id],
            ).fetchall()

            monthly = cur.execute(
                f"""
                SELECT substr(started_at, 1, 7) AS month,
                       MAX(total_files) AS total_files,
                       MAX(total_size) AS total_size
                FROM sqlite_db.scan_runs
                WHERE {base_where}
                GROUP BY month
                ORDER BY month DESC
                LIMIT 24
                """,
                [source_id],
            ).fetchall()

            daily = cur.execute(
                f"""
                SELECT substr(started_at, 1, 10) AS day,
                       MAX(total_files) AS total_files,
                       MAX(total_size) AS total_size
                FROM sqlite_db.scan_runs
                WHERE {base_where}
                GROUP BY day
                ORDER BY day DESC
                LIMIT 30
                """,
                [source_id],
            ).fetchall()

            total_scans = int(cur.execute(
                f"SELECT COUNT(*) FROM sqlite_db.scan_runs WHERE {base_where}",
                [source_id],
            ).fetchone()[0] or 0)

        def _rows(rs, key):
            return [
                {key: r[0], "total_files": int(r[1] or 0), "total_size": int(r[2] or 0)}
                for r in rs
            ]

        return {
            "yearly": _rows(yearly, "year"),
            "monthly": list(reversed(_rows(monthly, "month"))),
            "daily": list(reversed(_rows(daily, "day"))),
            "total_scans": total_scans,
            "engine": "duckdb",
        }

    def get_db_stats(self, tables: list, db_path: str,
                      wal_path: str, shm_path: str) -> dict:
        """Tablo satir sayilari + dosya boyutlari. Disk boyutlari cagiran
        tarafindan verilir (DuckDB SQLite dosyasinin mtime'ini sormaz)."""
        import os
        stats: dict = {}
        try:
            stats["db_size"] = os.path.getsize(db_path) if os.path.exists(db_path) else 0
            stats["wal_size"] = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
            shm = os.path.getsize(shm_path) if os.path.exists(shm_path) else 0
            stats["total_disk"] = stats["db_size"] + stats["wal_size"] + shm

            with self._cursor() as cur:
                # Tum tablo COUNT'lari tek UNION ALL sorgusunda
                union_parts = []
                for t in tables:
                    # sqlite_db.<table> semasi; kolon isimleri quote edilmez
                    union_parts.append(
                        f"SELECT '{t}' AS table_name, COUNT(*) AS cnt "
                        f"FROM sqlite_db.{t}"
                    )
                union_sql = " UNION ALL ".join(union_parts)
                for row in cur.execute(union_sql).fetchall():
                    stats[f"{row[0]}_count"] = int(row[1] or 0)

                # En eski/yeni tarama
                row = cur.execute(
                    "SELECT MIN(started_at), MAX(started_at) FROM sqlite_db.scan_runs"
                ).fetchone()
                stats["oldest_scan"] = row[0]
                stats["newest_scan"] = row[1]
            stats["engine"] = "duckdb"
        except Exception as e:
            return {"error": str(e)}
        return stats

    # ──────────────────────────────────────────────
    # Saglik / tanilama
    # ──────────────────────────────────────────────

    def health(self) -> dict:
        info = {
            "available": self.available,
            "duckdb_installed": _HAVE_DUCKDB,
            "enabled_config": self.enabled,
            "memory_limit": self.memory_limit,
            "threads": self.threads,
        }
        if _HAVE_DUCKDB:
            info["duckdb_version"] = duckdb.__version__
        if self._init_error:
            info["init_error"] = self._init_error
        if self.available:
            try:
                with self._cursor() as cur:
                    row = cur.execute(
                        "SELECT COUNT(*) FROM sqlite_db.scanned_files"
                    ).fetchone()
                    info["scanned_files_rows"] = int(row[0] or 0)
            except Exception as e:
                info["probe_error"] = str(e)
        return info
