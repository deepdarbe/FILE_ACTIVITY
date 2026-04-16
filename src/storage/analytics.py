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
        self._lock = threading.Lock()
        self._conn = None
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
            self._open()
            self.available = True
            logger.info(
                "AnalyticsEngine hazir (DuckDB %s, memory=%s, threads=%d)",
                duckdb.__version__, self.memory_limit, self.threads
            )
        except Exception as e:
            self._init_error = str(e)
            logger.warning("AnalyticsEngine baslatilamadi, SQLite fallback kullanilacak: %s", e)

    def _open(self):
        """DuckDB baglantisini ac, SQLite'i salt-okunur ATTACH et."""
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")

        # sqlite_scanner extension'i yukle (duckdb paketiyle beraber gelir)
        try:
            conn.execute("INSTALL sqlite")
        except Exception:
            # Offline ortamda zaten bundled olabilir; ignore
            pass
        conn.execute("LOAD sqlite")

        # SQLite'i salt-okunur attach et. Bazi surumlerde parametre adi
        # READ_ONLY, digerlerinde read_only olabilir; ikincide fallback dene.
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

        self._conn = conn

    @contextmanager
    def _cursor(self):
        """Thread-safe cursor. DuckDB tek baglanti + lock ile yeterli;
        ATTACH/schema durumunu yeniden kurmak pahali oldugu icin tek baglanti
        tutulur."""
        if not self.available or self._conn is None:
            raise RuntimeError("AnalyticsEngine kullanilamaz durumda")
        with self._lock:
            yield self._conn

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self.available = False

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
