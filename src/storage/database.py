"""SQLite veritabani yonetici modulu.

Tum tablo olusturma, CRUD islemleri ve FTS5 arama bu modulde yapilir.
Tek dosya, sifir bagimlilk - kurulum gerektirmez.
"""

import json
import os
import sqlite3
import logging
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from src.storage.models import (
    Source, ScanRun, ScannedFile, ArchivedFile,
    ArchivePolicy, ScheduledTask
)

logger = logging.getLogger("file_activity.database")


class DatabaseConnectionError(Exception):
    """SQLite baglanti hatasi - kullanici dostu mesaj icin."""
    pass


def dict_factory(cursor, row):
    """SQLite satirlarini dict olarak dondur."""
    fields = [col[0] for col in cursor.description]
    return dict(zip(fields, row))


class Database:
    """SQLite thread-safe veritabani yoneticisi."""

    def __init__(self, config: dict):
        self.config = config
        self.db_path = config.get("path", "data/file_activity.db")
        self.connected = False
        self._local = threading.local()
        self._lock = threading.Lock()

    def _get_conn(self):
        """Thread-local baglanti al veya olustur."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                timeout=30,
                check_same_thread=False
            )
            self._local.conn.row_factory = dict_factory
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn.execute("PRAGMA cache_size=-64000")  # 64MB
        return self._local.conn

    def connect(self):
        """Veritabanini baslat. Dosya yoksa olusturur."""
        try:
            # Dizin yoksa olustur
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

            conn = self._get_conn()
            conn.execute("SELECT 1")
            self.connected = True
            logger.info(f"SQLite baglantisi kuruldu: {self.db_path}")
            self._create_tables()
        except Exception as e:
            self.connected = False
            raise DatabaseConnectionError(
                f"\n"
                f"{'=' * 60}\n"
                f"  SQLite BAGLANTI HATASI\n"
                f"{'=' * 60}\n"
                f"\n"
                f"  Veritabani: {self.db_path}\n"
                f"  Hata:       {str(e)[:100]}\n"
                f"\n"
                f"  COZUM ADIMLARI:\n"
                f"\n"
                f"  1. Dizin yazma izni var mi?\n"
                f"     {os.path.dirname(os.path.abspath(self.db_path))}\n"
                f"\n"
                f"  2. Disk alani yeterli mi?\n"
                f"\n"
                f"  3. Dosya baska process tarafindan kilitli mi?\n"
                f"\n"
                f"  4. config.yaml'da yolu kontrol edin:\n"
                f"     database:\n"
                f"       path: data/file_activity.db\n"
                f"{'=' * 60}\n"
            )

    def try_connect(self):
        """Baglanti dene, basarili/basarisiz dondur (crash etmez)."""
        try:
            self.connect()
            size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            size_mb = size / (1024 * 1024)
            return True, f"SQLite baglantisi basarili. ({self.db_path}, {size_mb:.1f} MB)"
        except DatabaseConnectionError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Beklenmeyen hata: {e}"

    def close(self):
        """Baglantilari kapat."""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
        self.connected = False
        logger.info("SQLite baglantisi kapatildi")

    @contextmanager
    def get_conn(self):
        """Thread-safe baglanti al."""
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    @contextmanager
    def get_cursor(self):
        """Thread-safe cursor al."""
        with self.get_conn() as conn:
            cur = conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    # ──────────────────────────────────────────────
    # Tablo olusturma
    # ──────────────────────────────────────────────

    def _create_tables(self):
        """Tum tablolari olustur."""
        conn = self._get_conn()
        cur = conn.cursor()

        # Kaynaklar
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT UNIQUE NOT NULL,
                unc_path        TEXT NOT NULL,
                archive_dest    TEXT,
                enabled         INTEGER DEFAULT 1,
                created_at      TEXT DEFAULT (datetime('now','localtime')),
                last_scanned_at TEXT
            )
        """)

        # Tarama calistirmalari
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id       INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                started_at      TEXT DEFAULT (datetime('now','localtime')),
                completed_at    TEXT,
                total_files     INTEGER DEFAULT 0,
                total_size      INTEGER DEFAULT 0,
                errors          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'running'
            )
        """)

        # Taranan dosyalar
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scanned_files (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id        INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                scan_id          INTEGER NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
                file_path        TEXT NOT NULL,
                relative_path    TEXT NOT NULL,
                file_name        TEXT NOT NULL,
                extension        TEXT,
                file_size        INTEGER NOT NULL DEFAULT 0,
                creation_time    TEXT,
                last_access_time TEXT,
                last_modify_time TEXT,
                owner            TEXT,
                attributes       INTEGER
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_source ON scanned_files(source_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_scan ON scanned_files(scan_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_extension ON scanned_files(extension)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_access ON scanned_files(last_access_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_modify ON scanned_files(last_modify_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_size ON scanned_files(file_size)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_path ON scanned_files(file_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_name_size ON scanned_files(file_name, file_size)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_owner ON scanned_files(owner)")

        # Arsivlenmis dosyalar
        cur.execute("""
            CREATE TABLE IF NOT EXISTS archived_files (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id        INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                original_path    TEXT NOT NULL,
                relative_path    TEXT NOT NULL,
                archive_path     TEXT NOT NULL,
                file_name        TEXT NOT NULL,
                extension        TEXT,
                file_size        INTEGER NOT NULL DEFAULT 0,
                creation_time    TEXT,
                last_access_time TEXT,
                last_modify_time TEXT,
                owner            TEXT,
                archived_at      TEXT DEFAULT (datetime('now','localtime')),
                archived_by      TEXT,
                restored_at      TEXT,
                checksum         TEXT
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_af_source ON archived_files(source_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_af_original ON archived_files(original_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_af_name ON archived_files(file_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_af_extension ON archived_files(extension)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_af_archived ON archived_files(archived_at)")

        # Migration: operation_id sutunu ekle (v2)
        try:
            cur.execute("ALTER TABLE archived_files ADD COLUMN operation_id INTEGER REFERENCES archive_operations(id)")
        except Exception:
            pass  # Sutun zaten var
        cur.execute("CREATE INDEX IF NOT EXISTS idx_af_operation ON archived_files(operation_id)")

        # Arsiv politikalari
        cur.execute("""
            CREATE TABLE IF NOT EXISTS archive_policies (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT UNIQUE NOT NULL,
                source_id       INTEGER REFERENCES sources(id) ON DELETE SET NULL,
                rules_json      TEXT NOT NULL DEFAULT '[]',
                enabled         INTEGER DEFAULT 1,
                created_at      TEXT DEFAULT (datetime('now','localtime'))
            )
        """)

        # Zamanlanmis gorevler
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type       TEXT NOT NULL,
                source_id       INTEGER REFERENCES sources(id) ON DELETE CASCADE,
                policy_id       INTEGER REFERENCES archive_policies(id) ON DELETE SET NULL,
                cron_expression TEXT,
                enabled         INTEGER DEFAULT 1,
                last_run_at     TEXT,
                next_run_at     TEXT,
                created_at      TEXT DEFAULT (datetime('now','localtime'))
            )
        """)

        # Kullanici erisim loglari
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_access_logs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id        INTEGER REFERENCES sources(id) ON DELETE CASCADE,
                username         TEXT NOT NULL,
                domain           TEXT,
                file_path        TEXT NOT NULL,
                file_name        TEXT,
                extension        TEXT,
                access_type      TEXT NOT NULL,
                access_time      TEXT NOT NULL,
                client_ip        TEXT,
                file_size        INTEGER DEFAULT 0,
                event_id         INTEGER,
                collected_at     TEXT DEFAULT (datetime('now','localtime'))
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_ual_username ON user_access_logs(username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ual_source ON user_access_logs(source_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ual_time ON user_access_logs(access_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ual_type ON user_access_logs(access_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ual_path ON user_access_logs(file_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ual_ext ON user_access_logs(extension)")

        # Kullanici profilleri
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                username           TEXT UNIQUE NOT NULL,
                display_name       TEXT,
                department         TEXT,
                title              TEXT,
                email              TEXT,
                is_service_account INTEGER DEFAULT 0,
                created_at         TEXT DEFAULT (datetime('now','localtime')),
                updated_at         TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_up_dept ON user_profiles(department)")

        # Anomali kayitlari
        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_alerts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                username         TEXT NOT NULL,
                alert_type       TEXT NOT NULL,
                severity         TEXT NOT NULL DEFAULT 'info',
                description      TEXT,
                details_json     TEXT,
                detected_at      TEXT DEFAULT (datetime('now','localtime')),
                acknowledged     INTEGER DEFAULT 0,
                acknowledged_by  TEXT,
                acknowledged_at  TEXT
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_aa_user ON anomaly_alerts(username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aa_severity ON anomaly_alerts(severity)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aa_time ON anomaly_alerts(detected_at)")

        # Dosya denetim olaylari
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                event_time TEXT NOT NULL,
                event_type TEXT NOT NULL,
                username TEXT,
                file_path TEXT NOT NULL,
                file_name TEXT,
                details TEXT,
                detected_by TEXT DEFAULT 'watcher',
                created_at TEXT DEFAULT (datetime('now','localtime')),
                FOREIGN KEY (source_id) REFERENCES sources(id)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_event_time ON file_audit_events(event_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_event_type ON file_audit_events(event_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_username ON file_audit_events(username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_file_path ON file_audit_events(file_path)")

        # Arsiv islem kayitlari
        cur.execute("""
            CREATE TABLE IF NOT EXISTS archive_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_type TEXT NOT NULL,
                source_id INTEGER,
                total_files INTEGER DEFAULT 0,
                total_size INTEGER DEFAULT 0,
                trigger_type TEXT,
                trigger_detail TEXT,
                performed_by TEXT DEFAULT 'system',
                started_at TEXT DEFAULT (datetime('now','localtime')),
                completed_at TEXT,
                status TEXT DEFAULT 'running',
                error_message TEXT,
                files_json TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ao_source ON archive_operations(source_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ao_status ON archive_operations(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ao_started ON archive_operations(started_at)")

        # FTS5 full-text search (arsivlenmis dosyalar icin)
        cur.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS archived_files_fts USING fts5(
                file_name, relative_path, extension,
                content='archived_files',
                content_rowid='id'
            )
        """)

        # FTS trigger'lari
        cur.executescript("""
            CREATE TRIGGER IF NOT EXISTS af_fts_insert AFTER INSERT ON archived_files BEGIN
                INSERT INTO archived_files_fts(rowid, file_name, relative_path, extension)
                VALUES (new.id, new.file_name, new.relative_path, new.extension);
            END;

            CREATE TRIGGER IF NOT EXISTS af_fts_delete AFTER DELETE ON archived_files BEGIN
                INSERT INTO archived_files_fts(archived_files_fts, rowid, file_name, relative_path, extension)
                VALUES ('delete', old.id, old.file_name, old.relative_path, old.extension);
            END;

            CREATE TRIGGER IF NOT EXISTS af_fts_update AFTER UPDATE ON archived_files BEGIN
                INSERT INTO archived_files_fts(archived_files_fts, rowid, file_name, relative_path, extension)
                VALUES ('delete', old.id, old.file_name, old.relative_path, old.extension);
                INSERT INTO archived_files_fts(rowid, file_name, relative_path, extension)
                VALUES (new.id, new.file_name, new.relative_path, new.extension);
            END;
        """)

        conn.commit()
        cur.close()
        logger.info("Veritabani tablolari olusturuldu")

    # ──────────────────────────────────────────────
    # Sources CRUD
    # ──────────────────────────────────────────────

    def add_source(self, source: Source) -> int:
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO sources (name, unc_path, archive_dest, enabled)
                VALUES (?, ?, ?, ?)
            """, (source.name, source.unc_path, source.archive_dest,
                  1 if source.enabled else 0))
            return cur.lastrowid

    def get_sources(self, enabled_only: bool = False) -> list:
        with self.get_cursor() as cur:
            sql = "SELECT * FROM sources"
            if enabled_only:
                sql += " WHERE enabled = 1"
            sql += " ORDER BY name"
            cur.execute(sql)
            rows = cur.fetchall()
            result = []
            for row in rows:
                row["enabled"] = bool(row["enabled"])
                result.append(Source(**row))
            return result

    def get_source_by_name(self, name: str) -> Optional[Source]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM sources WHERE name = ?", (name,))
            row = cur.fetchone()
            if row:
                row["enabled"] = bool(row["enabled"])
                return Source(**row)
            return None

    def get_source_by_id(self, source_id: int) -> Optional[Source]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM sources WHERE id = ?", (source_id,))
            row = cur.fetchone()
            if row:
                row["enabled"] = bool(row["enabled"])
                return Source(**row)
            return None

    def remove_source(self, name: str) -> bool:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM sources WHERE name = ?", (name,))
            return cur.rowcount > 0

    def update_source_last_scanned(self, source_id: int):
        with self.get_cursor() as cur:
            cur.execute(
                "UPDATE sources SET last_scanned_at = datetime('now','localtime') WHERE id = ?",
                (source_id,)
            )

    # ──────────────────────────────────────────────
    # Scan Runs
    # ──────────────────────────────────────────────

    def create_scan_run(self, source_id: int) -> int:
        with self.get_cursor() as cur:
            cur.execute("INSERT INTO scan_runs (source_id) VALUES (?)", (source_id,))
            return cur.lastrowid

    def complete_scan_run(self, scan_id: int, total_files: int, total_size: int,
                          errors: int, status: str = "completed"):
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE scan_runs
                SET completed_at = datetime('now','localtime'), total_files = ?,
                    total_size = ?, errors = ?, status = ?
                WHERE id = ?
            """, (total_files, total_size, errors, status, scan_id))

    def get_scan_runs(self, source_id: Optional[int] = None, limit: int = 20) -> list:
        with self.get_cursor() as cur:
            sql = """SELECT sr.*, s.name as source_name
                     FROM scan_runs sr JOIN sources s ON sr.source_id = s.id"""
            params = []
            if source_id:
                sql += " WHERE sr.source_id = ?"
                params.append(source_id)
            sql += " ORDER BY sr.started_at DESC LIMIT ?"
            params.append(limit)
            cur.execute(sql, params)
            return cur.fetchall()

    def get_latest_scan_id(self, source_id: int, include_running: bool = True) -> Optional[int]:
        """Get latest scan ID. include_running=True allows live analysis during scan."""
        with self.get_cursor() as cur:
            if include_running:
                cur.execute("""
                    SELECT id FROM scan_runs
                    WHERE source_id = ? AND status IN ('completed', 'running')
                    ORDER BY started_at DESC LIMIT 1
                """, (source_id,))
            else:
                cur.execute("""
                    SELECT id FROM scan_runs
                    WHERE source_id = ? AND status = 'completed'
                    ORDER BY completed_at DESC LIMIT 1
                """, (source_id,))
            row = cur.fetchone()
            return row["id"] if row else None

    def get_incomplete_scan(self, source_id: int) -> dict:
        """Get incomplete scan run for resume capability."""
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT id, total_files, total_size FROM scan_runs
                WHERE source_id = ? AND status = 'running'
                ORDER BY started_at DESC LIMIT 1
            """, (source_id,))
            row = cur.fetchone()
            if row:
                return {"scan_id": row["id"], "total_files": row["total_files"], "total_size": row["total_size"]}
        return None

    def get_scanned_paths(self, scan_id: int) -> set:
        """Get set of already-scanned file paths for resume."""
        with self.get_cursor() as cur:
            cur.execute("SELECT file_path FROM scanned_files WHERE scan_id = ?", (scan_id,))
            return {row["file_path"] for row in cur.fetchall()}

    # ──────────────────────────────────────────────
    # Scanned Files
    # ──────────────────────────────────────────────

    def bulk_insert_scanned_files(self, files: list):
        """Toplu dosya ekleme - executemany ile yuksek performans."""
        if not files:
            return
        with self.get_conn() as conn:
            conn.executemany(
                """INSERT INTO scanned_files
                   (source_id, scan_id, file_path, relative_path, file_name,
                    extension, file_size, creation_time, last_access_time,
                    last_modify_time, owner, attributes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(
                    f["source_id"], f["scan_id"], f["file_path"],
                    f["relative_path"], f["file_name"], f.get("extension"),
                    f["file_size"], f.get("creation_time"),
                    f.get("last_access_time"), f.get("last_modify_time"),
                    f.get("owner"), f.get("attributes")
                ) for f in files]
            )

    def get_scanned_files_count(self, source_id: int, scan_id: Optional[int] = None) -> int:
        with self.get_cursor() as cur:
            if scan_id:
                cur.execute(
                    "SELECT COUNT(*) as cnt FROM scanned_files WHERE source_id = ? AND scan_id = ?",
                    (source_id, scan_id)
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) as cnt FROM scanned_files WHERE source_id = ?",
                    (source_id,)
                )
            return cur.fetchone()["cnt"]

    # ──────────────────────────────────────────────
    # Analysis Queries
    # ──────────────────────────────────────────────

    def get_frequency_analysis(self, source_id: int, scan_id: int, day_buckets: list) -> list:
        """Erisim sikligi analizi - her gun kovasi icin dosya sayisi ve boyut."""
        results = []
        with self.get_cursor() as cur:
            for days in sorted(day_buckets):
                cur.execute("""
                    SELECT COUNT(*) as file_count,
                           COALESCE(SUM(file_size), 0) as total_size
                    FROM scanned_files
                    WHERE source_id = ? AND scan_id = ?
                      AND last_access_time < datetime('now', ? || ' days')
                """, (source_id, scan_id, f"-{days}"))
                row = cur.fetchone()
                results.append({
                    "days": days,
                    "label": f"{days}+ gun erisilmemis",
                    "file_count": row["file_count"],
                    "total_size": row["total_size"],
                })
        return results

    def get_type_analysis(self, source_id: int, scan_id: int) -> list:
        """Dosya turu analizi."""
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(extension, 'uzantisiz') as extension,
                    COUNT(*) as file_count,
                    SUM(file_size) as total_size,
                    CAST(AVG(file_size) AS INTEGER) as avg_size,
                    MIN(file_size) as min_size,
                    MAX(file_size) as max_size,
                    MIN(creation_time) as oldest,
                    MAX(creation_time) as newest
                FROM scanned_files
                WHERE source_id = ? AND scan_id = ?
                GROUP BY extension
                ORDER BY total_size DESC
            """, (source_id, scan_id))
            return cur.fetchall()

    def get_size_analysis(self, source_id: int, scan_id: int, buckets: dict) -> list:
        """Boyut dagilimi analizi."""
        results = []
        thresholds = sorted(buckets.items(), key=lambda x: x[1])
        with self.get_cursor() as cur:
            prev = 0
            for label, max_bytes in thresholds:
                cur.execute("""
                    SELECT COUNT(*) as file_count,
                           COALESCE(SUM(file_size), 0) as total_size
                    FROM scanned_files
                    WHERE source_id = ? AND scan_id = ?
                      AND file_size >= ? AND file_size < ?
                """, (source_id, scan_id, prev, max_bytes))
                row = cur.fetchone()
                results.append({
                    "label": label,
                    "min_bytes": prev,
                    "max_bytes": max_bytes,
                    "file_count": row["file_count"],
                    "total_size": row["total_size"],
                })
                prev = max_bytes

            # huge: max_bytes ustu
            cur.execute("""
                SELECT COUNT(*) as file_count,
                       COALESCE(SUM(file_size), 0) as total_size
                FROM scanned_files
                WHERE source_id = ? AND scan_id = ?
                  AND file_size >= ?
            """, (source_id, scan_id, prev))
            row = cur.fetchone()
            results.append({
                "label": "huge",
                "min_bytes": prev,
                "max_bytes": None,
                "file_count": row["file_count"],
                "total_size": row["total_size"],
            })
        return results

    def get_status_summary(self, source_id: int, scan_id: int) -> dict:
        """Kaynak durum ozeti."""
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total_files,
                    COALESCE(SUM(file_size), 0) as total_size,
                    COUNT(DISTINCT extension) as type_count,
                    MIN(creation_time) as oldest_file,
                    MAX(creation_time) as newest_file
                FROM scanned_files
                WHERE source_id = ? AND scan_id = ?
            """, (source_id, scan_id))
            return cur.fetchone()

    def get_files_for_archiving(self, source_id: int, scan_id: int,
                                 access_older_than_days: Optional[int] = None,
                                 modify_older_than_days: Optional[int] = None,
                                 min_size: Optional[int] = None,
                                 max_size: Optional[int] = None,
                                 extensions: Optional[list] = None,
                                 exclude_extensions: Optional[list] = None,
                                 limit: int = 10000) -> list:
        """Arsivleme kriterlerine uyan dosyalari getir."""
        conditions = ["source_id = ?", "scan_id = ?"]
        params = [source_id, scan_id]

        if access_older_than_days:
            conditions.append("last_access_time < datetime('now', ? || ' days')")
            params.append(f"-{access_older_than_days}")

        if modify_older_than_days:
            conditions.append("last_modify_time < datetime('now', ? || ' days')")
            params.append(f"-{modify_older_than_days}")

        if min_size is not None:
            conditions.append("file_size >= ?")
            params.append(min_size)

        if max_size is not None:
            conditions.append("file_size <= ?")
            params.append(max_size)

        if extensions:
            placeholders = ",".join("?" * len(extensions))
            conditions.append(f"extension IN ({placeholders})")
            params.extend(extensions)

        if exclude_extensions:
            placeholders = ",".join("?" * len(exclude_extensions))
            conditions.append(f"extension NOT IN ({placeholders})")
            params.extend(exclude_extensions)

        where = " AND ".join(conditions)
        params.append(limit)

        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT * FROM scanned_files
                WHERE {where}
                ORDER BY last_access_time ASC
                LIMIT ?
            """, params)
            return cur.fetchall()

    # ──────────────────────────────────────────────
    # Archived Files
    # ──────────────────────────────────────────────

    def insert_archived_file(self, data: dict) -> int:
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO archived_files
                (source_id, original_path, relative_path, archive_path,
                 file_name, extension, file_size, creation_time,
                 last_access_time, last_modify_time, owner,
                 archived_by, checksum, operation_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data["source_id"], data["original_path"], data["relative_path"],
                data["archive_path"], data["file_name"], data.get("extension"),
                data["file_size"], data.get("creation_time"),
                data.get("last_access_time"), data.get("last_modify_time"),
                data.get("owner"), data.get("archived_by"), data.get("checksum"),
                data.get("operation_id")
            ))
            return cur.lastrowid

    def mark_restored(self, archive_id: int):
        with self.get_cursor() as cur:
            cur.execute(
                "UPDATE archived_files SET restored_at = datetime('now','localtime') WHERE id = ?",
                (archive_id,)
            )

    def get_archived_file_by_id(self, archive_id: int) -> Optional[dict]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM archived_files WHERE id = ?", (archive_id,))
            return cur.fetchone()

    def get_archived_file_by_path(self, original_path: str) -> Optional[dict]:
        with self.get_cursor() as cur:
            cur.execute(
                """SELECT * FROM archived_files
                   WHERE original_path = ? AND restored_at IS NULL
                   ORDER BY archived_at DESC LIMIT 1""",
                (original_path,)
            )
            return cur.fetchone()

    def search_archived_files(self, query: str, extension: Optional[str] = None,
                               page: int = 1, page_size: int = 50) -> dict:
        """Arsiv indeksinde FTS5 tam metin arama."""
        offset = (page - 1) * page_size
        conditions = []
        params = []

        if query:
            # FTS5 arama
            fts_query = " ".join(f'"{w}"' for w in query.split() if w)
            conditions.append("""
                id IN (SELECT rowid FROM archived_files_fts WHERE archived_files_fts MATCH ?)
            """)
            params.append(fts_query)

        if extension:
            conditions.append("extension = ?")
            params.append(extension.lower().lstrip("."))

        conditions.append("restored_at IS NULL")

        where = " AND ".join(conditions) if conditions else "1=1"

        with self.get_cursor() as cur:
            # Toplam sayi
            cur.execute(f"SELECT COUNT(*) as cnt FROM archived_files WHERE {where}", params)
            total = cur.fetchone()["cnt"]

            # Sayfalanmis sonuclar
            cur.execute(f"""
                SELECT * FROM archived_files
                WHERE {where}
                ORDER BY archived_at DESC
                LIMIT ? OFFSET ?
            """, params + [page_size, offset])
            rows = cur.fetchall()

        return {"total": total, "page": page, "page_size": page_size, "results": rows}

    def get_archive_stats(self) -> dict:
        """Arsiv genel istatistikleri."""
        from src.utils.size_formatter import format_size
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total_archived,
                    SUM(CASE WHEN restored_at IS NOT NULL THEN 1 ELSE 0 END) as total_restored,
                    SUM(CASE WHEN restored_at IS NULL THEN 1 ELSE 0 END) as currently_archived,
                    COALESCE(SUM(CASE WHEN restored_at IS NULL THEN file_size ELSE 0 END), 0) as archived_size,
                    COUNT(DISTINCT source_id) as source_count
                FROM archived_files
            """)
            result = cur.fetchone()
            result["archived_size_formatted"] = format_size(result["archived_size"])
            return result

    # ──────────────────────────────────────────────
    # Policies
    # ──────────────────────────────────────────────

    def add_policy(self, policy: ArchivePolicy) -> int:
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO archive_policies (name, source_id, rules_json, enabled)
                VALUES (?, ?, ?, ?)
            """, (policy.name, policy.source_id, policy.rules_json,
                  1 if policy.enabled else 0))
            return cur.lastrowid

    def get_policies(self) -> list:
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT ap.*, s.name as source_name
                FROM archive_policies ap
                LEFT JOIN sources s ON ap.source_id = s.id
                ORDER BY ap.name
            """)
            rows = cur.fetchall()
            for r in rows:
                r["enabled"] = bool(r["enabled"])
            return rows

    def get_policy_by_name(self, name: str) -> Optional[dict]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM archive_policies WHERE name = ?", (name,))
            return cur.fetchone()

    def remove_policy(self, name: str) -> bool:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM archive_policies WHERE name = ?", (name,))
            return cur.rowcount > 0

    def get_policy_by_id(self, policy_id: int) -> Optional[dict]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM archive_policies WHERE id = ?", (policy_id,))
            return cur.fetchone()

    # ──────────────────────────────────────────────
    # Scheduled Tasks
    # ──────────────────────────────────────────────

    def add_scheduled_task(self, task: ScheduledTask) -> int:
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO scheduled_tasks
                (task_type, source_id, policy_id, cron_expression, enabled)
                VALUES (?, ?, ?, ?, ?)
            """, (task.task_type, task.source_id, task.policy_id,
                  task.cron_expression, 1 if task.enabled else 0))
            return cur.lastrowid

    def get_scheduled_tasks(self, enabled_only: bool = False) -> list:
        with self.get_cursor() as cur:
            sql = """
                SELECT st.*, s.name as source_name, ap.name as policy_name
                FROM scheduled_tasks st
                LEFT JOIN sources s ON st.source_id = s.id
                LEFT JOIN archive_policies ap ON st.policy_id = ap.id
            """
            if enabled_only:
                sql += " WHERE st.enabled = 1"
            sql += " ORDER BY st.created_at"
            cur.execute(sql)
            rows = cur.fetchall()
            for r in rows:
                r["enabled"] = bool(r["enabled"])
            return rows

    def remove_scheduled_task(self, task_id: int) -> bool:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM scheduled_tasks WHERE id = ?", (task_id,))
            return cur.rowcount > 0

    def update_task_run(self, task_id: int, started_at, completed_at, status: str, result: dict):
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE scheduled_tasks
                SET last_run_at = ?
                WHERE id = ?
            """, (str(completed_at), task_id))

    # ──────────────────────────────────────────────
    # User Access Logs
    # ──────────────────────────────────────────────

    def bulk_insert_access_logs(self, logs: list):
        """Toplu erisim logu ekleme."""
        if not logs:
            return
        with self.get_conn() as conn:
            conn.executemany(
                """INSERT INTO user_access_logs
                   (source_id, username, domain, file_path, file_name,
                    extension, access_type, access_time, client_ip,
                    file_size, event_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [(
                    l.get("source_id"), l["username"], l.get("domain"),
                    l["file_path"], l.get("file_name"), l.get("extension"),
                    l["access_type"], l["access_time"], l.get("client_ip"),
                    l.get("file_size", 0), l.get("event_id")
                ) for l in logs]
            )

    def get_top_users(self, source_id: int = None, days: int = 30,
                      limit: int = 20) -> list:
        """En aktif kullanicilari getir."""
        conditions = ["access_time > datetime('now', ? || ' days')"]
        params = [f"-{days}"]
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        where = " AND ".join(conditions)
        params.append(limit)

        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT
                    username,
                    COUNT(*) as access_count,
                    COUNT(DISTINCT file_path) as unique_files,
                    COUNT(DISTINCT DATE(access_time)) as active_days,
                    COALESCE(SUM(file_size), 0) as total_data,
                    SUM(CASE WHEN access_type = 'read' THEN 1 ELSE 0 END) as reads,
                    SUM(CASE WHEN access_type = 'write' THEN 1 ELSE 0 END) as writes,
                    SUM(CASE WHEN access_type = 'delete' THEN 1 ELSE 0 END) as deletes,
                    MIN(access_time) as first_access,
                    MAX(access_time) as last_access
                FROM user_access_logs
                WHERE {where}
                GROUP BY username
                ORDER BY access_count DESC
                LIMIT ?
            """, params)
            return cur.fetchall()

    def get_user_activity(self, username: str, days: int = 30) -> dict:
        """Tek kullanicinin detayli aktivitesi."""
        with self.get_cursor() as cur:
            # Ozet
            cur.execute("""
                SELECT
                    COUNT(*) as total_access,
                    COUNT(DISTINCT file_path) as unique_files,
                    COUNT(DISTINCT DATE(access_time)) as active_days,
                    COALESCE(SUM(file_size), 0) as total_data,
                    SUM(CASE WHEN access_type = 'read' THEN 1 ELSE 0 END) as reads,
                    SUM(CASE WHEN access_type = 'write' THEN 1 ELSE 0 END) as writes,
                    SUM(CASE WHEN access_type = 'delete' THEN 1 ELSE 0 END) as deletes
                FROM user_access_logs
                WHERE username = ? AND access_time > datetime('now', ? || ' days')
            """, (username, f"-{days}"))
            summary = cur.fetchone()

            # Saatlik dagilim
            cur.execute("""
                SELECT CAST(strftime('%H', access_time) AS INTEGER) as hour,
                       COUNT(*) as count
                FROM user_access_logs
                WHERE username = ? AND access_time > datetime('now', ? || ' days')
                GROUP BY hour ORDER BY hour
            """, (username, f"-{days}"))
            hourly = {row["hour"]: row["count"] for row in cur.fetchall()}

            # Gunluk dagilim (haftanin gunleri)
            cur.execute("""
                SELECT CAST(strftime('%w', access_time) AS INTEGER) as dow,
                       COUNT(*) as count
                FROM user_access_logs
                WHERE username = ? AND access_time > datetime('now', ? || ' days')
                GROUP BY dow ORDER BY dow
            """, (username, f"-{days}"))
            daily = {row["dow"]: row["count"] for row in cur.fetchall()}

            # En cok erisilen uzantilar
            cur.execute("""
                SELECT COALESCE(extension, 'diger') as extension,
                       COUNT(*) as count
                FROM user_access_logs
                WHERE username = ? AND access_time > datetime('now', ? || ' days')
                GROUP BY extension ORDER BY count DESC LIMIT 10
            """, (username, f"-{days}"))
            top_extensions = cur.fetchall()

            # En cok erisilen dizinler
            cur.execute("""
                SELECT
                    CASE
                        WHEN INSTR(file_path, '\') > 0 THEN
                            SUBSTR(file_path, 1, LENGTH(file_path) - LENGTH(file_name) - 1)
                        ELSE file_path
                    END as directory,
                    COUNT(*) as count
                FROM user_access_logs
                WHERE username = ? AND access_time > datetime('now', ? || ' days')
                GROUP BY directory ORDER BY count DESC LIMIT 10
            """, (username, f"-{days}"))
            top_dirs = cur.fetchall()

            return {
                "username": username,
                "days": days,
                "summary": summary,
                "hourly_distribution": hourly,
                "daily_distribution": daily,
                "top_extensions": top_extensions,
                "top_directories": top_dirs,
            }

    def get_department_stats(self, days: int = 30) -> list:
        """Departman bazinda aktivite istatistikleri."""
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(up.department, 'Tanimsiz') as department,
                    COUNT(DISTINCT ual.username) as user_count,
                    COUNT(*) as total_access,
                    COUNT(DISTINCT ual.file_path) as unique_files,
                    COALESCE(SUM(ual.file_size), 0) as total_data
                FROM user_access_logs ual
                LEFT JOIN user_profiles up ON ual.username = up.username
                WHERE ual.access_time > datetime('now', ? || ' days')
                GROUP BY department
                ORDER BY total_access DESC
            """, (f"-{days}",))
            return cur.fetchall()

    def get_hourly_heatmap(self, source_id: int = None, days: int = 7) -> list:
        """Saat x Gun erisim heatmap verisi."""
        conditions = ["access_time > datetime('now', ? || ' days')"]
        params = [f"-{days}"]
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        where = " AND ".join(conditions)

        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT
                    CAST(strftime('%w', access_time) AS INTEGER) as dow,
                    CAST(strftime('%H', access_time) AS INTEGER) as hour,
                    COUNT(*) as count
                FROM user_access_logs
                WHERE {where}
                GROUP BY dow, hour
                ORDER BY dow, hour
            """, params)
            return cur.fetchall()

    def get_access_timeline(self, source_id: int = None, days: int = 30) -> list:
        """Gunluk erisim zaman serisi."""
        conditions = ["access_time > datetime('now', ? || ' days')"]
        params = [f"-{days}"]
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        where = " AND ".join(conditions)

        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT
                    DATE(access_time) as date,
                    COUNT(*) as total,
                    SUM(CASE WHEN access_type = 'read' THEN 1 ELSE 0 END) as reads,
                    SUM(CASE WHEN access_type = 'write' THEN 1 ELSE 0 END) as writes,
                    COUNT(DISTINCT username) as unique_users
                FROM user_access_logs
                WHERE {where}
                GROUP BY DATE(access_time)
                ORDER BY date
            """, params)
            return cur.fetchall()

    # ──────────────────────────────────────────────
    # User Profiles
    # ──────────────────────────────────────────────

    def upsert_user_profile(self, username: str, display_name: str = None,
                             department: str = None, title: str = None,
                             email: str = None, is_service: bool = False):
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO user_profiles (username, display_name, department, title, email, is_service_account)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (username) DO UPDATE SET
                    display_name = COALESCE(excluded.display_name, user_profiles.display_name),
                    department = COALESCE(excluded.department, user_profiles.department),
                    title = COALESCE(excluded.title, user_profiles.title),
                    email = COALESCE(excluded.email, user_profiles.email),
                    is_service_account = excluded.is_service_account,
                    updated_at = datetime('now','localtime')
            """, (username, display_name, department, title, email, 1 if is_service else 0))

    def get_user_profiles(self) -> list:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM user_profiles ORDER BY department, username")
            return cur.fetchall()

    # ──────────────────────────────────────────────
    # Anomaly Alerts
    # ──────────────────────────────────────────────

    def insert_anomaly(self, username: str, alert_type: str, severity: str,
                       description: str, details: dict = None):
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO anomaly_alerts (username, alert_type, severity, description, details_json)
                VALUES (?, ?, ?, ?, ?)
            """, (username, alert_type, severity, description,
                  json.dumps(details) if details else None))
            return cur.lastrowid

    def get_anomalies(self, severity: str = None, days: int = 7,
                      acknowledged: bool = None) -> list:
        conditions = ["detected_at > datetime('now', ? || ' days')"]
        params = [f"-{days}"]
        if severity:
            conditions.append("severity = ?")
            params.append(severity)
        if acknowledged is not None:
            conditions.append("acknowledged = ?")
            params.append(1 if acknowledged else 0)
        where = " AND ".join(conditions)

        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT * FROM anomaly_alerts
                WHERE {where}
                ORDER BY detected_at DESC
            """, params)
            return cur.fetchall()

    def acknowledge_anomaly(self, anomaly_id: int, by_user: str):
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE anomaly_alerts
                SET acknowledged = 1, acknowledged_by = ?, acknowledged_at = datetime('now','localtime')
                WHERE id = ?
            """, (by_user, anomaly_id))

    def get_anomaly_summary(self) -> dict:
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN severity = 'critical' AND NOT acknowledged THEN 1 ELSE 0 END) as critical_open,
                    SUM(CASE WHEN severity = 'warning' AND NOT acknowledged THEN 1 ELSE 0 END) as warning_open,
                    SUM(CASE WHEN severity = 'info' AND NOT acknowledged THEN 1 ELSE 0 END) as info_open,
                    SUM(CASE WHEN acknowledged THEN 1 ELSE 0 END) as acknowledged
                FROM anomaly_alerts
                WHERE detected_at > datetime('now', '-30 days')
            """)
            return cur.fetchone()

    # ──────────────────────────────────────────────
    # Drill-down Queries
    # ──────────────────────────────────────────────

    def get_file_owners_stats(self, source_id: int, scan_id: int = None) -> list:
        """Dosya sahiplik istatistikleri (owner GROUP BY)."""
        if not scan_id:
            scan_id = self.get_latest_scan_id(source_id)
        if not scan_id:
            return []
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(owner, 'Bilinmiyor') as owner,
                    COUNT(*) as file_count,
                    COALESCE(SUM(file_size), 0) as total_size
                FROM scanned_files
                WHERE source_id = ? AND scan_id = ?
                GROUP BY owner
                ORDER BY total_size DESC
            """, (source_id, scan_id))
            return cur.fetchall()

    def get_files_by_owner(self, source_id: int, scan_id: int, owner: str,
                           limit: int = 100, offset: int = 0) -> dict:
        """Sahibe gore dosya listesi (sayfalanmis)."""
        owner_cond = "owner IS NULL" if owner in ("Bilinmiyor", None, "") else "owner = ?"
        params_base = [source_id, scan_id]
        if owner not in ("Bilinmiyor", None, ""):
            params_base.append(owner)
        where = f"source_id = ? AND scan_id = ? AND {owner_cond}"

        with self.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM scanned_files WHERE {where}", params_base)
            total = cur.fetchone()["cnt"]
            cur.execute(f"""
                SELECT * FROM scanned_files WHERE {where}
                ORDER BY file_size DESC LIMIT ? OFFSET ?
            """, params_base + [limit, offset])
            files = [dict(r) for r in cur.fetchall()]
        return {"total": total, "files": files}

    def get_files_by_frequency(self, source_id: int, scan_id: int,
                                min_days: int, max_days: int = None,
                                limit: int = 100, offset: int = 0) -> dict:
        """Erisim sikligina gore dosyalar (julianday kullanarak)."""
        conditions = ["source_id = ?", "scan_id = ?", "last_access_time IS NOT NULL"]
        params = [source_id, scan_id]

        conditions.append("julianday('now','localtime') - julianday(last_access_time) >= ?")
        params.append(min_days)

        if max_days is not None:
            conditions.append("julianday('now','localtime') - julianday(last_access_time) < ?")
            params.append(max_days)

        where = " AND ".join(conditions)

        with self.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM scanned_files WHERE {where}", params)
            total = cur.fetchone()["cnt"]
            cur.execute(f"""
                SELECT * FROM scanned_files WHERE {where}
                ORDER BY last_access_time ASC LIMIT ? OFFSET ?
            """, params + [limit, offset])
            files = [dict(r) for r in cur.fetchall()]
        return {"total": total, "files": files}

    def get_files_by_extension(self, source_id: int, scan_id: int, extension: str,
                                limit: int = 100, offset: int = 0) -> dict:
        """Uzantiya gore dosyalar (sayfalanmis)."""
        ext = extension.lower().lstrip(".")
        ext_cond = "extension IS NULL" if ext in ("uzantisiz", "") else "extension = ?"
        params_base = [source_id, scan_id]
        if ext not in ("uzantisiz", ""):
            params_base.append(ext)
        where = f"source_id = ? AND scan_id = ? AND {ext_cond}"

        with self.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM scanned_files WHERE {where}", params_base)
            total = cur.fetchone()["cnt"]
            cur.execute(f"""
                SELECT * FROM scanned_files WHERE {where}
                ORDER BY file_size DESC LIMIT ? OFFSET ?
            """, params_base + [limit, offset])
            files = [dict(r) for r in cur.fetchall()]
        return {"total": total, "files": files}

    def get_files_by_size_range(self, source_id: int, scan_id: int,
                                 min_bytes: int, max_bytes: int = None,
                                 limit: int = 100, offset: int = 0) -> dict:
        """Boyut araligina gore dosyalar (sayfalanmis)."""
        conditions = ["source_id = ?", "scan_id = ?", "file_size >= ?"]
        params = [source_id, scan_id, min_bytes]

        if max_bytes is not None:
            conditions.append("file_size < ?")
            params.append(max_bytes)

        where = " AND ".join(conditions)

        with self.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM scanned_files WHERE {where}", params)
            total = cur.fetchone()["cnt"]
            cur.execute(f"""
                SELECT * FROM scanned_files WHERE {where}
                ORDER BY file_size DESC LIMIT ? OFFSET ?
            """, params + [limit, offset])
            files = [dict(r) for r in cur.fetchall()]
        return {"total": total, "files": files}

    def upsert_scanned_file(self, source_id: int, scan_id: int, file_data: dict):
        """Watcher icin tekil dosya ekleme/guncelleme (INSERT OR REPLACE)."""
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT OR REPLACE INTO scanned_files
                (source_id, scan_id, file_path, relative_path, file_name,
                 extension, file_size, creation_time, last_access_time,
                 last_modify_time, owner, attributes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                source_id, scan_id, file_data["file_path"],
                file_data.get("relative_path", ""), file_data.get("file_name", ""),
                file_data.get("extension"), file_data.get("file_size", 0),
                file_data.get("creation_time"), file_data.get("last_access_time"),
                file_data.get("last_modify_time"), file_data.get("owner"),
                file_data.get("attributes")
            ))

    def delete_scanned_file(self, source_id: int, scan_id: int, file_path: str):
        """Watcher icin silinen dosya kaydini kaldir."""
        with self.get_cursor() as cur:
            cur.execute("""
                DELETE FROM scanned_files
                WHERE source_id = ? AND scan_id = ? AND file_path = ?
            """, (source_id, scan_id, file_path))

    def has_access_log_data(self) -> bool:
        """Event log verisi var mi kontrolu."""
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM user_access_logs LIMIT 1")
            return cur.fetchone()["cnt"] > 0

    # ──────────────────────────────────────────────
    # File Audit Events
    # ──────────────────────────────────────────────

    def insert_audit_event(self, source_id, event_time, event_type, username,
                           file_path, file_name, details=None, detected_by='watcher'):
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO file_audit_events
                (source_id, event_time, event_type, username, file_path, file_name, details, detected_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, event_time, event_type, username, file_path, file_name,
                  json.dumps(details) if details else None, detected_by))

    def get_audit_events(self, source_id=None, event_type=None, username=None,
                         days=7, page=1, page_size=100):
        conditions = ["event_time > datetime('now', ? || ' days')"]
        params = [f"-{days}"]
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type)
        if username:
            conditions.append("username = ?")
            params.append(username)

        where = " AND ".join(conditions)
        offset = (page - 1) * page_size

        with self.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM file_audit_events WHERE {where}", params)
            total = cur.fetchone()["cnt"]

            cur.execute(f"""
                SELECT * FROM file_audit_events WHERE {where}
                ORDER BY event_time DESC LIMIT ? OFFSET ?
            """, params + [page_size, offset])
            events = [dict(r) for r in cur.fetchall()]

        return {"total": total, "events": events, "page": page}

    def get_audit_summary(self, source_id=None, days=7):
        cond = "WHERE event_time > datetime('now', ? || ' days')"
        params = [f"-{days}"]
        if source_id:
            cond += " AND source_id = ?"
            params.append(source_id)

        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT event_type, COUNT(*) as cnt
                FROM file_audit_events {cond}
                GROUP BY event_type ORDER BY cnt DESC
            """, params)
            by_type = [dict(r) for r in cur.fetchall()]

            cur.execute(f"""
                SELECT username, COUNT(*) as cnt,
                       SUM(CASE WHEN event_type='delete' THEN 1 ELSE 0 END) as deletes
                FROM file_audit_events {cond}
                GROUP BY username ORDER BY cnt DESC LIMIT 20
            """, params)
            by_user = [dict(r) for r in cur.fetchall()]

            cur.execute(f"SELECT COUNT(*) as cnt FROM file_audit_events {cond}", params)
            total = cur.fetchone()["cnt"]

        return {"total": total, "by_type": by_type, "by_user": by_user}

    # ──────────────────────────────────────────────
    # Archive Operations
    # ──────────────────────────────────────────────

    def create_archive_operation(self, op_type, source_id, trigger_type,
                                  trigger_detail, performed_by='system'):
        """Yeni arsiv islemi olustur, op_id dondur."""
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO archive_operations
                (operation_type, source_id, trigger_type, trigger_detail, performed_by)
                VALUES (?, ?, ?, ?, ?)
            """, (op_type, source_id, trigger_type, trigger_detail, performed_by))
            return cur.lastrowid

    def complete_archive_operation(self, op_id, total_files, total_size,
                                    status='completed', error=None, files_json=None):
        """Arsiv islemini tamamla."""
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE archive_operations
                SET total_files=?, total_size=?, status=?, error_message=?,
                    files_json=?, completed_at=datetime('now','localtime')
                WHERE id=?
            """, (total_files, total_size, status, error,
                  json.dumps(files_json) if files_json else None, op_id))

    def get_archive_operations(self, source_id=None, limit=50):
        """Arsiv islem gecmisini getir."""
        conditions = []
        params = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)
        with self.get_cursor() as cur:
            cur.execute(f"""
                SELECT * FROM archive_operations {where}
                ORDER BY started_at DESC LIMIT ?
            """, params)
            return [dict(r) for r in cur.fetchall()]

    def get_archive_operation_detail(self, op_id):
        """Arsiv islemi detayi + iliskili dosyalar."""
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM archive_operations WHERE id=?", (op_id,))
            operation = cur.fetchone()
            if not operation:
                return None
            operation = dict(operation)

            # Oncelikle operation_id ile dene (yeni yontem)
            cur.execute("""
                SELECT id, file_name, original_path, archive_path, file_size, archived_at, restored_at
                FROM archived_files
                WHERE operation_id=?
                ORDER BY archived_at DESC LIMIT 500
            """, (op_id,))
            files = [dict(r) for r in cur.fetchall()]

            # Fallback: eski kayitlar icin zaman araligini kullan
            if not files and operation.get("started_at") and operation.get("source_id"):
                end_time = operation.get("completed_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur.execute("""
                    SELECT id, file_name, original_path, archive_path, file_size, archived_at, restored_at
                    FROM archived_files
                    WHERE source_id=? AND archived_at >= ? AND archived_at <= ?
                    ORDER BY archived_at DESC LIMIT 500
                """, (operation["source_id"], operation["started_at"], end_time))
                files = [dict(r) for r in cur.fetchall()]

            operation["files"] = files
            return operation

    def get_archive_operation_files(self, op_id, page=1, page_size=100):
        """Arsiv islemindeki dosyalari sayfalanmis getir."""
        offset = (page - 1) * page_size
        with self.get_cursor() as cur:
            # Toplam sayi
            cur.execute("SELECT COUNT(*) as cnt FROM archived_files WHERE operation_id=?", (op_id,))
            total = cur.fetchone()["cnt"]

            # Fallback: eski kayitlar icin
            if total == 0:
                cur.execute("SELECT * FROM archive_operations WHERE id=?", (op_id,))
                op = cur.fetchone()
                if op and op.get("started_at") and op.get("source_id"):
                    end_time = op.get("completed_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute("""
                        SELECT COUNT(*) as cnt FROM archived_files
                        WHERE source_id=? AND archived_at >= ? AND archived_at <= ?
                    """, (op["source_id"], op["started_at"], end_time))
                    total = cur.fetchone()["cnt"]
                    cur.execute("""
                        SELECT id, file_name, original_path, archive_path, file_size,
                               owner, archived_at, restored_at
                        FROM archived_files
                        WHERE source_id=? AND archived_at >= ? AND archived_at <= ?
                        ORDER BY archived_at DESC LIMIT ? OFFSET ?
                    """, (op["source_id"], op["started_at"], end_time, page_size, offset))
                    files = [dict(r) for r in cur.fetchall()]
                    return {"total": total, "page": page, "page_size": page_size, "files": files}

            cur.execute("""
                SELECT id, file_name, original_path, archive_path, file_size,
                       owner, archived_at, restored_at
                FROM archived_files
                WHERE operation_id=?
                ORDER BY archived_at DESC LIMIT ? OFFSET ?
            """, (op_id, page_size, offset))
            files = [dict(r) for r in cur.fetchall()]
            return {"total": total, "page": page, "page_size": page_size, "files": files}

    def get_archive_history(self, source_id=None, page=1, page_size=20,
                            date_from=None, date_to=None, op_type=None):
        """Sayfalanmis arsiv islem gecmisi."""
        conditions = []
        params = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if date_from:
            conditions.append("started_at >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("started_at <= ?")
            params.append(date_to + " 23:59:59")
        if op_type:
            conditions.append("operation_type = ?")
            params.append(op_type)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        offset = (page - 1) * page_size

        with self.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM archive_operations {where}", params)
            total = cur.fetchone()["cnt"]

            cur.execute(f"""
                SELECT * FROM archive_operations {where}
                ORDER BY started_at DESC LIMIT ? OFFSET ?
            """, params + [page_size, offset])
            rows = [dict(r) for r in cur.fetchall()]

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "operations": rows
        }

    def insert_audit_event_simple(self, source_id, event_type, username,
                                   file_path, details=None, detected_by='system'):
        """Basitletirilmis audit event ekleme (event_time otomatik)."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name = os.path.basename(file_path) if file_path else None
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO file_audit_events
                (source_id, event_time, event_type, username, file_path, file_name, details, detected_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, now, event_type, username, file_path, file_name,
                  details, detected_by))

    # ──────────────────────────────────────────────
    # Duplike Analiz
    # ──────────────────────────────────────────────

    def get_duplicate_groups(self, source_id, scan_id=None, min_size=0,
                             page=1, page_size=50):
        """Kopya dosya gruplarini sayfalanmis getir (isim+boyut eslesmesi)."""
        offset = (page - 1) * page_size

        # Son scan_id'yi bul
        if not scan_id:
            with self.get_cursor() as cur:
                cur.execute("""
                    SELECT id FROM scan_runs WHERE source_id=? AND status='completed'
                    ORDER BY started_at DESC LIMIT 1
                """, (source_id,))
                row = cur.fetchone()
                if not row:
                    return {"total_groups": 0, "total_waste_size": 0, "total_files": 0,
                            "groups": [], "page": page, "page_size": page_size, "total_pages": 1}
                scan_id = row["id"]

        with self.get_cursor() as cur:
            # Toplam grup sayisi
            cur.execute("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT file_name, file_size
                    FROM scanned_files
                    WHERE scan_id=? AND file_size > ?
                    GROUP BY file_name, file_size
                    HAVING COUNT(*) > 1
                )
            """, (scan_id, min_size))
            total_groups = cur.fetchone()["cnt"]

            # Toplam israf
            cur.execute("""
                SELECT COALESCE(SUM((cnt - 1) * file_size), 0) as total_waste,
                       COALESCE(SUM(cnt), 0) as total_files
                FROM (
                    SELECT file_name, file_size, COUNT(*) as cnt
                    FROM scanned_files
                    WHERE scan_id=? AND file_size > ?
                    GROUP BY file_name, file_size
                    HAVING COUNT(*) > 1
                )
            """, (scan_id, min_size))
            waste_row = cur.fetchone()

            # Sayfalanmis gruplar (en cok israf eden grup once)
            cur.execute("""
                SELECT file_name, file_size, COUNT(*) as cnt,
                       (COUNT(*) - 1) * file_size as waste_size
                FROM scanned_files
                WHERE scan_id=? AND file_size > ?
                GROUP BY file_name, file_size
                HAVING COUNT(*) > 1
                ORDER BY waste_size DESC
                LIMIT ? OFFSET ?
            """, (scan_id, min_size, page_size, offset))
            groups_raw = cur.fetchall()

            # Her grup icin dosyalari getir
            groups = []
            for g in groups_raw:
                cur.execute("""
                    SELECT id, file_path, relative_path, owner, last_access_time, last_modify_time
                    FROM scanned_files
                    WHERE scan_id=? AND file_name=? AND file_size=?
                    ORDER BY last_modify_time DESC
                """, (scan_id, g["file_name"], g["file_size"]))
                files = [dict(r) for r in cur.fetchall()]
                groups.append({
                    "file_name": g["file_name"],
                    "file_size": g["file_size"],
                    "count": g["cnt"],
                    "waste_size": g["waste_size"],
                    "files": files
                })

            total_pages = max(1, -(-total_groups // page_size))
            return {
                "total_groups": total_groups,
                "total_waste_size": waste_row["total_waste"],
                "total_files": waste_row["total_files"],
                "groups": groups,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "scan_id": scan_id
            }

    # ──────────────────────────────────────────────
    # Buyume Istatistikleri
    # ──────────────────────────────────────────────

    def get_growth_stats(self, source_id):
        """Yillik, aylik, gunluk buyume istatistikleri."""
        with self.get_cursor() as cur:
            # Yillik buyume
            cur.execute("""
                SELECT strftime('%Y', started_at) as year,
                       MAX(total_files) as total_files,
                       MAX(total_size) as total_size
                FROM scan_runs
                WHERE source_id=? AND status='completed'
                GROUP BY strftime('%Y', started_at)
                ORDER BY year
            """, (source_id,))
            yearly = [dict(r) for r in cur.fetchall()]

            # Aylik buyume (son 24 ay)
            cur.execute("""
                SELECT strftime('%Y-%m', started_at) as month,
                       MAX(total_files) as total_files,
                       MAX(total_size) as total_size
                FROM scan_runs
                WHERE source_id=? AND status='completed'
                GROUP BY strftime('%Y-%m', started_at)
                ORDER BY month DESC LIMIT 24
            """, (source_id,))
            monthly = list(reversed([dict(r) for r in cur.fetchall()]))

            # Gunluk buyume (son 30 gun)
            cur.execute("""
                SELECT strftime('%Y-%m-%d', started_at) as day,
                       MAX(total_files) as total_files,
                       MAX(total_size) as total_size
                FROM scan_runs
                WHERE source_id=? AND status='completed'
                GROUP BY strftime('%Y-%m-%d', started_at)
                ORDER BY day DESC LIMIT 30
            """, (source_id,))
            daily = list(reversed([dict(r) for r in cur.fetchall()]))

            # Toplam tarama sayisi
            cur.execute("""
                SELECT COUNT(*) as cnt FROM scan_runs
                WHERE source_id=? AND status='completed'
            """, (source_id,))
            total_scans = cur.fetchone()["cnt"]

            return {
                "yearly": yearly,
                "monthly": monthly,
                "daily": daily,
                "total_scans": total_scans
            }

    def get_top_file_creators(self, source_id, scan_id=None, limit=20):
        """En cok dosya olusturan kullanicilar (owner bazli)."""
        # Son scan_id'yi bul
        if not scan_id:
            with self.get_cursor() as cur:
                cur.execute("""
                    SELECT id FROM scan_runs WHERE source_id=? AND status='completed'
                    ORDER BY started_at DESC LIMIT 1
                """, (source_id,))
                row = cur.fetchone()
                if not row:
                    return []
                scan_id = row["id"]

        with self.get_cursor() as cur:
            # Toplam dosya sayisi
            cur.execute("SELECT COUNT(*) as total FROM scanned_files WHERE scan_id=?", (scan_id,))
            total_files = cur.fetchone()["total"]

            cur.execute("""
                SELECT owner, COUNT(*) as file_count,
                       SUM(file_size) as total_size
                FROM scanned_files
                WHERE scan_id=? AND owner IS NOT NULL AND owner != ''
                GROUP BY owner
                ORDER BY file_count DESC
                LIMIT ?
            """, (scan_id, limit))
            creators = [dict(r) for r in cur.fetchall()]

            for c in creators:
                c["percentage"] = (c["file_count"] / total_files * 100) if total_files > 0 else 0

            return creators

    def health_check(self) -> dict:
        """Veritabani saglik kontrolu."""
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT 1 as ok, datetime('now','localtime') as server_time")
                row = cur.fetchone()
                return {"status": "ok", "server_time": str(row["server_time"])}
        except Exception as e:
            return {"status": "error", "message": str(e)}
