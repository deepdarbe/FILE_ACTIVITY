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
        # Optional callback fired when verify_audit_chain returns
        # verified=False. Wired by the dashboard / service container to
        # forward integrity-break events to syslog/SIEM (issue #50).
        self._audit_break_callback = None

    def set_audit_break_callback(self, callback) -> None:
        """Register a callback invoked on every audit chain verification
        failure. Signature: ``callback(broken_seq: int, reason: str)``.
        Exceptions in the callback are swallowed.
        """
        self._audit_break_callback = callback

    def _notify_audit_break(self, broken_seq, reason: str) -> None:
        cb = self._audit_break_callback
        if cb is None:
            return
        try:
            cb(broken_seq, reason or "")
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Audit-break callback failed: %s", e)

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
            # WAL auto-checkpoint: her 1000 sayfada (default 1000, ~4MB)
            self._local.conn.execute("PRAGMA wal_autocheckpoint=1000")
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

            # Baslangicta WAL checkpoint — buyuk WAL dosyalarini temizle.
            # Strateji: 10 MB-1 GB arasi PASSIVE dene (aktif reader kilitlemesin),
            # 1 GB ustunde TRUNCATE (agresif, WAL dosyasini tamamen sifirlar).
            # Bir musteri 156 GB WAL ile takildi; PASSIVE hic kuculdememisti,
            # TRUNCATE bu durumu surekli olarak onlemek icin.
            try:
                wal_path = self.db_path + "-wal"
                if os.path.exists(wal_path):
                    wal_size = os.path.getsize(wal_path)
                    if wal_size > 10_000_000:  # 10MB'dan buyukse checkpoint yap
                        mode = "TRUNCATE" if wal_size > 1_073_741_824 else "PASSIVE"
                        logger.info(
                            f"WAL checkpoint baslatiliyor ({wal_size / 1048576:.1f} MB, mode={mode})..."
                        )
                        conn.execute(f"PRAGMA wal_checkpoint({mode})")
                        wal_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
                        logger.info(
                            f"WAL checkpoint tamamlandi: {wal_size / 1048576:.1f} MB "
                            f"-> {wal_after / 1048576:.1f} MB"
                        )
                        # PASSIVE kuculmediyse TRUNCATE'e yukselt
                        if mode == "PASSIVE" and wal_after >= wal_size * 0.9 and wal_after > 500_000_000:
                            logger.warning(
                                "PASSIVE checkpoint WAL'i kuculmedi (%.1f MB), TRUNCATE deneniyor",
                                wal_after / 1048576,
                            )
                            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                            wal_after2 = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
                            logger.info(
                                f"TRUNCATE sonucu: {wal_after / 1048576:.1f} MB "
                                f"-> {wal_after2 / 1048576:.1f} MB"
                            )
            except Exception as e:
                logger.warning(f"WAL checkpoint hatasi (kritik degil): {e}")

            # Baslangicta eski scan'leri ve orphan satirlari temizle.
            # Iki ayri kontrol:
            # (1) RETENTION: Her kaynak icin son N scan tutulur (default 3).
            # (2) ORPHAN: scanned_files'ta scan_run'a bagli olmayan satirlari
            #     SIL — eski versiyonlardan kalmis 1.27M+ orphan satir
            #     dashboard'u dakikalarca asiyordu.
            # Orphan kontrolu retention'dan bagimsiz ve hep calisir;
            # tek scan + milyonlarca orphan en kotu durumdur.
            try:
                retention = self.config.get("retention", {}) or {}
                if retention.get("auto_cleanup_on_startup", True):
                    keep_n = int(retention.get("keep_last_n_scans", 3))
                    # Hizli orphan tespiti — eger varsa bunlari da temizleyecegiz
                    # NOT: conn.row_factory = dict_factory oldugu icin
                    # row[0] -> KeyError(0) atar. Sutun adiyla erismek
                    # zorundayiz veya tek-deger SELECT'i icin scalar() yerine
                    # alias + dict erisim. Asagida hep alias kullaniyoruz.
                    orphan_row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM scanned_files "
                        "WHERE scan_id NOT IN (SELECT id FROM scan_runs)"
                    ).fetchone()
                    orphan_count = (orphan_row["cnt"] if orphan_row else 0) or 0

                    count_row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM scan_runs"
                    ).fetchone()
                    total_scans = (count_row["cnt"] if count_row else 0) or 0
                    source_row = conn.execute(
                        "SELECT COUNT(DISTINCT source_id) AS cnt FROM scan_runs"
                    ).fetchone()
                    total_sources = (source_row["cnt"] if source_row else 0) or 0

                    needs_retention = total_scans > keep_n * max(total_sources, 1)
                    needs_orphan = orphan_count > 0

                    if needs_retention or needs_orphan:
                        if needs_retention:
                            logger.info(
                                "Startup retention temizligi: %d scan var, her kaynak icin son %d tutuluyor",
                                total_scans, keep_n,
                            )
                        if needs_orphan:
                            logger.info(
                                "Orphan scanned_files satirlari tespit edildi: %d adet (scan_run'a bagli degil), siliniyor",
                                orphan_count,
                            )
                        result = self.cleanup_old_scans(keep_last_n=keep_n)
                        if result.get("deleted_runs") or result.get("deleted_orphans"):
                            logger.info(
                                "Temizlendi: %d scan_run, %d dosya kaydi (eski scan), %d orphan satir",
                                result.get("deleted_runs", 0),
                                result.get("deleted_files", 0),
                                result.get("deleted_orphans", 0),
                            )
                            try:
                                conn.execute("PRAGMA incremental_vacuum(1000)")
                            except Exception:
                                pass
                        elif "error" in result:
                            logger.warning("Retention temizligi hatasi: %s", result["error"])
            except Exception as e:
                logger.warning("Retention temizligi sirasinda hata (kritik degil): %s", e)

            # Scan summary backfill — summary_json olmayan scan'ler icin
            # hesapla. Dashboard Overview bunu okur, file table'i taramaz.
            # Ilk acilista birkac saniye sürebilir, sonrasi anlik.
            try:
                self.backfill_missing_summaries()
            except Exception as e:
                logger.warning("Summary backfill hatasi (kritik degil): %s", e)

            # Parquet staging orphan replay: kazadan sonra kalmis .parquet
            # dosyalarini SQLite'a ingest et. Dashboard ilk acilmadan once
            # calismali ki kullanicilar eksik kayit gormesin. Stager kendi
            # kosullarini (pyarrow + duckdb + db_path) ic icin kontrol eder.
            try:
                from src.storage.staging import ParquetStager
                # Config Database'e sadece database alt-kismi ile geldigi
                # icin parquet_staging.staging_dir override edilemez burada;
                # default path (data/staging) kullanilir. Tarama sirasinda
                # yazan stager ayni default'u kullaniyor (config orada full).
                stager = ParquetStager(self, {"scanner": {"parquet_staging": {}}})
                if stager.available:
                    stager.replay_orphans()
            except Exception as e:
                logger.warning(
                    "Parquet staging orphan replay basarisiz (kritik degil): %s", e
                )
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
                status          TEXT DEFAULT 'running',
                summary_json    TEXT,
                summary_computed_at TEXT,
                insights_json   TEXT,
                insights_computed_at TEXT
            )
        """)

        # Eski veritabanlari icin ALTER TABLE — summary kolonu yoksa ekle.
        # SQLite'ta IF NOT EXISTS ALTER ADD COLUMN yok, bu yuzden try/except.
        for col_def in (
            "summary_json TEXT",
            "summary_computed_at TEXT",
            "insights_json TEXT",
            "insights_computed_at TEXT",
        ):
            col_name = col_def.split()[0]
            try:
                cur.execute(f"ALTER TABLE scan_runs ADD COLUMN {col_def}")
                logger.info("scan_runs tablosuna %s kolonu eklendi", col_name)
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning("scan_runs ALTER %s hatasi: %s", col_name, e)

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

        # Temel indexler
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_source ON scanned_files(source_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_scan ON scanned_files(scan_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_extension ON scanned_files(extension)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_access ON scanned_files(last_access_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_modify ON scanned_files(last_modify_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_size ON scanned_files(file_size)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_path ON scanned_files(file_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_name_size ON scanned_files(file_name, file_size)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_owner ON scanned_files(owner)")
        # Composite indexler - KRITIK performans (source_id+scan_id tum sorgularda kullanilir)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_source_scan ON scanned_files(source_id, scan_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_src_scan_access ON scanned_files(source_id, scan_id, last_access_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_src_scan_ext ON scanned_files(source_id, scan_id, extension)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_src_scan_size ON scanned_files(source_id, scan_id, file_size)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_src_scan_owner ON scanned_files(source_id, scan_id, owner)")

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

        # Tamper-evident audit log chain (issue #38).
        # Hash-chained mirror of file_audit_events; each row references the
        # previous row's hash so any retroactive UPDATE/DELETE on
        # file_audit_events breaks the chain at re-verification time.
        # Genesis row uses prev_hash = 64*"0".
        # No FK to file_audit_events: we want chain rows to survive even
        # if event ids are reordered or rebased (e.g. WORM export then prune).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_log_chain (
                seq        INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id   INTEGER NOT NULL,
                prev_hash  TEXT NOT NULL,
                row_hash   TEXT NOT NULL,
                signed_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_chain_event ON audit_log_chain(event_id)")

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

        # Content-hash duplicate detection (issue #35).
        # Tiered pipeline persistuje: size -> prefix hash -> full SHA-256.
        # duplicate_hash_groups = gercek icerik kopyalarinin ozeti,
        # duplicate_hash_members = o grubun dosya listesi.
        # Mevcut `duplicate_groups` (file_name+file_size) hizli pre-filter
        # olarak korunur, bu tablo icerik-tabanli kesin eslesme saglar.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_hash_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL,
                waste_size INTEGER NOT NULL,
                computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(scan_id, content_hash, file_size)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_hash_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL REFERENCES duplicate_hash_groups(id) ON DELETE CASCADE,
                file_path TEXT NOT NULL,
                file_id INTEGER
            )
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dhg_scan_waste "
            "ON duplicate_hash_groups(scan_id, waste_size DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dhm_group ON duplicate_hash_members(group_id)"
        )

        # Ransomware alerts (issue #37) — canary access, rename velocity,
        # mass deletion and risky-extension rules persist here. Idempotent.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ransomware_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_id INTEGER,
                username TEXT,
                rule_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                file_count INTEGER,
                sample_paths TEXT,
                details_json TEXT,
                auto_kill_attempted INTEGER DEFAULT 0,
                session_killed INTEGER DEFAULT 0,
                acknowledged_at TIMESTAMP,
                acknowledged_by TEXT
            )
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ransomware_triggered "
            "ON ransomware_alerts(triggered_at DESC)"
        )

        # NTFS ACL snapshots (#49). Per-file DACL rows captured during a
        # scan; one row per ACE so the dashboard can answer "where does
        # this trustee have access?" and "which trustees are
        # over-permissioned?" without re-walking the filesystem.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_acl_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER REFERENCES scan_runs(id) ON DELETE CASCADE,
                file_path TEXT NOT NULL,
                trustee_sid TEXT NOT NULL,
                trustee_name TEXT,
                permissions_mask INTEGER,
                permission_name TEXT,
                is_inherited INTEGER DEFAULT 0,
                ace_type TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_acl_path ON file_acl_snapshots(file_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_acl_trustee ON file_acl_snapshots(trustee_sid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_acl_scan ON file_acl_snapshots(scan_id)")

        # Orphan-SID cache (#56). Memoises AD lookup results keyed by the
        # owner string (SID or DOMAIN\Name) so re-running detect_orphans
        # doesn't hammer AD for SIDs we just checked. resolved=0 means
        # the principal didn't resolve last time we asked; the analyzer
        # rechecks once cache_ttl_minutes have elapsed.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orphan_sid_cache (
                sid TEXT PRIMARY KEY,
                resolved INTEGER NOT NULL DEFAULT 0,
                resolved_name TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_orphan_sid_resolved "
            "ON orphan_sid_cache(resolved)"
        )

        # GDPR PII findings (issue #58). Per-file detection rows produced
        # by ``PiiEngine.scan_source``; each row records a pattern hit
        # count + a redacted sample snippet so an Article 17/30 export
        # can reconstruct "every file mentioning data subject X" without
        # ever persisting raw PII (snippets are masked).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pii_findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER REFERENCES scan_runs(id) ON DELETE CASCADE,
                file_path TEXT NOT NULL,
                pattern_name TEXT NOT NULL,
                hit_count INTEGER NOT NULL,
                sample_snippet TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pii_pattern ON pii_findings(pattern_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pii_path ON pii_findings(file_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pii_scan ON pii_findings(scan_id)")

        # GDPR retention policies (issue #58). Operator-defined rules of
        # the form "files matching <fnmatch> older than <N> days ->
        # archive|delete". Applied in dry-run by default; non-dry-run
        # appends a ``retention_archive`` / ``retention_delete`` row to
        # ``file_audit_events`` for the attestation report.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS retention_policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                pattern_match TEXT,
                retain_days INTEGER NOT NULL,
                action TEXT NOT NULL CHECK (action IN ('archive', 'delete')),
                enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Legal holds (issue #59) — glob-based path freeze registry. Holds
        # block archive / retention / cleanup of matching scanned_files
        # rows. Application code must NEVER DELETE from this table; the
        # only mutation is UPDATE released_at via release_hold().
        cur.execute("""
            CREATE TABLE IF NOT EXISTS legal_holds (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                path_pattern    TEXT NOT NULL,
                reason          TEXT NOT NULL,
                case_reference  TEXT,
                created_by      TEXT NOT NULL,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                released_at     TIMESTAMP,
                released_by     TEXT
            )
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_legal_hold_active "
            "ON legal_holds(released_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_legal_hold_created "
            "ON legal_holds(created_at DESC)"
        )

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

    def update_scan_progress(self, scan_id: int, total_files: int, total_size: int):
        """Tarama sirasinda periyodik ilerleme guncelleme (dashboard aninda gorsun)."""
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE scan_runs SET total_files=?, total_size=?
                WHERE id=? AND status='running'
            """, (total_files, total_size, scan_id))

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
        """Erisim sikligina gore dosyalar (date karsilastirmasi ile - index kullanir)."""
        from datetime import datetime, timedelta
        conditions = ["source_id = ?", "scan_id = ?", "last_access_time IS NOT NULL"]
        params = [source_id, scan_id]

        # julianday yerine date kullan → composite index calisir
        min_date = (datetime.now() - timedelta(days=min_days)).strftime('%Y-%m-%d')
        conditions.append("last_access_time <= ?")
        params.append(min_date)

        if max_days is not None:
            max_date = (datetime.now() - timedelta(days=max_days)).strftime('%Y-%m-%d')
            conditions.append("last_access_time > ?")
            params.append(max_date)

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
        """file_audit_events satiri ekle, lastrowid dondur (chain icin lazim)."""
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO file_audit_events
                (source_id, event_time, event_type, username, file_path, file_name, details, detected_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, event_time, event_type, username, file_path, file_name,
                  json.dumps(details) if details else None, detected_by))
            return cur.lastrowid

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
            return cur.lastrowid

    # ──────────────────────────────────────────────
    # Tamper-evident audit log chain (issue #38)
    # ──────────────────────────────────────────────

    _GENESIS_HASH = "0" * 64

    @staticmethod
    def _canonical_event_json(event_row: dict) -> str:
        """Stable JSON for hashing (sorted keys, no whitespace, all fields).

        ``default=str`` keeps datetime/Decimal/etc. deterministic without
        forcing callers to coerce ahead of time. Result is the canonical
        bytes fed into the SHA-256 chain.
        """
        import json as _json
        return _json.dumps(event_row, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _row_hash(seq: int, event_id: int, prev_hash: str, canonical: str) -> str:
        import hashlib as _hashlib
        payload = f"{seq}|{event_id}|{prev_hash}|{canonical}"
        return _hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _audit_chain_enabled(self) -> bool:
        """Read audit.chain_enabled from outer config (default False).

        ``self.config`` here is the ``database:`` sub-dict, so we walk up
        only if the parent injected the full config under ``_full_config``.
        Otherwise look for a sibling 'audit' key — Database is normally
        constructed with the database sub-dict, so the audit flag must be
        passed via the database dict's "audit" key for this to be True.
        Callers that want to opt-in pass it explicitly via the new
        ``audit_chain_enabled`` constructor flow OR via config injection.
        """
        # Prefer explicit flag set by the application bootstrap.
        if hasattr(self, "_audit_chain_enabled_flag"):
            return bool(self._audit_chain_enabled_flag)
        # Fallback: nested under self.config["audit"] (when caller passes
        # the full app config rather than just the database sub-dict).
        audit_cfg = (self.config or {}).get("audit") or {}
        return bool(audit_cfg.get("chain_enabled", False))

    def set_audit_chain_enabled(self, enabled: bool) -> None:
        """Toggle hash-chain wrapping on insert at runtime."""
        self._audit_chain_enabled_flag = bool(enabled)

    def _append_chain_row(self, event_id: int) -> Optional[dict]:
        """Hash the just-inserted event row and append a chain entry.

        Returns the inserted chain row dict, or None on failure (logged).
        Chain integrity uses the *current* DB representation of the event
        row, which is exactly what verification re-reads later — so any
        post-insert UPDATE will break the chain.
        """
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "SELECT * FROM file_audit_events WHERE id = ?", (event_id,)
                )
                event_row = cur.fetchone()
                if event_row is None:
                    logger.warning("Chain skip: event_id %s not found", event_id)
                    return None

                cur.execute(
                    "SELECT row_hash FROM audit_log_chain "
                    "ORDER BY seq DESC LIMIT 1"
                )
                last = cur.fetchone()
                prev_hash = last["row_hash"] if last else self._GENESIS_HASH

                # Predict next seq (AUTOINCREMENT). We need it as part of
                # the hash payload, so reserve via sqlite_sequence read +1
                # — race-safe because this whole function runs under the
                # cursor's transaction (single writer for SQLite).
                cur.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS s FROM audit_log_chain"
                )
                next_seq = (cur.fetchone()["s"] or 0) + 1

                canonical = self._canonical_event_json(event_row)
                row_hash = self._row_hash(next_seq, event_id, prev_hash, canonical)

                cur.execute(
                    "INSERT INTO audit_log_chain (seq, event_id, prev_hash, row_hash) "
                    "VALUES (?, ?, ?, ?)",
                    (next_seq, event_id, prev_hash, row_hash),
                )
                return {
                    "seq": next_seq,
                    "event_id": event_id,
                    "prev_hash": prev_hash,
                    "row_hash": row_hash,
                }
        except Exception as e:
            logger.warning("audit_log_chain append failed for event %s: %s",
                           event_id, e)
            return None

    def insert_audit_event_chained(self, event_data: dict) -> Optional[int]:
        """Insert an audit event and (if chain enabled) append a chain row.

        ``event_data`` mirrors ``insert_audit_event`` kwargs:
            source_id, event_time, event_type, username, file_path,
            file_name, details, detected_by

        When ``audit.chain_enabled`` is False this is identical to
        ``insert_audit_event`` — same lastrowid, no chain row. Default off.
        """
        event_id = self.insert_audit_event(
            source_id=event_data.get("source_id"),
            event_time=event_data.get("event_time")
            or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            event_type=event_data["event_type"],
            username=event_data.get("username"),
            file_path=event_data.get("file_path"),
            file_name=event_data.get("file_name")
            or (os.path.basename(event_data["file_path"])
                if event_data.get("file_path") else None),
            details=event_data.get("details"),
            detected_by=event_data.get("detected_by", "watcher"),
        )
        if self._audit_chain_enabled() and event_id is not None:
            self._append_chain_row(event_id)
        return event_id

    def verify_audit_chain(self, start_seq: int = 1,
                           end_seq: Optional[int] = None) -> dict:
        """Walk the chain and recompute hashes — returns first break, if any.

        Returns:
            {
                verified: bool,
                total: <rows scanned>,
                broken_at: <seq of first bad row> | None,
                broken_reason: <human string> | None,
            }
        """
        with self.get_cursor() as cur:
            if end_seq is not None:
                cur.execute(
                    "SELECT seq, event_id, prev_hash, row_hash "
                    "FROM audit_log_chain WHERE seq >= ? AND seq <= ? "
                    "ORDER BY seq ASC",
                    (start_seq, end_seq),
                )
            else:
                cur.execute(
                    "SELECT seq, event_id, prev_hash, row_hash "
                    "FROM audit_log_chain WHERE seq >= ? "
                    "ORDER BY seq ASC",
                    (start_seq,),
                )
            chain_rows = list(cur.fetchall())

            total = len(chain_rows)
            if total == 0:
                return {"verified": True, "total": 0,
                        "broken_at": None, "broken_reason": None}

            # If we are not starting from seq=1 we must seed prev_hash from
            # the row just before start_seq. For start_seq=1 the genesis
            # zero-hash applies.
            if start_seq <= 1:
                expected_prev = self._GENESIS_HASH
            else:
                cur.execute(
                    "SELECT row_hash FROM audit_log_chain "
                    "WHERE seq = ?", (start_seq - 1,)
                )
                prev_row = cur.fetchone()
                expected_prev = prev_row["row_hash"] if prev_row else self._GENESIS_HASH

            for chain in chain_rows:
                seq = chain["seq"]
                event_id = chain["event_id"]
                stored_prev = chain["prev_hash"]
                stored_hash = chain["row_hash"]

                if stored_prev != expected_prev:
                    reason = (
                        f"prev_hash mismatch at seq {seq}: "
                        f"expected {expected_prev[:12]}.., "
                        f"got {stored_prev[:12]}.."
                    )
                    self._notify_audit_break(seq, reason)
                    return {
                        "verified": False, "total": total,
                        "broken_at": seq,
                        "broken_reason": reason,
                    }

                cur.execute(
                    "SELECT * FROM file_audit_events WHERE id = ?", (event_id,)
                )
                ev = cur.fetchone()
                if ev is None:
                    reason = f"event_id {event_id} missing from file_audit_events"
                    self._notify_audit_break(seq, reason)
                    return {
                        "verified": False, "total": total,
                        "broken_at": seq,
                        "broken_reason": reason,
                    }
                canonical = self._canonical_event_json(ev)
                recomputed = self._row_hash(seq, event_id, stored_prev, canonical)
                if recomputed != stored_hash:
                    reason = (
                        f"row_hash mismatch at seq {seq} "
                        f"(event {event_id}): event row tampered"
                    )
                    self._notify_audit_break(seq, reason)
                    return {
                        "verified": False, "total": total,
                        "broken_at": seq,
                        "broken_reason": reason,
                    }
                expected_prev = stored_hash

        return {"verified": True, "total": total,
                "broken_at": None, "broken_reason": None}

    def get_audit_chain_page(self, page: int = 1, page_size: int = 100) -> dict:
        """Paginated chain rows joined with events (newest seq first)."""
        page = max(1, int(page))
        page_size = max(1, min(1000, int(page_size)))
        offset = (page - 1) * page_size
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM audit_log_chain")
            total = cur.fetchone()["cnt"]
            cur.execute(
                """
                SELECT c.seq, c.event_id, c.prev_hash, c.row_hash, c.signed_at,
                       e.event_time, e.event_type, e.username,
                       e.file_path, e.file_name, e.source_id, e.detected_by
                FROM audit_log_chain c
                LEFT JOIN file_audit_events e ON e.id = c.event_id
                ORDER BY c.seq DESC
                LIMIT ? OFFSET ?
                """,
                (page_size, offset),
            )
            rows = [dict(r) for r in cur.fetchall()]
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "rows": rows,
        }

    def get_audit_chain_for_export(self, start_date: Optional[str] = None,
                                    end_date: Optional[str] = None) -> list:
        """Chain rows + joined event for WORM export, filtered by event_time."""
        sql = (
            "SELECT c.seq, c.event_id, c.prev_hash, c.row_hash, c.signed_at, "
            "       e.event_time, e.event_type, e.username, e.file_path, "
            "       e.file_name, e.source_id, e.detected_by, e.details "
            "FROM audit_log_chain c "
            "LEFT JOIN file_audit_events e ON e.id = c.event_id "
        )
        conds = []
        params: list = []
        if start_date:
            conds.append("e.event_time >= ?")
            params.append(start_date)
        if end_date:
            conds.append("e.event_time <= ?")
            params.append(end_date)
        if conds:
            sql += "WHERE " + " AND ".join(conds) + " "
        sql += "ORDER BY c.seq ASC"
        with self.get_cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

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
            # Tek CTE ile ozetleri (toplam grup, toplam israf, toplam dosya)
            # ve sayfalanmis gruplari iki ayri sorguda tek aggregate uzerinden
            # alir. Onceki kodda ayni GROUP BY uc kere calisiyordu; buyuk
            # tarama setlerinde bariz yavaslamaya yol aciyordu.
            cur.execute("""
                WITH dup AS (
                    SELECT file_name, file_size, COUNT(*) AS cnt
                    FROM scanned_files
                    WHERE scan_id = ? AND file_size > ?
                    GROUP BY file_name, file_size
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COUNT(*) AS total_groups,
                    COALESCE(SUM(cnt), 0) AS total_files,
                    COALESCE(SUM((cnt - 1) * file_size), 0) AS total_waste
                FROM dup
            """, (scan_id, min_size))
            summary = cur.fetchone()
            total_groups = summary["total_groups"]
            waste_row = {"total_waste": summary["total_waste"],
                         "total_files": summary["total_files"]}

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

    def compute_scan_summary(self, scan_id: int) -> dict:
        """scan_runs.summary_json'a tek bir sorgu dizisiyle KPI'lari yaz.

        Dashboard Overview sayfasi bu JSON'u okur, scanned_files tablosunu
        hic taramaz. 2.5M+ satirli taramalarda da overview saniye alti
        acilir. Summary bir defa hesaplanir, scan sirasinda +1, scan
        bittikten sonra yenilenmez (scan verisi degismez).

        Hesaplanan KPI'lar (v2):
          total_files, total_size, owner_count
          stale_count, stale_size (1+ yil erisilmemis)
          risky_count (.exe .bat .ps1 .vbs .cmd .com .scr .msi .js .wsf)
          large_count (>100MB), large_size
          duplicate_groups, duplicate_waste_size, duplicate_files
          top_extensions (ilk 10, dosya sayisina gore)
          top_owners (ilk 10, boyuta gore)
          age_buckets (0-30, 31-90, 91-180, 181-365, 366+)
          size_buckets (config.analysis.size_buckets)
          extension_size_breakdown (top 20, toplam boyuta gore)
          top_risky_files (top 50 riskli dosya, boyuta gore)
          top_large_files (top 50 buyuk dosya)
          orphan_owner_count (owner NULL veya bos)
          summary_json_version = 2
        """
        from datetime import datetime, timedelta
        summary: dict = {}
        # Riskli uzantilari: dashboard/api.py ile ayni set tutulur
        risky_exts = ("exe", "bat", "ps1", "vbs", "cmd", "com", "scr", "msi", "js", "wsf")
        now_dt = datetime.now()
        stale_cutoff = (now_dt - timedelta(days=365)).strftime('%Y-%m-%d')
        oversized_bytes = 100 * 1024 * 1024

        # Yas kovasi kesim noktalari — last_access_time veya last_modify_time
        # bugunden N gun onceki tarih (yyyy-mm-dd) olarak karsilastirilir.
        age_bucket_defs = [
            ("0-30", 0, 30),
            ("31-90", 31, 90),
            ("91-180", 91, 180),
            ("181-365", 181, 365),
            ("366+", 366, None),
        ]

        def _age_cutoff(days: int) -> str:
            return (now_dt - timedelta(days=days)).strftime('%Y-%m-%d')

        # Boyut kovasi tanimlari — config'ten oku, yoksa DEFAULT_CONFIG ile ayni
        size_buckets_cfg = self._get_size_buckets_config()
        # Siralamayi garanti altina al: deger kucukten buyuge
        sb_sorted = sorted(size_buckets_cfg.items(), key=lambda kv: kv[1])
        # Kovalar: [(label, min_bytes, max_bytes_exclusive_or_None)]
        size_bucket_defs = []
        prev_max = 0
        for label, threshold in sb_sorted:
            size_bucket_defs.append((label, prev_max, threshold))
            prev_max = threshold
        # En ustu "huge" acik ust sinir
        size_bucket_defs.append(("huge", prev_max, None))

        with self.get_cursor() as cur:
            # Temel sayim
            row = cur.execute(
                "SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s, "
                "COUNT(DISTINCT owner) o FROM scanned_files WHERE scan_id=?",
                (scan_id,),
            ).fetchone()
            summary["total_files"] = row["c"]
            summary["total_size"] = row["s"]
            summary["owner_count"] = row["o"]

            # Stale (1+ yil erisilmemis)
            row = cur.execute(
                "SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s "
                "FROM scanned_files WHERE scan_id=? "
                "AND last_access_time IS NOT NULL AND last_access_time <= ?",
                (scan_id, stale_cutoff),
            ).fetchone()
            summary["stale_count"] = row["c"]
            summary["stale_size"] = row["s"]

            # Riskli uzantili dosyalar
            placeholders = ",".join(["?"] * len(risky_exts))
            row = cur.execute(
                f"SELECT COUNT(*) c FROM scanned_files "
                f"WHERE scan_id=? AND extension IN ({placeholders})",
                (scan_id, *risky_exts),
            ).fetchone()
            summary["risky_count"] = row["c"]

            # Buyuk dosyalar
            row = cur.execute(
                "SELECT COUNT(*) c, COALESCE(SUM(file_size),0) s "
                "FROM scanned_files WHERE scan_id=? AND file_size > ?",
                (scan_id, oversized_bytes),
            ).fetchone()
            summary["large_count"] = row["c"]
            summary["large_size"] = row["s"]

            # Duplike gruplar (ayni isim + boyut)
            row = cur.execute(
                """
                SELECT COUNT(*) g, COALESCE(SUM(cnt),0) f,
                       COALESCE(SUM((cnt-1)*file_size),0) w
                FROM (
                    SELECT file_name, file_size, COUNT(*) AS cnt
                    FROM scanned_files
                    WHERE scan_id=? AND file_size > 0
                    GROUP BY file_name, file_size
                    HAVING COUNT(*) > 1
                )
                """,
                (scan_id,),
            ).fetchone()
            summary["duplicate_groups"] = row["g"]
            summary["duplicate_files"] = row["f"]
            summary["duplicate_waste_size"] = row["w"]

            # Top 10 uzanti
            ext_rows = cur.execute(
                "SELECT extension, COUNT(*) c, COALESCE(SUM(file_size),0) s "
                "FROM scanned_files WHERE scan_id=? "
                "GROUP BY extension ORDER BY c DESC LIMIT 10",
                (scan_id,),
            ).fetchall()
            summary["top_extensions"] = [
                {"extension": r["extension"] or "(uzantisiz)",
                 "count": r["c"], "size": r["s"]}
                for r in ext_rows
            ]

            # Top 10 sahip
            owner_rows = cur.execute(
                "SELECT owner, COUNT(*) c, COALESCE(SUM(file_size),0) s "
                "FROM scanned_files WHERE scan_id=? AND owner IS NOT NULL "
                "GROUP BY owner ORDER BY s DESC LIMIT 10",
                (scan_id,),
            ).fetchall()
            summary["top_owners"] = [
                {"owner": r["owner"], "count": r["c"], "size": r["s"]}
                for r in owner_rows
            ]

            # --- v2: Yeni aggregate'ler ---

            # age_buckets: last_access_time NULL ise last_modify_time kullan.
            # Tek bir CASE-WHEN GROUP BY sorgusu.
            age_case_parts = []
            age_params = []
            for label, dmin, dmax in age_bucket_defs:
                # ts >= cutoff_min (en fazla dmin gun once)
                cutoff_max = _age_cutoff(dmin)  # yeni sinir (0 gun -> bugun)
                if dmax is None:
                    # 366+: ts < _age_cutoff(dmin)
                    age_case_parts.append(
                        "WHEN ts IS NOT NULL AND ts < ? THEN ?"
                    )
                    age_params.extend([cutoff_max, label])
                else:
                    cutoff_min = _age_cutoff(dmax + 1)  # bundan eskileri hariç tut
                    # dmin-dmax aralık: cutoff_min < ts <= cutoff_max
                    age_case_parts.append(
                        "WHEN ts IS NOT NULL AND ts > ? AND ts <= ? THEN ?"
                    )
                    age_params.extend([cutoff_min, cutoff_max, label])
            age_case_sql = " ".join(age_case_parts)
            age_rows = cur.execute(
                f"""
                SELECT bucket, COUNT(*) c, COALESCE(SUM(file_size),0) s FROM (
                    SELECT file_size,
                        CASE {age_case_sql} ELSE NULL END AS bucket
                    FROM (
                        SELECT file_size,
                            COALESCE(last_access_time, last_modify_time) AS ts
                        FROM scanned_files WHERE scan_id=?
                    )
                )
                WHERE bucket IS NOT NULL
                GROUP BY bucket
                """,
                (*age_params, scan_id),
            ).fetchall()
            age_counts = {r["bucket"]: (r["c"], r["s"]) for r in age_rows}
            summary["age_buckets"] = []
            for label, dmin, dmax in age_bucket_defs:
                c, s = age_counts.get(label, (0, 0))
                summary["age_buckets"].append({
                    "label": label,
                    "days_min": dmin,
                    "days_max": dmax,
                    "file_count": c,
                    "total_size": s,
                })

            # size_buckets: tek CASE-WHEN GROUP BY
            size_case_parts = []
            size_params = []
            for label, bmin, bmax in size_bucket_defs:
                if bmax is None:
                    size_case_parts.append("WHEN file_size >= ? THEN ?")
                    size_params.extend([bmin, label])
                else:
                    size_case_parts.append(
                        "WHEN file_size >= ? AND file_size < ? THEN ?"
                    )
                    size_params.extend([bmin, bmax, label])
            size_case_sql = " ".join(size_case_parts)
            size_rows = cur.execute(
                f"""
                SELECT bucket, COUNT(*) c, COALESCE(SUM(file_size),0) s FROM (
                    SELECT file_size,
                        CASE {size_case_sql} ELSE NULL END AS bucket
                    FROM scanned_files WHERE scan_id=?
                )
                WHERE bucket IS NOT NULL
                GROUP BY bucket
                """,
                (*size_params, scan_id),
            ).fetchall()
            size_counts = {r["bucket"]: (r["c"], r["s"]) for r in size_rows}
            summary["size_buckets"] = []
            for label, bmin, bmax in size_bucket_defs:
                c, s = size_counts.get(label, (0, 0))
                summary["size_buckets"].append({
                    "label": label,
                    "bytes_min": bmin,
                    "bytes_max": bmax,
                    "file_count": c,
                    "total_size": s,
                })

            # extension_size_breakdown: top 20, toplam boyuta gore
            ext_size_rows = cur.execute(
                "SELECT extension, COUNT(*) c, COALESCE(SUM(file_size),0) s "
                "FROM scanned_files WHERE scan_id=? "
                "GROUP BY extension ORDER BY s DESC LIMIT 20",
                (scan_id,),
            ).fetchall()
            summary["extension_size_breakdown"] = [
                {"extension": r["extension"] or "(uzantisiz)",
                 "count": r["c"], "size": r["s"]}
                for r in ext_size_rows
            ]

            # top_risky_files: boyuta gore en buyuk 50 riskli dosya
            risky_rows = cur.execute(
                f"SELECT file_path, relative_path, file_size, owner, "
                f"last_access_time, extension FROM scanned_files "
                f"WHERE scan_id=? AND extension IN ({placeholders}) "
                f"ORDER BY file_size DESC LIMIT 50",
                (scan_id, *risky_exts),
            ).fetchall()
            summary["top_risky_files"] = [
                {
                    "file_path": r["file_path"],
                    "relative_path": r["relative_path"],
                    "file_size": r["file_size"],
                    "owner": r["owner"],
                    "last_access_time": r["last_access_time"],
                    "extension": r["extension"],
                }
                for r in risky_rows
            ]

            # top_large_files: en buyuk 50 dosya (uzanti farketmez)
            large_rows = cur.execute(
                "SELECT file_path, relative_path, file_size, owner, "
                "last_access_time, extension FROM scanned_files "
                "WHERE scan_id=? ORDER BY file_size DESC LIMIT 50",
                (scan_id,),
            ).fetchall()
            summary["top_large_files"] = [
                {
                    "file_path": r["file_path"],
                    "relative_path": r["relative_path"],
                    "file_size": r["file_size"],
                    "owner": r["owner"],
                    "last_access_time": r["last_access_time"],
                    "extension": r["extension"],
                }
                for r in large_rows
            ]

            # orphan_owner_count: owner NULL veya bos string
            row = cur.execute(
                "SELECT COUNT(*) c FROM scanned_files "
                "WHERE scan_id=? AND (owner IS NULL OR owner='')",
                (scan_id,),
            ).fetchone()
            summary["orphan_owner_count"] = row["c"]

            # Versiyon isareti — backfill bu anahtara bakar
            summary["summary_json_version"] = 2

            # Kaydet (kompakt JSON, ensure_ascii=False)
            now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                "UPDATE scan_runs SET summary_json=?, summary_computed_at=? WHERE id=?",
                (json.dumps(summary, ensure_ascii=False, separators=(",", ":")), now, scan_id),
            )

        summary["scan_id"] = scan_id
        summary["computed_at"] = now
        return summary

    def _get_size_buckets_config(self) -> dict:
        """config.yaml'dan analysis.size_buckets oku. Database __init__'e
        sadece 'database' subsection veriliyor; tam config'i config.yaml'dan
        yuklemeye calis. Yoksa DEFAULT_CONFIG ile ayni varsayilanlari don.
        """
        default = {
            "tiny": 102400,
            "small": 1048576,
            "medium": 104857600,
            "large": 1073741824,
        }
        try:
            # Database init'inde 'analysis' key'i varsa kullan (test yolu)
            analysis = self.config.get("analysis") if isinstance(self.config, dict) else None
            if analysis and isinstance(analysis.get("size_buckets"), dict):
                return analysis["size_buckets"]
        except Exception:
            pass
        try:
            # config.yaml'i disk'ten oku (runtime yolu)
            from src.utils.config_loader import load_config
            cfg_path = self.config.get("_config_path", "config.yaml") if isinstance(self.config, dict) else "config.yaml"
            full = load_config(cfg_path)
            sb = full.get("analysis", {}).get("size_buckets")
            if isinstance(sb, dict) and sb:
                return sb
        except Exception as e:
            logger.debug("size_buckets config yuklenemedi, default kullaniliyor: %s", e)
        return default

    def save_scan_insights(self, scan_id: int, insights_payload: dict) -> None:
        """InsightsEngine cikti'sini scan_runs'a kaydet.

        Payload: {insights: [...], score: N, generated_at, scan_id}
        """
        from datetime import datetime as _dt
        now = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.get_cursor() as cur:
            cur.execute(
                "UPDATE scan_runs SET insights_json=?, insights_computed_at=? WHERE id=?",
                (json.dumps(insights_payload, ensure_ascii=False, default=str), now, scan_id),
            )

    def get_scan_insights(self, scan_id: int) -> Optional[dict]:
        """Kayitli insights'i oku. None ise hic hesaplanmamis."""
        with self.get_cursor() as cur:
            row = cur.execute(
                "SELECT insights_json, insights_computed_at FROM scan_runs WHERE id=?",
                (scan_id,),
            ).fetchone()
        if not row or not row["insights_json"]:
            return None
        try:
            d = json.loads(row["insights_json"])
            d["cached_at"] = row["insights_computed_at"]
            return d
        except Exception:
            return None

    def get_scan_summary(self, scan_id: int) -> Optional[dict]:
        """Kayitli scan summary'yi oku. Hic hesaplanmamissa None doner."""
        with self.get_cursor() as cur:
            row = cur.execute(
                "SELECT summary_json, summary_computed_at FROM scan_runs WHERE id=?",
                (scan_id,),
            ).fetchone()
        if not row or not row["summary_json"]:
            return None
        try:
            d = json.loads(row["summary_json"])
        except Exception:
            return None
        d["scan_id"] = scan_id
        d["computed_at"] = row["summary_computed_at"]
        return d

    def backfill_missing_summaries(self) -> int:
        """summary_json olmayan ya da eski versiyonlu scan'ler icin hesapla.

        Startup'ta calistirilir. Mevcut eski kurulumlarda scan'ler
        summary'siz geldigi icin ilk acilista backfill yapilir (her scan
        icin birkac saniye). Sonrasi acilislar anlik.

        v2: summary_json_version mevcut degilse veya < 2 ise yeniden
        hesaplar — boylece mevcut kurulumlar ilk startup'ta yeni
        aggregate'leri otomatik kazanir.
        """
        current_version = 2
        with self.get_cursor() as cur:
            rows = cur.execute(
                "SELECT id, summary_json FROM scan_runs "
                "WHERE status='completed'"
            ).fetchall()
        pending = []
        for r in rows:
            sj = r["summary_json"]
            if not sj:
                pending.append(r["id"])
                continue
            try:
                parsed = json.loads(sj)
                ver = parsed.get("summary_json_version")
                if not isinstance(ver, int) or ver < current_version:
                    pending.append(r["id"])
            except Exception:
                # Bozuk JSON — yeniden hesapla
                pending.append(r["id"])
        if not pending:
            return 0
        logger.info("Backfill: %d scan icin summary hesaplanacak", len(pending))
        for sid in pending:
            try:
                self.compute_scan_summary(sid)
            except Exception as e:
                logger.warning("Summary backfill hatasi scan %s: %s", sid, e)
        logger.info("Backfill tamamlandi: %d scan", len(pending))
        return len(pending)

    def cleanup_old_scans(self, keep_last_n: int = 5) -> dict:
        """Eski tarama verilerini temizle. Her kaynak icin son N taramayi koru.

        Ek olarak: hangi scan_run'a ait olmayan orphan scanned_files
        satirlarini da siler. Eski versiyonlarin bug'i veya elle yapilan
        scan_run silmeleri 1.27M+ orphan birakabiliyordu.
        """
        deleted_runs = 0
        deleted_files = 0
        deleted_orphans = 0
        try:
            with self.get_cursor() as cur:
                # Her kaynak icin son N tarama disindakileri bul
                cur.execute("SELECT DISTINCT source_id FROM scan_runs")
                source_ids = [r["source_id"] for r in cur.fetchall()]
                for sid in source_ids:
                    cur.execute("""
                        SELECT id FROM scan_runs WHERE source_id=?
                        ORDER BY started_at DESC LIMIT -1 OFFSET ?
                    """, (sid, keep_last_n))
                    old_run_ids = [r["id"] for r in cur.fetchall()]
                    if old_run_ids:
                        placeholders = ','.join(['?'] * len(old_run_ids))
                        cur.execute(f"DELETE FROM scanned_files WHERE scan_id IN ({placeholders})", old_run_ids)
                        deleted_files += cur.rowcount
                        cur.execute(f"DELETE FROM scan_runs WHERE id IN ({placeholders})", old_run_ids)
                        deleted_runs += cur.rowcount

                # Orphan satirlari da temizle (silinmis scan_run'lara ait dosyalar)
                cur.execute(
                    "DELETE FROM scanned_files "
                    "WHERE scan_id NOT IN (SELECT id FROM scan_runs)"
                )
                deleted_orphans = cur.rowcount

            return {
                "deleted_runs": deleted_runs,
                "deleted_files": deleted_files,
                "deleted_orphans": deleted_orphans,
            }
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            return {"error": str(e)}

    def optimize_database(self) -> dict:
        """VACUUM ve ANALYZE ile veritabanini optimize et."""
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            # DB boyutunu al (oncesi)
            conn.execute("SELECT page_count * page_size FROM pragma_page_count, pragma_page_size")
            # WAL + SHM + DB toplam boyut
            wal_path = self.db_path + "-wal"
            shm_path = self.db_path + "-shm"
            size_before = sum(
                os.path.getsize(f) for f in [self.db_path, wal_path, shm_path]
                if os.path.exists(f)
            )
            wal_size_before = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0

            # WAL checkpoint (TRUNCATE modu - WAL dosyasini sifirlar)
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("ANALYZE")
            conn.execute("VACUUM")
            conn.close()

            size_after = sum(
                os.path.getsize(f) for f in [self.db_path, wal_path, shm_path]
                if os.path.exists(f)
            )
            saved = size_before - size_after
            return {
                "status": "ok",
                "size_before": size_before,
                "size_after": size_after,
                "saved": saved,
                "wal_cleared": wal_size_before,
            }
        except Exception as e:
            logger.error(f"Optimize error: {e}")
            return {"error": str(e)}

    def get_db_stats(self) -> dict:
        """Veritabani istatistikleri (WAL/SHM dahil)."""
        try:
            stats = {}
            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            wal_path = self.db_path + "-wal"
            shm_path = self.db_path + "-shm"
            wal_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
            shm_size = os.path.getsize(shm_path) if os.path.exists(shm_path) else 0
            stats["db_size"] = db_size
            stats["wal_size"] = wal_size
            stats["total_disk"] = db_size + wal_size + shm_size
            with self.get_cursor() as cur:
                for table in ["scanned_files", "scan_runs", "archived_files", "user_access_logs", "sources"]:
                    try:
                        cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                        stats[f"{table}_count"] = cur.fetchone()["cnt"]
                    except Exception:
                        stats[f"{table}_count"] = 0
                # En eski ve en yeni tarama
                cur.execute("SELECT MIN(started_at) as oldest, MAX(started_at) as newest FROM scan_runs")
                r = cur.fetchone()
                stats["oldest_scan"] = r["oldest"]
                stats["newest_scan"] = r["newest"]
            return stats
        except Exception as e:
            return {"error": str(e)}

    def health_check(self) -> dict:
        """Veritabani saglik kontrolu."""
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT 1 as ok, datetime('now','localtime') as server_time")
                row = cur.fetchone()
                return {"status": "ok", "server_time": str(row["server_time"])}
        except Exception as e:
            return {"status": "error", "message": str(e)}
