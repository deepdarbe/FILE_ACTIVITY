"""FastAPI Web Dashboard.

Tum raporlama, kaynak yonetimi, arsiv ve zamanlama islemlerini
web arayuzu uzerinden sunar.

ONEMLI: Tum API endpoint'leri source_id (integer) kullanir,
source_name veya UNC path URL'de KULLANILMAZ (encoding sorunlari).
"""

import os
import json
import logging
import threading
import uuid
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List

# ── Arka plan export kuyrugu ──
_export_jobs = {}  # job_id -> {status, progress, file_path, error, created_at, ...}
_export_lock = threading.Lock()

logger = logging.getLogger("file_activity.dashboard")


# --- Issue #82 (Bug 1): /api/system/open-folder dual-behaviour helper ---
#
# The dashboard is typically served from a Windows file server while users
# browse it from their own workstations. Calling `subprocess.Popen(["explorer",
# ...])` server-side opens a window on the *server*, which is invisible to the
# remote user and makes the "Konuma Git" buttons look broken.
#
# This helper implements two modes:
#   * Local client (127.0.0.1 / ::1 / localhost): spawn Explorer natively and
#     return {"success": True, "mode": "native"}.
#   * Remote client: do NOT touch subprocess. Return HTTP 200 with
#     {"success": False, "mode": "remote_client", ...} so the frontend can
#     copy the path to the user's clipboard and surface a friendly hint.
#
# Path resolution (`os.path.realpath(os.path.normpath(...))`) and the
# `shell=False` argv-list Popen form are preserved for security. Missing paths
# still produce HTTP 404 via HTTPException.

_LOCAL_CLIENT_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


def open_folder_impl(body: dict, client_host: str, popen=None):
    """Run the open-folder decision logic.

    Pure-ish helper shared by the HTTP endpoint and the unit tests. Returns
    the JSON-serialisable response dict on success, or raises HTTPException
    for invalid input / missing paths. ``popen`` lets tests inject a stub in
    place of ``subprocess.Popen``.
    """
    if popen is None:
        import subprocess
        popen = subprocess.Popen

    folder = body.get("path", "") if isinstance(body, dict) else ""
    if not folder or not isinstance(folder, str):
        raise HTTPException(400, "path gerekli")

    # Guvenlik: normalize + gercek yol cozumleme (symlink/junction eskape koruma)
    folder = os.path.realpath(os.path.normpath(folder))
    if not (os.path.isdir(folder) or os.path.isfile(folder)):
        raise HTTPException(404, f"Dizin bulunamadi: {folder}")

    is_local = client_host in _LOCAL_CLIENT_HOSTS
    if not is_local:
        return {
            "success": False,
            "mode": "remote_client",
            "path": folder,
            "hint": (
                "Explorer cannot be opened on the server for a remote "
                "client. Use the copied path on your own machine."
            ),
        }

    # Yerel istemci: dosya/dizine gore Explorer'i acar.
    # shell=False ile argv listesi kullanilarak komut enjeksiyonu onlenir.
    if os.path.isdir(folder):
        popen(["explorer", folder], shell=False)
    else:  # os.path.isfile(folder)
        popen(["explorer", "/select,", folder], shell=False)
    return {"success": True, "mode": "native", "path": folder}


# --- Pydantic Models ---

class SourceCreate(BaseModel):
    name: str
    unc_path: str
    archive_dest: Optional[str] = None

class PolicyCreate(BaseModel):
    name: str
    source_id: Optional[int] = None
    access_days: Optional[int] = None
    modify_days: Optional[int] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    extensions: Optional[List[str]] = None
    exclude_extensions: Optional[List[str]] = None

class ScheduleCreate(BaseModel):
    task_type: str
    source_id: int
    policy_name: Optional[str] = None
    cron_expression: str

    @field_validator("task_type")
    @classmethod
    def _validate_task_type(cls, v: str) -> str:
        # Issue #38: 'audit_export' added for scheduled WORM export.
        if v not in ("scan", "archive", "notify_users", "audit_export"):
            raise ValueError(
                "task_type must be 'scan', 'archive', 'notify_users' or 'audit_export'"
            )
        return v

    @field_validator("cron_expression")
    @classmethod
    def _validate_cron(cls, v: str) -> str:
        parts = v.strip().split()
        if len(parts) != 5:
            raise ValueError("cron_expression must have 5 fields (minute hour day month dow)")
        try:
            from apscheduler.triggers.cron import CronTrigger
            CronTrigger(
                minute=parts[0], hour=parts[1],
                day=parts[2], month=parts[3],
                day_of_week=parts[4],
            )
        except Exception as e:
            raise ValueError(f"invalid cron expression: {e}")
        return v.strip()

class ArchiveRequest(BaseModel):
    source_id: int
    policy_name: Optional[str] = None
    days: Optional[int] = None

class RestoreRequest(BaseModel):
    archive_id: Optional[int] = None
    original_path: Optional[str] = None


def _get_source(db, source_id: int):
    """Source ID'den kaynak bul, yoksa 404."""
    src = db.get_source_by_id(source_id)
    if not src:
        raise HTTPException(404, f"Kaynak bulunamadi (ID: {source_id})")
    return src


def _read_version() -> str:
    """Proje kok dizinindeki VERSION dosyasindan sürümü oku.

    Sürüm tek bir dosyada yasar (repo kok dizininde `VERSION`). Dashboard
    ve FastAPI title'i bu tek kaynaktan beslenir. Dosya yoksa 'unknown'
    doner — hardcoded bir yedek yok ki yanlis bir deger yayilmasin.
    """
    for candidate in (
        os.path.join(os.path.dirname(__file__), "..", "..", "VERSION"),
        os.path.join(os.getcwd(), "VERSION"),
    ):
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                v = f.read().strip()
                if v:
                    return v
        except (OSError, IOError):
            continue
    return "unknown"


APP_VERSION = _read_version()


def create_app(db, config, analytics=None, ad_lookup=None, email_notifier=None):
    """FastAPI uygulamasini olustur.

    analytics: Opsiyonel AnalyticsEngine. Verilmezse config.analytics'e gore
    olusturulur; DuckDB yoksa veya basarisiz olursa `available=False` ile
    doner ve endpoint'ler SQLite fallback'ine duser.

    ad_lookup: Opsiyonel ADLookup. Verilmezse config.active_directory'den
    olusturulur; ldap3 yoksa veya enabled=false ise available=False ile
    doner ve endpoint'ler cache degeri / None doner.

    email_notifier: Opsiyonel EmailNotifier. Verilmezse config.smtp'den
    olusturulur; smtp.enabled=false ise available=False ile doner ve
    e-posta endpoint'leri {"skipped": true} doner.
    """
    if analytics is None:
        from src.storage.analytics import AnalyticsEngine
        analytics = AnalyticsEngine(db.db_path, config.get("analytics", {}))
    if ad_lookup is None:
        from src.user_activity.ad_lookup import ADLookup
        ad_lookup = ADLookup(db, config)
    if email_notifier is None:
        from src.user_activity.email_notifier import EmailNotifier
        email_notifier = EmailNotifier(db, config)

    app = FastAPI(title="FILE ACTIVITY Dashboard", version=APP_VERSION)
    app.state.analytics = analytics
    app.state.ad_lookup = ad_lookup
    app.state.email_notifier = email_notifier

    # Ransomware detector (#37) — watcher pushes events here. Construction is
    # cheap; safe to do unconditionally. The detector pulls its own config
    # block out of `config["security"]["ransomware"]`.
    try:
        from src.security.ransomware_detector import RansomwareDetector
        ransomware = RansomwareDetector(db, config)
        ransomware.email_notifier = email_notifier
        app.state.ransomware = ransomware
    except Exception as e:  # pragma: no cover - defensive only
        logger.warning("RansomwareDetector init failed: %s", e)
        app.state.ransomware = None

    # Syslog/CEF forwarder (#50) — bridges security events to a downstream
    # SIEM (Splunk / Elastic / Sentinel / QRadar). Always constructed; it
    # is a no-op when disabled in config.
    try:
        from src.integrations.syslog_forwarder import SyslogForwarder
        syslog = SyslogForwarder(config)
        app.state.syslog = syslog
        # Wire detector → syslog if both are available.
        if app.state.ransomware is not None and syslog.available:
            app.state.ransomware.set_external_emitter(
                syslog.emit_ransomware_alert
            )
        # Wire audit-chain integrity break → syslog.
        if syslog.available:
            try:
                db.set_audit_break_callback(syslog.emit_audit_break)
            except AttributeError:
                # Older Database without the hook — just skip.
                pass
    except Exception as e:  # pragma: no cover - defensive only
        logger.warning("SyslogForwarder init failed: %s", e)
        app.state.syslog = None

    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # --- HTML Endpoint ---

    @app.get("/", response_class=HTMLResponse)
    async def index():
        html_path = os.path.join(static_dir, "index.html")
        if os.path.exists(html_path):
            with open(html_path, "r", encoding="utf-8") as f:
                return f.read()
        return "<h1>FILE ACTIVITY Dashboard</h1><p>static/index.html bulunamadi.</p>"

    # --- SOURCE API (ID-based) ---

    @app.get("/api/sources")
    async def get_sources():
        sources = db.get_sources()
        return [s.__dict__ for s in sources]

    @app.get("/api/dashboard/init")
    async def dashboard_init():
        """Hizli baslangic - scan_runs + fallback scanned_files."""
        from src.utils.size_formatter import format_size
        sources = db.get_sources()
        source_list = [s.__dict__ for s in sources]

        summaries = {}
        for s in sources:
            try:
                with db.get_cursor() as cur:
                    # 1) Son tamamlanmis tarama (completed once priority)
                    cur.execute("""
                        SELECT id, total_files, total_size, started_at, completed_at, status
                        FROM scan_runs WHERE source_id=?
                        ORDER BY
                            CASE WHEN status='completed' THEN 0 ELSE 1 END,
                            started_at DESC
                        LIMIT 1
                    """, (s.id,))
                    scan = cur.fetchone()
                    if not scan:
                        summaries[s.id] = {"has_data": False, "scan_id": None}
                        continue

                    file_count = scan["total_files"] or 0
                    total_size = scan["total_size"] or 0

                    # 2) FALLBACK: total_files=0 ama dosyalar var olabilir
                    if file_count == 0:
                        cur.execute("""
                            SELECT COUNT(*) as cnt, COALESCE(SUM(file_size),0) as sz
                            FROM scanned_files WHERE source_id=? AND scan_id=?
                        """, (s.id, scan["id"]))
                        real = cur.fetchone()
                        if real["cnt"] > 0:
                            file_count = real["cnt"]
                            total_size = real["sz"]
                            # scan_runs'i da guncelle (bir dahaki sefere hizli gelsin)
                            cur.execute("""
                                UPDATE scan_runs SET total_files=?, total_size=?
                                WHERE id=? AND total_files=0
                            """, (file_count, total_size, scan["id"]))

                    # Pre-computed KPI summary (scan tamamlanirken
                    # yazilmisti). Varsa Overview bu satirdan render eder,
                    # scanned_files tablosuna hic dokunmaz.
                    kpi = db.get_scan_summary(scan["id"])

                    summaries[s.id] = {
                        "has_data": file_count > 0,
                        "scan_id": scan["id"],
                        "file_count": file_count,
                        "total_size": total_size,
                        "total_size_formatted": format_size(total_size),
                        "scan_status": {
                            "started_at": scan["started_at"],
                            "completed_at": scan["completed_at"],
                            "status": scan["status"],
                        },
                        "kpi": kpi,  # None ise frontend eski yola duser
                    }
            except Exception:
                summaries[s.id] = {"has_data": False, "scan_id": None}

        return {
            "sources": source_list,
            "summaries": summaries,
            "auto_select": source_list[0]["id"] if len(source_list) == 1 else (source_list[0]["id"] if source_list else None),
        }

    @app.post("/api/sources")
    async def add_source(data: SourceCreate):
        from src.storage.models import Source
        s = Source(name=data.name, unc_path=data.unc_path, archive_dest=data.archive_dest)
        try:
            sid = db.add_source(s)
            return {"id": sid, "message": f"Kaynak eklendi: {data.name}"}
        except Exception as e:
            raise HTTPException(400, str(e))

    @app.delete("/api/sources/{source_id}")
    async def remove_source(source_id: int):
        src = _get_source(db, source_id)
        if db.remove_source(src.name):
            return {"message": f"Kaynak silindi: {src.name}"}
        raise HTTPException(500, "Silme basarisiz")

    @app.post("/api/sources/{source_id}/test")
    async def test_source(source_id: int):
        from src.scanner.share_resolver import test_connectivity
        src = _get_source(db, source_id)
        ok, msg = test_connectivity(src.unc_path)
        return {"success": ok, "message": msg}

    # --- SCAN API (ID-based, async background) ---

    import threading
    _scan_threads = {}  # source_id -> thread
    _scan_results = {}  # source_id -> result

    @app.post("/api/scan/{source_id}")
    async def run_scan(source_id: int):
        from src.scanner.file_scanner import FileScanner

        # Zaten tarama yapiliyor mu?
        if source_id in _scan_threads and _scan_threads[source_id].is_alive():
            return {"status": "already_running", "message": "Bu kaynak zaten taraniyor"}

        src = _get_source(db, source_id)

        def _scan_worker():
            scanner = FileScanner(db, config)
            result = scanner.scan_source(src.id, src.name, src.unc_path)
            _scan_results[source_id] = result

        t = threading.Thread(target=_scan_worker, daemon=True)
        _scan_threads[source_id] = t
        _scan_results.pop(source_id, None)
        t.start()

        return {"status": "started", "message": f"Tarama baslatildi: {src.name}"}

    @app.get("/api/scan/progress/{source_id}")
    async def scan_progress(source_id: int):
        from src.scanner.file_scanner import get_scan_progress
        progress = get_scan_progress(source_id)

        # Thread durumunu kontrol et
        is_running = source_id in _scan_threads and _scan_threads[source_id].is_alive()
        result = _scan_results.get(source_id)

        if result and not is_running:
            # Tarama bitti, sonucu dondur
            return {**result, "status": "completed", "finished": True}

        if progress:
            return {**progress, "finished": False}

        return {"status": "idle", "file_count": 0, "total_size": 0, "finished": False}

    # --- COMPATIBILITY REPORT API ---

    @app.get("/api/scan/compatibility/{source_id}")
    async def scan_compatibility(source_id: int):
        """Son taramanin dosya adi uyumluluk raporu."""
        result = _scan_results.get(source_id)
        if result and "compatibility" in result:
            return result["compatibility"]
        # Progress'ten al
        from src.scanner.file_scanner import get_scan_progress
        progress = get_scan_progress(source_id)
        if progress and "compatibility" in progress:
            return progress["compatibility"]
        return {"total_files_analyzed": 0, "health_score": 100, "issues": [], "summary": {}}

    # --- REPORT API (ID-based) ---

    @app.get("/api/reports/status/{source_id}")
    async def report_status(source_id: int):
        from src.analyzer.report_generator import ReportGenerator
        src = _get_source(db, source_id)
        gen = ReportGenerator(db, config)
        return gen.generate_status_report(src.id)

    @app.get("/api/reports/frequency/{source_id}")
    async def report_frequency(source_id: int, days: Optional[str] = None):
        from src.analyzer.report_generator import ReportGenerator
        src = _get_source(db, source_id)
        # Once v2 summary_json'daki age_buckets'a bak — scanned_files'i
        # hic tarama. Custom days verildiyse klasik hesaplamaya dus.
        if not days:
            scan_id = db.get_latest_scan_id(src.id, include_running=True)
            if scan_id:
                summary = db.get_scan_summary(scan_id)
                if summary and isinstance(summary, dict) and "age_buckets" in summary:
                    from datetime import datetime
                    return {
                        "source": {"id": source_id, "name": src.name},
                        "scan_id": scan_id,
                        "age_buckets": summary.get("age_buckets"),
                        "frequency": {
                            "age_buckets": summary.get("age_buckets"),
                            "total_files": summary.get("total_files"),
                            "total_size": summary.get("total_size"),
                        },
                        "from_summary": True,
                        "generated_at": datetime.now().isoformat(),
                    }
        gen = ReportGenerator(db, config)
        custom = [int(d) for d in days.split(",")] if days else None
        return gen.generate_frequency_report(src.id, custom)

    @app.get("/api/reports/types/{source_id}")
    async def report_types(source_id: int):
        from src.analyzer.report_generator import ReportGenerator
        src = _get_source(db, source_id)
        gen = ReportGenerator(db, config)
        return gen.generate_type_report(src.id)

    @app.get("/api/reports/sizes/{source_id}")
    async def report_sizes(source_id: int):
        from src.analyzer.report_generator import ReportGenerator
        src = _get_source(db, source_id)
        gen = ReportGenerator(db, config)
        return gen.generate_size_report(src.id)

    @app.get("/api/reports/full/{source_id}")
    async def report_full(source_id: int):
        from src.analyzer.report_generator import ReportGenerator
        src = _get_source(db, source_id)
        gen = ReportGenerator(db, config)
        return gen.generate_full_report(src.id)

    # --- ARCHIVE API ---

    @app.post("/api/archive/run")
    async def run_archive(data: ArchiveRequest):
        from src.archiver.archive_policy import ArchivePolicyEngine
        from src.archiver.archive_engine import ArchiveEngine

        src = _get_source(db, data.source_id)
        if not src.archive_dest:
            raise HTTPException(400, "Arsiv hedefi tanimli degil")

        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")

        policy_engine = ArchivePolicyEngine(db)

        if data.policy_name:
            files = policy_engine.get_files_by_policy(src.id, scan_id, data.policy_name)
            archived_by = data.policy_name
        elif data.days:
            files = policy_engine.get_files_by_days(src.id, scan_id, data.days)
            archived_by = f"manual:{data.days}days"
        else:
            raise HTTPException(400, "policy_name veya days belirtmelisiniz")

        if not files:
            return {"archived": 0, "message": "Arsivlenecek dosya yok"}

        engine = ArchiveEngine(db, config)
        return engine.archive_files(files, src.archive_dest, src.unc_path, src.id, archived_by)

    @app.post("/api/archive/dry-run")
    async def archive_dry_run(data: ArchiveRequest):
        from src.archiver.archive_policy import ArchivePolicyEngine
        from src.utils.size_formatter import format_size

        src = _get_source(db, data.source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")

        policy_engine = ArchivePolicyEngine(db)
        days = data.days or 365
        files = policy_engine.get_files_by_days(src.id, scan_id, days)

        total_size = sum(f["file_size"] for f in files) if files else 0
        return {
            "file_count": len(files) if files else 0,
            "total_size": total_size,
            "total_size_formatted": format_size(total_size),
            "sample": files[:20] if files else []
        }

    @app.get("/api/archive/search")
    async def archive_search(q: str, extension: Optional[str] = None,
                              page: int = Query(1, ge=1, le=10000)):
        return db.search_archived_files(q, extension=extension, page=page)

    @app.get("/api/archive/stats")
    async def archive_stats():
        return db.get_archive_stats()

    @app.post("/api/archive/restore")
    async def restore_file(data: RestoreRequest):
        from src.archiver.restore_engine import RestoreEngine
        engine = RestoreEngine(db)
        if data.archive_id:
            return engine.restore_by_id(data.archive_id)
        elif data.original_path:
            return engine.restore_by_path(data.original_path)
        raise HTTPException(400, "archive_id veya original_path gerekli")

    # --- POLICY API ---

    @app.get("/api/policies")
    async def get_policies():
        return db.get_policies()

    @app.post("/api/policies")
    async def add_policy(data: PolicyCreate):
        from src.archiver.archive_policy import ArchivePolicyEngine
        from src.storage.models import ArchivePolicy

        engine = ArchivePolicyEngine(db)
        rules = engine.create_policy_rules(
            access_days=data.access_days, modify_days=data.modify_days,
            min_size=data.min_size, max_size=data.max_size,
            extensions=data.extensions, exclude_extensions=data.exclude_extensions
        )
        pol = ArchivePolicy(name=data.name, source_id=data.source_id, rules_json=rules)
        pid = db.add_policy(pol)
        return {"id": pid, "message": f"Politika olusturuldu: {data.name}"}

    @app.delete("/api/policies/{policy_id}")
    async def remove_policy(policy_id: int):
        pol = db.get_policy_by_id(policy_id)
        if not pol:
            raise HTTPException(404, "Politika bulunamadi")
        if db.remove_policy(pol["name"]):
            return {"message": f"Politika silindi: {pol['name']}"}
        raise HTTPException(500, "Silme basarisiz")

    # --- SCHEDULE API ---

    @app.get("/api/schedules")
    async def get_schedules():
        return db.get_scheduled_tasks()

    @app.post("/api/schedules")
    async def add_schedule(data: ScheduleCreate):
        from src.storage.models import ScheduledTask
        # audit_export tasks are global — no source. Accept source_id=0
        # as the "ignored" sentinel; require a real source for everything else.
        if data.task_type == "audit_export":
            source_id_val = data.source_id  # stored but ignored by runner
        else:
            src = _get_source(db, data.source_id)
            source_id_val = src.id

        policy_id = None
        if data.policy_name:
            pol = db.get_policy_by_name(data.policy_name)
            if not pol:
                raise HTTPException(404, "Politika bulunamadi")
            policy_id = pol["id"]

        task = ScheduledTask(
            task_type=data.task_type, source_id=source_id_val,
            policy_id=policy_id, cron_expression=data.cron_expression
        )
        tid = db.add_scheduled_task(task)
        return {"id": tid, "message": "Zamanlanmis gorev olusturuldu"}

    @app.delete("/api/schedules/{task_id}")
    async def remove_schedule(task_id: int):
        if db.remove_scheduled_task(task_id):
            return {"message": "Gorev silindi"}
        raise HTTPException(404, "Gorev bulunamadi")

    @app.post("/api/schedules/notify-users/run-now/{source_id}")
    async def notify_users_run_now(source_id: int):
        """notify_users gorevi zamanlayiciyi beklemeden hemen calistir.

        EmailNotifier ve ADLookup app.state'ten alinir; ikisi de yoksa
        scheduler'in _run_notify_users'i 'skipped' sonucu doner (hata
        atmaz). Gonderim sonucu: her kullanici icin sayisal ozet.
        """
        from src.scheduler.task_scheduler import TaskScheduler
        src = _get_source(db, source_id)
        ad = getattr(app.state, "ad_lookup", None)
        notifier = getattr(app.state, "email_notifier", None)
        ts = TaskScheduler(db, config, ad_lookup=ad, email_notifier=notifier)
        fake_task = {"id": 0, "source_id": src.id, "task_type": "notify_users"}
        return ts._run_notify_users(fake_task)

    # --- REPORT EXPORT API ---

    @app.get("/api/reports/export/{source_id}")
    async def report_export(source_id: int):
        """HTML rapor dosyasi olustur ve indir."""
        from src.analyzer.report_generator import ReportGenerator
        from src.analyzer.report_exporter import ReportExporter
        src = _get_source(db, source_id)
        gen = ReportGenerator(db, config)
        data = gen.generate_full_report(src.id)
        if "error" in data:
            raise HTTPException(400, data["error"])
        exporter = ReportExporter(config)
        paths = exporter.export_full_report(data, src.name)
        if paths and os.path.exists(paths.get("html_path", "")):
            return FileResponse(
                paths["html_path"],
                filename=os.path.basename(paths["html_path"]),
                media_type="text/html"
            )
        raise HTTPException(500, "Rapor olusturulamadi")

    # --- DRILL-DOWN API ---

    def _run_drilldown(duckdb_fn, sqlite_fn):
        """DuckDB varsa oradan calistir, yoksa SQLite'a dus.

        duckdb_fn: AnalyticsEngine uzerinde cagrilacak bound method
        sqlite_fn: db (Database) uzerinde cagrilacak bound method
        Her ikisi de (*args) -> {"total": int, "files": list} doner.
        """
        def call(*args):
            if analytics.available:
                try:
                    return duckdb_fn(*args)
                except Exception as e:
                    logger.warning("DuckDB drilldown basarisiz, SQLite fallback: %s", e)
            return sqlite_fn(*args)
        return call

    @app.get("/api/drilldown/frequency/{source_id}")
    async def drilldown_frequency(source_id: int, min_days: int = 0,
                                   max_days: Optional[int] = None,
                                   page: int = Query(1, ge=1, le=10000),
                                   limit: int = Query(100, ge=1, le=500)):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        run = _run_drilldown(analytics.get_files_by_frequency, db.get_files_by_frequency)
        result = run(src.id, scan_id, min_days, max_days, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    @app.get("/api/drilldown/type/{source_id}")
    async def drilldown_type(source_id: int, extension: str = "",
                              page: int = Query(1, ge=1, le=10000),
                              limit: int = Query(100, ge=1, le=500)):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        run = _run_drilldown(analytics.get_files_by_extension, db.get_files_by_extension)
        result = run(src.id, scan_id, extension, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    @app.get("/api/drilldown/size/{source_id}")
    async def drilldown_size(source_id: int, min_bytes: int = 0,
                              max_bytes: Optional[int] = None,
                              page: int = Query(1, ge=1, le=10000),
                              limit: int = Query(100, ge=1, le=500)):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        run = _run_drilldown(analytics.get_files_by_size_range, db.get_files_by_size_range)
        result = run(src.id, scan_id, min_bytes, max_bytes, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    @app.get("/api/drilldown/owner/{source_id}")
    async def drilldown_owner(source_id: int, owner: str = "",
                               page: int = Query(1, ge=1, le=10000),
                               limit: int = Query(100, ge=1, le=500)):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        run = _run_drilldown(analytics.get_files_by_owner, db.get_files_by_owner)
        result = run(src.id, scan_id, owner, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    class DrilldownArchiveRequest(BaseModel):
        source_id: int
        filter_type: str  # "frequency", "type", "size", "owner"
        min_days: Optional[int] = None
        max_days: Optional[int] = None
        extension: Optional[str] = None
        min_bytes: Optional[int] = None
        max_bytes: Optional[int] = None
        owner: Optional[str] = None

    @app.post("/api/drilldown/archive")
    async def drilldown_archive(data: DrilldownArchiveRequest):
        from src.archiver.archive_engine import ArchiveEngine
        src = _get_source(db, data.source_id)
        if not src.archive_dest:
            raise HTTPException(400, "Arsiv hedefi tanimli degil")
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")

        # Collect files based on filter_type
        if data.filter_type == "frequency":
            result = db.get_files_by_frequency(src.id, scan_id, data.min_days or 0, data.max_days, 10000, 0)
        elif data.filter_type == "type":
            result = db.get_files_by_extension(src.id, scan_id, data.extension or "", 10000, 0)
        elif data.filter_type == "size":
            result = db.get_files_by_size_range(src.id, scan_id, data.min_bytes or 0, data.max_bytes, 10000, 0)
        elif data.filter_type == "owner":
            result = db.get_files_by_owner(src.id, scan_id, data.owner or "", 10000, 0)
        else:
            raise HTTPException(400, "Gecersiz filter_type")

        files = result.get("files", [])
        if not files:
            return {"archived": 0, "message": "Arsivlenecek dosya yok"}

        engine = ArchiveEngine(db, config)
        archived_by = f"drilldown:{data.filter_type}"
        return engine.archive_files(files, src.archive_dest, src.unc_path, src.id, archived_by)

    # --- EXPORT API (XLS / PDF) ---

    @app.get("/api/export/xls/{source_id}")
    async def export_xls(source_id: int):
        from fastapi.responses import StreamingResponse
        from io import BytesIO
        src = _get_source(db, source_id)
        try:
            from src.analyzer.report_exporter_v2 import XLSExporter
            exporter = XLSExporter(db, config)
            data = exporter.export_full_report(src.id)
            return StreamingResponse(
                BytesIO(data),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename=FileActivity_{src.name}_{datetime.now().strftime('%Y%m%d')}.xlsx"}
            )
        except ImportError:
            raise HTTPException(500, "openpyxl kurulu degil. pip install openpyxl")
        except Exception as e:
            logger.error("XLS export error: %s", e)
            raise HTTPException(500, str(e))

    @app.get("/api/export/pdf/{source_id}")
    async def export_pdf(source_id: int):
        from fastapi.responses import StreamingResponse
        from io import BytesIO
        src = _get_source(db, source_id)
        try:
            from src.analyzer.report_exporter_v2 import PDFExporter
            exporter = PDFExporter(db, config)
            data = exporter.export_full_report(src.id)
            return StreamingResponse(
                BytesIO(data),
                media_type="application/pdf",
                headers={"Content-Disposition": f"attachment; filename=FileActivity_{src.name}_{datetime.now().strftime('%Y%m%d')}.pdf"}
            )
        except ImportError:
            raise HTTPException(500, "reportlab kurulu degil. pip install reportlab")
        except Exception as e:
            logger.error("PDF export error: %s", e)
            raise HTTPException(500, str(e))

    # --- WATCHER CHANGES API ---

    @app.get("/api/watcher/{source_id}/changes")
    async def watcher_changes(source_id: int):
        from src.scanner.file_watcher import _watchers
        if source_id in _watchers:
            w = _watchers[source_id]
            return {
                "changes": w.stats.get("last_changes", []),
                "total_changes": w.stats.get("total_changes", 0),
                "running": w._running,
            }
        return {"changes": [], "total_changes": 0, "running": False}

    # --- USER ACTIVITY API ---

    @app.get("/api/users/overview")
    async def users_overview(source_id: Optional[int] = None, days: int = 30):
        # Check if we have event log data; if not, fallback to file ownership
        has_logs = db.has_access_log_data()
        if has_logs:
            return {
                "source": "event_log",
                "top_users": db.get_top_users(source_id=source_id, days=days),
                "department_stats": db.get_department_stats(days=days),
                "access_timeline": db.get_access_timeline(source_id=source_id, days=days),
                "heatmap": db.get_hourly_heatmap(source_id=source_id, days=min(days, 7))
            }
        else:
            # Fallback to file ownership from scanned_files
            owners = []
            if source_id:
                owners = db.get_file_owners_stats(source_id)
            else:
                # Try all sources
                sources = db.get_sources()
                for s in sources:
                    sid = db.get_latest_scan_id(s.id)
                    if sid:
                        owners = db.get_file_owners_stats(s.id, sid)
                        break
            return {
                "source": "file_ownership",
                "owners": owners,
                "top_users": [],
                "department_stats": [],
                "access_timeline": [],
                "heatmap": []
            }

    @app.get("/api/users/heatmap")
    async def users_heatmap(source_id: Optional[int] = None, days: int = 7):
        """Haftalik saatlik erisim heatmap'i - ayri endpoint."""
        raw = db.get_hourly_heatmap(source_id=source_id, days=days)
        # HTML expects: {matrix: [[24 vals]*7], max_value: N, days: [str*7]}
        day_names = ["Pazartesi", "Sali", "Carsamba", "Persembe", "Cuma", "Cumartesi", "Pazar"]
        matrix = [[0]*24 for _ in range(7)]
        max_val = 0
        if isinstance(raw, list):
            for entry in raw:
                d = entry.get("dow", 0)
                h = entry.get("hour", 0)
                c = entry.get("count", 0)
                if 0 <= d < 7 and 0 <= h < 24:
                    matrix[d][h] = c
                    if c > max_val:
                        max_val = c
        elif isinstance(raw, dict):
            matrix = raw.get("matrix", matrix)
            max_val = raw.get("max_value", max_val)
        return {"matrix": matrix, "max_value": max_val or 1, "days": day_names}

    @app.get("/api/users/{username}/activity")
    async def user_activity(username: str, days: int = 30):
        return db.get_user_activity(username, days=days)

    @app.get("/api/users/{username}/efficiency")
    async def user_efficiency(username: str,
                               source_id: Optional[int] = None,
                               scan_id: Optional[int] = None):
        """Kullanici verimlilik skoru + uyumsuzluk raporu.

        0-100 arasi skor, faktorler, somut oneriler. source_id ve scan_id
        opsiyonel; hic biri verilmezse son tamamlanmis tarama kullanilir.
        """
        from src.user_activity.efficiency_score import compute_user_score
        return compute_user_score(db, username, source_id=source_id, scan_id=scan_id)

    @app.get("/api/users/{username}/detail")
    async def user_detail(username: str, days: int = 30):
        """Kullanici detay raporu - HTML dashboard icin genisletilmis format."""
        from src.utils.size_formatter import format_size
        activity = db.get_user_activity(username, days=days)

        # hourly distribution - list of 24 values
        hourly_dict = activity.get("hourly_distribution", {})
        hourly = [0] * 24
        if isinstance(hourly_dict, dict):
            for h_str, cnt in hourly_dict.items():
                try:
                    hourly[int(h_str)] = cnt
                except (ValueError, IndexError):
                    pass
        elif isinstance(hourly_dict, list):
            hourly = hourly_dict[:24] if len(hourly_dict) >= 24 else hourly_dict + [0]*(24-len(hourly_dict))

        # summary - fields are nested inside activity["summary"]
        summary = activity.get("summary", {}) or {}
        total_access = summary.get("total_access", 0) or 0
        unique_files = summary.get("unique_files", 0) or 0
        active_days = summary.get("active_days", 0) or 0
        total_bytes = summary.get("total_data", 0) or 0
        reads = summary.get("reads", 0) or 0
        writes = summary.get("writes", 0) or 0
        deletes = summary.get("deletes", 0) or 0

        # risk score - compute from hourly_distribution and summary
        risk = {"score": 0, "level": "normal", "factors": []}

        # top extensions
        top_ext = activity.get("top_extensions", [])

        # action_summary - build from summary data
        action_summary = {
            "read": reads, "write": writes, "delete": deletes,
            "modify": 0, "create": 0, "copy": 0, "rename": 0, "permission_change": 0
        }

        # recent_activity - not available from get_user_activity
        recent = []

        # AD lookup: e-posta + display name (yoksa null alanlar)
        ad_info = ad_lookup.lookup(username) or {
            "email": None, "display_name": None, "found": False, "source": None
        }

        return {
            "username": username,
            "ad": {
                "email": ad_info.get("email"),
                "display_name": ad_info.get("display_name"),
                "found": ad_info.get("found", False),
                "source": ad_info.get("source"),
            },
            "summary": {
                "total_access": total_access,
                "unique_files": unique_files,
                "active_days": active_days,
                "total_data_formatted": format_size(total_bytes),
                "reads": reads,
                "writes": writes,
                "deletes": deletes
            },
            "risk_score": risk,
            "hourly": hourly,
            "top_extensions": top_ext,
            "action_summary": action_summary,
            "recent_activity": recent
        }

    # --- ANOMALY API ---

    @app.get("/api/anomalies")
    async def get_anomalies(severity: Optional[str] = None, days: int = 7):
        """Anomali listesi - HTML duz array bekliyor."""
        alerts = db.get_anomalies(severity=severity, days=days)
        # HTML expects flat array, not {alerts:[], summary:{}}
        if isinstance(alerts, list):
            return alerts
        return []

    @app.post("/api/anomalies/{anomaly_id}/acknowledge")
    async def acknowledge_anomaly(anomaly_id: int, by_user: str = "admin"):
        db.acknowledge_anomaly(anomaly_id, by_user)
        return {"message": "Anomali onaylandi"}

    # --- FILE WATCHER API ---

    @app.post("/api/watcher/{source_id}/start")
    async def start_watcher(source_id: int,
                             interval: Optional[int] = Query(
                                 default=None, ge=10, le=3600,
                                 description="Polling araligi (saniye). "
                                             "Bos birakilirsa config.watcher.poll_interval_seconds "
                                             "veya 60 saniye kullanilir."
                             )):
        from src.scanner.file_watcher import FileWatcher, DEFAULT_POLL_INTERVAL
        src = _get_source(db, source_id)
        if interval is None:
            interval = int(config.get("watcher", {}).get("poll_interval_seconds",
                                                          DEFAULT_POLL_INTERVAL))
        ransomware = getattr(app.state, "ransomware", None)
        watcher = FileWatcher(db, src.id, src.unc_path, interval,
                                ransomware_detector=ransomware)
        watcher.start()
        return {"status": "started", "interval": watcher.interval}

    @app.post("/api/watcher/{source_id}/stop")
    async def stop_watcher(source_id: int):
        from src.scanner.file_watcher import _watchers
        if source_id in _watchers:
            _watchers[source_id].stop()
            return {"status": "stopped"}
        return {"status": "not_running"}

    @app.get("/api/watcher/status")
    async def watcher_status(source_id: int = None):
        from src.scanner.file_watcher import get_watcher_status
        return get_watcher_status(source_id)

    # --- AUDIT API ---

    @app.get("/api/audit/events")
    async def audit_events(source_id: int = None, event_type: str = None,
                           username: str = None,
                           days: int = Query(7, ge=1, le=365),
                           page: int = Query(1, ge=1, le=10000)):
        return db.get_audit_events(source_id, event_type, username, days, page)

    @app.get("/api/audit/summary")
    async def audit_summary(source_id: int = None, days: int = 7):
        return db.get_audit_summary(source_id, days)

    # --- AUDIT CHAIN API (issue #38) ---

    @app.get("/api/audit/verify")
    async def audit_verify(since_seq: int = Query(1, ge=1),
                           end_seq: Optional[int] = None):
        """Verify the tamper-evident audit chain from since_seq onward.

        Returns ``{verified, total, broken_at, broken_reason}``.
        """
        return db.verify_audit_chain(start_seq=since_seq, end_seq=end_seq)

    @app.get("/api/audit/chain")
    async def audit_chain(page: int = Query(1, ge=1),
                          page_size: int = Query(100, ge=1, le=1000)):
        """Paginated chain rows joined with file_audit_events (newest first)."""
        return db.get_audit_chain_page(page=page, page_size=page_size)

    @app.post("/api/audit/export")
    async def audit_export(start_date: Optional[str] = None,
                           end_date: Optional[str] = None):
        """Trigger a WORM JSONL export of the chain in [start_date, end_date]."""
        from src.storage.audit_export import AuditExporter
        exporter = AuditExporter(db, config)
        return exporter.export_range(start_date, end_date)

    # --- INSIGHTS API ---

    @app.get("/api/insights/{source_id}")
    async def get_insights(source_id: int, refresh: bool = False):
        """AI Insights: son scan icin cached sonucu doner, yoksa hesaplar.

        refresh=true ile yeniden hesaplamayi force ederek cache'i tazeler.
        Cache'te yoksa ilk cagri agir; sonraki cagrilar anlik.
        """
        from src.analyzer.ai_insights import InsightsEngine
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not refresh and scan_id:
            cached = db.get_scan_insights(scan_id)
            if cached:
                cached["from_cache"] = True
                return cached

        engine = InsightsEngine(db)
        result = engine.generate_insights(source_id)
        # Sonraki acilislar anlik olsun diye scan_id varsa cache'e yaz
        if scan_id:
            try:
                db.save_scan_insights(scan_id, result)
            except Exception as e:
                logger.warning("insights cache yazilamadi: %s", e)
        result["from_cache"] = False
        return result

    @app.post("/api/insights/{source_id}/recompute")
    async def insights_recompute(source_id: int):
        """Insights'i yeniden hesapla ve cache'i tazele."""
        from src.analyzer.ai_insights import InsightsEngine
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            raise HTTPException(404, "Tamamlanmis scan yok")
        result = InsightsEngine(db).generate_insights(source_id)
        db.save_scan_insights(scan_id, result)
        return {"status": "ok", "scan_id": scan_id,
                "insights_count": len(result.get("insights", []))}

    # --- RISK SCORE API ---

    @app.get("/api/reports/mit-naming/{source_id}")
    async def mit_naming_report(source_id: int):
        """MIT Libraries dosya adlandirma standartlarina uyum analizi."""
        from src.scanner.file_scanner import MITNamingAnalyzer

        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")

        analyzer = MITNamingAnalyzer()
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT file_path, file_name FROM scanned_files
                WHERE scan_id=?
            """, (scan_id,))
            for row in cur:
                analyzer.analyze(row["file_path"], row["file_name"])

        return analyzer.get_report()

    @app.get("/api/reports/mit-naming/{source_id}/files")
    async def mit_naming_files(source_id: int, code: str = "R1",
                                page: int = Query(1, ge=1, le=10000),
                                page_size: int = Query(100, ge=1, le=500)):
        """MIT ihlal koduna gore dosya listesi (R1,R2,R3,R4,B1,B2,B3,B4,B5,B6)."""
        import re as re_mod
        from src.utils.size_formatter import format_size

        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")

        # Kural tanimlar
        checks = {
            "R1": lambda p, n: bool(re_mod.search(r'\s', n)),
            "R2": lambda p, n: bool(n) and not re_mod.match(r'^[a-zA-Z]', n),
            "R3": lambda p, n: bool(n) and '.' in n and not re_mod.match(r'^[a-zA-Z0-9._-]+$', n[:n.rfind('.')]),
            "R4": lambda p, n: '.' not in n or not n.rsplit('.', 1)[-1].isalpha(),
            "B1": lambda p, n: len(n) > 31,
            "B2": lambda p, n: len(p) > 256,
            "B3": lambda p, n: '.' in n and n[:n.rfind('.')].count('.') > 0,
            "B4": lambda p, n: bool(re_mod.search(r'[A-Z]', n[:n.rfind('.')] if '.' in n else n)),
            "B5": lambda p, n: len(n) > 10 and '_' not in n and '-' not in n,
            "B6": lambda p, n: any('.' in part and part not in ('', '.', '..') for part in p.replace('\\', '/').split('/')),
        }

        check_fn = checks.get(code.upper())
        if not check_fn:
            raise HTTPException(400, f"Gecersiz kod: {code}. Gecerli: {', '.join(checks.keys())}")

        # Dosyalari tara ve ihlal edenleri topla
        matching = []
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT id, file_path, file_name, file_size, owner, last_modify_time
                FROM scanned_files WHERE scan_id=?
            """, (scan_id,))
            for row in cur:
                if check_fn(row["file_path"], row["file_name"]):
                    matching.append(dict(row))

        total = len(matching)
        offset = (page - 1) * page_size
        page_files = matching[offset:offset + page_size]
        for f in page_files:
            f["file_size_formatted"] = format_size(f.get("file_size", 0))
            f["directory"] = f["file_path"].rsplit('\\', 1)[0] if '\\' in f["file_path"] else f["file_path"].rsplit('/', 1)[0]

        return {
            "code": code.upper(),
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "files": page_files
        }

    @app.get("/api/reports/mit-naming/{source_id}/export")
    async def mit_naming_export(source_id: int):
        """MIT ihlal raporunu Excel olarak export et."""
        import re as re_mod
        from fastapi.responses import StreamingResponse
        import io

        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")

        checks = {
            "R1": ("Bosluk Iceren", lambda p, n: bool(re_mod.search(r'\s', n))),
            "R2": ("Ilk Karakter Harf Degil", lambda p, n: bool(n) and not re_mod.match(r'^[a-zA-Z]', n)),
            "R3": ("Yasak Karakter", lambda p, n: bool(n) and '.' in n and not re_mod.match(r'^[a-zA-Z0-9._-]+$', n[:n.rfind('.')])),
            "R4": ("Uzanti Sorunu", lambda p, n: '.' not in n or not n.rsplit('.', 1)[-1].isalpha()),
            "B1": ("Uzun Ad (>31)", lambda p, n: len(n) > 31),
            "B3": ("Base Nokta", lambda p, n: '.' in n and n[:n.rfind('.')].count('.') > 0),
            "B4": ("Buyuk Harf", lambda p, n: bool(re_mod.search(r'[A-Z]', n[:n.rfind('.')] if '.' in n else n))),
            "B5": ("Ayirici Yok", lambda p, n: len(n) > 10 and '_' not in n and '-' not in n),
        }

        # Tum dosyalari tara
        violations = {code: [] for code in checks}
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT file_path, file_name, file_size, owner, last_modify_time
                FROM scanned_files WHERE source_id=? AND scan_id=?
            """, (source_id, scan_id))
            for row in cur:
                for code, (label, fn) in checks.items():
                    if fn(row["file_path"], row["file_name"]):
                        violations[code].append(dict(row))

        # CSV olustur (Excel uyumlu)
        output = io.StringIO()
        output.write('\ufeff')  # BOM for Excel UTF-8
        output.write('Ihlal Kodu,Ihlal Turu,Dosya Adi,Tam Yol,Boyut,Sahip,Son Degisiklik\n')
        for code, (label, _) in checks.items():
            for f in violations[code][:5000]:  # Her kod icin max 5000
                path = f["file_path"].replace('"', '""')
                name = f["file_name"].replace('"', '""')
                owner = (f.get("owner") or "").replace('"', '""')
                output.write(f'{code},{label},"{name}","{path}",{f.get("file_size",0)},"{owner}",{f.get("last_modify_time","")}\n')

        output.seek(0)
        from datetime import datetime
        filename = f"MIT_Naming_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    @app.get("/api/insights/{source_id}/files")
    async def insight_files(source_id: int, insight_type: str = "stale_1year",
                            page: int = Query(1, ge=1, le=10000),
                            page_size: int = Query(100, ge=1, le=500)):
        """AI insight tipine gore dosya listesi."""
        from src.utils.size_formatter import format_size
        from src.analyzer.ai_insights import get_insight_files

        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")

        files = get_insight_files(db, scan_id, insight_type)
        total = len(files)
        offset = (page - 1) * page_size
        page_files = files[offset:offset + page_size]

        for f in page_files:
            f["file_size_formatted"] = format_size(f.get("file_size", 0))
            f["directory"] = f["file_path"].rsplit('\\', 1)[0] if '\\' in f["file_path"] else f["file_path"].rsplit('/', 1)[0]

        return {
            "insight_type": insight_type,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "files": page_files
        }

    @app.get("/api/overview/{source_id}")
    async def overview(source_id: int):
        """Instant Overview: pre-computed summary'den okur, scanned_files
        tablosuna dokunmaz. Scan tamamlaniyorken kaydedilen JSON'dan gelir.

        Summary henuz hesaplanmamissa (ornek: scan calisiyor, eski scan)
        frontend kendi fallback'ine duser (has_data=false).
        """
        from src.utils.size_formatter import format_size
        _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            return {"has_data": False, "reason": "no_completed_scan"}

        kpi = db.get_scan_summary(scan_id)
        if not kpi:
            return {"has_data": False, "scan_id": scan_id,
                    "reason": "summary_not_computed"}

        # Boyutlari formatla
        kpi["total_size_formatted"] = format_size(kpi.get("total_size", 0))
        kpi["stale_size_formatted"] = format_size(kpi.get("stale_size", 0))
        kpi["large_size_formatted"] = format_size(kpi.get("large_size", 0))
        kpi["duplicate_waste_formatted"] = format_size(kpi.get("duplicate_waste_size", 0))
        for ext in kpi.get("top_extensions", []):
            ext["size_formatted"] = format_size(ext.get("size", 0))
        for owner in kpi.get("top_owners", []):
            owner["size_formatted"] = format_size(owner.get("size", 0))

        kpi["has_data"] = True
        return kpi

    @app.post("/api/overview/{source_id}/recompute")
    async def overview_recompute(source_id: int):
        """Manuel: son scan icin summary'yi yeniden hesapla.

        Kullanim: scan bittigi sirada summary yazilmadi (crash vs.) ise
        veya KPI algoritmasi guncellendikten sonra.
        """
        _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            raise HTTPException(404, "Tamamlanmis scan yok")
        summary = db.compute_scan_summary(scan_id)
        return {"status": "ok", "scan_id": scan_id,
                "total_files": summary.get("total_files")}

    @app.get("/api/risk-score/{source_id}")
    async def risk_score(source_id: int):
        """Supervisor risk score - TEK optimized sorgu (6 yerine 2)."""
        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"risk_score": 0, "kpis": {}}

        # Hizli yol: pre-computed summary varsa oradan hesapla
        kpi = db.get_scan_summary(scan_id)
        if kpi:
            total = kpi.get("total_files", 0) or 1
            risky = kpi.get("risky_count", 0)
            stale = kpi.get("stale_count", 0)
            dup_waste = kpi.get("duplicate_waste_size", 0)
            total_size = kpi.get("total_size", 0) or 1
            # Basit formul: risky % + stale % + (dup_waste / total_size) %
            score = int(min(100,
                (risky / total) * 40 +
                (stale / total) * 30 +
                (dup_waste / total_size) * 30 * 100
            ))
            return {
                "risk_score": score,
                "kpis": {
                    "total_files": kpi.get("total_files"),
                    "risky_files": risky,
                    "stale_files": stale,
                    "duplicate_groups": kpi.get("duplicate_groups"),
                    "total_size": kpi.get("total_size"),
                    "stale_size": kpi.get("stale_size"),
                    "risky_pct": round(risky * 100 / total, 1),
                    "stale_pct": round(stale * 100 / total, 1),
                },
                "source": "precomputed",
            }

        risky_exts = ('exe','bat','ps1','vbs','cmd','msi','scr','com','js','wsf')
        placeholders = ','.join(['?'] * len(risky_exts))
        stale_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        with db.get_cursor() as cur:
            # TEK SORGU: 5 metrigi bir seferde hesapla (6 ayri sorgu yerine)
            cur.execute(f"""
                SELECT
                    COUNT(*) as total_files,
                    COALESCE(SUM(file_size), 0) as total_size,
                    SUM(CASE WHEN last_access_time < ? THEN 1 ELSE 0 END) as stale_count,
                    COALESCE(SUM(CASE WHEN last_access_time < ? THEN file_size ELSE 0 END), 0) as stale_size,
                    SUM(CASE WHEN LOWER(extension) IN ({placeholders}) THEN 1 ELSE 0 END) as risky_count,
                    COUNT(DISTINCT CASE WHEN owner IS NOT NULL AND owner != '' THEN owner END) as owner_count,
                    SUM(CASE WHEN file_size > 104857600 THEN 1 ELSE 0 END) as large_count,
                    COALESCE(SUM(CASE WHEN file_size > 104857600 THEN file_size ELSE 0 END), 0) as large_size
                FROM scanned_files
                WHERE source_id=? AND scan_id=?
            """, (stale_date, stale_date) + risky_exts + (source_id, scan_id))
            r = cur.fetchone()
            total_files = r["total_files"]
            total_size = r["total_size"]
            stale_count = r["stale_count"]
            stale_size = r["stale_size"]
            risky_count = r["risky_count"]
            owner_count = r["owner_count"]
            large_count = r["large_count"]
            large_size = r["large_size"]
            stale_pct = round(stale_count * 100 / max(total_files, 1), 1)

            # Duplikasyon sorgusu (ayri cunku GROUP BY gerekli)
            cur.execute("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT file_name, file_size FROM scanned_files
                    WHERE source_id=? AND scan_id=? AND file_size > 1048576
                    GROUP BY file_name, file_size HAVING COUNT(*) > 1
                )
            """, (source_id, scan_id))
            dup_groups = cur.fetchone()["cnt"]

        # Risk skoru hesapla (0-100)
        risk = 0
        risk += min(stale_pct * 0.4, 30)
        risk += min(risky_count / max(total_files, 1) * 1000, 20)
        risk += min(large_size / max(total_size, 1) * 100, 15)
        risk += min(dup_groups * 2, 15)
        risk += 10 if owner_count <= 1 else 0
        risk += 10 if stale_pct > 50 else 0
        risk_score_val = min(round(risk), 100)

        # Watcher
        try:
            from src.scanner.file_watcher import get_watcher_status
            watcher = get_watcher_status(source_id)
            changes_24h = watcher.get("total_changes", 0) if isinstance(watcher, dict) else 0
        except Exception:
            changes_24h = 0

        return {
            "risk_score": risk_score_val,
            "risk_level": "critical" if risk_score_val >= 70 else "warning" if risk_score_val >= 40 else "good",
            "total_files": total_files,
            "total_size": total_size,
            "kpis": {
                "stale_pct": stale_pct,
                "stale_count": stale_count,
                "stale_size": stale_size,
                "risky_files": risky_count,
                "owner_count": owner_count,
                "large_files": large_count,
                "large_size": large_size,
                "dup_groups": dup_groups,
                "changes_24h": changes_24h,
            }
        }

    # --- SCAN TREND API ---

    @app.get("/api/trend/{source_id}")
    async def scan_trend(source_id: int):
        """Storage growth trend from scan history."""
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT id, started_at, completed_at, total_files, total_size, status
                FROM scan_runs WHERE source_id = ? ORDER BY started_at DESC LIMIT 20
            """, (source_id,))
            scans = [dict(r) for r in cur.fetchall()]

        growth = None
        if len(scans) >= 2:
            latest = scans[0]
            previous = scans[1]
            if latest["total_files"] and previous["total_files"]:
                growth = {
                    "file_diff": (latest["total_files"] or 0) - (previous["total_files"] or 0),
                    "size_diff": (latest["total_size"] or 0) - (previous["total_size"] or 0),
                }

        return {"scans": scans[::-1], "growth": growth}

    # --- DUPLIKE RAPOR ve SECICI ARSIV ---

    @app.get("/api/reports/duplicates/{source_id}")
    async def duplicate_report(source_id: int,
                                page: int = Query(1, ge=1, le=10000),
                                page_size: int = Query(50, ge=1, le=500),
                                min_size: int = 0):
        """Kopya dosya raporu - gruplandirmali.

        DuckDB kurulu ve ATTACH basarili ise tek-gecis CTE ile calisir,
        aksi halde SQLite yoluna duser.
        """
        from src.utils.size_formatter import format_size
        result = None
        if analytics.available:
            try:
                scan_id = db.get_latest_scan_id(source_id, include_running=False)
                if scan_id:
                    result = analytics.get_duplicate_groups(
                        scan_id, min_size, page, page_size
                    )
            except Exception as e:
                logger.warning("DuckDB duplicate sorgusu basarisiz, SQLite fallback: %s", e)
                result = None
        if result is None:
            result = db.get_duplicate_groups(
                source_id, min_size=min_size, page=page, page_size=page_size
            )
        # Boyut formatlama
        result["total_waste_size_formatted"] = format_size(result.get("total_waste_size", 0))
        for g in result.get("groups", []):
            g["file_size_formatted"] = format_size(g.get("file_size", 0))
            g["waste_size_formatted"] = format_size(g.get("waste_size", 0))
        return result

    @app.get("/api/export/duplicates/{source_id}")
    async def export_duplicates(source_id: int):
        """Kopya dosyalari CSV olarak export et."""
        from src.utils.size_formatter import format_size
        from fastapi.responses import StreamingResponse
        from datetime import datetime
        import io

        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")

        # Tum duplike gruplari getir (max 10000 grup)
        result = db.get_duplicate_groups(source_id, scan_id=scan_id, page=1, page_size=10000)

        output = io.StringIO()
        output.write('\ufeff')  # BOM for Excel
        output.write('Grup,Dosya Adi,Boyut,Boyut (Okunur),Kopya Sayisi,Dosya Yolu,Sahip,Son Erisim,Son Degisiklik\n')

        for idx, g in enumerate(result.get("groups", []), 1):
            for f in g.get("files", []):
                path = (f.get("file_path") or "").replace('"', '""')
                name = (f.get("file_name") or g.get("file_name", "")).replace('"', '""')
                owner = (f.get("owner") or "").replace('"', '""')
                output.write(f'{idx},"{name}",{g.get("file_size",0)},"{format_size(g.get("file_size",0))}",{g.get("count",0)},"{path}","{owner}",{f.get("last_access_time","")},{f.get("last_modify_time","")}\n')

        output.seek(0)
        filename = f"Duplicates_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    # --- CONTENT-HASH DUPLICATE DETECTION (issue #35) ---
    # Tiered pipeline: size -> 4 KB prefix hash -> full SHA-256.
    # Recompute endpoint'i hash'leri yeniden hesaplar ve persist eder;
    # GET endpoint'i cached sonuclari (duplicate_hash_groups) okur.

    @app.post("/api/duplicates/content/{source_id}/compute")
    async def content_duplicates_compute(source_id: int):
        """Icerik-tabanli kopya tespitini calistir ve sonuclari persist et.

        Son tamamlanmis scan_id bulunur, `ContentDuplicateEngine.compute`
        tetiklenir. Donus degeri pipeline istatistikleri.
        """
        from src.analyzer.content_duplicates import ContentDuplicateEngine
        from src.utils.size_formatter import format_size

        _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            raise HTTPException(404, "Tamamlanmis scan yok")

        engine = ContentDuplicateEngine(db, config)
        stats = engine.compute(scan_id)
        stats["scan_id"] = scan_id
        stats["bytes_hashed_formatted"] = format_size(stats.get("bytes_hashed", 0))
        return stats

    @app.get("/api/duplicates/content/{source_id}")
    async def content_duplicates_report(
        source_id: int,
        page: int = Query(1, ge=1, le=10000),
        page_size: int = Query(50, ge=1, le=500),
    ):
        """Cached icerik-hash kopya raporunu oku (waste_size DESC sirali).

        Hesaplama yapmaz — `POST .../compute` endpoint'i ile uretilir.
        """
        from src.analyzer.content_duplicates import ContentDuplicateEngine
        from src.utils.size_formatter import format_size

        _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            return {
                "has_data": False,
                "reason": "no_completed_scan",
                "scan_id": None,
                "total_groups": 0,
                "groups": [],
                "page": page,
                "page_size": page_size,
                "total_pages": 1,
            }

        engine = ContentDuplicateEngine(db, config)
        result = engine.get_report(scan_id, page=page, page_size=page_size)
        result["has_data"] = True
        result["total_waste_size_formatted"] = format_size(result.get("total_waste_size", 0))
        for g in result.get("groups", []):
            g["file_size_formatted"] = format_size(g.get("file_size", 0))
            g["waste_size_formatted"] = format_size(g.get("waste_size", 0))
        return result

    @app.post("/api/archive/selective")
    async def archive_selective(request):
        """Secili dosyalari arsivle (duplicate cleanup icin)."""
        from src.archiver.archive_engine import ArchiveEngine
        from src.utils.size_formatter import format_size

        body = await request.json()
        source_id = body.get("source_id")
        file_ids = body.get("file_ids", [])

        if not source_id or not file_ids:
            raise HTTPException(400, "source_id ve file_ids gerekli")

        # Kaynak bilgisi
        source = db.get_source(source_id)
        if not source:
            raise HTTPException(404, "Kaynak bulunamadi")

        archive_dest = source.get("archive_dest")
        if not archive_dest:
            raise HTTPException(400, "Arsiv hedefi tanimli degil")

        # Secili dosyalari veritabanindan al
        files = []
        with db.get_cursor() as cur:
            placeholders = ','.join('?' * len(file_ids))
            cur.execute(f"""
                SELECT * FROM scanned_files WHERE id IN ({placeholders}) AND source_id=?
            """, file_ids + [source_id])
            files = [dict(r) for r in cur.fetchall()]

        if not files:
            raise HTTPException(404, "Secili dosyalar bulunamadi")

        engine = ArchiveEngine(db, config)
        result = engine.archive_files(
            files, archive_dest, source["unc_path"], source_id,
            archived_by="duplicate_cleanup",
            trigger_type="manual",
            trigger_detail="duplicate_cleanup"
        )
        return result

    # --- BUYUME ANALIZI ---

    @app.get("/api/growth/{source_id}")
    async def growth_stats(source_id: int):
        """Yillik/aylik/gunluk buyume istatistikleri."""
        stats = None
        if analytics.available:
            try:
                stats = analytics.get_growth_stats(source_id)
            except Exception as e:
                logger.warning("DuckDB growth sorgusu basarisiz, SQLite fallback: %s", e)
        if stats is None:
            stats = db.get_growth_stats(source_id)
        creators = db.get_top_file_creators(source_id)
        stats["top_creators"] = creators
        return stats

    @app.get("/api/reports/top-creators/{source_id}")
    async def top_creators(source_id: int, limit: int = 20):
        """En cok dosya olusturan kullanicilar."""
        return db.get_top_file_creators(source_id, limit=limit)

    # --- ARCHIVE BY INSIGHT API ---

    @app.post("/api/archive/by-insight")
    async def archive_by_insight(request):
        """Archive files based on AI insight recommendation."""
        from starlette.requests import Request
        body = await request.json()
        insight_type = body.get("type")  # "stale_1year", "stale_3year", "temp_files", "large_files", "duplicates"
        source_id = body.get("source_id")
        confirm = body.get("confirm", False)

        if not insight_type or not source_id:
            raise HTTPException(400, "type ve source_id zorunlu")

        src = _get_source(db, source_id)
        if not src.archive_dest:
            raise HTTPException(400, "Arsiv hedefi tanimli degil")

        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")

        from src.utils.size_formatter import format_size

        # Build query based on insight_type
        with db.get_cursor() as cur:
            if insight_type == "stale_1year":
                cur.execute("""
                    SELECT * FROM scanned_files WHERE source_id=? AND scan_id=?
                    AND julianday('now') - julianday(last_access_time) > 365
                    ORDER BY last_access_time ASC LIMIT 10000
                """, (source_id, scan_id))
            elif insight_type == "stale_3year":
                cur.execute("""
                    SELECT * FROM scanned_files WHERE source_id=? AND scan_id=?
                    AND julianday('now') - julianday(last_access_time) > 1095
                    ORDER BY last_access_time ASC LIMIT 10000
                """, (source_id, scan_id))
            elif insight_type == "temp_files":
                cur.execute("""
                    SELECT * FROM scanned_files WHERE source_id=? AND scan_id=?
                    AND (LOWER(extension) IN ('tmp','temp','bak','old','log','cache')
                         OR file_name LIKE '~$%' OR file_name LIKE '%.tmp')
                    ORDER BY file_size DESC LIMIT 10000
                """, (source_id, scan_id))
            elif insight_type == "large_files":
                cur.execute("""
                    SELECT * FROM scanned_files WHERE source_id=? AND scan_id=?
                    AND file_size > 104857600
                    AND julianday('now') - julianday(last_access_time) > 180
                    ORDER BY file_size DESC LIMIT 10000
                """, (source_id, scan_id))
            elif insight_type == "duplicates":
                # Get duplicate groups first
                cur.execute("""
                    SELECT file_name, file_size FROM scanned_files
                    WHERE source_id=? AND scan_id=? AND file_size > 1048576
                    GROUP BY file_name, file_size HAVING COUNT(*) > 1
                """, (source_id, scan_id))
                dup_groups = [dict(r) for r in cur.fetchall()]
                # Get all but newest of each duplicate group
                files = []
                for grp in dup_groups[:100]:
                    cur.execute("""
                        SELECT * FROM scanned_files
                        WHERE source_id=? AND scan_id=? AND file_name=? AND file_size=?
                        ORDER BY last_modify_time DESC
                    """, (source_id, scan_id, grp["file_name"], grp["file_size"]))
                    group_files = [dict(r) for r in cur.fetchall()]
                    if len(group_files) > 1:
                        files.extend(group_files[1:])  # Keep newest, archive rest
                if not confirm:
                    total_size = sum(f["file_size"] for f in files)
                    return {
                        "preview": True,
                        "file_count": len(files),
                        "total_size": total_size,
                        "total_size_formatted": format_size(total_size),
                        "sample": [{"file_path": f["file_path"], "file_name": f["file_name"], "file_size": f["file_size"]} for f in files[:20]]
                    }
                # For confirm, use collected files
                from src.archiver.archive_engine import ArchiveEngine
                engine = ArchiveEngine(db, config)
                return engine.archive_files(
                    files, src.archive_dest, src.unc_path, src.id,
                    archived_by=f"ai_insight:{insight_type}",
                    trigger_type='ai_insight', trigger_detail=insight_type
                )
            else:
                raise HTTPException(400, f"Gecersiz insight type: {insight_type}")

            files = [dict(r) for r in cur.fetchall()] if insight_type != "duplicates" else []

        if insight_type == "duplicates":
            return {"preview": True, "file_count": 0, "total_size": 0, "total_size_formatted": "0 B", "sample": []}

        if not files:
            return {"preview": True, "file_count": 0, "total_size": 0, "total_size_formatted": "0 B", "sample": []}

        total_size = sum(f["file_size"] for f in files)

        if not confirm:
            return {
                "preview": True,
                "file_count": len(files),
                "total_size": total_size,
                "total_size_formatted": format_size(total_size),
                "sample": [{"file_path": f["file_path"], "file_name": f["file_name"], "file_size": f["file_size"]} for f in files[:20]]
            }

        # Confirmed - execute archive
        from src.archiver.archive_engine import ArchiveEngine
        engine = ArchiveEngine(db, config)
        return engine.archive_files(
            files, src.archive_dest, src.unc_path, src.id,
            archived_by=f"ai_insight:{insight_type}",
            trigger_type='ai_insight', trigger_detail=insight_type
        )

    # --- ARCHIVE OPERATIONS API ---

    @app.get("/api/archive/operations")
    async def get_operations(source_id: int = None, limit: int = 50):
        return db.get_archive_operations(source_id, limit)

    @app.get("/api/archive/operations/{op_id}")
    async def get_operation_detail(op_id: int):
        result = db.get_archive_operation_detail(op_id)
        if not result:
            raise HTTPException(404, "Islem bulunamadi")
        return result

    @app.post("/api/restore/by-operation/{op_id}")
    async def restore_by_operation(op_id: int):
        """Bir arsiv operasyonundaki TUM dosyalari geri yukle."""
        from src.archiver.restore_engine import RestoreEngine

        operation = db.get_archive_operation_detail(op_id)
        if not operation:
            raise HTTPException(404, "Islem bulunamadi")

        if operation["operation_type"] != "archive":
            raise HTTPException(400, "Sadece arsiv islemleri geri yuklenebilir")

        # Restore operation kaydi olustur
        restore_op_id = None
        try:
            restore_op_id = db.create_archive_operation(
                'restore', operation.get("source_id"),
                'manual', f'Restore of operation #{op_id}'
            )
        except Exception:
            pass

        engine = RestoreEngine(db)
        restored = 0
        failed = 0
        total_size = 0
        errors = []

        # Islem zamanindaki arsivlenmis dosyalari getir
        files = operation.get("files", [])
        if not files:
            # Fallback: files_json'dan al
            import json
            fj = operation.get("files_json")
            if fj and isinstance(fj, str):
                try:
                    files = json.loads(fj)
                except Exception:
                    pass

        for f in files:
            archive_id = f.get("id")
            if not archive_id:
                continue
            if f.get("restored_at"):
                continue
            result = engine.restore_by_id(archive_id, op_id=restore_op_id)
            if result.get("success"):
                restored += 1
                total_size += f.get("file_size", 0)
            else:
                failed += 1
                errors.append({"id": archive_id, "error": result.get("error", "")})

        # Tamamla
        if restore_op_id:
            try:
                status = 'completed' if failed == 0 else 'partial'
                db.complete_archive_operation(restore_op_id, restored, total_size, status)
            except Exception:
                pass

        return {
            "restored": restored,
            "failed": failed,
            "total_size": total_size,
            "errors": errors[:10],
            "operation_id": restore_op_id
        }

    # --- TOPLU GERI YUKLEME ---

    @app.post("/api/restore/bulk")
    async def bulk_restore(request):
        """Toplu geri yukleme - onizleme veya gercek."""
        from src.archiver.restore_engine import RestoreEngine
        from src.utils.size_formatter import format_size

        body = await request.json()
        archive_ids = body.get("archive_ids", [])
        confirm = body.get("confirm", False)

        if not archive_ids:
            raise HTTPException(400, "archive_ids gerekli")

        engine = RestoreEngine(db)

        if not confirm:
            # Onizleme
            preview = engine.preview_restore(archive_ids)
            preview["total_size_formatted"] = format_size(preview.get("total_size", 0))
            return {"preview": True, **preview}
        else:
            # Gercek geri yukleme
            # source_id al (ilk dosyadan)
            first = db.get_archived_file_by_id(archive_ids[0])
            source_id = first.get("source_id") if first else None
            result = engine.bulk_restore(archive_ids, source_id)
            result["total_size_formatted"] = format_size(result.get("total_size", 0))
            return result

    @app.get("/api/archive/browse")
    async def browse_archived(source_id: int = None,
                               page: int = Query(1, ge=1, le=10000),
                               page_size: int = Query(50, ge=1, le=500)):
        """Arsivlenmis dosyalara goz at (geri yukleme UI icin)."""
        from src.utils.size_formatter import format_size
        result = db.search_archived_files("", page=page, page_size=page_size)
        for f in result.get("results", []):
            f["file_size_formatted"] = format_size(f.get("file_size", 0))
        return result

    # --- ARSIV GECMISI ---

    @app.get("/api/archive/history")
    async def archive_history(source_id: int = None,
                              page: int = Query(1, ge=1, le=10000),
                              page_size: int = Query(20, ge=1, le=500),
                              date_from: str = None,
                              date_to: str = None, op_type: str = None):
        """Sayfalanmis arsiv islem gecmisi."""
        from src.utils.size_formatter import format_size
        result = db.get_archive_history(source_id, page, page_size,
                                        date_from, date_to, op_type)
        for op in result["operations"]:
            op["total_size_formatted"] = format_size(op.get("total_size") or 0)
        return result

    @app.get("/api/archive/operations/{op_id}/files")
    async def operation_files(op_id: int,
                               page: int = Query(1, ge=1, le=10000),
                               page_size: int = Query(100, ge=1, le=500)):
        """Arsiv islemindeki dosyalari sayfalanmis getir."""
        from src.utils.size_formatter import format_size
        result = db.get_archive_operation_files(op_id, page, page_size)
        for f in result.get("files", []):
            f["file_size_formatted"] = format_size(f.get("file_size") or 0)
        return result

    # --- SYSTEM API ---

    @app.post("/api/system/open-folder")
    async def open_folder(request: Request):
        """Dizini Windows Explorer'da ac (yalnizca yerel istemci icin)."""
        body = await request.json()
        client_host = (request.client.host if request.client else "")
        return open_folder_impl(body, client_host)

    @app.get("/api/system/health")
    async def health():
        # WAL ve toplam disk durumu
        wal_warning = None
        try:
            wal_path = db.db_path + "-wal"
            wal_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
            if wal_size > 500_000_000:
                wal_warning = {
                    "wal_size_bytes": wal_size,
                    "wal_size_formatted": f"{wal_size / 1073741824:.2f} GB"
                                          if wal_size > 1073741824
                                          else f"{wal_size / 1048576:.0f} MB",
                    "severity": "critical" if wal_size > 5_000_000_000 else "warning",
                    "recommendation": "Kaynaklar -> Veritabani Bakimi -> Optimize Et butonuna basarak "
                                       "WAL'i temizleyin.",
                }
        except Exception:
            pass

        # Issue #64: surface which PII regex backend is in effect
        # (hyperscan when available + opted-in, else stdlib re).
        try:
            from src.compliance._pii_backends import (
                hyperscan_available, hyperscan_version,
            )
            pii_cfg = (
                ((config or {}).get("compliance", {}) or {}).get("pii", {}) or {}
            )
            pii_engine_pref = pii_cfg.get("engine", "auto")
            hs_ok = hyperscan_available()
            if pii_engine_pref == "re":
                effective = "re"
            elif pii_engine_pref == "hyperscan":
                effective = "hyperscan" if hs_ok else "re"
            else:  # auto
                effective = "hyperscan" if hs_ok else "re"
            pii_backend_info = {
                "backend": effective,
                "configured": pii_engine_pref,
                "hyperscan_available": hs_ok,
                "version": hyperscan_version() if effective == "hyperscan" else None,
            }
        except Exception:
            pii_backend_info = {"backend": "re", "configured": "auto",
                                 "hyperscan_available": False, "version": None}

        return {
            "status": "ok",
            "time": datetime.now().isoformat(),
            "version": APP_VERSION,
            "database": db.health_check(),
            "analytics": analytics.health(),
            "email": email_notifier.health(),
            "wal_warning": wal_warning,
            "pii_backend": pii_backend_info,
        }

    @app.get("/api/system/analytics")
    async def analytics_status():
        """DuckDB analitik motor durumunu dondur."""
        return analytics.health()

    # --- NOTIFICATIONS API ---

    class TestEmailRequest(BaseModel):
        to: str
        username: Optional[str] = "test"
        display_name: Optional[str] = None

    @app.get("/api/notifications/status")
    async def notifications_status():
        """SMTP konfigurasyon ozeti + canli bind testi."""
        info = email_notifier.health()
        if email_notifier.available:
            info["probe"] = email_notifier.test_connection()
        return info

    @app.post("/api/notifications/test")
    async def notifications_test(payload: TestEmailRequest):
        """Belirtilen adrese kucuk bir dogrulama e-postasi gonder.

        Kullanim: SMTP ayarlarini dogrulamak icin. Gercek skor degil,
        sabit ornek skor verisi gonderilir. Admin CC varsa CC'ye eklenir.
        """
        if not email_notifier.available:
            raise HTTPException(400, f"SMTP kullanilamaz: {email_notifier._init_error}")
        fake_score = {
            "score": 85, "grade": "B", "total_penalty": 15,
            "factors": [
                {"name": "stale_files", "label": "Test: 1+ yildir erisilmeyen dosyalar",
                 "count": 42, "penalty": 8, "max": 30},
                {"name": "oversized_files", "label": "Test: 100 MB'dan buyuk dosyalar",
                 "count": 7, "penalty": 7, "max": 15},
            ],
            "non_compliance": {"stale_files": 42, "oversized_files": 7,
                                "naming_violations": 0, "duplicate_files": 0,
                                "dormant": False},
            "suggestions": [
                "Bu bir TEST mesajidir. SMTP ayarlariniz dogru calisiyor.",
                "Gercek rapor zamanli bildirim zamanlayicisi (PR D) ile gelecek.",
            ],
            "total_files": 100, "total_size": 5 * 1024 * 1024 * 1024,
        }
        result = email_notifier.send_user_report(
            username=payload.username or "test",
            email=payload.to,
            score_result=fake_score,
            display_name=payload.display_name,
        )
        if not result.get("ok"):
            raise HTTPException(500, result.get("error") or "Gonderim basarisiz")
        return result

    @app.post("/api/notifications/send-to/{username}")
    async def notifications_send_to(username: str):
        """Tek kullaniciya gercek verimlilik raporu gonder.

        Kullanici e-postasi AD'den cozulur; yoksa 400 doner.
        """
        if not email_notifier.available:
            raise HTTPException(400, f"SMTP kullanilamaz: {email_notifier._init_error}")

        # AD lookup opsiyonel; ADLookup mevcutsa kullan
        ad_info = None
        try:
            ad = getattr(app.state, "ad_lookup", None)
            if ad and ad.available:
                ad_info = ad.lookup(username)
        except Exception:
            ad_info = None

        if not ad_info or not ad_info.get("email"):
            raise HTTPException(404, f"{username} icin e-posta bulunamadi (AD lookup sonucu yok)")

        from src.user_activity.efficiency_score import compute_user_score
        score = compute_user_score(db, username)
        result = email_notifier.send_user_report(
            username=username,
            email=ad_info["email"],
            score_result=score,
            display_name=ad_info.get("display_name"),
        )
        if not result.get("ok"):
            raise HTTPException(500, result.get("error") or "Gonderim basarisiz")
        return {"sent_to": ad_info["email"], "cc": result.get("cc"), "score": score["score"]}

    @app.get("/api/notifications/log")
    async def notifications_log(username: Optional[str] = None,
                                 status: Optional[str] = None,
                                 page: int = Query(1, ge=1, le=10000),
                                 page_size: int = Query(50, ge=1, le=500)):
        """Gonderim log listesi (sayfalanmis)."""
        offset = (page - 1) * page_size
        conditions = []
        params: list = []
        if username:
            conditions.append("username = ?")
            params.append(username)
        if status:
            conditions.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        with db.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM notification_log{where}", params)
            total = cur.fetchone()["cnt"]
            cur.execute(
                f"""SELECT id, username, email, cc, subject, status, error, sent_at
                    FROM notification_log{where}
                    ORDER BY sent_at DESC LIMIT ? OFFSET ?""",
                params + [page_size, offset],
            )
            rows = [dict(r) for r in cur.fetchall()]
        return {"total": total, "rows": rows, "page": page, "page_size": page_size}

    @app.get("/api/system/version")
    async def version():
        """Calisan sürüm (repo kokundeki VERSION dosyasindan)."""
        return {"version": APP_VERSION}

    @app.get("/api/system/ad/status")
    async def ad_status():
        """AD/LDAP baglanti durumu + bind testi."""
        return ad_lookup.health()

    @app.get("/api/users/{username}/ad-info")
    async def user_ad_info(username: str, refresh: bool = False):
        """Kullanici adindan e-posta + display name cozumle.

        AD devre disi veya erisilmez ise cache'ten doner; cache yoksa
        null alanlarla sessizce doner.
        """
        info = ad_lookup.lookup(username, force_refresh=refresh)
        if info is None:
            return {
                "username": username,
                "email": None,
                "display_name": None,
                "found": False,
                "source": None,
            }
        return info

    def _parse_ver(v: str):
        """Semver-ish karsilastirma icin tuple parse et.

        '1.6.0'      -> (1,6,0, False) — stabil
        '1.7.0-dev'  -> (1,7,0, True)  — pre-release (master'da, release'ten onde)
        """
        v = v.strip().lstrip("v")
        main, _, suffix = v.partition("-")
        parts = main.split(".")
        try:
            nums = tuple(int(p) for p in parts)
        except ValueError:
            nums = (0,)
        return nums + (bool(suffix),)

    @app.get("/api/system/version-check")
    async def version_check():
        """GitHub latest release ile yerel sürümü karsilastir.

        Kurumsal aglarda GitHub API'sine erisim yoksa hata yerine
        `error` alani ile temiz doner — UI banner'i sessizce gizlenir.
        """
        import urllib.request
        import urllib.error
        import ssl

        api_url = "https://api.github.com/repos/deepdarbe/FILE_ACTIVITY/releases/latest"
        local = APP_VERSION
        try:
            req = urllib.request.Request(api_url, headers={"User-Agent": "file-activity-dashboard"})
            # TLS dogrulama: kurumsal proxy arkasinda ilerleyebilsin diye
            # sistem CA store'u kullan; basarisiz olursa sessizce dus
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            remote = (data.get("tag_name") or "").lstrip("v")
            if not remote:
                return {"local": local, "remote": None, "update_available": False,
                        "error": "no tag_name"}
            local_tuple = _parse_ver(local)
            remote_tuple = _parse_ver(remote)
            # Pre-release (suffix=True) kendi base'iyle esitse ilerdedir, update yok
            local_core = local_tuple[:-1]
            remote_core = remote_tuple[:-1]
            is_pre = local_tuple[-1]
            update_available = (local_core < remote_core) and not (is_pre and local_core == remote_core)
            return {
                "local": local,
                "remote": remote,
                "update_available": update_available,
                "release_url": data.get("html_url"),
                "published_at": data.get("published_at"),
            }
        except urllib.error.URLError as e:
            return {"local": local, "remote": None, "update_available": False,
                    "error": f"github API erisilemedi: {e.reason}"}
        except (ssl.SSLError, TimeoutError, OSError) as e:
            return {"local": local, "remote": None, "update_available": False,
                    "error": f"ag hatasi: {e}"}
        except Exception as e:
            logger.warning("version-check beklenmeyen hata: %s", e)
            return {"local": local, "remote": None, "update_available": False,
                    "error": "beklenmeyen hata"}

    @app.post("/api/system/update")
    async def update():
        """Yerel update.cmd'yi detached olarak baslat.

        update.cmd setup-source.ps1'i calistirir, bu da calisan python
        process'ini durdurur ve master'in en son halini indirir. Dashboard
        30 saniye sonra yeniden baslatilir.
        """
        import subprocess
        import sys

        if sys.platform != "win32":
            raise HTTPException(400, "Guncelleme sadece Windows uzerinde desteklenir")

        # update.cmd'yi cwd veya kurulum dizininde ara
        candidates = [
            os.path.join(os.getcwd(), "update.cmd"),
            r"C:\FileActivity\update.cmd",
        ]
        update_cmd = next((p for p in candidates if os.path.exists(p)), None)
        if not update_cmd:
            raise HTTPException(
                404,
                f"update.cmd bulunamadi. Kontrol edilen yollar: {candidates}. "
                "Manuel guncelleme icin setup-source.ps1'i tekrar calistirin."
            )

        # Detached process: dashboard'u hemen donduruyor, update.cmd
        # kendi console'unda devam ediyor. CREATE_NEW_CONSOLE ile kullanici
        # ilerlemeyi gorebilsin.
        DETACHED_PROCESS = 0x00000008
        CREATE_NEW_CONSOLE = 0x00000010
        CREATE_NEW_PROCESS_GROUP = 0x00000200

        subprocess.Popen(
            ["cmd.exe", "/c", update_cmd],
            creationflags=DETACHED_PROCESS | CREATE_NEW_CONSOLE | CREATE_NEW_PROCESS_GROUP,
            close_fds=True,
            cwd=os.path.dirname(update_cmd),
        )
        logger.info("Guncelleme tetiklendi: %s", update_cmd)
        return {
            "status": "started",
            "message": "Guncelleme baslatildi. 30-60 saniye sonra dashboard'u yenileyin.",
            "update_cmd": update_cmd,
        }

    @app.get("/api/db/stats")
    async def db_stats():
        """Veritabani istatistikleri."""
        if analytics.available:
            try:
                wal_path = db.db_path + "-wal"
                shm_path = db.db_path + "-shm"
                tables = ["scanned_files", "scan_runs", "archived_files",
                          "user_access_logs", "sources"]
                return analytics.get_db_stats(tables, db.db_path, wal_path, shm_path)
            except Exception as e:
                logger.warning("DuckDB db_stats basarisiz, SQLite fallback: %s", e)
        return db.get_db_stats()

    @app.post("/api/db/cleanup")
    async def db_cleanup(keep_last: int = Query(default=5, ge=1, le=50)):
        """Eski tarama verilerini temizle. Son N taramayi korur."""
        result = db.cleanup_old_scans(keep_last_n=keep_last)
        return result

    @app.post("/api/db/optimize")
    async def db_optimize():
        """VACUUM + ANALYZE ile veritabanini optimize et."""
        result = db.optimize_database()
        return result

    # ── AD-HOC SQL SORGU PANELI (issue #48) ──
    # Whitelist-guarded read-only DuckDB executor. Frontend "Sorgu" sekmesi
    # buradan beslenir; SQL'i once SqlQueryGuard.validate suzgecinden gecirir,
    # ardindan audit_event yazip DuckDB uzerinden SQLite'a salt-okunur calistirir.

    class QueryRequest(BaseModel):
        sql: str
        max_rows: int = Field(default=1000, ge=1, le=10000)

    @app.post("/api/analytics/query")
    async def analytics_query(req: QueryRequest, request: Request):
        from src.dashboard.sql_query import SqlQueryGuard
        panel_cfg = (config.get("analytics", {}) or {}).get("query_panel", {}) or {}
        if not panel_cfg.get("enabled", True):
            raise HTTPException(403, "Sorgu paneli devre disi")
        max_rows = min(int(req.max_rows), int(panel_cfg.get("max_rows", 10000)))
        guard = SqlQueryGuard(
            max_rows=max_rows,
            timeout_seconds=int(panel_cfg.get("timeout_seconds", 30)),
        )
        ok, reason = guard.validate(req.sql)
        if not ok:
            raise HTTPException(400, f"Sorgu reddedildi: {reason}")
        try:
            db.insert_audit_event_simple(
                source_id=None, event_type="sql_query", username="admin",
                file_path=None, details=req.sql[:500], detected_by="dashboard",
            )
        except Exception as e:  # pragma: no cover - audit best-effort
            logger.warning("sql_query audit yazilamadi: %s", e)
        try:
            return guard.execute(db, req.sql)
        except Exception as e:
            raise HTTPException(500, f"Sorgu hatasi: {e}")

    # ── ARKA PLAN EXPORT SISTEMI ──

    def _export_worker(job_id: str, report_type: str, source_id: int, scan_id: int, params: dict):
        """Arka planda XLS olusturma worker'i."""
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
        from src.utils.size_formatter import format_size
        import re as re_mod

        try:
            with _export_lock:
                _export_jobs[job_id]["status"] = "running"
                _export_jobs[job_id]["progress"] = 0

            export_dir = os.path.join(os.path.dirname(db.db_path), "exports")
            os.makedirs(export_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            wb = openpyxl.Workbook()
            ws = wb.active

            # Stiller
            header_font = Font(bold=True, color="FFFFFF", size=11)
            header_fill = PatternFill(start_color="2B5797", end_color="2B5797", fill_type="solid")
            alt_fill = PatternFill(start_color="F2F6FC", end_color="F2F6FC", fill_type="solid")
            border = Border(
                left=Side(style='thin', color='D0D5DD'),
                right=Side(style='thin', color='D0D5DD'),
                top=Side(style='thin', color='D0D5DD'),
                bottom=Side(style='thin', color='D0D5DD')
            )

            if report_type == "mit_naming":
                ws.title = "MIT Adlandirma Uyumu"
                filename = f"MIT_Naming_{timestamp}.xlsx"
                headers = ["Ihlal Kodu", "Ihlal Turu", "Ciddiyet", "Dosya Adi", "Tam Yol", "Boyut", "Boyut (Okunan)", "Sahip", "Son Degisiklik"]
                checks = {
                    "R1": ("Bosluk Iceren", "Zorunlu", lambda p, n: bool(re_mod.search(r'\s', n))),
                    "R2": ("Ilk Karakter Harf Degil", "Zorunlu", lambda p, n: bool(n) and not re_mod.match(r'^[a-zA-Z]', n)),
                    "R3": ("Yasak Karakter", "Zorunlu", lambda p, n: bool(n) and '.' in n and not re_mod.match(r'^[a-zA-Z0-9._-]+$', n[:n.rfind('.')])),
                    "R4": ("Uzanti Sorunu", "Zorunlu", lambda p, n: '.' not in n or not n.rsplit('.', 1)[-1].isalpha()),
                    "B1": ("Uzun Ad (>31)", "Oneri", lambda p, n: len(n) > 31),
                    "B3": ("Base Nokta", "Oneri", lambda p, n: '.' in n and n[:n.rfind('.')].count('.') > 0),
                    "B4": ("Buyuk Harf", "Oneri", lambda p, n: bool(re_mod.search(r'[A-Z]', n[:n.rfind('.')] if '.' in n else n))),
                    "B5": ("Ayirici Yok", "Oneri", lambda p, n: len(n) > 10 and '_' not in n and '-' not in n),
                }
                for i, h in enumerate(headers, 1):
                    cell = ws.cell(row=1, column=i, value=h)
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = Alignment(horizontal='center')

                row = 2
                with db.get_cursor() as cur:
                    cur.execute("SELECT COUNT(*) as cnt FROM scanned_files WHERE source_id=? AND scan_id=?", (source_id, scan_id))
                    total = cur.fetchone()["cnt"]
                    cur.execute("SELECT file_path, file_name, file_size, owner, last_modify_time FROM scanned_files WHERE source_id=? AND scan_id=?", (source_id, scan_id))
                    processed = 0
                    for rec in cur:
                        processed += 1
                        if processed % 5000 == 0:
                            with _export_lock:
                                _export_jobs[job_id]["progress"] = int(processed * 100 / max(total, 1))
                        for code, (label, severity, fn) in checks.items():
                            if fn(rec["file_path"], rec["file_name"]):
                                ws.cell(row=row, column=1, value=code)
                                ws.cell(row=row, column=2, value=label)
                                ws.cell(row=row, column=3, value=severity)
                                ws.cell(row=row, column=4, value=rec["file_name"])
                                ws.cell(row=row, column=5, value=rec["file_path"])
                                ws.cell(row=row, column=6, value=rec.get("file_size", 0))
                                ws.cell(row=row, column=7, value=format_size(rec.get("file_size", 0)))
                                ws.cell(row=row, column=8, value=rec.get("owner", ""))
                                ws.cell(row=row, column=9, value=rec.get("last_modify_time", ""))
                                if row % 2 == 0:
                                    for c in range(1, 10):
                                        ws.cell(row=row, column=c).fill = alt_fill
                                row += 1

                # Ozet sayfasi
                ws_sum = wb.create_sheet("Ozet", 0)
                ws_sum.cell(row=1, column=1, value="MIT Libraries File Naming Scheme - Uyum Raporu").font = Font(bold=True, size=14)
                ws_sum.cell(row=2, column=1, value=f"Tarih: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                ws_sum.cell(row=3, column=1, value=f"Toplam dosya: {total:,}")
                ws_sum.cell(row=4, column=1, value=f"Ihlal satiri: {row-2:,}")

            elif report_type == "duplicates":
                ws.title = "Kopya Dosyalar"
                filename = f"Duplicates_{timestamp}.xlsx"
                headers = ["Grup", "Dosya Adi", "Boyut", "Boyut (Okunan)", "Tam Yol", "Sahip", "Son Degisiklik"]
                for i, h in enumerate(headers, 1):
                    cell = ws.cell(row=1, column=i, value=h)
                    cell.font = header_font
                    cell.fill = header_fill

                row = 2
                with db.get_cursor() as cur:
                    cur.execute("""
                        SELECT file_name, file_size, file_path, owner, last_modify_time
                        FROM scanned_files WHERE source_id=? AND scan_id=? AND file_size > 1048576
                        ORDER BY file_name, file_size, file_path
                    """, (source_id, scan_id))
                    prev_key = None
                    group_num = 0
                    for rec in cur:
                        key = (rec["file_name"], rec["file_size"])
                        if key != prev_key:
                            group_num += 1
                            prev_key = key
                        ws.cell(row=row, column=1, value=group_num)
                        ws.cell(row=row, column=2, value=rec["file_name"])
                        ws.cell(row=row, column=3, value=rec["file_size"])
                        ws.cell(row=row, column=4, value=format_size(rec["file_size"]))
                        ws.cell(row=row, column=5, value=rec["file_path"])
                        ws.cell(row=row, column=6, value=rec.get("owner", ""))
                        ws.cell(row=row, column=7, value=rec.get("last_modify_time", ""))
                        row += 1

            elif report_type == "full":
                ws.title = "Tum Dosyalar"
                filename = f"FullReport_{timestamp}.xlsx"
                headers = ["Dosya Adi", "Uzanti", "Boyut", "Boyut (Okunan)", "Tam Yol", "Sahip", "Olusturma", "Son Erisim", "Son Degisiklik"]
                for i, h in enumerate(headers, 1):
                    cell = ws.cell(row=1, column=i, value=h)
                    cell.font = header_font
                    cell.fill = header_fill

                row = 2
                with db.get_cursor() as cur:
                    cur.execute("SELECT COUNT(*) as cnt FROM scanned_files WHERE source_id=? AND scan_id=?", (source_id, scan_id))
                    total = cur.fetchone()["cnt"]
                    cur.execute("SELECT * FROM scanned_files WHERE source_id=? AND scan_id=? ORDER BY file_path", (source_id, scan_id))
                    processed = 0
                    for rec in cur:
                        processed += 1
                        if processed % 5000 == 0:
                            with _export_lock:
                                _export_jobs[job_id]["progress"] = int(processed * 100 / max(total, 1))
                        ws.cell(row=row, column=1, value=rec["file_name"])
                        ws.cell(row=row, column=2, value=rec.get("extension", ""))
                        ws.cell(row=row, column=3, value=rec.get("file_size", 0))
                        ws.cell(row=row, column=4, value=format_size(rec.get("file_size", 0)))
                        ws.cell(row=row, column=5, value=rec["file_path"])
                        ws.cell(row=row, column=6, value=rec.get("owner", ""))
                        ws.cell(row=row, column=7, value=rec.get("creation_time", ""))
                        ws.cell(row=row, column=8, value=rec.get("last_access_time", ""))
                        ws.cell(row=row, column=9, value=rec.get("last_modify_time", ""))
                        if row % 2 == 0:
                            for c in range(1, 10):
                                ws.cell(row=row, column=c).fill = alt_fill
                        row += 1
            else:
                raise ValueError(f"Bilinmeyen rapor tipi: {report_type}")

            # Sutun genisliklerini ayarla
            for ws_sheet in wb.worksheets:
                for col in range(1, ws_sheet.max_column + 1):
                    max_len = max(len(str(ws_sheet.cell(row=r, column=col).value or "")) for r in range(1, min(ws_sheet.max_row + 1, 100)))
                    ws_sheet.column_dimensions[get_column_letter(col)].width = min(max_len + 2, 50)

            file_path = os.path.join(export_dir, filename)
            wb.save(file_path)

            with _export_lock:
                _export_jobs[job_id]["status"] = "completed"
                _export_jobs[job_id]["progress"] = 100
                _export_jobs[job_id]["file_path"] = file_path
                _export_jobs[job_id]["file_name"] = filename
                _export_jobs[job_id]["file_size"] = os.path.getsize(file_path)

        except Exception as e:
            logging.getLogger("file_activity").error(f"Export error: {e}")
            with _export_lock:
                _export_jobs[job_id]["status"] = "error"
                _export_jobs[job_id]["error"] = str(e)

    @app.post("/api/export/start")
    async def start_export(report_type: str = Query(...), source_id: int = Query(...)):
        """Arka planda XLS export baslat. Tarama devam ederken bile calisir."""
        # Son tamamlanmis taramayi kullan (aktif tarama kilitlemesin)
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT id FROM scan_runs WHERE source_id=? AND status='completed'
                ORDER BY started_at DESC LIMIT 1
            """, (source_id,))
            completed = cur.fetchone()
            if completed:
                scan_id = completed["id"]
            else:
                # Tamamlanmis yoksa son taramayi al
                scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")

        job_id = str(uuid.uuid4())[:8]
        with _export_lock:
            _export_jobs[job_id] = {
                "status": "queued",
                "progress": 0,
                "report_type": report_type,
                "source_id": source_id,
                "scan_id": scan_id,
                "created_at": datetime.now().isoformat(),
                "file_path": None,
                "file_name": None,
                "error": None,
            }

        thread = threading.Thread(
            target=_export_worker,
            args=(job_id, report_type, source_id, scan_id, {}),
            daemon=True
        )
        thread.start()
        return {"job_id": job_id, "status": "queued"}

    @app.get("/api/export/status/{job_id}")
    async def export_status(job_id: str):
        """Export is durumu sorgula."""
        with _export_lock:
            job = _export_jobs.get(job_id)
        if not job:
            raise HTTPException(404, "Export isi bulunamadi")
        return {
            "job_id": job_id,
            "status": job["status"],
            "progress": job["progress"],
            "file_name": job.get("file_name"),
            "file_size": job.get("file_size"),
            "error": job.get("error"),
        }

    @app.get("/api/export/download/{job_id}")
    async def export_download(job_id: str):
        """Tamamlanmis export dosyasini indir."""
        with _export_lock:
            job = _export_jobs.get(job_id)
        if not job:
            raise HTTPException(404, "Export isi bulunamadi")
        if job["status"] != "completed" or not job.get("file_path"):
            raise HTTPException(400, "Export henuz tamamlanmadi")
        return FileResponse(
            job["file_path"],
            filename=job["file_name"],
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    @app.get("/api/export/jobs")
    async def export_jobs():
        """Tum export islerini listele."""
        with _export_lock:
            return [{"job_id": k, **{kk: vv for kk, vv in v.items() if kk != "file_path"}} for k, v in _export_jobs.items()]

    # --- RANSOMWARE DETECTOR API (issue #37) ---

    def _get_detector():
        det = getattr(app.state, "ransomware", None)
        if det is None:
            raise HTTPException(503, "Ransomware detector kullanilamiyor")
        return det

    @app.get("/api/security/ransomware/alerts")
    async def list_ransomware_alerts(since_minutes: int = Query(60, ge=1, le=10080)):
        """Son N dakikadaki ransomware uyarilarini listele (yeni once)."""
        det = _get_detector()
        return det.get_active_alerts(since_minutes=since_minutes)

    @app.post("/api/security/ransomware/alerts/{alert_id}/acknowledge")
    async def acknowledge_ransomware_alert(alert_id: int, by_user: str = "admin"):
        """Bir uyariyi onayla — acknowledged_at + acknowledged_by yazilir."""
        with db.get_cursor() as cur:
            cur.execute(
                """UPDATE ransomware_alerts
                   SET acknowledged_at = datetime('now','localtime'),
                       acknowledged_by = ?
                   WHERE id = ?""",
                (by_user, alert_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(404, f"Uyari bulunamadi (ID: {alert_id})")
        return {"acknowledged": True, "id": alert_id, "by": by_user}

    @app.post("/api/security/ransomware/canaries/{source_id}/deploy")
    async def deploy_ransomware_canaries(source_id: int):
        """Kaynagin paylasim koküne canary dosyalarini birak."""
        src = _get_source(db, source_id)
        det = _get_detector()
        placed = det.deploy_canaries(src.id, src.unc_path)
        return {
            "source_id": src.id,
            "share_root": src.unc_path,
            "placed": placed,
            "canary_names": sorted(det.canary_names),
        }

    @app.post("/api/security/ransomware/test")
    async def ransomware_test(source_id: Optional[int] = None,
                                username: str = "test_user",
                                rule: str = "rename_velocity"):
        """Sentetik olay enjeksiyonu — kural + e-posta + SMB kill (dry-run)
        boru hattini hizlica dogrulamak icin. Production'da kullanilmamali.

        rule: 'rename_velocity' | 'risky_extension' | 'mass_deletion' | 'canary_access'
        """
        det = _get_detector()
        if source_id is not None:
            _get_source(db, source_id)

        rule = (rule or "").strip().lower()
        last_alert = None
        if rule == "rename_velocity":
            for i in range(det.rename_threshold + 1):
                last_alert = det.consume_event({
                    "source_id": source_id,
                    "username": username,
                    "file_path": f"/test/synth_{i}.bin",
                    "old_path": f"/test/synth_{i}.txt",
                    "event_type": "rename",
                }) or last_alert
        elif rule == "risky_extension":
            last_alert = det.consume_event({
                "source_id": source_id,
                "username": username,
                "file_path": "/test/secret.docx.encrypted",
                "event_type": "modify",
            })
        elif rule == "mass_deletion":
            for i in range(det.delete_threshold + 1):
                last_alert = det.consume_event({
                    "source_id": source_id,
                    "username": username,
                    "file_path": f"/test/del_{i}.bin",
                    "event_type": "delete",
                }) or last_alert
        elif rule == "canary_access":
            canary = sorted(det.canary_names)[0] if det.canary_names else "_AAAA_canary_DO_NOT_DELETE.txt"
            last_alert = det.consume_event({
                "source_id": source_id,
                "username": username,
                "file_path": f"/test/{canary}",
                "event_type": "access",
            })
        else:
            raise HTTPException(400, f"Bilinmeyen kural: {rule}")

        # SMB kill her zaman dry-run pipeline'i ile dogrulanir.
        smb_dry = None
        try:
            from src.security.smb_session import kill_user_session
            smb_dry = kill_user_session(username, dry_run=True)
        except Exception as e:  # pragma: no cover - defensive
            smb_dry = {"error": f"smb_session_unavailable: {e}", "killed": 0}

        return {
            "rule": rule,
            "alert": last_alert,
            "smb_dry_run": smb_dry,
            "auto_kill_session": det.auto_kill_session,
            "notification_email": det.notification_email or None,
        }

    # ─────────────────────────────────────────────────────────────────
    # NTFS ACL / effective-permissions analyzer (#49)
    # ─────────────────────────────────────────────────────────────────

    def _get_acl_analyzer():
        existing = getattr(app.state, "acl_analyzer", None)
        if existing is not None:
            return existing
        from src.security.acl_analyzer import AclAnalyzer
        analyzer = AclAnalyzer(db, config, ad_lookup=ad_lookup)
        app.state.acl_analyzer = analyzer
        return analyzer

    @app.get("/api/security/acl")
    async def get_effective_acl(path: str = Query(..., min_length=1)):
        """Live effective DACL read for one path. Windows-only."""
        analyzer = _get_acl_analyzer()
        if not analyzer.is_supported():
            raise HTTPException(501, "ACL live read requires Windows + pywin32")
        try:
            return analyzer.get_effective_acl(path)
        except FileNotFoundError as e:
            raise HTTPException(404, f"Path not found: {path}") from e
        except PermissionError as e:
            raise HTTPException(403, f"Access denied reading ACL: {e}") from e
        except Exception as e:
            raise HTTPException(500, f"ACL read failed: {e}") from e

    @app.get("/api/security/acl/trustee/{sid}/paths")
    async def acl_paths_for_trustee(sid: str,
                                    limit: int = Query(100, ge=1, le=10000)):
        analyzer = _get_acl_analyzer()
        return {
            "trustee_sid": sid,
            "limit": limit,
            "paths": analyzer.find_paths_for_trustee(sid, limit=limit),
        }

    @app.get("/api/security/acl/sprawl")
    async def acl_sprawl(scan_id: Optional[int] = None,
                         severity_threshold: Optional[int] = None):
        analyzer = _get_acl_analyzer()
        thr = severity_threshold if severity_threshold is not None else analyzer.sprawl_threshold_mask
        return {
            "scan_id": scan_id,
            "severity_threshold": int(thr),
            "trustees": analyzer.detect_sprawl(scan_id=scan_id,
                                                severity_threshold=int(thr)),
        }

    @app.post("/api/security/acl/scan/{source_id}")
    async def acl_snapshot(source_id: int,
                           max_files: Optional[int] = None):
        analyzer = _get_acl_analyzer()
        if not analyzer.is_supported():
            raise HTTPException(501, "ACL snapshot requires Windows + pywin32")
        src = _get_source(db, source_id)
        with db.get_cursor() as cur:
            cur.execute(
                """SELECT id FROM scan_runs WHERE source_id=?
                   ORDER BY CASE WHEN status='completed' THEN 0 ELSE 1 END,
                            started_at DESC LIMIT 1""",
                (src.id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(409, f"No scan_runs found for source {source_id}")
        scan_id = row["id"]
        import asyncio

        def _do_snapshot():
            return analyzer.snapshot_source(src.id, scan_id, max_files=max_files)

        result = await asyncio.get_event_loop().run_in_executor(None, _do_snapshot)
        result["source_id"] = src.id
        result["scan_id"] = scan_id
        return result

    # ─────────────────────────────────────────────────────────────────
    # Orphaned-SID report + bulk reassignment (#56)
    # ─────────────────────────────────────────────────────────────────

    def _get_orphan_analyzer():
        existing = getattr(app.state, "orphan_sid_analyzer", None)
        if existing is not None:
            return existing
        from src.security.orphan_sid import OrphanSidAnalyzer
        analyzer = OrphanSidAnalyzer(db, config, ad_lookup=ad_lookup)
        app.state.orphan_sid_analyzer = analyzer
        return analyzer

    @app.get("/api/security/orphan-sids/{source_id}")
    async def orphan_sids_report(source_id: int,
                                 max_unique_sids: Optional[int] = None):
        """Detect orphan owner SIDs in the latest scan for ``source_id``."""
        analyzer = _get_orphan_analyzer()
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            raise HTTPException(404, f"No scan_runs found for source {source_id}")
        cap = max_unique_sids if max_unique_sids is not None else analyzer.max_unique_sids_default
        result = analyzer.detect_orphans(scan_id, max_unique_sids=int(cap))
        result["source_id"] = source_id
        return result

    @app.get("/api/security/orphan-sids/{source_id}/files")
    async def orphan_sid_files(source_id: int,
                               sid: str = Query(..., min_length=1),
                               page: int = Query(1, ge=1),
                               page_size: int = Query(100, ge=1, le=1000)):
        analyzer = _get_orphan_analyzer()
        return analyzer.get_orphan_files(source_id, sid, page=page, page_size=page_size)

    class OrphanReassignRequest(BaseModel):
        source_id: int
        sid: str
        new_owner: str
        dry_run: bool = True
        max_files: Optional[int] = None

    @app.post("/api/security/orphan-sids/reassign")
    async def orphan_sid_reassign(req: OrphanReassignRequest):
        analyzer = _get_orphan_analyzer()
        # Honour the opt-in dual-approval rule: refuse non-dry-run runs
        # unless the caller explicitly asked for it. (Dual-approval UX
        # itself is out of scope for this PR — this just blocks an
        # accidental single-button live reassignment.)
        if (not req.dry_run) and analyzer.require_dual_approval_for_reassign:
            raise HTTPException(
                403,
                "Live reassignment requires dual approval; submit via the "
                "approval workflow or set dry_run=true to preview.",
            )
        if (not req.dry_run) and not analyzer.is_supported():
            raise HTTPException(501, "Live reassignment requires Windows + pywin32")
        try:
            return analyzer.reassign_owner(
                req.source_id, req.sid, req.new_owner,
                dry_run=req.dry_run, max_files=req.max_files,
            )
        except NotImplementedError as e:
            raise HTTPException(501, str(e)) from e
        except ValueError as e:
            raise HTTPException(400, str(e)) from e

    @app.get("/api/security/orphan-sids/{source_id}/export.csv")
    async def orphan_sid_export_csv(source_id: int):
        """Streaming CSV download of orphan files for offline review."""
        from fastapi.responses import StreamingResponse
        import io

        analyzer = _get_orphan_analyzer()
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            raise HTTPException(404, f"No scan_runs found for source {source_id}")

        # Detect, then stream the file rows. We re-implement the row
        # generation here (instead of writing to a temp file) so the
        # response can stream incrementally on million-row shares.
        report = analyzer.detect_orphans(scan_id)
        orphan_sids = [row["sid"] for row in report.get("orphan_sids", [])]

        def _iter():
            buf = io.StringIO()
            writer = __import__("csv").writer(buf)
            writer.writerow([
                "path", "owner_sid", "file_size",
                "last_modify_time", "owner_resolved",
            ])
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

            if not orphan_sids:
                return

            placeholders = ",".join(["?"] * len(orphan_sids))
            params: list = [source_id, scan_id, *orphan_sids]
            with db.get_cursor() as cur:
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
                        r["file_path"], r["owner"], r["file_size"],
                        r["last_modify_time"] or "", "false",
                    ])
                    yield buf.getvalue()
                    buf.seek(0)
                    buf.truncate(0)

        filename = f"orphan_sids_source{source_id}_scan{scan_id}.csv"
        return StreamingResponse(
            _iter(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    # ─────────────────────────────────────────────────────────────────
    # Syslog/CEF integration (#50)
    # ─────────────────────────────────────────────────────────────────

    @app.get("/api/integrations/syslog/status")
    async def syslog_status():
        forwarder = getattr(app.state, "syslog", None)
        if forwarder is None:
            return {"available": False, "configured": False,
                    "reason": "forwarder_not_initialized"}
        return forwarder.health()

    @app.post("/api/integrations/syslog/test")
    async def syslog_test():
        forwarder = getattr(app.state, "syslog", None)
        if forwarder is None:
            return {"sent": False, "error": "forwarder_not_initialized"}
        if not forwarder.available:
            return {"sent": False, "error": "forwarder_disabled_or_unconfigured"}
        ok = forwarder.emit(
            "info",
            "test_event",
            {"msg": "FILE ACTIVITY syslog test event",
             "source": "dashboard", "version": APP_VERSION},
        )
        if not ok:
            return {"sent": False, "error": forwarder.health().get("last_error")}
        return {"sent": True}

    # ─────────────────────────────────────────────────────────────────
    # GDPR PII detection + retention engine (#58)
    # ─────────────────────────────────────────────────────────────────

    def _get_pii_engine():
        existing = getattr(app.state, "pii_engine", None)
        if existing is not None:
            return existing
        from src.compliance.pii_engine import PiiEngine
        engine = PiiEngine(db, config)
        app.state.pii_engine = engine
        return engine

    def _get_retention_engine():
        existing = getattr(app.state, "retention_engine", None)
        if existing is not None:
            return existing
        from src.compliance.retention import RetentionEngine
        # Lazy archive engine — only constructed if a policy actually
        # needs it (action='archive' + non-dry-run apply).
        archive_engine = None
        try:
            from src.archiver.archive_engine import ArchiveEngine
            archive_engine = ArchiveEngine(db, config)
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("ArchiveEngine init skipped for retention: %s", e)
        engine = RetentionEngine(db, config, archive_engine=archive_engine)
        app.state.retention_engine = engine
        return engine

    @app.post("/api/compliance/pii/scan/{source_id}")
    async def pii_scan(source_id: int,
                       max_files: Optional[int] = None,
                       overwrite_existing: bool = False):
        """Run PiiEngine.scan_source against the latest scan of source_id."""
        engine = _get_pii_engine()
        src = _get_source(db, source_id)
        import asyncio

        def _run():
            return engine.scan_source(
                src.id,
                max_files=max_files,
                overwrite_existing=overwrite_existing,
            )

        result = await asyncio.get_event_loop().run_in_executor(None, _run)
        result["source_id"] = src.id
        return result

    @app.get("/api/compliance/pii/findings")
    async def pii_findings(pattern: Optional[str] = None,
                           page: int = Query(1, ge=1),
                           page_size: int = Query(50, ge=1, le=1000)):
        """Browse persisted pii_findings rows. Optional ?pattern= filter."""
        _get_pii_engine()  # ensure engine constructable / config sane
        offset = (page - 1) * page_size
        params: list = []
        where = ""
        if pattern:
            where = "WHERE pattern_name = ?"
            params.append(pattern)
        with db.get_cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM pii_findings {where}",
                params,
            )
            total = cur.fetchone()["cnt"]
            cur.execute(
                f"""SELECT id, scan_id, file_path, pattern_name,
                           hit_count, sample_snippet, detected_at
                    FROM pii_findings {where}
                    ORDER BY detected_at DESC, id DESC
                    LIMIT ? OFFSET ?""",
                params + [page_size, offset],
            )
            rows = [dict(r) for r in cur.fetchall()]
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "findings": rows,
        }

    @app.get("/api/compliance/pii/subject")
    async def pii_subject(term: str = Query(..., min_length=1),
                          format: str = Query("json", pattern="^(json|csv)$")):
        """Article 17/30 export — every file mentioning ``term``."""
        engine = _get_pii_engine()
        if format == "csv":
            from fastapi.responses import StreamingResponse
            import io
            import csv as _csv
            rows = engine.find_for_subject(term, limit=100_000)
            buf = io.StringIO()
            writer = _csv.writer(buf)
            writer.writerow([
                "file_path", "match_count", "last_modify_time",
                "owner", "patterns", "sample_snippets",
            ])
            for r in rows:
                patterns = ";".join(h["pattern_name"] for h in r["hits"])
                snippets = ";".join(
                    h.get("sample_snippet") or "" for h in r["hits"]
                )
                writer.writerow([
                    r["file_path"], r["match_count"],
                    r["last_modify_time"] or "",
                    r["owner"] or "",
                    patterns, snippets,
                ])
            buf.seek(0)
            safe_term = "".join(c if c.isalnum() else "_" for c in term)[:40]
            return StreamingResponse(
                iter([buf.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition":
                        f"attachment; filename=pii_subject_{safe_term}.csv"
                },
            )
        results = engine.find_for_subject(term)
        return {"term": term, "matches": len(results), "files": results}

    @app.get("/api/compliance/pii/backend")
    async def pii_backend_status():
        """Capability probe (issue #64) — which regex backend the
        PII engine is actually using and the optional package version.

        Returns ``{"backend": "hyperscan"|"re",
                   "version": <pkg version or null>,
                   "hyperscan_available": bool,
                   "configured": <raw engine config value>}``.
        Mirrors the ``OrphanSidAnalyzer.is_supported`` pattern of
        surfacing optional-acceleration availability over the API.
        """
        from src.compliance._pii_backends import (
            hyperscan_available, hyperscan_version,
        )
        engine = _get_pii_engine()
        configured = (
            ((config or {}).get("compliance", {}) or {}).get("pii", {}) or {}
        ).get("engine", "auto")
        return {
            "backend": engine.engine_name,
            "version": hyperscan_version() if engine.engine_name == "hyperscan" else None,
            "hyperscan_available": hyperscan_available(),
            "configured": configured,
        }

    @app.get("/api/compliance/retention/policies")
    async def retention_policies_list():
        engine = _get_retention_engine()
        return {"policies": engine.list_policies()}

    class _RetentionPolicyCreate(BaseModel):
        name: str
        pattern_match: Optional[str] = ""
        retain_days: int
        action: str

        @field_validator("action")
        @classmethod
        def _validate_action(cls, v: str) -> str:
            if v not in ("archive", "delete"):
                raise ValueError("action must be 'archive' or 'delete'")
            return v

    @app.post("/api/compliance/retention/policies")
    async def retention_policy_create(data: _RetentionPolicyCreate):
        engine = _get_retention_engine()
        try:
            pid = engine.add_policy(
                data.name,
                data.pattern_match or "",
                data.retain_days,
                data.action,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            # Most likely a UNIQUE-name collision.
            raise HTTPException(400, str(e))
        return {"id": pid, "name": data.name}

    @app.delete("/api/compliance/retention/policies/{name}")
    async def retention_policy_remove(name: str):
        engine = _get_retention_engine()
        if not engine.remove_policy(name):
            raise HTTPException(404, f"Policy not found: {name}")
        return {"removed": True, "name": name}

    @app.post("/api/compliance/retention/apply/{policy_name}")
    async def retention_policy_apply(policy_name: str,
                                     dry_run: bool = True):
        engine = _get_retention_engine()
        import asyncio

        def _run():
            return engine.apply(policy_name, dry_run=dry_run)

        try:
            result = await asyncio.get_event_loop().run_in_executor(None, _run)
        except ValueError as e:
            raise HTTPException(404, str(e))
        except RuntimeError as e:
            raise HTTPException(503, str(e))
        return result

    @app.get("/api/compliance/retention/attestation")
    async def retention_attestation(since_days: int = Query(30, ge=1, le=3650)):
        engine = _get_retention_engine()
        return engine.attestation_report(since_days=since_days)

    # ──────────────────────────────────────────────
    # Compliance — Legal Hold Registry (issue #59)
    # ──────────────────────────────────────────────
    # Constructed once per app; the registry itself is stateless beyond
    # the DB handle, so a single shared instance is safe across requests.
    from src.compliance.legal_hold import LegalHoldRegistry
    app.state.legal_hold = LegalHoldRegistry(db, config)

    @app.get("/api/compliance/legal-holds/active")
    async def legal_holds_active():
        return {"holds": app.state.legal_hold.list_active()}

    @app.get("/api/compliance/legal-holds/history")
    async def legal_holds_history(page: int = Query(1, ge=1),
                                  page_size: int = Query(50, ge=1, le=500)):
        return app.state.legal_hold.list_history(page=page, page_size=page_size)

    @app.post("/api/compliance/legal-holds")
    async def legal_holds_add(body: dict):
        pattern = (body or {}).get("pattern")
        reason = (body or {}).get("reason")
        case_ref = (body or {}).get("case_ref")
        created_by = (body or {}).get("created_by") or "dashboard"
        try:
            hold_id = app.state.legal_hold.add_hold(
                pattern=pattern,
                reason=reason,
                case_ref=case_ref,
                created_by=created_by,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        return {"id": hold_id, "ok": True}

    @app.post("/api/compliance/legal-holds/{hold_id}/release")
    async def legal_holds_release(hold_id: int, body: dict):
        released_by = (body or {}).get("released_by") or "dashboard"
        try:
            ok = app.state.legal_hold.release_hold(hold_id, released_by)
        except ValueError as e:
            raise HTTPException(400, str(e))
        if not ok:
            # Either unknown id or already released — same response so
            # the dashboard can treat it idempotently.
            return {"ok": False, "reason": "not_found_or_already_released"}
        return {"ok": True}

    @app.get("/api/compliance/legal-holds/check")
    async def legal_holds_check(path: str = Query(..., min_length=1)):
        held = app.state.legal_hold.is_held(path)
        return {"path": path, "is_held": held is not None, "hold": held}

    @app.get("/api/compliance/legal-holds/badge")
    async def legal_holds_badge():
        """Sidebar badge data — cheap polling endpoint."""
        registry = app.state.legal_hold
        actives = registry.list_active()
        try:
            held_paths = registry.count_held_paths()
        except Exception as e:  # pragma: no cover - defensive only
            logger.warning("legal_hold count_held_paths failed: %s", e)
            held_paths = 0
        return {
            "active_count": len(actives),
            "held_paths_count": held_paths,
        }

    return app
