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

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List

# ── Arka plan export kuyrugu ──
_export_jobs = {}  # job_id -> {status, progress, file_path, error, created_at, ...}
_export_lock = threading.Lock()

logger = logging.getLogger("file_activity.dashboard")


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


def create_app(db, config):
    """FastAPI uygulamasini olustur."""

    app = FastAPI(title="FILE ACTIVITY Dashboard", version="1.0.0")

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
    async def archive_search(q: str, extension: Optional[str] = None, page: int = 1):
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
        src = _get_source(db, data.source_id)

        policy_id = None
        if data.policy_name:
            pol = db.get_policy_by_name(data.policy_name)
            if not pol:
                raise HTTPException(404, "Politika bulunamadi")
            policy_id = pol["id"]

        task = ScheduledTask(
            task_type=data.task_type, source_id=src.id,
            policy_id=policy_id, cron_expression=data.cron_expression
        )
        tid = db.add_scheduled_task(task)
        return {"id": tid, "message": "Zamanlanmis gorev olusturuldu"}

    @app.delete("/api/schedules/{task_id}")
    async def remove_schedule(task_id: int):
        if db.remove_scheduled_task(task_id):
            return {"message": "Gorev silindi"}
        raise HTTPException(404, "Gorev bulunamadi")

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

    @app.get("/api/drilldown/frequency/{source_id}")
    async def drilldown_frequency(source_id: int, min_days: int = 0,
                                   max_days: Optional[int] = None,
                                   page: int = 1, limit: int = 100):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        result = db.get_files_by_frequency(src.id, scan_id, min_days, max_days, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    @app.get("/api/drilldown/type/{source_id}")
    async def drilldown_type(source_id: int, extension: str = "",
                              page: int = 1, limit: int = 100):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        result = db.get_files_by_extension(src.id, scan_id, extension, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    @app.get("/api/drilldown/size/{source_id}")
    async def drilldown_size(source_id: int, min_bytes: int = 0,
                              max_bytes: Optional[int] = None,
                              page: int = 1, limit: int = 100):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        result = db.get_files_by_size_range(src.id, scan_id, min_bytes, max_bytes, limit, offset)
        result["page"] = page
        result["limit"] = limit
        return result

    @app.get("/api/drilldown/owner/{source_id}")
    async def drilldown_owner(source_id: int, owner: str = "",
                               page: int = 1, limit: int = 100):
        src = _get_source(db, source_id)
        scan_id = db.get_latest_scan_id(src.id, include_running=True)
        if not scan_id:
            raise HTTPException(400, "Tarama verisi bulunamadi")
        offset = (page - 1) * limit
        result = db.get_files_by_owner(src.id, scan_id, owner, limit, offset)
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

        return {
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
    async def start_watcher(source_id: int, interval: int = 300):
        from src.scanner.file_watcher import FileWatcher, get_watcher_status
        src = _get_source(db, source_id)
        watcher = FileWatcher(db, src.id, src.unc_path, interval)
        watcher.start()
        return {"status": "started", "interval": interval}

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
                           username: str = None, days: int = 7, page: int = 1):
        return db.get_audit_events(source_id, event_type, username, days, page)

    @app.get("/api/audit/summary")
    async def audit_summary(source_id: int = None, days: int = 7):
        return db.get_audit_summary(source_id, days)

    # --- INSIGHTS API ---

    @app.get("/api/insights/{source_id}")
    async def get_insights(source_id: int):
        from src.analyzer.ai_insights import InsightsEngine
        engine = InsightsEngine(db)
        return engine.generate_insights(source_id)

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
    async def mit_naming_files(source_id: int, code: str = "R1", page: int = 1, page_size: int = 100):
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
                            page: int = 1, page_size: int = 100):
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

    @app.get("/api/risk-score/{source_id}")
    async def risk_score(source_id: int):
        """Supervisor risk score - TEK optimized sorgu (6 yerine 2)."""
        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"risk_score": 0, "kpis": {}}

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
    async def duplicate_report(source_id: int, page: int = 1,
                                page_size: int = 50, min_size: int = 0):
        """Kopya dosya raporu - gruplandirmali."""
        from src.utils.size_formatter import format_size
        result = db.get_duplicate_groups(source_id, min_size=min_size,
                                          page=page, page_size=page_size)
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
    async def browse_archived(source_id: int = None, page: int = 1,
                               page_size: int = 50):
        """Arsivlenmis dosyalara goz at (geri yukleme UI icin)."""
        from src.utils.size_formatter import format_size
        result = db.search_archived_files("", page=page, page_size=page_size)
        for f in result.get("results", []):
            f["file_size_formatted"] = format_size(f.get("file_size", 0))
        return result

    # --- ARSIV GECMISI ---

    @app.get("/api/archive/history")
    async def archive_history(source_id: int = None, page: int = 1,
                              page_size: int = 20, date_from: str = None,
                              date_to: str = None, op_type: str = None):
        """Sayfalanmis arsiv islem gecmisi."""
        from src.utils.size_formatter import format_size
        result = db.get_archive_history(source_id, page, page_size,
                                        date_from, date_to, op_type)
        for op in result["operations"]:
            op["total_size_formatted"] = format_size(op.get("total_size") or 0)
        return result

    @app.get("/api/archive/operations/{op_id}/files")
    async def operation_files(op_id: int, page: int = 1, page_size: int = 100):
        """Arsiv islemindeki dosyalari sayfalanmis getir."""
        from src.utils.size_formatter import format_size
        result = db.get_archive_operation_files(op_id, page, page_size)
        for f in result.get("files", []):
            f["file_size_formatted"] = format_size(f.get("file_size") or 0)
        return result

    # --- SYSTEM API ---

    @app.post("/api/system/open-folder")
    async def open_folder(request):
        """Dizini Windows Explorer'da ac."""
        import subprocess
        body = await request.json()
        folder = body.get("path", "")
        if not folder:
            raise HTTPException(400, "path gerekli")
        # Guvenlik: sadece mevcut dizinleri ac
        folder = os.path.normpath(folder)
        if os.path.isdir(folder):
            subprocess.Popen(f'explorer "{folder}"', shell=True)
            return {"success": True, "path": folder}
        elif os.path.isfile(folder):
            subprocess.Popen(f'explorer /select,"{folder}"', shell=True)
            return {"success": True, "path": folder}
        else:
            raise HTTPException(404, f"Dizin bulunamadi: {folder}")

    @app.get("/api/system/health")
    async def health():
        return {
            "status": "ok",
            "time": datetime.now().isoformat(),
            "database": db.health_check()
        }

    @app.get("/api/db/stats")
    async def db_stats():
        """Veritabani istatistikleri."""
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

    return app
