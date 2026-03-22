"""Development server - Dashboard'u demo modunda başlatır.

PostgreSQL bağlantısı yoksa mock data ile çalışır.
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, List

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dev_server")


# ─── Mock Database ───

class MockDB:
    """PostgreSQL olmadan dashboard'u test etmek için mock veritabanı."""

    def __init__(self):
        self._sources = [
            {"id": 1, "name": "FileServer01", "unc_path": "\\\\fileserver01\\shared",
             "archive_dest": "\\\\archive\\fileserver01", "enabled": True,
             "created_at": "2025-01-15T10:00:00", "last_scanned_at": "2026-03-17T02:00:00"},
            {"id": 2, "name": "DeptShare", "unc_path": "\\\\dc01\\departments",
             "archive_dest": "\\\\archive\\departments", "enabled": True,
             "created_at": "2025-03-01T08:00:00", "last_scanned_at": "2026-03-16T03:00:00"},
            {"id": 3, "name": "ProjectDrive", "unc_path": "\\\\nas02\\projects",
             "archive_dest": None, "enabled": False,
             "created_at": "2025-06-10T14:00:00", "last_scanned_at": None},
        ]
        self._policies = [
            {"id": 1, "name": "eski-dosyalar", "source_id": None, "source_name": "Tumu",
             "rules_json": {"access_days": 365}, "enabled": True},
            {"id": 2, "name": "buyuk-medya", "source_id": 1, "source_name": "FileServer01",
             "rules_json": {"min_size": 104857600, "extensions": ["mp4", "avi", "mov"]}, "enabled": True},
        ]
        self._schedules = [
            {"id": 1, "task_type": "scan", "source_id": 1, "source_name": "FileServer01",
             "policy_id": None, "policy_name": None, "cron_expression": "0 2 * * *",
             "enabled": True, "last_run_at": "2026-03-17T02:00:00", "created_at": "2025-01-20"},
            {"id": 2, "task_type": "archive", "source_id": 1, "source_name": "FileServer01",
             "policy_id": 1, "policy_name": "eski-dosyalar", "cron_expression": "0 3 * * 0",
             "enabled": True, "last_run_at": "2026-03-15T03:00:00", "created_at": "2025-02-01"},
        ]

    def get_sources(self, enabled_only=False):
        class Src:
            def __init__(self, d): self.__dict__.update(d)
        sources = self._sources
        if enabled_only:
            sources = [s for s in sources if s["enabled"]]
        return [Src(s) for s in sources]

    def get_source_by_name(self, name):
        class Src:
            def __init__(self, d): self.__dict__.update(d)
        for s in self._sources:
            if s["name"] == name:
                return Src(s)
        return None

    def add_source(self, s):
        new_id = max(x["id"] for x in self._sources) + 1 if self._sources else 1
        self._sources.append({
            "id": new_id, "name": s.name, "unc_path": s.unc_path,
            "archive_dest": s.archive_dest, "enabled": True,
            "created_at": datetime.now().isoformat(), "last_scanned_at": None
        })
        return new_id

    def remove_source(self, name):
        before = len(self._sources)
        self._sources = [s for s in self._sources if s["name"] != name]
        return len(self._sources) < before

    def get_policies(self):
        return self._policies

    def add_policy(self, pol):
        new_id = max(p["id"] for p in self._policies) + 1 if self._policies else 1
        self._policies.append({
            "id": new_id, "name": pol.name, "source_id": pol.source_id,
            "source_name": "?", "rules_json": json.loads(pol.rules_json) if isinstance(pol.rules_json, str) else pol.rules_json,
            "enabled": True
        })
        return new_id

    def get_policy_by_name(self, name):
        for p in self._policies:
            if p["name"] == name:
                return p
        return None

    def remove_policy(self, name):
        before = len(self._policies)
        self._policies = [p for p in self._policies if p["name"] != name]
        return len(self._policies) < before

    def get_scheduled_tasks(self, enabled_only=False):
        tasks = self._schedules
        if enabled_only:
            tasks = [t for t in tasks if t["enabled"]]
        return tasks

    def add_scheduled_task(self, task):
        new_id = max(t["id"] for t in self._schedules) + 1 if self._schedules else 1
        self._schedules.append({
            "id": new_id, "task_type": task.task_type, "source_id": task.source_id,
            "source_name": "?", "policy_id": task.policy_id, "policy_name": None,
            "cron_expression": task.cron_expression, "enabled": True,
            "last_run_at": None, "created_at": datetime.now().isoformat()
        })
        return new_id

    def remove_scheduled_task(self, task_id):
        before = len(self._schedules)
        self._schedules = [t for t in self._schedules if t["id"] != task_id]
        return len(self._schedules) < before

    def get_latest_scan_id(self, source_id):
        return 1

    def get_archive_stats(self):
        return {
            "total_archived": 2847,
            "total_restored": 156,
            "currently_archived": 2691,
            "archived_size": 187_539_456_000,
            "archived_size_formatted": "174.7 GB",
            "source_count": 2
        }

    def search_archived_files(self, query, extension=None, page=1):
        results = []
        if query:
            for i in range(5):
                results.append({
                    "id": i + 1,
                    "file_name": f"{query}_sample_{i+1}.docx",
                    "file_size": 1024 * (i + 1) * 50,
                    "archived_at": "2026-02-15T10:30:00",
                    "original_path": f"\\\\fileserver01\\shared\\docs\\{query}_sample_{i+1}.docx"
                })
        return {"total": len(results), "page": page, "page_size": 50, "results": results}

    def health_check(self):
        return {"status": "demo", "server_time": datetime.now().isoformat()}


def _mock_format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/1024**2:.1f} MB"
    elif size_bytes < 1024**4:
        return f"{size_bytes/1024**3:.1f} GB"
    return f"{size_bytes/1024**4:.1f} TB"


# ─── Pydantic Models ───

class SourceCreate(BaseModel):
    name: str
    unc_path: str
    archive_dest: Optional[str] = None

class PolicyCreate(BaseModel):
    name: str
    source_name: Optional[str] = None
    access_days: Optional[int] = None
    modify_days: Optional[int] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    extensions: Optional[List[str]] = None
    exclude_extensions: Optional[List[str]] = None

class ScheduleCreate(BaseModel):
    task_type: str
    source_name: str
    policy_name: Optional[str] = None
    cron_expression: str

class ArchiveRequest(BaseModel):
    source_name: str
    policy_name: Optional[str] = None
    days: Optional[int] = None

class RestoreRequest(BaseModel):
    archive_id: Optional[int] = None
    original_path: Optional[str] = None


# ─── App ───

app = FastAPI(title="FILE ACTIVITY Dashboard (Dev)", version="1.0.0-dev")
db = MockDB()

static_dir = os.path.join(os.path.dirname(__file__), "src", "dashboard", "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = os.path.join(static_dir, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/dashboard/init")
async def dashboard_init():
    source_list = [s.__dict__ for s in db.get_sources()]
    summaries = {}
    for s in source_list:
        summaries[s["id"]] = {
            "has_data": True,
            "scan_id": 1,
            "file_count": 474_000,
            "total_size": 5_800_000_000_000,
            "total_size_formatted": "5.3 TB",
            "scan_status": {"started_at": "2026-03-22T14:30:00", "completed_at": "2026-03-22T15:45:00", "status": "completed"}
        }
    return {"sources": source_list, "summaries": summaries, "auto_select": source_list[0]["id"] if source_list else None}

@app.get("/api/sources")
async def get_sources():
    return [s.__dict__ for s in db.get_sources()]

@app.get("/api/trend/{source_id}")
async def trend(source_id: int):
    scans = [
        {"started_at": f"2026-03-{10+i}T14:00:00", "total_files": 460000 + i*2000, "total_size": 5_500_000_000_000 + i*50_000_000_000, "status": "completed"}
        for i in range(8)
    ]
    return {"scans": scans, "growth": {"file_diff": 14000, "size_diff": 350_000_000_000}}

@app.get("/api/watcher/status")
async def watcher_status(source_id: int = 0):
    return {"running": False, "total_changes": 0}

@app.post("/api/sources")
async def add_source(data: SourceCreate):
    class S:
        def __init__(self, **kw): self.__dict__.update(kw)
    s = S(name=data.name, unc_path=data.unc_path, archive_dest=data.archive_dest, enabled=True)
    sid = db.add_source(s)
    return {"id": sid, "message": f"Kaynak eklendi: {data.name}"}

@app.delete("/api/sources/{name}")
async def remove_source(name: str):
    if db.remove_source(name):
        return {"message": f"Kaynak silindi: {name}"}
    raise HTTPException(404, "Kaynak bulunamadi")

@app.post("/api/sources/{name}/test")
async def test_source(name: str):
    src = db.get_source_by_name(name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    return {"success": False, "message": f"Demo mod: {src.unc_path} baglanti testi simule edildi"}

@app.post("/api/scan/{source_name}")
async def run_scan(source_name: str):
    src = db.get_source_by_name(source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    return {
        "status": "completed", "total_files": 15842, "total_size": 82_345_678_900, "errors": 0,
        "report": {
            "generated": True,
            "html_path": "reports/FileServer01_20260318_120000.html",
            "json_path": "reports/FileServer01_20260318_120000.json"
        }
    }

@app.get("/api/reports/status/{source_name}")
async def report_status(source_name: str):
    src = db.get_source_by_name(source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    return {
        "source": {"name": src.name, "path": src.unc_path},
        "total_files": 15842,
        "total_size_formatted": "76.7 GB",
        "type_count": 47,
        "oldest_file": "2018-03-12",
        "newest_file": "2026-03-17",
        "generated_at": datetime.now().isoformat()
    }

@app.get("/api/reports/frequency/{source_id}")
async def report_frequency(source_id: int, days: Optional[str] = None):
    return {"frequency": [
        {"label": "30+ gun erisilmemis", "days": 30, "file_count": 12450, "total_size": 65_000_000_000, "total_size_formatted": "60.5 GB"},
        {"label": "90+ gun erisilmemis", "days": 90, "file_count": 10200, "total_size": 54_000_000_000, "total_size_formatted": "50.3 GB"},
        {"label": "180+ gun erisilmemis", "days": 180, "file_count": 8100, "total_size": 42_000_000_000, "total_size_formatted": "39.1 GB"},
        {"label": "365+ gun erisilmemis", "days": 365, "file_count": 5800, "total_size": 31_000_000_000, "total_size_formatted": "28.9 GB"},
        {"label": "730+ gun erisilmemis", "days": 730, "file_count": 3200, "total_size": 18_000_000_000, "total_size_formatted": "16.8 GB"},
        {"label": "1095+ gun erisilmemis", "days": 1095, "file_count": 1500, "total_size": 8_500_000_000, "total_size_formatted": "7.9 GB"},
    ]}

@app.get("/api/reports/types/{source_name}")
async def report_types(source_name: str):
    src = db.get_source_by_name(source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    types = [
        ("docx", 4200, 12_500_000_000), ("pdf", 3100, 18_700_000_000),
        ("xlsx", 2800, 8_900_000_000), ("pptx", 1200, 15_200_000_000),
        ("jpg", 1800, 4_300_000_000), ("png", 950, 2_100_000_000),
        ("mp4", 320, 28_500_000_000), ("zip", 580, 6_700_000_000),
        ("txt", 620, 180_000_000), ("csv", 410, 950_000_000),
        ("msg", 380, 1_200_000_000), ("dwg", 150, 3_800_000_000),
    ]
    return {"types": [
        {"extension": ext, "file_count": cnt, "total_size": sz,
         "total_size_formatted": _mock_format_size(sz),
         "avg_size_formatted": _mock_format_size(sz // cnt),
         "min_size_formatted": _mock_format_size(1024),
         "max_size_formatted": _mock_format_size(sz // 3)}
        for ext, cnt, sz in types
    ]}

@app.get("/api/reports/sizes/{source_name}")
async def report_sizes(source_name: str):
    src = db.get_source_by_name(source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    return {"sizes": [
        {"label": "Tiny", "range_formatted": "0 - 100 KB", "file_count": 4200, "total_size": 120_000_000, "total_size_formatted": "114.4 MB"},
        {"label": "Small", "range_formatted": "100 KB - 1 MB", "file_count": 5100, "total_size": 2_800_000_000, "total_size_formatted": "2.6 GB"},
        {"label": "Medium", "range_formatted": "1 - 10 MB", "file_count": 3800, "total_size": 18_500_000_000, "total_size_formatted": "17.2 GB"},
        {"label": "Large", "range_formatted": "10 - 100 MB", "file_count": 2100, "total_size": 45_000_000_000, "total_size_formatted": "41.9 GB"},
        {"label": "XLarge", "range_formatted": "100 MB - 1 GB", "file_count": 520, "total_size": 28_000_000_000, "total_size_formatted": "26.1 GB"},
        {"label": "Huge", "range_formatted": "1 GB+", "file_count": 22, "total_size": 35_000_000_000, "total_size_formatted": "32.6 GB"},
    ]}

@app.post("/api/archive/run")
async def run_archive(data: ArchiveRequest):
    return {"archived": 245, "failed": 2, "total_size_formatted": "1.8 GB"}

@app.post("/api/archive/dry-run")
async def archive_dry_run(data: ArchiveRequest):
    sample = [
        {"relative_path": f"departments\\finance\\reports\\2022\\report_{i}.xlsx",
         "file_size": 1024 * 500 * (i + 1), "file_size_formatted": _mock_format_size(1024 * 500 * (i + 1))}
        for i in range(15)
    ]
    return {"file_count": 5800, "total_size": 31_000_000_000,
            "total_size_formatted": "28.9 GB", "sample": sample}

@app.get("/api/archive/search")
async def archive_search(q: str, extension: Optional[str] = None, page: int = 1):
    return db.search_archived_files(q, extension=extension, page=page)

@app.get("/api/archive/stats")
async def archive_stats():
    return db.get_archive_stats()

@app.post("/api/archive/restore")
async def restore_file(data: RestoreRequest):
    return {"success": True, "original_path": "\\\\fileserver01\\shared\\docs\\sample.docx"}

@app.get("/api/policies")
async def get_policies():
    return db.get_policies()

@app.post("/api/policies")
async def add_policy(data: PolicyCreate):
    class P:
        def __init__(self, **kw): self.__dict__.update(kw)
    rules = {}
    if data.access_days: rules["access_days"] = data.access_days
    if data.min_size: rules["min_size"] = data.min_size
    if data.extensions: rules["extensions"] = data.extensions
    p = P(name=data.name, source_id=None, rules_json=rules, enabled=True)
    pid = db.add_policy(p)
    return {"id": pid, "message": f"Politika olusturuldu: {data.name}"}

@app.delete("/api/policies/{name}")
async def remove_policy(name: str):
    if db.remove_policy(name):
        return {"message": f"Politika silindi: {name}"}
    raise HTTPException(404, "Politika bulunamadi")

@app.get("/api/schedules")
async def get_schedules():
    return db.get_scheduled_tasks()

@app.post("/api/schedules")
async def add_schedule(data: ScheduleCreate):
    class T:
        def __init__(self, **kw): self.__dict__.update(kw)
    src = db.get_source_by_name(data.source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    t = T(task_type=data.task_type, source_id=src.id,
          policy_id=None, cron_expression=data.cron_expression, enabled=True)
    tid = db.add_scheduled_task(t)
    return {"id": tid, "message": "Zamanlanmis gorev olusturuldu"}

@app.delete("/api/schedules/{task_id}")
async def remove_schedule(task_id: int):
    if db.remove_scheduled_task(task_id):
        return {"message": "Gorev silindi"}
    raise HTTPException(404, "Gorev bulunamadi")

@app.get("/api/reports/full/{source_name}")
async def report_full(source_name: str):
    """Tam birleştirilmiş rapor."""
    src = db.get_source_by_name(source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    freq = (await report_frequency(source_name))["frequency"]
    types_data = (await report_types(source_name))["types"]
    sizes_data = (await report_sizes(source_name))["sizes"]
    return {
        "source": {"name": src.name, "path": src.unc_path},
        "summary": {
            "total_files": 15842, "total_size": 82_345_678_900,
            "total_size_formatted": "76.7 GB", "type_count": 47,
            "oldest_file": "2018-03-12", "newest_file": "2026-03-17"
        },
        "frequency": freq, "types": types_data, "sizes": sizes_data,
        "generated_at": datetime.now().isoformat()
    }

@app.get("/api/reports/export/{source_name}")
async def report_export_html(source_name: str):
    """Tam raporu HTML olarak indir."""
    from fastapi.responses import HTMLResponse
    src = db.get_source_by_name(source_name)
    if not src:
        raise HTTPException(404, "Kaynak bulunamadi")
    data = await report_full(source_name)

    from src.analyzer.report_exporter import ReportExporter
    exporter = ReportExporter({})
    html = exporter._render_html(data)

    return HTMLResponse(
        content=html,
        headers={"Content-Disposition": f"attachment; filename={source_name}_report.html"}
    )

# ─── USER ACTIVITY API (Mock) ───

@app.get("/api/users/overview")
async def users_overview(days: int = 30):
    import random
    users = [
        ("ahmet.yilmaz", "Finans", 2847, 1890, 820, 137, 45_200_000_000),
        ("elif.demir", "IT", 2105, 1650, 390, 65, 38_700_000_000),
        ("mehmet.kaya", "Muhasebe", 1890, 1420, 410, 60, 22_100_000_000),
        ("zeynep.aksoy", "Hukuk", 1654, 1380, 240, 34, 18_900_000_000),
        ("burak.celik", "IT", 1432, 980, 420, 32, 31_500_000_000),
        ("ayse.ozturk", "IK", 1210, 1050, 140, 20, 8_700_000_000),
        ("can.arslan", "Satis", 1089, 890, 180, 19, 12_300_000_000),
        ("fatma.sahin", "Finans", 987, 780, 190, 17, 15_600_000_000),
        ("emre.yildiz", "Muhendislik", 876, 620, 230, 26, 28_900_000_000),
        ("selin.gunes", "Pazarlama", 754, 610, 130, 14, 9_200_000_000),
    ]
    top = [{"username": u[0], "department": u[1], "access_count": u[2],
            "reads": u[3], "writes": u[4], "deletes": u[5],
            "unique_files": u[2]//2, "active_days": min(days, random.randint(15,28)),
            "total_data": u[6], "total_data_formatted": _mock_format_size(u[6]),
            "first_access": "2026-03-01T08:15:00", "last_access": "2026-03-18T09:45:00"
            } for u in users]

    depts = [
        {"department": "Finans", "user_count": 8, "total_access": 5420, "unique_files": 3200, "total_data": 68_000_000_000, "total_data_formatted": "63.3 GB"},
        {"department": "IT", "user_count": 6, "total_access": 4890, "unique_files": 2800, "total_data": 72_000_000_000, "total_data_formatted": "67.1 GB"},
        {"department": "Muhasebe", "user_count": 5, "total_access": 3200, "unique_files": 1900, "total_data": 28_000_000_000, "total_data_formatted": "26.1 GB"},
        {"department": "Hukuk", "user_count": 4, "total_access": 2100, "unique_files": 1400, "total_data": 22_000_000_000, "total_data_formatted": "20.5 GB"},
        {"department": "IK", "user_count": 3, "total_access": 1800, "unique_files": 1100, "total_data": 12_000_000_000, "total_data_formatted": "11.2 GB"},
        {"department": "Muhendislik", "user_count": 7, "total_access": 3500, "unique_files": 2100, "total_data": 55_000_000_000, "total_data_formatted": "51.2 GB"},
    ]

    timeline = [{"date": f"2026-03-{d:02d}", "total": random.randint(800,2200),
                 "reads": random.randint(600,1600), "writes": random.randint(150,500),
                 "unique_users": random.randint(20,45)} for d in range(1, 19)]

    return {
        "summary": {"total_users": 47, "total_access": 18420, "total_data": 285_000_000_000,
                     "total_data_formatted": "265.4 GB", "period_days": days},
        "top_users": top, "departments": depts, "timeline": timeline,
        "anomalies": {"total": 12, "critical_open": 2, "warning_open": 5, "info_open": 3, "acknowledged": 2},
    }

@app.get("/api/users/{username}/detail")
async def user_detail(username: str, days: int = 30):
    import random
    hourly = [random.randint(0, 15) if h < 7 or h > 21 else random.randint(30, 180) for h in range(24)]
    hourly[9] = 245; hourly[10] = 310; hourly[14] = 280; hourly[11] = 220
    daily = [
        {"day": d, "dow": i, "count": random.randint(200, 600) if i in range(1,6) else random.randint(10,50)}
        for i, d in enumerate(["Paz","Pzt","Sal","Car","Per","Cum","Cmt"])
    ]
    return {
        "username": username, "days": days,
        "summary": {"total_access": 2847, "unique_files": 1423, "active_days": 22,
                     "total_data": 45_200_000_000, "total_data_formatted": "42.1 GB",
                     "reads": 1890, "writes": 820, "deletes": 137},
        "hourly": hourly, "daily": daily,
        "top_extensions": [
            {"extension": "xlsx", "count": 680}, {"extension": "pdf", "count": 520},
            {"extension": "docx", "count": 410}, {"extension": "pptx", "count": 280},
            {"extension": "csv", "count": 190}, {"extension": "jpg", "count": 120},
        ],
        "top_directories": [
            {"directory": "\\\\fileserver01\\shared\\finance\\reports", "count": 450},
            {"directory": "\\\\fileserver01\\shared\\finance\\budgets", "count": 320},
            {"directory": "\\\\fileserver01\\shared\\common\\templates", "count": 180},
        ],
        "risk_score": {"score": 25, "level": "normal", "factors": ["Orta gunluk ortalama: 129"]},
        "action_summary": {
            "read": 1890, "write": 502, "modify": 318, "delete": 137,
            "rename": 45, "copy": 89, "create": 234, "permission_change": 12
        },
        "recent_activity": [
            {"time": "2026-03-18T09:45:12", "action": "modify", "file": "\\\\fileserver01\\shared\\finance\\reports\\Q1_2026.xlsx", "size": 2457600, "size_fmt": "2.3 MB"},
            {"time": "2026-03-18T09:42:05", "action": "read", "file": "\\\\fileserver01\\shared\\finance\\budgets\\annual_budget.pdf", "size": 5242880, "size_fmt": "5.0 MB"},
            {"time": "2026-03-18T09:38:30", "action": "create", "file": "\\\\fileserver01\\shared\\finance\\reports\\draft_summary.docx", "size": 184320, "size_fmt": "180 KB"},
            {"time": "2026-03-18T09:35:18", "action": "copy", "file": "\\\\fileserver01\\shared\\common\\templates\\invoice_template.xlsx", "size": 1048576, "size_fmt": "1.0 MB"},
            {"time": "2026-03-18T09:30:00", "action": "delete", "file": "\\\\fileserver01\\shared\\finance\\temp\\old_draft_v2.docx", "size": 204800, "size_fmt": "200 KB"},
            {"time": "2026-03-18T09:22:45", "action": "rename", "file": "\\\\fileserver01\\shared\\finance\\reports\\Q1_draft.xlsx -> Q1_2026.xlsx", "size": 0, "size_fmt": "-"},
            {"time": "2026-03-18T09:18:10", "action": "modify", "file": "\\\\fileserver01\\shared\\finance\\budgets\\dept_costs.xlsx", "size": 3145728, "size_fmt": "3.0 MB"},
            {"time": "2026-03-18T09:12:33", "action": "read", "file": "\\\\fileserver01\\shared\\common\\policies\\travel_policy_2026.pdf", "size": 892416, "size_fmt": "871 KB"},
            {"time": "2026-03-18T09:05:20", "action": "write", "file": "\\\\fileserver01\\shared\\finance\\reports\\expense_march.csv", "size": 524288, "size_fmt": "512 KB"},
            {"time": "2026-03-18T08:55:00", "action": "delete", "file": "\\\\fileserver01\\shared\\finance\\temp\\scratch_notes.txt", "size": 4096, "size_fmt": "4 KB"},
            {"time": "2026-03-17T17:45:22", "action": "modify", "file": "\\\\fileserver01\\shared\\finance\\presentations\\board_review.pptx", "size": 15728640, "size_fmt": "15.0 MB"},
            {"time": "2026-03-17T17:30:11", "action": "copy", "file": "\\\\fileserver01\\shared\\finance\\reports\\Q4_2025_final.pdf", "size": 8388608, "size_fmt": "8.0 MB"},
            {"time": "2026-03-17T16:50:45", "action": "permission_change", "file": "\\\\fileserver01\\shared\\finance\\confidential\\salary_review.xlsx", "size": 0, "size_fmt": "-"},
            {"time": "2026-03-17T16:20:30", "action": "create", "file": "\\\\fileserver01\\shared\\finance\\reports\\new_analysis.xlsx", "size": 102400, "size_fmt": "100 KB"},
            {"time": "2026-03-17T15:45:18", "action": "read", "file": "\\\\fileserver01\\shared\\common\\org_chart_2026.pdf", "size": 2097152, "size_fmt": "2.0 MB"},
        ],
    }

@app.get("/api/users/heatmap")
async def users_heatmap(days: int = 7):
    import random
    matrix = []
    for dow in range(7):
        row = []
        for h in range(24):
            if dow in range(1, 6):  # hafta ici
                if 9 <= h <= 17:
                    row.append(random.randint(50, 200))
                elif 7 <= h <= 9 or 17 <= h <= 19:
                    row.append(random.randint(15, 60))
                else:
                    row.append(random.randint(0, 8))
            else:  # hafta sonu
                row.append(random.randint(0, 12))
        matrix.append(row)
    return {"matrix": matrix, "max_value": 200,
            "days": ["Paz","Pzt","Sal","Car","Per","Cum","Cmt"], "hours": list(range(24))}

@app.get("/api/anomalies")
async def get_anomalies(severity: Optional[str] = None):
    return [
        {"id": 1, "username": "burak.celik", "alert_type": "high_volume", "severity": "critical",
         "description": "burak.celik: 1 saatte 520 erisim (esik: 200)", "detected_at": "2026-03-18T08:30:00", "acknowledged": False},
        {"id": 2, "username": "elif.demir", "alert_type": "night_access", "severity": "warning",
         "description": "elif.demir: Gece saatlerinde 45 erisim", "detected_at": "2026-03-17T23:15:00", "acknowledged": False},
        {"id": 3, "username": "emre.yildiz", "alert_type": "large_transfer", "severity": "warning",
         "description": "emre.yildiz: 2 saatte 8.3 GB veri okuma", "detected_at": "2026-03-17T14:00:00", "acknowledged": True},
        {"id": 4, "username": "mehmet.kaya", "alert_type": "mass_delete", "severity": "critical",
         "description": "mehmet.kaya: 1 saatte 85 dosya silme!", "detected_at": "2026-03-16T16:45:00", "acknowledged": False},
        {"id": 5, "username": "can.arslan", "alert_type": "night_access", "severity": "info",
         "description": "can.arslan: Gece saatlerinde 18 erisim", "detected_at": "2026-03-16T01:20:00", "acknowledged": True},
    ]

@app.get("/api/reports/mit-naming/{source_id}")
async def mit_naming_report(source_id: int):
    """MIT Libraries dosya adlandirma uyum analizi (mock)."""
    total = 474000
    return {
        "total_files_analyzed": total,
        "compliance_score": 52.3,
        "requirement_compliance": 61.8,
        "full_compliance": 30.2,
        "fully_compliant_count": 143148,
        "req_compliant_count": 292932,
        "requirements": [
            {"code": "R1", "label": "Bosluk Iceren", "count": 142800, "percentage": 30.13,
             "severity": "critical", "description": "Dosya adinda bosluk var. MIT: 'Filenames must not include spaces.'",
             "samples": ["Annual Report 2024.pdf", "meeting notes jan.docx", "budget plan Q1.xlsx",
                          "project status update.pptx", "team photo summer 2025.jpg"]},
            {"code": "R2", "label": "Ilk Karakter Harf Degil", "count": 28440, "percentage": 6.0,
             "severity": "warning", "description": "Ilk karakter ASCII harf (a-z/A-Z) olmali.",
             "samples": ["2024_annual_report.pdf", "123_invoice.xlsx", "_temp_file.dat",
                          "01-meeting-notes.docx", "#readme.txt"]},
            {"code": "R3", "label": "Yasak Karakter", "count": 47400, "percentage": 10.0,
             "severity": "critical", "description": "Base'de sadece ASCII harf, rakam, tire, alt cizgi, nokta kullanilmali.",
             "samples": ["rapor_özet.pdf", "bütçe_2024.xlsx", "müşteri_listesi.csv",
                          "proje_değerlendirme.docx", "fotoğraf_arşiv.zip"]},
            {"code": "R4", "label": "Uzanti Sorunu", "count": 4740, "percentage": 1.0,
             "severity": "info", "description": "Dosya tek nokta + uygun uzanti ile bitmeli.",
             "samples": ["Makefile", "LICENSE", "Dockerfile", "README", ".gitignore"]},
        ],
        "best_practices": [
            {"code": "B1", "label": "Uzun Dosya Adi (>31 kar)", "count": 189600, "percentage": 40.0,
             "severity": "warning", "description": "MIT: 'File names should be limited to 31 characters or fewer.'",
             "samples": ["quarterly_financial_report_2024_Q4_final_v3.xlsx (52 kar)",
                          "department_budget_analysis_summary_approved.pdf (50 kar)"]},
            {"code": "B3", "label": "Base'de Nokta", "count": 71100, "percentage": 15.0,
             "severity": "info", "description": "MIT: 'Periods should be avoided in base filenames.'",
             "samples": ["report.v2.final.docx", "backup.2024.01.zip", "config.local.json"]},
            {"code": "B4", "label": "Buyuk Harf Kullanimi", "count": 284400, "percentage": 60.0,
             "severity": "info", "description": "MIT: 'It is preferable that all letters be lowercase.'",
             "samples": []},
            {"code": "B5", "label": "Ayirici Yok (>10 kar)", "count": 94800, "percentage": 20.0,
             "severity": "info", "description": "MIT: 'Distinct portions should be separated by underscores.'",
             "samples": ["quarterlyreport.pdf", "meetingnotes.docx", "invoicedetails.xlsx"]},
            {"code": "B6", "label": "Dizin Adinda Nokta", "count": 23700, "percentage": 5.0,
             "severity": "info", "description": "MIT: 'Directory names should not include periods.'",
             "samples": ["node_modules", ".config", "v2.0.backup"]},
        ],
        "all_requirements": [],
        "all_best_practices": [],
        "summary": {
            "total_requirement_violations": 223380,
            "total_bp_violations": 663600,
            "top_issue": "Buyuk Harf Kullanimi"
        }
    }

@app.get("/api/reports/mit-naming/{source_id}/files")
async def mit_naming_files(source_id: int, code: str = "R1", page: int = 1, page_size: int = 100):
    files = [
        {"id": i+1, "file_name": f"{'Annual Report 2024' if code=='R1' else '2024_report' if code=='R2' else 'rapor_özet' if code=='R3' else 'Makefile'}_{i+1}.{'xlsx' if code!='R4' else ''}",
         "file_path": f"\\\\fileserver01\\shared\\finance\\reports\\sample_{i+1}.xlsx",
         "file_size": 1024*1024*(i+1), "file_size_formatted": _mock_format_size(1024*1024*(i+1)),
         "owner": ["ahmet.yilmaz","elif.demir","mehmet.kaya"][i%3],
         "last_modify_time": f"2025-{(i%12)+1:02d}-15",
         "directory": "\\\\fileserver01\\shared\\finance\\reports"}
        for i in range(min(page_size, 50))
    ]
    return {"code": code, "total": 500, "page": page, "page_size": page_size, "total_pages": 5, "files": files}

@app.get("/api/reports/mit-naming/{source_id}/export")
async def mit_naming_export(source_id: int):
    from fastapi.responses import StreamingResponse
    import io
    csv = '\ufeffIhlal Kodu,Ihlal Turu,Dosya Adi,Tam Yol,Boyut,Sahip,Son Degisiklik\n'
    csv += 'R1,Bosluk Iceren,"Annual Report 2024.xlsx","\\\\server\\share\\docs\\Annual Report 2024.xlsx",2457600,"ahmet.yilmaz",2025-06-15\n'
    csv += 'R3,Yasak Karakter,"rapor_özet.pdf","\\\\server\\share\\docs\\rapor_özet.pdf",1048576,"elif.demir",2025-03-20\n'
    return StreamingResponse(io.BytesIO(csv.encode('utf-8-sig')), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=MIT_Naming_Report.csv"})

@app.get("/api/insights/{source_id}")
async def insights_report(source_id: int):
    return {
        "insights": [
            {"category": "stale", "insight_type": "stale_1year", "severity": "critical", "title": "1 Yildan Eski Erisim: %42",
             "description": "198,450 dosya (2.8 TB) 1 yildir erisilmemis. Toplam dosyalarin %42'si.",
             "action": "Bu dosyalari arsivlemeyi planlayin", "impact_size": 2_800_000_000_000, "file_count": 198450},
            {"category": "stale", "insight_type": "stale_3year", "severity": "critical", "title": "3+ Yillik Eski Veri: 1.2 TB",
             "description": "85,200 dosya 3 yildir hic erisilmemis.",
             "action": "Acil arsivleme oneriliyor", "impact_size": 1_200_000_000_000, "file_count": 85200},
            {"category": "storage", "insight_type": "temp_files", "severity": "warning", "title": "Gecici Dosyalar Tespit Edildi",
             "description": "12,340 gecici/yedek dosya (45.2 GB) temizlenebilir.",
             "action": "Bu dosyalari arsivleyin veya silin", "impact_size": 45_200_000_000, "file_count": 12340},
            {"category": "storage", "insight_type": "very_large", "severity": "warning", "title": "28 Buyuk Dosya (>1 GB)",
             "description": "Toplam 156 GB yer kapliyor.",
             "action": "Buyuk dosyalari arsivlemeyi dusunun", "impact_size": 156_000_000_000, "file_count": 28},
            {"category": "duplicates", "insight_type": "duplicates", "severity": "warning", "title": "Olasi Kopya Dosyalar: 1,240",
             "description": "Ayni ad ve boyuttaki dosyalar 75.9 MB israf ediyor olabilir.",
             "action": "Kopya dosyalari inceleyin", "impact_size": 75_900_000, "file_count": 1240},
            {"category": "security", "insight_type": "temp_files", "severity": "warning", "title": "342 Calistirilabilir Dosya",
             "description": "Paylasimda .exe, .bat, .ps1 gibi dosyalar bulundu.",
             "action": "Guvenlik riski - inceleyin", "file_count": 342},
            {"category": "growth", "insight_type": "stale_180", "severity": "info", "title": "Buyume Trendi: Aylik 75.1 GB",
             "description": "Mevcut hizla 12 ay sonra tahmini 900 GB ek alan gerekecek.",
             "file_count": 0},
            {"category": "recommendation", "insight_type": "large_files", "severity": "info", "title": "Temizlik Onerisi",
             "description": "Eski ve gecici dosyalari temizleyerek 3.8 TB alan kazanabilirsiniz.",
             "action": "Otomatik arsivleme politikasi olusturun", "impact_size": 3_800_000_000_000},
        ],
        "score": 45,
        "generated_at": "2026-03-23T12:00:00"
    }

@app.get("/api/risk-score/{source_id}")
async def risk_score(source_id: int):
    return {"score": 45, "factors": [
        {"name": "Eski Dosyalar", "weight": 35, "value": 42},
        {"name": "Kopya Dosyalar", "weight": 20, "value": 15},
        {"name": "Gecici Dosyalar", "weight": 15, "value": 8},
        {"name": "Guvenlik Riskleri", "weight": 30, "value": 5}
    ]}

@app.get("/api/insights/{source_id}/files")
async def insight_files(source_id: int, insight_type: str = "stale_1year", page: int = 1, page_size: int = 100):
    files = [
        {"id": i+1, "file_name": f"old_document_{i+1}.xlsx",
         "file_path": f"\\\\fileserver01\\shared\\archive\\2020\\old_document_{i+1}.xlsx",
         "file_size": 1024*1024*(i%10+1), "file_size_formatted": _mock_format_size(1024*1024*(i%10+1)),
         "owner": ["ahmet.yilmaz","elif.demir","mehmet.kaya","burak.celik"][i%4],
         "last_access_time": f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}",
         "last_modify_time": f"2023-{(i%12)+1:02d}-15",
         "directory": f"\\\\fileserver01\\shared\\archive\\{2020+i%4}"}
        for i in range(min(page_size, 50))
    ]
    return {"insight_type": insight_type, "total": 1200, "page": page, "page_size": page_size, "total_pages": 12, "files": files}

@app.post("/api/system/open-folder")
async def open_folder(request):
    body = await request.json()
    return {"success": True, "path": body.get("path", "")}

@app.get("/api/system/health")
async def health():
    return {"status": "ok", "mode": "demo", "time": datetime.now().isoformat()}

# ─── ARSIV GECMISI (Mock) ───

@app.get("/api/archive/history")
async def archive_history(source_id: Optional[int] = None, page: int = 1,
                          page_size: int = 20, date_from: Optional[str] = None,
                          date_to: Optional[str] = None, op_type: Optional[str] = None):
    ops = [
        {"id": 1, "operation_type": "archive", "source_id": 1, "trigger_type": "ai_insight",
         "trigger_detail": "stale_1year", "total_files": 542, "total_size": 6_900_000_000,
         "total_size_formatted": "6.4 GB", "status": "completed", "performed_by": "system",
         "started_at": "2026-03-15T03:00:00", "completed_at": "2026-03-15T03:45:00"},
        {"id": 2, "operation_type": "archive", "source_id": 1, "trigger_type": "manual",
         "trigger_detail": "duplicate_cleanup", "total_files": 128, "total_size": 2_100_000_000,
         "total_size_formatted": "2.0 GB", "status": "completed", "performed_by": "system",
         "started_at": "2026-03-12T14:00:00", "completed_at": "2026-03-12T14:15:00"},
        {"id": 3, "operation_type": "restore", "source_id": 1, "trigger_type": "manual",
         "trigger_detail": "bulk_restore", "total_files": 45, "total_size": 890_000_000,
         "total_size_formatted": "848.8 MB", "status": "completed", "performed_by": "system",
         "started_at": "2026-03-10T09:00:00", "completed_at": "2026-03-10T09:10:00"},
        {"id": 4, "operation_type": "archive", "source_id": 1, "trigger_type": "policy",
         "trigger_detail": "eski-dosyalar", "total_files": 1205, "total_size": 15_600_000_000,
         "total_size_formatted": "14.5 GB", "status": "completed", "performed_by": "system",
         "started_at": "2026-03-08T03:00:00", "completed_at": "2026-03-08T04:20:00"},
        {"id": 5, "operation_type": "archive", "source_id": 2, "trigger_type": "ai_insight",
         "trigger_detail": "temp_files", "total_files": 398, "total_size": 4_300_000_000,
         "total_size_formatted": "4.0 GB", "status": "partial", "performed_by": "system",
         "started_at": "2026-03-05T02:00:00", "completed_at": "2026-03-05T02:30:00"},
    ]
    if op_type:
        ops = [o for o in ops if o["operation_type"] == op_type]
    if source_id:
        ops = [o for o in ops if o["source_id"] == source_id]
    total = len(ops)
    start = (page - 1) * page_size
    ops = ops[start:start + page_size]
    return {"total": total, "page": page, "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)), "operations": ops}

@app.get("/api/archive/operations/{op_id}/files")
async def operation_files(op_id: int, page: int = 1, page_size: int = 100):
    files = [
        {"id": i, "file_name": f"report_{2020+i%5}_Q{i%4+1}.xlsx", "original_path": f"\\\\fileserver01\\shared\\finance\\reports\\{2020+i%5}\\report_{2020+i%5}_Q{i%4+1}.xlsx",
         "archive_path": f"\\\\archive\\fileserver01\\finance\\reports\\{2020+i%5}\\report_{2020+i%5}_Q{i%4+1}.xlsx",
         "file_size": 1024*1024*(i%10+1), "file_size_formatted": _mock_format_size(1024*1024*(i%10+1)),
         "owner": ["ahmet.yilmaz","elif.demir","mehmet.kaya"][i%3],
         "archived_at": f"2026-03-{15-i%10:02d}T03:{i%60:02d}:00", "restored_at": None}
        for i in range(20)
    ]
    return {"total": 20, "page": page, "page_size": page_size, "files": files}

@app.get("/api/archive/operations")
async def get_archive_operations(source_id: Optional[int] = None, limit: int = 50):
    result = await archive_history(source_id=source_id, page=1, page_size=limit)
    return result.get("operations", [])

# ─── KOPYA DOSYALAR (Mock) ───

@app.get("/api/reports/duplicates/{source_id}")
async def duplicate_report(source_id: int, page: int = 1, page_size: int = 50, min_size: int = 0):
    groups = [
        {"file_name": "budget_template.xlsx", "file_size": 2_457_600, "count": 5, "waste_size": 9_830_400,
         "file_size_formatted": "2.3 MB", "waste_size_formatted": "9.4 MB",
         "files": [{"id": 100+i, "file_path": f"\\\\fileserver01\\shared\\{['finance','hr','legal','sales','marketing'][i]}\\templates\\budget_template.xlsx",
                    "owner": ["ahmet.yilmaz","ayse.ozturk","zeynep.aksoy","can.arslan","selin.gunes"][i],
                    "last_access_time": f"2026-0{i+1}-15", "last_modify_time": f"2025-1{i%3+1}-20"} for i in range(5)]},
        {"file_name": "company_logo.png", "file_size": 524_288, "count": 12, "waste_size": 5_767_168,
         "file_size_formatted": "512 KB", "waste_size_formatted": "5.5 MB",
         "files": [{"id": 200+i, "file_path": f"\\\\fileserver01\\shared\\dept{i+1}\\assets\\company_logo.png",
                    "owner": "system", "last_access_time": "2026-03-10", "last_modify_time": "2024-06-15"} for i in range(12)]},
        {"file_name": "annual_report_2024.pdf", "file_size": 15_728_640, "count": 4, "waste_size": 47_185_920,
         "file_size_formatted": "15.0 MB", "waste_size_formatted": "45.0 MB",
         "files": [{"id": 300+i, "file_path": f"\\\\fileserver01\\shared\\{['finance','board','legal','archive'][i]}\\reports\\annual_report_2024.pdf",
                    "owner": ["ahmet.yilmaz","burak.celik","zeynep.aksoy","fatma.sahin"][i],
                    "last_access_time": f"2026-0{i+1}-{10+i}", "last_modify_time": "2025-02-28"} for i in range(4)]},
        {"file_name": "project_plan_v3.pptx", "file_size": 8_388_608, "count": 3, "waste_size": 16_777_216,
         "file_size_formatted": "8.0 MB", "waste_size_formatted": "16.0 MB",
         "files": [{"id": 400+i, "file_path": f"\\\\fileserver01\\shared\\{['projects','presentations','archive'][i]}\\project_plan_v3.pptx",
                    "owner": ["emre.yildiz","burak.celik","emre.yildiz"][i],
                    "last_access_time": f"2026-02-{20+i}", "last_modify_time": "2025-11-15"} for i in range(3)]},
    ]
    total_waste = sum(g["waste_size"] for g in groups)
    total_files = sum(g["count"] for g in groups)
    return {"total_groups": len(groups), "total_waste_size": total_waste,
            "total_waste_size_formatted": _mock_format_size(total_waste),
            "total_files": total_files, "groups": groups,
            "page": page, "page_size": page_size, "total_pages": 1}

@app.post("/api/archive/selective")
async def archive_selective(request):
    body = await request.json()
    file_ids = body.get("file_ids", [])
    return {"archived": len(file_ids), "failed": 0, "total_size": len(file_ids) * 2_000_000,
            "total_size_formatted": _mock_format_size(len(file_ids) * 2_000_000),
            "operation_id": 99}

# ─── DUPLICATE EXPORT (Mock) ───

@app.get("/api/export/duplicates/{source_id}")
async def export_duplicates(source_id: int):
    import io
    from starlette.responses import StreamingResponse
    output = io.StringIO()
    output.write('\ufeff')
    output.write('Grup,Dosya Adi,Boyut,Boyut (Okunur),Kopya Sayisi,Dosya Yolu,Sahip,Son Erisim,Son Degisiklik\n')
    output.write('1,"budget_template.xlsx",2457600,"2.3 MB",5,"\\\\fileserver\\finance\\budget_template.xlsx","ahmet.yilmaz","2024-06-15","2024-03-20"\n')
    output.write('1,"budget_template.xlsx",2457600,"2.3 MB",5,"\\\\fileserver\\hr\\budget_template.xlsx","elif.demir","2024-07-10","2024-04-12"\n')
    output.write('2,"company_logo.png",15728640,"15 MB",3,"\\\\fileserver\\marketing\\company_logo.png","mehmet.ozturk","2024-08-01","2024-01-15"\n')
    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=Duplicates_Report.csv"}
    )

# ─── BUYUME ANALIZI (Mock) ───

@app.get("/api/growth/{source_id}")
async def growth_stats(source_id: int):
    yearly = [
        {"year": "2022", "total_files": 180_000, "total_size": 1_800_000_000_000},
        {"year": "2023", "total_files": 285_000, "total_size": 3_200_000_000_000},
        {"year": "2024", "total_files": 380_000, "total_size": 4_500_000_000_000},
        {"year": "2025", "total_files": 450_000, "total_size": 5_400_000_000_000},
        {"year": "2026", "total_files": 474_000, "total_size": 5_800_000_000_000},
    ]
    monthly = [
        {"month": f"2025-{m:02d}", "total_files": 380_000 + m * 6000, "total_size": 4_500_000_000_000 + m * 80_000_000_000}
        for m in range(1, 13)
    ] + [
        {"month": f"2026-{m:02d}", "total_files": 450_000 + m * 8000, "total_size": 5_400_000_000_000 + m * 130_000_000_000}
        for m in range(1, 4)
    ]
    import random
    daily = [
        {"day": f"2026-03-{d:02d}", "total_files": 465_000 + d * 300 + random.randint(-200, 200),
         "total_size": 5_600_000_000_000 + d * 8_000_000_000 + random.randint(-2_000_000_000, 2_000_000_000)}
        for d in range(1, 21)
    ]
    top_creators = [
        {"owner": "ahmet.yilmaz", "file_count": 45_200, "total_size": 128_000_000_000, "percentage": 9.5},
        {"owner": "elif.demir", "file_count": 38_700, "total_size": 95_000_000_000, "percentage": 8.2},
        {"owner": "emre.yildiz", "file_count": 32_100, "total_size": 210_000_000_000, "percentage": 6.8},
        {"owner": "burak.celik", "file_count": 28_500, "total_size": 185_000_000_000, "percentage": 6.0},
        {"owner": "mehmet.kaya", "file_count": 25_800, "total_size": 72_000_000_000, "percentage": 5.4},
        {"owner": "zeynep.aksoy", "file_count": 22_400, "total_size": 58_000_000_000, "percentage": 4.7},
        {"owner": "fatma.sahin", "file_count": 19_600, "total_size": 45_000_000_000, "percentage": 4.1},
        {"owner": "can.arslan", "file_count": 16_800, "total_size": 38_000_000_000, "percentage": 3.5},
    ]
    return {"yearly": yearly, "monthly": monthly, "daily": daily,
            "total_scans": 285, "top_creators": top_creators}

# ─── TOPLU GERI YUKLEME (Mock) ───

@app.post("/api/restore/bulk")
async def bulk_restore(request):
    body = await request.json()
    archive_ids = body.get("archive_ids", [])
    confirm = body.get("confirm", False)
    if not confirm:
        return {"preview": True, "restorable_count": len(archive_ids), "conflict_count": 0,
                "missing_count": 0, "dirs_to_create": ["\\\\fileserver01\\shared\\finance\\reports\\2022",
                "\\\\fileserver01\\shared\\finance\\reports\\2023"],
                "dirs_to_create_count": 2, "conflicts": [], "restorable": [],
                "total_size": len(archive_ids) * 2_000_000,
                "total_size_formatted": _mock_format_size(len(archive_ids) * 2_000_000)}
    return {"restored": len(archive_ids), "failed": 0,
            "total_size": len(archive_ids) * 2_000_000,
            "total_size_formatted": _mock_format_size(len(archive_ids) * 2_000_000),
            "errors": [], "operation_id": 100}

@app.get("/api/archive/browse")
async def browse_archived(source_id: Optional[int] = None, page: int = 1, page_size: int = 50):
    results = [
        {"id": i+1, "file_name": f"old_report_{i+1}.xlsx", "file_size": 1024*1024*(i+1),
         "file_size_formatted": _mock_format_size(1024*1024*(i+1)),
         "original_path": f"\\\\fileserver01\\shared\\archive_test\\old_report_{i+1}.xlsx",
         "archived_at": f"2026-03-{10+i%10:02d}", "restored_at": None}
        for i in range(10)
    ]
    return {"total": 10, "page": page, "page_size": page_size, "results": results}


if __name__ == "__main__":
    print("FILE ACTIVITY Dashboard - Dev Mode")
    print(f"http://localhost:8085")
    uvicorn.run(app, host="0.0.0.0", port=8085, log_level="info")
