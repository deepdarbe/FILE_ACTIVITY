"""AI-powered insights and recommendations engine.

Analyzes scan data to provide actionable insights without external AI APIs.
Uses statistical analysis and rule-based heuristics.
"""

import logging
from datetime import datetime
from src.storage.database import Database
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.insights")


class InsightsEngine:
    def __init__(self, db: Database):
        self.db = db

    def generate_insights(self, source_id: int) -> dict:
        scan_id = self.db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"insights": [], "score": 0, "generated_at": datetime.now().isoformat()}

        insights = []
        insights.extend(self._storage_efficiency(source_id, scan_id))
        insights.extend(self._stale_data_analysis(source_id, scan_id))
        insights.extend(self._stale_savings_calc(source_id, scan_id))
        insights.extend(self._duplicate_risk(source_id, scan_id))
        insights.extend(self._duplicate_summary(source_id, scan_id))
        insights.extend(self._security_insights(source_id, scan_id))
        insights.extend(self._riskiest_folders(source_id, scan_id))
        insights.extend(self._growth_prediction(source_id))
        insights.extend(self._growth_rate_warning(source_id))
        insights.extend(self._cleanup_recommendations(source_id, scan_id))
        insights.extend(self._audit_insights(source_id))

        # Sort by priority
        priority_order = {"critical": 0, "warning": 1, "info": 2, "success": 3}
        insights.sort(key=lambda x: priority_order.get(x.get("severity", "info"), 99))

        # Health score (0-100)
        score = self._calculate_health_score(insights)

        return {
            "insights": insights,
            "score": score,
            "generated_at": datetime.now().isoformat(),
            "scan_id": scan_id
        }

    def _storage_efficiency(self, source_id, scan_id):
        insights = []
        with self.db.get_cursor() as cur:
            # Temporary files
            cur.execute("""
                SELECT COUNT(*) as cnt, COALESCE(SUM(file_size),0) as size
                FROM scanned_files WHERE source_id=? AND scan_id=?
                AND (LOWER(extension) IN ('tmp','temp','bak','old','log','cache')
                     OR file_name LIKE '~$%' OR file_name LIKE '%.tmp')
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 0:
                insights.append({
                    "category": "storage",
                    "severity": "warning" if r["size"] > 1024**3 else "info",
                    "title": "Gecici Dosyalar Tespit Edildi",
                    "description": f"{r['cnt']:,} gecici/yedek dosya ({format_size(r['size'])}) temizlenebilir.",
                    "action": "Bu dosyalari arsivleyin veya silin",
                    "impact_size": r["size"],
                    "file_count": r["cnt"]
                })

            # Empty files
            cur.execute("""
                SELECT COUNT(*) as cnt FROM scanned_files
                WHERE source_id=? AND scan_id=? AND file_size = 0
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 100:
                insights.append({
                    "category": "storage",
                    "severity": "info",
                    "title": f"{r['cnt']:,} Bos Dosya",
                    "description": "Boyutu 0 olan dosyalar temizlenebilir.",
                    "action": "Bos dosyalari inceleyin",
                    "file_count": r["cnt"]
                })

            # Very large files (>1GB)
            cur.execute("""
                SELECT COUNT(*) as cnt, COALESCE(SUM(file_size),0) as size
                FROM scanned_files WHERE source_id=? AND scan_id=? AND file_size > 1073741824
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 0:
                insights.append({
                    "category": "storage",
                    "severity": "warning",
                    "title": f"{r['cnt']:,} Buyuk Dosya (>1 GB)",
                    "description": f"Toplam {format_size(r['size'])} yer kapliyor.",
                    "action": "Buyuk dosyalari arsivlemeyi dusunun",
                    "impact_size": r["size"],
                    "file_count": r["cnt"]
                })

        return insights

    def _stale_data_analysis(self, source_id, scan_id):
        insights = []
        with self.db.get_cursor() as cur:
            # Files not accessed in 1+ year
            cur.execute("""
                SELECT COUNT(*) as cnt, COALESCE(SUM(file_size),0) as size
                FROM scanned_files WHERE source_id=? AND scan_id=?
                AND last_access_time IS NOT NULL
                AND julianday('now') - julianday(last_access_time) > 365
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 0:
                pct_query = "SELECT COUNT(*) as total FROM scanned_files WHERE source_id=? AND scan_id=?"
                cur.execute(pct_query, (source_id, scan_id))
                total = cur.fetchone()["total"]
                pct = (r["cnt"] / total * 100) if total > 0 else 0

                severity = "critical" if pct > 50 else "warning" if pct > 25 else "info"
                insights.append({
                    "category": "stale",
                    "severity": severity,
                    "title": f"1 Yildan Eski Erisim: %{pct:.0f}",
                    "description": f"{r['cnt']:,} dosya ({format_size(r['size'])}) 1 yildir erisilmemis. Toplam dosyalarin %{pct:.0f}'i.",
                    "action": "Bu dosyalari arsivlemeyi planlayin",
                    "impact_size": r["size"],
                    "file_count": r["cnt"],
                    "percentage": pct
                })

            # Files not accessed in 3+ years
            cur.execute("""
                SELECT COUNT(*) as cnt, COALESCE(SUM(file_size),0) as size
                FROM scanned_files WHERE source_id=? AND scan_id=?
                AND last_access_time IS NOT NULL
                AND julianday('now') - julianday(last_access_time) > 1095
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 0:
                insights.append({
                    "category": "stale",
                    "severity": "critical",
                    "title": f"3+ Yillik Eski Veri: {format_size(r['size'])}",
                    "description": f"{r['cnt']:,} dosya 3 yildir hic erisilmemis.",
                    "action": "Acil arsivleme oneriliyor",
                    "impact_size": r["size"],
                    "file_count": r["cnt"]
                })

        return insights

    def _duplicate_risk(self, source_id, scan_id):
        insights = []
        with self.db.get_cursor() as cur:
            # Files with same name and size (potential duplicates)
            cur.execute("""
                SELECT file_name, file_size, COUNT(*) as cnt
                FROM scanned_files WHERE source_id=? AND scan_id=? AND file_size > 1048576
                GROUP BY file_name, file_size HAVING COUNT(*) > 1
                ORDER BY file_size * COUNT(*) DESC LIMIT 1
            """, (source_id, scan_id))
            dupes = cur.fetchall()
            if dupes:
                total_dupes = sum(r["cnt"] - 1 for r in dupes)
                total_waste = sum(r["file_size"] * (r["cnt"] - 1) for r in dupes)
                if total_dupes > 0:
                    insights.append({
                        "category": "duplicates",
                        "severity": "warning" if total_waste > 1024**3 else "info",
                        "title": f"Olasi Kopya Dosyalar: {total_dupes:,}",
                        "description": f"Ayni ad ve boyuttaki dosyalar {format_size(total_waste)} israf ediyor olabilir.",
                        "action": "Kopya dosyalari inceleyin",
                        "impact_size": total_waste,
                        "file_count": total_dupes
                    })
        return insights

    def _security_insights(self, source_id, scan_id):
        insights = []
        with self.db.get_cursor() as cur:
            # Executable files in share
            cur.execute("""
                SELECT COUNT(*) as cnt FROM scanned_files
                WHERE source_id=? AND scan_id=?
                AND LOWER(extension) IN ('exe','bat','cmd','ps1','vbs','js','msi','scr','com','pif')
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 0:
                insights.append({
                    "category": "security",
                    "severity": "warning",
                    "title": f"{r['cnt']:,} Calistirilabilir Dosya",
                    "description": "Paylasimda .exe, .bat, .ps1 gibi dosyalar bulundu.",
                    "action": "Guvenlik riski - inceleyin",
                    "file_count": r["cnt"]
                })
        return insights

    def _growth_prediction(self, source_id):
        insights = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT total_size, started_at FROM scan_runs
                WHERE source_id = ? AND status = 'completed'
                ORDER BY started_at DESC LIMIT 5
            """, (source_id,))
            scans = cur.fetchall()
            if len(scans) >= 2:
                newest = scans[0]
                oldest = scans[-1]
                size_diff = (newest["total_size"] or 0) - (oldest["total_size"] or 0)
                if size_diff > 0:
                    insights.append({
                        "category": "growth",
                        "severity": "info",
                        "title": f"Depolama Buyumesi: +{format_size(size_diff)}",
                        "description": f"Son {len(scans)} tarama arasinda {format_size(size_diff)} artis.",
                        "action": "Buyume trendini izleyin",
                        "impact_size": size_diff
                    })
        return insights

    def _cleanup_recommendations(self, source_id, scan_id):
        insights = []
        with self.db.get_cursor() as cur:
            # Sum of all reclaimable space
            cur.execute("""
                SELECT COALESCE(SUM(file_size),0) as size FROM scanned_files
                WHERE source_id=? AND scan_id=? AND (
                    LOWER(extension) IN ('tmp','temp','bak','old','log','cache')
                    OR file_size = 0
                    OR (last_access_time IS NOT NULL AND julianday('now') - julianday(last_access_time) > 365)
                )
            """, (source_id, scan_id))
            total_reclaimable = cur.fetchone()["size"]

        if total_reclaimable > 1024**3:  # >1GB
            insights.append({
                "category": "recommendation",
                "severity": "success",
                "title": f"Kazanilabilir Alan: {format_size(total_reclaimable)}",
                "description": "Eski, gecici ve bos dosyalar temizlenerek bu alan kazanilabilir.",
                "action": "Arsivleme politikasi olusturun",
                "impact_size": total_reclaimable
            })

        return insights

    def _audit_insights(self, source_id):
        insights = []
        try:
            with self.db.get_cursor() as cur:
                cur.execute("""
                    SELECT event_type, COUNT(*) as cnt FROM file_audit_events
                    WHERE source_id = ? AND event_time > datetime('now', '-7 days')
                    GROUP BY event_type
                """, (source_id,))
                events = {r["event_type"]: r["cnt"] for r in cur.fetchall()}

                if events.get("delete", 0) > 100:
                    insights.append({
                        "category": "audit",
                        "severity": "warning",
                        "title": f"Son 7 Gunde {events['delete']:,} Dosya Silindi",
                        "description": "Yuksek silme aktivitesi tespit edildi.",
                        "action": "Silme islemlerini inceleyin"
                    })
        except Exception:
            pass  # Table might not exist yet
        return insights

    def _stale_savings_calc(self, source_id, scan_id):
        """Calculate exact savings from archiving stale data."""
        insights = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt, COALESCE(SUM(file_size),0) as size
                FROM scanned_files WHERE source_id=? AND scan_id=?
                AND last_access_time IS NOT NULL
                AND julianday('now') - julianday(last_access_time) > 365
            """, (source_id, scan_id))
            r = cur.fetchone()
            if r["cnt"] > 0 and r["size"] > 104857600:  # >100MB worth archiving
                insights.append({
                    "category": "recommendation",
                    "severity": "success",
                    "title": f"Bayat Veri Arsivleme: {format_size(r['size'])} Tasarruf",
                    "description": f"1 yildir erisilmemis {r['cnt']:,} dosya arsivlenirse {format_size(r['size'])} disk alani kazanilir.",
                    "action": "Arsivleme politikasi olustur",
                    "action_button": "Arsivle",
                    "impact_size": r["size"],
                    "file_count": r["cnt"]
                })
        return insights

    def _riskiest_folders(self, source_id, scan_id):
        """Top 5 riskiest folders (large + old + many owners)."""
        insights = []
        try:
            with self.db.get_cursor() as cur:
                # Get parent folders with risk metrics
                cur.execute("""
                    SELECT
                        CASE WHEN INSTR(file_path, '/') > 0
                             THEN SUBSTR(file_path, 1, LENGTH(file_path) - LENGTH(file_name) - 1)
                             ELSE SUBSTR(file_path, 1, LENGTH(file_path) - LENGTH(file_name) - 1)
                        END as folder,
                        COUNT(*) as cnt,
                        SUM(file_size) as total_size,
                        COUNT(DISTINCT owner) as owner_count,
                        AVG(CASE WHEN last_access_time IS NOT NULL
                            THEN julianday('now') - julianday(last_access_time) ELSE 0 END) as avg_age
                    FROM scanned_files WHERE source_id=? AND scan_id=?
                    GROUP BY folder HAVING cnt > 10
                    ORDER BY (total_size * avg_age * CASE WHEN owner_count > 3 THEN 2 ELSE 1 END) DESC
                    LIMIT 5
                """, (source_id, scan_id))
                folders = cur.fetchall()
                if folders:
                    desc_parts = []
                    for f in folders[:5]:
                        folder_name = f["folder"] or "?"
                        if len(folder_name) > 60:
                            folder_name = "..." + folder_name[-57:]
                        desc_parts.append(f"{folder_name} ({format_size(f['total_size'])}, {f['cnt']} dosya)")
                    insights.append({
                        "category": "security",
                        "severity": "warning",
                        "title": "En Riskli 5 Klasor",
                        "description": " | ".join(desc_parts[:3]),
                        "action": "Bu klasorleri inceleyin ve yetkileri kontrol edin",
                        "action_button": "Incele",
                        "file_count": sum(f["cnt"] for f in folders)
                    })
        except Exception:
            pass
        return insights

    def _duplicate_summary(self, source_id, scan_id):
        """Detailed duplicate detection summary."""
        insights = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT file_name, file_size, COUNT(*) as cnt
                FROM scanned_files WHERE source_id=? AND scan_id=? AND file_size > 1048576
                GROUP BY file_name, file_size HAVING COUNT(*) > 1
                ORDER BY file_size * COUNT(*) DESC LIMIT 10
            """, (source_id, scan_id))
            dupes = cur.fetchall()
            if len(dupes) >= 3:
                total_waste = sum(r["file_size"] * (r["cnt"] - 1) for r in dupes)
                top3 = [f"{r['file_name']} ({r['cnt']}x, {format_size(r['file_size'])})" for r in dupes[:3]]
                insights.append({
                    "category": "duplicates",
                    "severity": "info",
                    "title": f"Duplikasyon Detayi: {len(dupes)} Grup",
                    "description": "En buyuk kopyalar: " + ", ".join(top3),
                    "action": "Kopya dosyalari temizleyin",
                    "action_button": "Temizle",
                    "impact_size": total_waste,
                    "file_count": sum(r["cnt"] - 1 for r in dupes)
                })
        return insights

    def _growth_rate_warning(self, source_id):
        """Warning if growth rate is too high."""
        insights = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT total_size, total_files, started_at FROM scan_runs
                WHERE source_id = ? AND status = 'completed'
                ORDER BY started_at DESC LIMIT 3
            """, (source_id,))
            scans = cur.fetchall()
            if len(scans) >= 2:
                latest = scans[0]
                prev = scans[1]
                size_diff = (latest["total_size"] or 0) - (prev["total_size"] or 0)
                file_diff = (latest["total_files"] or 0) - (prev["total_files"] or 0)
                # Alert if >10% growth
                prev_size = prev["total_size"] or 1
                growth_pct = (size_diff / prev_size) * 100
                if growth_pct > 10:
                    insights.append({
                        "category": "growth",
                        "severity": "warning",
                        "title": f"Hizli Buyume Uyarisi: %{growth_pct:.0f}",
                        "description": f"Son taramadan bu yana +{format_size(size_diff)} (+{file_diff:,} dosya) artis. Bu hizla kapasite planlama gerekebilir.",
                        "action": "Arsivleme politikasi ve kapasite planlama",
                        "action_button": "Plan Olustur",
                        "impact_size": size_diff
                    })
        return insights

    def _calculate_health_score(self, insights):
        score = 100
        for i in insights:
            sev = i.get("severity", "info")
            if sev == "critical":
                score -= 15
            elif sev == "warning":
                score -= 8
            elif sev == "info":
                score -= 2
        return max(0, min(100, score))


def get_insight_files(db: Database, scan_id: int, insight_type: str) -> list:
    """Insight tipine gore dosya listesi dondur."""
    queries = {
        "stale_1year": """
            SELECT id, file_path, file_name, file_size, owner, last_access_time, last_modify_time
            FROM scanned_files WHERE scan_id=?
            AND last_access_time < datetime('now', '-365 days')
            ORDER BY file_size DESC
        """,
        "stale_3year": """
            SELECT id, file_path, file_name, file_size, owner, last_access_time, last_modify_time
            FROM scanned_files WHERE scan_id=?
            AND last_access_time < datetime('now', '-1095 days')
            ORDER BY file_size DESC
        """,
        "large_files": """
            SELECT id, file_path, file_name, file_size, owner, last_access_time, last_modify_time
            FROM scanned_files WHERE scan_id=?
            AND file_size > 104857600
            ORDER BY file_size DESC
        """,
        "temp_files": """
            SELECT id, file_path, file_name, file_size, owner, last_access_time, last_modify_time
            FROM scanned_files WHERE scan_id=?
            AND (extension IN ('tmp','temp','bak','old','cache','log')
                 OR file_name LIKE '~$%' OR file_name LIKE '%.tmp')
            ORDER BY file_size DESC
        """,
        "duplicates": """
            SELECT sf.id, sf.file_path, sf.file_name, sf.file_size, sf.owner,
                   sf.last_access_time, sf.last_modify_time
            FROM scanned_files sf
            INNER JOIN (
                SELECT file_name, file_size
                FROM scanned_files WHERE scan_id=? AND file_size > 1048576
                GROUP BY file_name, file_size HAVING COUNT(*) > 1
            ) dup ON sf.file_name = dup.file_name AND sf.file_size = dup.file_size
            WHERE sf.scan_id=?
            ORDER BY sf.file_size DESC
        """,
    }

    query = queries.get(insight_type)
    if not query:
        return []

    with db.get_cursor() as cur:
        if insight_type == "duplicates":
            cur.execute(query, (scan_id, scan_id))
        else:
            cur.execute(query, (scan_id,))
        return [dict(r) for r in cur.fetchall()]
