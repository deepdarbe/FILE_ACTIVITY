"""Kullanıcı Aktivite Analiz Motoru.

Kullanıcı erişim verilerini analiz eder:
- En aktif kullanıcılar
- Erişim desenleri (saat/gün heatmap)
- Departman analizi
- Anomali tespiti
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from src.storage.database import Database
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.user_activity.analyzer")

DAY_NAMES_TR = {0: "Paz", 1: "Pzt", 2: "Sal", 3: "Car", 4: "Per", 5: "Cum", 6: "Cmt"}


class UserAnalyzer:
    """Kullanıcı aktivitelerini analiz eden motor."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config.get("user_activity", {})

    def get_overview(self, source_id: int = None, days: int = 30) -> dict:
        """Kullanıcı aktivite genel bakış."""
        top_users = self.db.get_top_users(source_id, days, limit=10)
        dept_stats = self.db.get_department_stats(days)
        timeline = self.db.get_access_timeline(source_id, days)
        anomaly_summary = self.db.get_anomaly_summary()

        total_access = sum(u["access_count"] for u in top_users)
        total_users = len(top_users)
        total_data = sum(u["total_data"] for u in top_users)

        return {
            "summary": {
                "total_users": total_users,
                "total_access": total_access,
                "total_data": total_data,
                "total_data_formatted": format_size(total_data),
                "period_days": days,
            },
            "top_users": [{
                **u,
                "total_data_formatted": format_size(u["total_data"]),
                "first_access": str(u["first_access"])[:19] if u["first_access"] else None,
                "last_access": str(u["last_access"])[:19] if u["last_access"] else None,
            } for u in top_users],
            "departments": [{
                **d,
                "total_data_formatted": format_size(d["total_data"]),
            } for d in dept_stats],
            "timeline": [{
                **t,
                "date": str(t["date"]),
            } for t in timeline],
            "anomalies": anomaly_summary,
            "generated_at": datetime.now().isoformat(),
        }

    def get_user_detail(self, username: str, days: int = 30) -> dict:
        """Tek kullanıcının detaylı analizi."""
        data = self.db.get_user_activity(username, days)

        # Saat dağılımını 0-23 dizisine dönüştür
        hourly = [data["hourly_distribution"].get(h, 0) for h in range(24)]

        # Gün dağılımını isimlendir
        daily = []
        for dow in range(7):
            daily.append({
                "day": DAY_NAMES_TR.get(dow, str(dow)),
                "dow": dow,
                "count": data["daily_distribution"].get(dow, 0),
            })

        summary = data["summary"]
        summary["total_data_formatted"] = format_size(summary.get("total_data", 0))

        # Risk skoru hesapla
        risk_score = self._calculate_risk_score(data)

        return {
            "username": username,
            "days": days,
            "summary": summary,
            "hourly": hourly,
            "daily": daily,
            "top_extensions": data["top_extensions"],
            "top_directories": data["top_directories"],
            "risk_score": risk_score,
            "generated_at": datetime.now().isoformat(),
        }

    def get_heatmap(self, source_id: int = None, days: int = 7) -> dict:
        """Saat x Gün heatmap verisi."""
        raw = self.db.get_hourly_heatmap(source_id, days)

        # 7x24 matris oluştur
        matrix = [[0] * 24 for _ in range(7)]
        max_val = 0
        for row in raw:
            dow = row["dow"]
            hour = row["hour"]
            count = row["count"]
            if 0 <= dow < 7 and 0 <= hour < 24:
                matrix[dow][hour] = count
                max_val = max(max_val, count)

        return {
            "matrix": matrix,
            "max_value": max_val,
            "days": [DAY_NAMES_TR.get(i, str(i)) for i in range(7)],
            "hours": list(range(24)),
        }

    def _calculate_risk_score(self, user_data: dict) -> dict:
        """Kullanıcı risk skoru hesapla (0-100)."""
        summary = user_data["summary"]
        hourly = user_data["hourly_distribution"]
        score = 0
        factors = []

        total = summary.get("total_access", 0)
        deletes = summary.get("deletes", 0)
        writes = summary.get("writes", 0)

        # Yüksek silme oranı
        if total > 0:
            delete_ratio = deletes / total
            if delete_ratio > 0.3:
                score += 30
                factors.append(f"Yuksek silme orani: %{delete_ratio*100:.0f}")
            elif delete_ratio > 0.1:
                score += 15
                factors.append(f"Orta silme orani: %{delete_ratio*100:.0f}")

        # Gece erişimi (22:00-06:00)
        night_access = sum(hourly.get(h, 0) for h in list(range(0, 6)) + [22, 23])
        if total > 0:
            night_ratio = night_access / total
            if night_ratio > 0.4:
                score += 25
                factors.append(f"Yogun gece erisimi: %{night_ratio*100:.0f}")
            elif night_ratio > 0.2:
                score += 10
                factors.append(f"Gece erisimi: %{night_ratio*100:.0f}")

        # Aşırı aktivite (günlük ortalama)
        active_days = summary.get("active_days", 1) or 1
        daily_avg = total / active_days
        if daily_avg > 500:
            score += 20
            factors.append(f"Yuksek gunluk ortalama: {daily_avg:.0f}")
        elif daily_avg > 200:
            score += 10
            factors.append(f"Orta gunluk ortalama: {daily_avg:.0f}")

        # Çok fazla benzersiz dosya
        unique = summary.get("unique_files", 0)
        if unique > 1000:
            score += 15
            factors.append(f"Cok sayida benzersiz dosya: {unique:,}")

        score = min(score, 100)

        if score >= 70:
            level = "critical"
        elif score >= 40:
            level = "warning"
        else:
            level = "normal"

        return {"score": score, "level": level, "factors": factors}


class AnomalyDetector:
    """Anormal kullanıcı davranışlarını tespit eder."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config.get("user_activity", {}).get("anomaly", {})
        self.thresholds = {
            "high_access_per_hour": self.config.get("high_access_per_hour", 200),
            "high_delete_count": self.config.get("high_delete_count", 50),
            "night_access_threshold": self.config.get("night_access_threshold", 30),
            "large_data_transfer_gb": self.config.get("large_data_transfer_gb", 5),
        }

    def run_detection(self, hours: int = 1) -> list[dict]:
        """Son N saat için anomali tespiti çalıştır."""
        alerts = []

        alerts.extend(self._detect_high_volume(hours))
        alerts.extend(self._detect_mass_delete(hours))
        alerts.extend(self._detect_night_access(hours))
        alerts.extend(self._detect_large_transfer(hours))

        # Veritabanına kaydet
        for alert in alerts:
            self.db.insert_anomaly(
                alert["username"], alert["type"], alert["severity"],
                alert["description"], alert.get("details")
            )

        if alerts:
            logger.warning(f"Anomali tespiti: {len(alerts)} uyarı")
        return alerts

    def _detect_high_volume(self, hours: int) -> list[dict]:
        """Saatlik yuksek erisim hacmi."""
        threshold = self.thresholds["high_access_per_hour"]
        alerts = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT username, COUNT(*) as cnt
                FROM user_access_logs
                WHERE access_time > datetime('now', ? || ' hours')
                GROUP BY username
                HAVING COUNT(*) > ?
            """, (f"-{hours}", threshold * hours))
            for row in cur.fetchall():
                alerts.append({
                    "username": row["username"],
                    "type": "high_volume",
                    "severity": "warning" if row["cnt"] < threshold * hours * 3 else "critical",
                    "description": f"{row['username']}: {hours} saatte {row['cnt']:,} erisim (esik: {threshold * hours})",
                    "details": {"count": row["cnt"], "hours": hours, "threshold": threshold * hours}
                })
        return alerts

    def _detect_mass_delete(self, hours: int) -> list[dict]:
        """Toplu dosya silme tespiti."""
        threshold = self.thresholds["high_delete_count"]
        alerts = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT username, COUNT(*) as cnt
                FROM user_access_logs
                WHERE access_time > datetime('now', ? || ' hours')
                  AND access_type = 'delete'
                GROUP BY username
                HAVING COUNT(*) > ?
            """, (f"-{hours}", threshold))
            for row in cur.fetchall():
                alerts.append({
                    "username": row["username"],
                    "type": "mass_delete",
                    "severity": "critical",
                    "description": f"{row['username']}: {hours} saatte {row['cnt']:,} dosya silme!",
                    "details": {"delete_count": row["cnt"], "hours": hours}
                })
        return alerts

    def _detect_night_access(self, hours: int) -> list[dict]:
        """Gece saatlerinde (22:00-06:00) anormal erisim."""
        threshold = self.thresholds["night_access_threshold"]
        alerts = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT username, COUNT(*) as cnt
                FROM user_access_logs
                WHERE access_time > datetime('now', ? || ' hours')
                  AND (CAST(strftime('%H', access_time) AS INTEGER) >= 22
                       OR CAST(strftime('%H', access_time) AS INTEGER) < 6)
                GROUP BY username
                HAVING COUNT(*) > ?
            """, (f"-{hours}", threshold))
            for row in cur.fetchall():
                alerts.append({
                    "username": row["username"],
                    "type": "night_access",
                    "severity": "warning",
                    "description": f"{row['username']}: Gece saatlerinde {row['cnt']:,} erisim",
                    "details": {"night_count": row["cnt"], "hours": hours}
                })
        return alerts

    def _detect_large_transfer(self, hours: int) -> list[dict]:
        """Buyuk veri transferi tespiti."""
        threshold_bytes = self.thresholds["large_data_transfer_gb"] * 1024**3
        alerts = []
        with self.db.get_cursor() as cur:
            cur.execute("""
                SELECT username, COALESCE(SUM(file_size), 0) as total_bytes
                FROM user_access_logs
                WHERE access_time > datetime('now', ? || ' hours')
                  AND access_type = 'read'
                GROUP BY username
                HAVING SUM(file_size) > ?
            """, (f"-{hours}", threshold_bytes))
            for row in cur.fetchall():
                gb = row["total_bytes"] / 1024**3
                alerts.append({
                    "username": row["username"],
                    "type": "large_transfer",
                    "severity": "warning" if gb < 10 else "critical",
                    "description": f"{row['username']}: {hours} saatte {gb:.1f} GB veri okuma",
                    "details": {"total_bytes": row["total_bytes"], "hours": hours}
                })
        return alerts
