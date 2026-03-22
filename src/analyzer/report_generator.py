"""Birleştirici rapor motoru - tüm analizleri birleştirir."""

import logging
from datetime import datetime
from src.storage.database import Database
from src.analyzer.frequency_analyzer import FrequencyAnalyzer
from src.analyzer.type_analyzer import TypeAnalyzer
from src.analyzer.size_analyzer import SizeAnalyzer
from src.utils.size_formatter import format_size
from src.i18n.messages import t

logger = logging.getLogger("file_activity.analyzer.report")


class ReportGenerator:
    """Tüm analizleri birleştiren rapor motoru."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config
        self.freq_analyzer = FrequencyAnalyzer(db, config)
        self.type_analyzer = TypeAnalyzer(db)
        self.size_analyzer = SizeAnalyzer(db, config)

    def generate_status_report(self, source_id: int) -> dict:
        """Mevcut durum raporu."""
        scan_id = self.db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"error": t("no_scan_data")}

        source = self.db.get_source_by_id(source_id)
        summary = self.db.get_status_summary(source_id, scan_id)

        return {
            "source": {"id": source_id, "name": source.name, "path": source.unc_path},
            "scan_id": scan_id,
            "total_files": summary["total_files"],
            "total_size": summary["total_size"],
            "total_size_formatted": format_size(summary["total_size"]),
            "type_count": summary["type_count"],
            "oldest_file": str(summary["oldest_file"]) if summary["oldest_file"] else None,
            "newest_file": str(summary["newest_file"]) if summary["newest_file"] else None,
            "generated_at": datetime.now().isoformat(),
        }

    def generate_frequency_report(self, source_id: int, custom_days: list[int] = None) -> dict:
        """Erişim sıklığı raporu."""
        scan_id = self.db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"error": t("no_scan_data")}

        source = self.db.get_source_by_id(source_id)
        buckets = custom_days if custom_days else None

        return {
            "source": {"id": source_id, "name": source.name},
            "scan_id": scan_id,
            "frequency": self.freq_analyzer.analyze(source_id, scan_id, buckets),
            "generated_at": datetime.now().isoformat(),
        }

    def generate_type_report(self, source_id: int) -> dict:
        """Dosya türü raporu."""
        scan_id = self.db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"error": t("no_scan_data")}

        source = self.db.get_source_by_id(source_id)

        return {
            "source": {"id": source_id, "name": source.name},
            "scan_id": scan_id,
            "types": self.type_analyzer.analyze(source_id, scan_id),
            "generated_at": datetime.now().isoformat(),
        }

    def generate_size_report(self, source_id: int) -> dict:
        """Boyut dağılımı raporu."""
        scan_id = self.db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"error": t("no_scan_data")}

        source = self.db.get_source_by_id(source_id)

        return {
            "source": {"id": source_id, "name": source.name},
            "scan_id": scan_id,
            "sizes": self.size_analyzer.analyze(source_id, scan_id),
            "generated_at": datetime.now().isoformat(),
        }

    def generate_full_report(self, source_id: int) -> dict:
        """Tam birleştirilmiş rapor."""
        scan_id = self.db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            return {"error": t("no_scan_data")}

        source = self.db.get_source_by_id(source_id)
        summary = self.db.get_status_summary(source_id, scan_id)

        return {
            "source": {"id": source_id, "name": source.name, "path": source.unc_path},
            "scan_id": scan_id,
            "summary": {
                "total_files": summary["total_files"],
                "total_size": summary["total_size"],
                "total_size_formatted": format_size(summary["total_size"]),
                "type_count": summary["type_count"],
                "oldest_file": str(summary["oldest_file"]) if summary["oldest_file"] else None,
                "newest_file": str(summary["newest_file"]) if summary["newest_file"] else None,
            },
            "frequency": self.freq_analyzer.analyze(source_id, scan_id),
            "types": self.type_analyzer.analyze(source_id, scan_id),
            "sizes": self.size_analyzer.analyze(source_id, scan_id),
            "generated_at": datetime.now().isoformat(),
        }
