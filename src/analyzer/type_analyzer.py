"""Dosya türü analiz modülü."""

import logging
from src.storage.database import Database
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.analyzer.type")


class TypeAnalyzer:
    """Dosya uzantısına göre dağılım analizi."""

    def __init__(self, db: Database):
        self.db = db

    def analyze(self, source_id: int, scan_id: int) -> list[dict]:
        """Dosya türü analizi çalıştır.

        Returns:
            [{"extension": "docx", "file_count": N, "total_size": N, ...}, ...]
        """
        results = self.db.get_type_analysis(source_id, scan_id)

        for r in results:
            r["total_size_formatted"] = format_size(r["total_size"])
            r["avg_size_formatted"] = format_size(r["avg_size"])
            r["min_size_formatted"] = format_size(r["min_size"])
            r["max_size_formatted"] = format_size(r["max_size"])

        logger.info("Dosya türü analizi tamamlandı: %d uzantı", len(results))
        return results
