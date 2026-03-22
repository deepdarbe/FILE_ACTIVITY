"""Dosya erişim sıklığı analiz modülü."""

import logging
from src.storage.database import Database
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.analyzer.frequency")


class FrequencyAnalyzer:
    """Dosya erişim sıklığını gün aralıklarına göre analiz eder."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.buckets = config.get("analysis", {}).get("frequency_buckets", [30, 60, 90, 180, 365])

    def analyze(self, source_id: int, scan_id: int, custom_buckets: list[int] = None) -> list[dict]:
        """Erişim sıklığı analizi çalıştır.

        Returns:
            [{"days": 30, "label": "...", "file_count": N, "total_size": N}, ...]
        """
        buckets = custom_buckets or self.buckets
        results = self.db.get_frequency_analysis(source_id, scan_id, buckets)

        for r in results:
            r["total_size_formatted"] = format_size(r["total_size"])

        logger.info("Erişim sıklığı analizi tamamlandı: %d kova", len(results))
        return results
