"""Dosya boyutu dağılımı analiz modülü."""

import logging
from src.storage.database import Database
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.analyzer.size")


class SizeAnalyzer:
    """Dosya boyutlarını kategorilere göre analiz eder."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.buckets = config.get("analysis", {}).get("size_buckets", {
            "tiny": 102400,
            "small": 1048576,
            "medium": 104857600,
            "large": 1073741824,
        })

    def analyze(self, source_id: int, scan_id: int) -> list[dict]:
        """Boyut dağılımı analizi çalıştır.

        Returns:
            [{"label": "tiny", "file_count": N, "total_size": N, ...}, ...]
        """
        results = self.db.get_size_analysis(source_id, scan_id, self.buckets)

        for r in results:
            r["total_size_formatted"] = format_size(r["total_size"])
            min_f = format_size(r["min_bytes"])
            max_f = format_size(r["max_bytes"]) if r["max_bytes"] else "∞"
            r["range_formatted"] = f"{min_f} - {max_f}"

        logger.info("Boyut dağılımı analizi tamamlandı: %d kategori", len(results))
        return results
