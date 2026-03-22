"""Arşiv politika kural motoru.

Kuralları JSON olarak saklar ve dosyaları veritabanı sorgusu ile filtreler.
"""

import json
import logging
from typing import Optional
from src.storage.database import Database

logger = logging.getLogger("file_activity.archiver.policy")


class ArchivePolicyEngine:
    """Arşivleme kurallarını yönetir ve uygular."""

    def __init__(self, db: Database):
        self.db = db

    def get_files_by_policy(self, source_id: int, scan_id: int, policy_name: str,
                             limit: int = 10000) -> list[dict]:
        """Politika kurallarını uygulayarak arşivlenecek dosyaları getir."""
        policy = self.db.get_policy_by_name(policy_name)
        if not policy:
            logger.error("Politika bulunamadı: %s", policy_name)
            return []

        rules = json.loads(policy["rules_json"]) if isinstance(policy["rules_json"], str) else policy["rules_json"]

        # Kuralları veritabanı parametrelerine çevir
        params = self._rules_to_query_params(rules)
        params["limit"] = limit

        return self.db.get_files_for_archiving(source_id, scan_id, **params)

    def get_files_by_days(self, source_id: int, scan_id: int, days: int,
                           limit: int = 10000) -> list[dict]:
        """Basit gün bazlı kriter ile dosyaları getir."""
        return self.db.get_files_for_archiving(
            source_id, scan_id,
            access_older_than_days=days,
            limit=limit
        )

    def create_policy_rules(self,
                             access_days: Optional[int] = None,
                             modify_days: Optional[int] = None,
                             min_size: Optional[int] = None,
                             max_size: Optional[int] = None,
                             extensions: Optional[list[str]] = None,
                             exclude_extensions: Optional[list[str]] = None) -> str:
        """Kural parametrelerinden JSON rules oluştur."""
        rules = []

        if access_days:
            rules.append({"field": "last_access_days", "operator": "gte", "value": access_days})
        if modify_days:
            rules.append({"field": "last_modify_days", "operator": "gte", "value": modify_days})
        if min_size is not None:
            rules.append({"field": "file_size", "operator": "gte", "value": min_size})
        if max_size is not None:
            rules.append({"field": "file_size", "operator": "lte", "value": max_size})
        if extensions:
            rules.append({"field": "extension", "operator": "in", "value": extensions})
        if exclude_extensions:
            rules.append({"field": "extension", "operator": "not_in", "value": exclude_extensions})

        return json.dumps(rules)

    def _rules_to_query_params(self, rules: list[dict]) -> dict:
        """JSON kurallarını veritabanı sorgu parametrelerine çevir."""
        params = {}

        for rule in rules:
            field = rule.get("field", "")
            value = rule.get("value")

            if field == "last_access_days":
                params["access_older_than_days"] = value
            elif field == "last_modify_days":
                params["modify_older_than_days"] = value
            elif field == "file_size":
                op = rule.get("operator", "gte")
                if op in ("gte", "gt"):
                    params["min_size"] = value
                elif op in ("lte", "lt"):
                    params["max_size"] = value
            elif field == "extension":
                op = rule.get("operator", "in")
                if op == "in":
                    params["extensions"] = value
                elif op == "not_in":
                    params["exclude_extensions"] = value

        return params
