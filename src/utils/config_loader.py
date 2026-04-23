"""YAML konfigürasyon yükleyici."""

import os
import logging
import yaml

logger = logging.getLogger("file_activity.config")

DEFAULT_CONFIG = {
    "general": {"language": "tr", "log_level": "INFO", "log_file": "logs/file_activity.log"},
    "database": {
        "path": "data/file_activity.db",
    },
    "sources": [],
    "scanner": {
        "max_workers": 4, "batch_size": 1000,
        "skip_hidden": True, "skip_system": True,
        "exclude_patterns": ["*.tmp", "~$*", "Thumbs.db", "desktop.ini"],
        "read_owner": False, "incremental": True,
    },
    "analysis": {
        "frequency_buckets": [30, 60, 90, 180, 365],
        "size_buckets": {
            "tiny": 102400, "small": 1048576,
            "medium": 104857600, "large": 1073741824,
        },
    },
    "archiving": {
        "verify_checksum": True, "dry_run": False, "cleanup_empty_dirs": True,
    },
    "dashboard": {"host": "0.0.0.0", "port": 8085},
    # Issue #77: auto-backup defaults. Match config.yaml. The Phase 2
    # auto_restore_on_corruption field is already here so older
    # configs parse cleanly once Phase 2 lands.
    "backup": {
        "enabled": True,
        "dir": "data/backups",
        "keep_last_n": 10,
        "keep_weekly": 4,
        "daily_snapshot_hour": 2,
        "snapshot_on_update": True,
        "snapshot_on_apply": True,
        "auto_restore_on_corruption": False,
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    """Override'daki değerler base'in üzerine yazılır."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: str = "config.yaml") -> dict:
    """Konfigürasyonu yükle, varsayılanlarla birleştir."""
    config = DEFAULT_CONFIG.copy()

    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            user_config = yaml.safe_load(f) or {}
        config = _deep_merge(config, user_config)
        logger.info("Konfigürasyon yüklendi: %s", config_path)
    else:
        logger.warning("Konfigürasyon dosyası bulunamadı: %s, varsayılanlar kullanılıyor", config_path)

    return config
