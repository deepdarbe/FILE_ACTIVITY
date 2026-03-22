"""Loglama yapılandırması."""

import os
import logging
import sys


def setup_logging(config: dict):
    """Log sistemini yapılandır."""
    general = config.get("general", {})
    level = getattr(logging, general.get("log_level", "INFO").upper(), logging.INFO)
    log_file = general.get("log_file", "logs/file_activity.log")

    # Log dizinini oluştur
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    root = logging.getLogger("file_activity")
    root.setLevel(level)

    # Temizle
    root.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    # File handler
    try:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)
    except OSError as e:
        root.warning("Log dosyası oluşturulamadı: %s", e)
