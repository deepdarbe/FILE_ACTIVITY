"""İki dilli mesaj kataloğu (TR/EN)."""

MESSAGES = {
    "scan_started":       {"tr": "Tarama başladı: {source}", "en": "Scan started: {source}"},
    "scan_completed":     {"tr": "Tarama tamamlandı: {count} dosya, {size}", "en": "Scan completed: {count} files, {size}"},
    "scan_failed":        {"tr": "Tarama başarısız: {error}", "en": "Scan failed: {error}"},
    "scan_skipped":       {"tr": "Atlanan dosya: {path} ({reason})", "en": "Skipped file: {path} ({reason})"},
    "scan_error":         {"tr": "Tarama hatası: {path} - {error}", "en": "Scan error: {path} - {error}"},
    "source_added":       {"tr": "Kaynak eklendi: {name}", "en": "Source added: {name}"},
    "source_removed":     {"tr": "Kaynak silindi: {name}", "en": "Source removed: {name}"},
    "source_not_found":   {"tr": "Kaynak bulunamadı: {name}", "en": "Source not found: {name}"},
    "source_reachable":   {"tr": "Kaynak erişilebilir: {path}", "en": "Source reachable: {path}"},
    "source_unreachable": {"tr": "Kaynak erişilemiyor: {path}", "en": "Source unreachable: {path}"},
    "archive_started":    {"tr": "Arşivleme başladı: {source}", "en": "Archiving started: {source}"},
    "archive_file":       {"tr": "Arşivleniyor: {path}", "en": "Archiving: {path}"},
    "archive_completed":  {"tr": "Arşivleme tamamlandı: {count} dosya, {size}", "en": "Archiving completed: {count} files, {size}"},
    "archive_dry_run":    {"tr": "[KURU ÇALIŞTIRMA] Arşivlenecek: {path} ({size})", "en": "[DRY RUN] Would archive: {path} ({size})"},
    "archive_checksum_ok":{"tr": "Checksum doğrulandı: {path}", "en": "Checksum verified: {path}"},
    "archive_checksum_fail":{"tr": "CHECKSUM HATASI: {path}", "en": "CHECKSUM MISMATCH: {path}"},
    "restore_completed":  {"tr": "Geri yükleme tamamlandı: {path}", "en": "Restore completed: {path}"},
    "restore_not_found":  {"tr": "Arşivde bulunamadı: {identifier}", "en": "Not found in archive: {identifier}"},
    "policy_added":       {"tr": "Politika eklendi: {name}", "en": "Policy added: {name}"},
    "policy_removed":     {"tr": "Politika silindi: {name}", "en": "Policy removed: {name}"},
    "schedule_added":     {"tr": "Zamanlama eklendi: {type} - {cron}", "en": "Schedule added: {type} - {cron}"},
    "schedule_removed":   {"tr": "Zamanlama silindi: #{id}", "en": "Schedule removed: #{id}"},
    "db_connected":       {"tr": "Veritabanına bağlandı", "en": "Connected to database"},
    "db_error":           {"tr": "Veritabanı hatası: {error}", "en": "Database error: {error}"},
    "ntfs_warning":       {"tr": "UYARI: NtfsDisableLastAccessUpdate aktif - erişim zamanları güvenilmez olabilir", "en": "WARNING: NtfsDisableLastAccessUpdate is enabled - access times may be unreliable"},
    "dashboard_started":  {"tr": "Dashboard başlatıldı: http://{host}:{port}", "en": "Dashboard started: http://{host}:{port}"},
    "no_scan_data":       {"tr": "Tarama verisi bulunamadı. Önce tarama yapın.", "en": "No scan data found. Run a scan first."},
}

_current_lang = "tr"


def set_language(lang: str):
    global _current_lang
    _current_lang = lang if lang in ("tr", "en") else "tr"


def t(key: str, lang: str = None, **kwargs) -> str:
    """Mesajı çevir ve formatla."""
    lang = lang or _current_lang
    msg_dict = MESSAGES.get(key, {})
    template = msg_dict.get(lang, msg_dict.get("tr", key))
    try:
        return template.format(**kwargs)
    except (KeyError, AttributeError):
        return template
