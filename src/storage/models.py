"""Veri modelleri - tüm tablo yapıları için dataclass tanımları."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Source:
    """Dosya paylaşım kaynağı."""
    id: Optional[int] = None
    name: str = ""
    unc_path: str = ""
    archive_dest: Optional[str] = None
    enabled: bool = True
    created_at: Optional[datetime] = None
    last_scanned_at: Optional[datetime] = None


@dataclass
class ScanRun:
    """Tek bir tarama çalıştırması."""
    id: Optional[int] = None
    source_id: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_files: int = 0
    total_size: int = 0
    errors: int = 0
    status: str = "running"  # running, completed, failed


@dataclass
class ScannedFile:
    """Taranan dosya kaydı."""
    id: Optional[int] = None
    source_id: int = 0
    scan_id: int = 0
    file_path: str = ""
    relative_path: str = ""
    file_name: str = ""
    extension: Optional[str] = None
    file_size: int = 0
    creation_time: Optional[datetime] = None
    last_access_time: Optional[datetime] = None
    last_modify_time: Optional[datetime] = None
    owner: Optional[str] = None
    attributes: Optional[int] = None


@dataclass
class ArchivedFile:
    """Arşivlenmiş dosya kaydı."""
    id: Optional[int] = None
    source_id: int = 0
    original_path: str = ""
    relative_path: str = ""
    archive_path: str = ""
    file_name: str = ""
    extension: Optional[str] = None
    file_size: int = 0
    creation_time: Optional[datetime] = None
    last_access_time: Optional[datetime] = None
    last_modify_time: Optional[datetime] = None
    owner: Optional[str] = None
    archived_at: Optional[datetime] = None
    archived_by: Optional[str] = None  # policy adı veya 'manual'
    restored_at: Optional[datetime] = None
    checksum: Optional[str] = None


@dataclass
class ArchivePolicy:
    """Arşiv politikası."""
    id: Optional[int] = None
    name: str = ""
    source_id: Optional[int] = None  # None = tüm kaynaklar
    rules_json: str = "[]"
    enabled: bool = True
    created_at: Optional[datetime] = None


@dataclass
class PolicyRule:
    """Tek bir politika kuralı."""
    field: str = ""           # last_access_days, last_modify_days, file_size, extension, path_pattern
    operator: str = ""        # gt, lt, gte, lte, eq, in, not_in, matches
    value: object = None      # karşılaştırma değeri


@dataclass
class ScheduledTask:
    """Zamanlanmış görev."""
    id: Optional[int] = None
    task_type: str = ""       # scan, archive
    source_id: Optional[int] = None
    policy_id: Optional[int] = None
    cron_expression: Optional[str] = None
    enabled: bool = True
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


@dataclass
class ScanSummary:
    """Tarama özet raporu."""
    source_name: str = ""
    total_files: int = 0
    total_size: int = 0
    file_type_count: int = 0
    oldest_file: Optional[datetime] = None
    newest_file: Optional[datetime] = None
    last_scan: Optional[datetime] = None


@dataclass
class FrequencyBucket:
    """Erişim sıklığı kovası."""
    label: str = ""
    days: int = 0
    file_count: int = 0
    total_size: int = 0


@dataclass
class TypeStats:
    """Dosya türü istatistikleri."""
    extension: str = ""
    file_count: int = 0
    total_size: int = 0
    avg_size: float = 0.0
    min_size: int = 0
    max_size: int = 0
    oldest: Optional[datetime] = None
    newest: Optional[datetime] = None


@dataclass
class SizeBucket:
    """Boyut dağılımı kovası."""
    label: str = ""
    min_bytes: int = 0
    max_bytes: int = 0
    file_count: int = 0
    total_size: int = 0
