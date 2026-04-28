"""Ana dosya tarama modulu.

Recursive olarak dizin tarar, dosya bilgilerini toplar ve veritabanina yazar.
os.scandir() ile performansli calisir, batch insert yapar.
Hem UNC hem lokal yollari destekler.
Tarama sirasinda ilerleme bilgisi loglar.
"""

from __future__ import annotations

import os
import re
import time
import fnmatch
import logging
import threading
import unicodedata
from datetime import datetime

from src.scanner.win_attributes import (
    get_file_times, is_hidden, is_system, check_ntfs_last_access_enabled, _long_path
)
from src.scanner.share_resolver import get_relative_path, test_connectivity
from src.scanner.backends import ScannerBackend
from src.scanner.backends.ntfs_mft import NtfsMftBackend
from src.scanner.backends.smb_parallel import SmbParallelBackend
from src.storage.database import Database
from src.storage.staging import ParquetStager
from src.i18n.messages import t
from src.utils.size_formatter import format_size


# ═══════════════════════════════════════════════════
# DOSYA ADI UYUMLULUK ANALİZCİSİ
# ═══════════════════════════════════════════════════

# Windows'ta dosya adlarinda yasak karakterler
_INVALID_CHARS_RE = re.compile(r'[<>:"|?*\x00-\x1f]')
# Turkce/ozel Unicode karakterler (ASCII disinda)
_NON_ASCII_RE = re.compile(r'[^\x00-\x7F]')
# Boslukla baslayan/biten
_SPACE_EDGE_RE = re.compile(r'^\s|\s$')
# Nokta ile biten (Windows sorunlu)
_DOT_END_RE = re.compile(r'\.$')
# Cift uzanti (gizli uzanti saldirisi: rapor.pdf.exe)
_DOUBLE_EXT_RE = re.compile(r'\.\w{2,5}\.\w{2,5}$')

# MIT Libraries Naming Standartlari
_MIT_ASCII_START_RE = re.compile(r'^[a-zA-Z]')  # Ilk karakter ASCII harf olmali
_MIT_VALID_BASE_RE = re.compile(r'^[a-zA-Z0-9._-]+$')  # Base'de sadece izinli karakterler
_MIT_SPACE_RE = re.compile(r'\s')  # Bosluk yasak
_MIT_MULTI_PERIOD_RE = re.compile(r'\..*\.')  # Base'de birden fazla nokta
_MIT_UPPERCASE_RE = re.compile(r'[A-Z]')  # Buyuk harf (best practice: kucuk harf tercih)
_MIT_UNDERSCORE_SEP_RE = re.compile(r'_')  # Alt cizgi ayirici (best practice)


class FileNameAnalyzer:
    """Dosya adi uyumluluk analizcisi - tarama sirasinda istatistik toplar."""

    def __init__(self):
        self.total = 0
        self.long_path_count = 0        # 260+ karakter yol
        self.very_long_path_count = 0   # 500+ karakter yol
        self.turkish_char_count = 0     # Turkce karakter iceren
        self.unicode_count = 0          # Genel Unicode (non-ASCII)
        self.invalid_char_count = 0     # Yasak karakter iceren
        self.space_edge_count = 0       # Boslukla baslayan/biten
        self.dot_end_count = 0          # Nokta ile biten
        self.double_ext_count = 0       # Cift uzantili
        self.max_path_length = 0        # En uzun yol
        self.max_name_length = 0        # En uzun dosya adi
        self.long_name_count = 0        # 100+ karakter dosya adi

        # Turkce karakterler
        self._turkish_chars = set("cCgGiIoOsSuU")

        # Ornekler (ilk 5 sorunlu dosya)
        self.samples_long_path = []
        self.samples_turkish = []
        self.samples_invalid = []
        self.samples_unicode = []

    def analyze(self, file_path: str, file_name: str):
        """Tek dosyayi analiz et."""
        self.total += 1
        path_len = len(file_path)
        name_len = len(file_name)

        # Yol uzunlugu
        if path_len > self.max_path_length:
            self.max_path_length = path_len
        if name_len > self.max_name_length:
            self.max_name_length = name_len

        if path_len > 260:
            self.long_path_count += 1
            if len(self.samples_long_path) < 5:
                self.samples_long_path.append({"path": file_path[:150] + "...", "length": path_len})
        if path_len > 500:
            self.very_long_path_count += 1
        if name_len > 100:
            self.long_name_count += 1

        # Turkce karakter kontrol
        has_turkish = False
        for ch in file_name:
            if ch in self._turkish_chars:
                has_turkish = True
                break
        if has_turkish:
            self.turkish_char_count += 1
            if len(self.samples_turkish) < 5:
                self.samples_turkish.append(file_name[:80])

        # Unicode (non-ASCII)
        if _NON_ASCII_RE.search(file_name):
            self.unicode_count += 1
            if len(self.samples_unicode) < 5 and not has_turkish:
                self.samples_unicode.append(file_name[:80])

        # Yasak karakterler
        if _INVALID_CHARS_RE.search(file_name):
            self.invalid_char_count += 1
            if len(self.samples_invalid) < 5:
                self.samples_invalid.append(file_name[:80])

        # Bosluk kenar
        if _SPACE_EDGE_RE.search(file_name):
            self.space_edge_count += 1

        # Nokta ile biten
        if _DOT_END_RE.search(os.path.splitext(file_name)[0]):
            self.dot_end_count += 1

        # Cift uzanti
        if _DOUBLE_EXT_RE.search(file_name):
            self.double_ext_count += 1

    def get_report(self) -> dict:
        """Uyumluluk raporu olustur."""
        if self.total == 0:
            return {"total": 0, "issues": [], "health_score": 100}

        issues = []

        def _add(label, count, severity, desc, samples=None):
            if count > 0:
                pct = count / self.total * 100
                issues.append({
                    "label": label,
                    "count": count,
                    "percentage": round(pct, 2),
                    "severity": severity,
                    "description": desc,
                    "samples": samples or []
                })

        _add("Uzun Yol (260+)", self.long_path_count, "warning",
             "Windows 260 karakter limitini asiyor. Bazi uygulamalar erisemeyebilir.",
             self.samples_long_path)
        _add("Cok Uzun Yol (500+)", self.very_long_path_count, "critical",
             "Yol 500 karakteri asiyor. Ciddi uyumluluk sorunu.",
             [])
        _add("Turkce Karakter", self.turkish_char_count, "info",
             "Turkce ozel karakterler iceriyor (c,g,i,o,s,u). Cross-platform sorun olabilir.",
             self.samples_turkish)
        _add("Unicode Karakter", self.unicode_count, "info",
             "ASCII disi Unicode karakterler iceriyor.",
             self.samples_unicode)
        _add("Yasak Karakter", self.invalid_char_count, "critical",
             "Windows'ta yasak karakterler iceriyor (< > : \" | ? *).",
             self.samples_invalid)
        _add("Bosluk (Bas/Son)", self.space_edge_count, "warning",
             "Dosya adi boslukla basliyor veya bitiyor.",
             [])
        _add("Nokta ile Biten", self.dot_end_count, "warning",
             "Dosya adi nokta ile bitiyor. Windows sorunlu olabilir.",
             [])
        _add("Cift Uzanti", self.double_ext_count, "info",
             "Cift uzanti iceriyor (ornek: rapor.pdf.exe). Guvenlik riski olabilir.",
             [])
        _add("Uzun Dosya Adi (100+)", self.long_name_count, "warning",
             "Dosya adi 100 karakterden uzun.",
             [])

        # Saglik skoru hesapla (100'den dusur)
        total_issues = (
            self.invalid_char_count * 5 +
            self.very_long_path_count * 3 +
            self.long_path_count * 1 +
            self.space_edge_count * 2 +
            self.dot_end_count * 1
        )
        health = max(0, 100 - int(total_issues / max(self.total, 1) * 100))

        return {
            "total_files_analyzed": self.total,
            "max_path_length": self.max_path_length,
            "max_name_length": self.max_name_length,
            "health_score": health,
            "health_label": "Iyi" if health >= 80 else ("Orta" if health >= 50 else "Kotu"),
            "issues": issues,
            "summary": {
                "long_paths": self.long_path_count,
                "turkish_chars": self.turkish_char_count,
                "unicode_chars": self.unicode_count,
                "invalid_chars": self.invalid_char_count,
                "compatibility_issues": self.invalid_char_count + self.space_edge_count + self.dot_end_count,
            }
        }


class MITNamingAnalyzer:
    """MIT Libraries File Naming Scheme uyum analizcisi.

    Referans: MIT Libraries File Naming Scheme (2011)
    Kontrol edilen kurallar:
      Requirements (Zorunlu):
        R1: Bosluk icermemeli
        R2: Ilk karakter ASCII harf olmali (a-z, A-Z)
        R3: Base sadece ASCII harf, rakam, tire, alt cizgi, nokta icermeli
        R4: Tek nokta + uzanti ile bitmeli
        R5: Uygun uzanti (3-4 harf)
      Best Practices (Onerilen):
        B1: Dosya adi <= 31 karakter (nokta+uzanti dahil)
        B2: Toplam yol <= 256 karakter
        B3: Base'de nokta kullanilmamali (sadece uzantidan once)
        B4: Tum harfler kucuk olmali (CamelCase istisna)
        B5: Bolumler alt cizgi ile ayrilmali
        B6: Dizin adlarinda nokta olmamali
        B7: Sira numaralari sifirla doldurulmali
    """

    # Yaygin uzantilar (3-4+ harf)
    _VALID_EXTENSIONS = {
        'jpg', 'jpeg', 'png', 'gif', 'tif', 'tiff', 'bmp', 'svg', 'webp',
        'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'csv',
        'xml', 'json', 'html', 'htm', 'css', 'js', 'ts', 'py', 'java',
        'cpp', 'hpp', 'zip', 'rar', 'tar', 'gz', 'bz2', '7z',
        'mp3', 'mp4', 'wav', 'avi', 'mov', 'mkv', 'flv', 'wmv',
        'aiff', 'djvu', 'mj2', 'log', 'ini', 'cfg', 'dat', 'sql',
        'md5', 'sha', 'msg', 'eml', 'dwg', 'dxf', 'psd', 'ai',
        'exe', 'dll', 'bat', 'ps1', 'sh', 'cmd', 'msi', 'iso',
        'db', 'bak', 'tmp', 'md', 'rst', 'yaml', 'yml', 'toml',
    }

    def __init__(self):
        self.total = 0
        # Requirements ihlalleri
        self.r_space = 0           # R1: Bosluk
        self.r_first_char = 0     # R2: Ilk karakter ASCII harf degil
        self.r_invalid_base = 0   # R3: Base'de yasak karakter
        self.r_bad_extension = 0  # R4/R5: Uzanti sorunu
        # Best practice ihlalleri
        self.b_long_name = 0      # B1: >31 karakter
        self.b_long_path = 0      # B2: >256 karakter yol
        self.b_multi_period = 0   # B3: Base'de birden fazla nokta
        self.b_uppercase = 0      # B4: Buyuk harf iceren
        self.b_no_underscore = 0  # B5: Alt cizgi ayirici yok (tek parca isim)
        self.b_dir_period = 0     # B6: Dizin adinda nokta
        # Ornekler
        self.samples = {k: [] for k in [
            'r_space', 'r_first_char', 'r_invalid_base', 'r_bad_extension',
            'b_long_name', 'b_long_path', 'b_multi_period', 'b_uppercase',
            'b_no_underscore', 'b_dir_period'
        ]}
        # Uyumlu dosya sayisi
        self.fully_compliant = 0
        self.req_compliant = 0  # Sadece requirements'a uyumlu

    def _add_sample(self, key, text, max_samples=5):
        if len(self.samples[key]) < max_samples:
            self.samples[key].append(str(text)[:120])

    def analyze(self, file_path: str, file_name: str):
        """Tek dosyayi MIT standartlarina gore analiz et."""
        self.total += 1
        req_ok = True
        bp_ok = True

        # Base ve uzanti ayir
        if '.' in file_name:
            last_dot = file_name.rfind('.')
            base = file_name[:last_dot]
            ext = file_name[last_dot + 1:]
        else:
            base = file_name
            ext = ''

        # === REQUIREMENTS ===

        # R1: Bosluk kontrolu
        if _MIT_SPACE_RE.search(file_name):
            self.r_space += 1
            self._add_sample('r_space', file_name)
            req_ok = False

        # R2: Ilk karakter ASCII harf olmali
        if file_name and not _MIT_ASCII_START_RE.match(file_name):
            self.r_first_char += 1
            self._add_sample('r_first_char', file_name)
            req_ok = False

        # R3: Base'de sadece izinli karakterler
        if base and not _MIT_VALID_BASE_RE.match(base):
            self.r_invalid_base += 1
            self._add_sample('r_invalid_base', file_name)
            req_ok = False

        # R4/R5: Uzanti kontrolu
        if not ext:
            self.r_bad_extension += 1
            self._add_sample('r_bad_extension', file_name)
            req_ok = False
        elif not ext.isascii() or not ext.replace('-', '').replace('_', '').isalnum():
            self.r_bad_extension += 1
            self._add_sample('r_bad_extension', file_name)
            req_ok = False

        # === BEST PRACTICES ===

        # B1: Dosya adi <= 31 karakter
        if len(file_name) > 31:
            self.b_long_name += 1
            self._add_sample('b_long_name', f"{file_name} ({len(file_name)} kar)")
            bp_ok = False

        # B2: Toplam yol <= 256 karakter
        if len(file_path) > 256:
            self.b_long_path += 1
            self._add_sample('b_long_path', f"...{file_path[-80:]} ({len(file_path)} kar)")
            bp_ok = False

        # B3: Base'de birden fazla nokta
        if base.count('.') > 0:
            self.b_multi_period += 1
            self._add_sample('b_multi_period', file_name)
            bp_ok = False

        # B4: Buyuk harf iceren
        if _MIT_UPPERCASE_RE.search(base):
            self.b_uppercase += 1
            bp_ok = False

        # B5: Alt cizgi ayirici yok (dosya adi 10+ karakter ve alt cizgi/tire yok)
        if len(base) > 10 and '_' not in base and '-' not in base:
            self.b_no_underscore += 1
            self._add_sample('b_no_underscore', file_name)
            bp_ok = False

        # B6: Dizin adinda nokta
        dir_path = os.path.dirname(file_path)
        for part in dir_path.replace('\\', '/').split('/'):
            if '.' in part and part not in ('', '.', '..'):
                self.b_dir_period += 1
                self._add_sample('b_dir_period', part)
                bp_ok = False
                break

        if req_ok:
            self.req_compliant += 1
        if req_ok and bp_ok:
            self.fully_compliant += 1

    def get_report(self) -> dict:
        """MIT uyum raporu olustur."""
        if self.total == 0:
            return {"total": 0, "compliance_score": 100, "requirements": [], "best_practices": []}

        req_score = (self.req_compliant / self.total * 100) if self.total > 0 else 0
        full_score = (self.fully_compliant / self.total * 100) if self.total > 0 else 0

        requirements = []
        best_practices = []

        def _add_req(code, label, count, desc, key):
            pct = count / self.total * 100
            requirements.append({
                "code": code, "label": label, "count": count,
                "percentage": round(pct, 2),
                "severity": "critical" if pct > 10 else ("warning" if pct > 1 else "info"),
                "description": desc,
                "samples": self.samples.get(key, [])
            })

        def _add_bp(code, label, count, desc, key):
            pct = count / self.total * 100
            best_practices.append({
                "code": code, "label": label, "count": count,
                "percentage": round(pct, 2),
                "severity": "warning" if pct > 20 else "info",
                "description": desc,
                "samples": self.samples.get(key, [])
            })

        # Requirements
        _add_req("R1", "Bosluk Iceren", self.r_space,
                 "Dosya adinda bosluk var. MIT: 'Filenames must not include spaces.'",
                 'r_space')
        _add_req("R2", "Ilk Karakter Harf Degil", self.r_first_char,
                 "Ilk karakter ASCII harf (a-z/A-Z) olmali. MIT: 'The first character must be an ASCII letter.'",
                 'r_first_char')
        _add_req("R3", "Yasak Karakter", self.r_invalid_base,
                 "Base'de sadece ASCII harf, rakam, tire, alt cizgi, nokta kullanilmali.",
                 'r_invalid_base')
        _add_req("R4", "Uzanti Sorunu", self.r_bad_extension,
                 "Dosya tek nokta + uygun uzanti ile bitmeli (3+ harf: jpg, pdf, tif).",
                 'r_bad_extension')

        # Best Practices
        _add_bp("B1", "Uzun Dosya Adi (>31 kar)", self.b_long_name,
                "MIT: 'File names should be limited to 31 characters or fewer.'",
                'b_long_name')
        _add_bp("B2", "Uzun Yol (>256 kar)", self.b_long_path,
                "MIT: 'Total path length should not exceed 256 characters.'",
                'b_long_path')
        _add_bp("B3", "Base'de Nokta", self.b_multi_period,
                "MIT: 'Periods should be avoided in base filenames.' Bazi programlar sorun yasayabilir.",
                'b_multi_period')
        _add_bp("B4", "Buyuk Harf Kullanimi", self.b_uppercase,
                "MIT: 'It is preferable that all letters be lowercase.' CamelCase istisna.",
                'b_uppercase')
        _add_bp("B5", "Ayirici Yok (>10 kar)", self.b_no_underscore,
                "MIT: 'Distinct portions should be separated by underscores.'",
                'b_no_underscore')
        _add_bp("B6", "Dizin Adinda Nokta", self.b_dir_period,
                "MIT: 'Directory names should not include periods.'",
                'b_dir_period')

        # Genel skor: requirements %70, best practices %30
        compliance_score = round(req_score * 0.7 + full_score * 0.3, 1)

        return {
            "total_files_analyzed": self.total,
            "compliance_score": compliance_score,
            "requirement_compliance": round(req_score, 1),
            "full_compliance": round(full_score, 1),
            "fully_compliant_count": self.fully_compliant,
            "req_compliant_count": self.req_compliant,
            "requirements": [r for r in requirements if r["count"] > 0],
            "best_practices": [b for b in best_practices if b["count"] > 0],
            "all_requirements": requirements,
            "all_best_practices": best_practices,
            "summary": {
                "total_requirement_violations": self.r_space + self.r_first_char + self.r_invalid_base + self.r_bad_extension,
                "total_bp_violations": self.b_long_name + self.b_long_path + self.b_multi_period + self.b_uppercase + self.b_no_underscore + self.b_dir_period,
                "top_issue": max(
                    [("Bosluk", self.r_space), ("Yasak Karakter", self.r_invalid_base),
                     ("Ilk Karakter", self.r_first_char), ("Uzanti", self.r_bad_extension),
                     ("Uzun Ad", self.b_long_name), ("Buyuk Harf", self.b_uppercase)],
                    key=lambda x: x[1]
                )[0] if self.total > 0 else None
            }
        }


logger = logging.getLogger("file_activity.scanner")


# Global scan progress tracking (for dashboard API)
_scan_progress = {}

# Issue #131 — process-local registry of cancellation events keyed by
# source_id. The dashboard ``POST /api/scan/{id}/stop`` endpoint sets the
# event; the scan loop checks ``is_set()`` after each batch and exits
# cleanly. The registry is held in a module-level dict so the stop
# endpoint can find the event without holding a Scanner reference (the
# scan thread owns the FileScanner; the request thread does not).
_cancel_events: dict[int, threading.Event] = {}
_cancel_events_lock = threading.Lock()


def get_scan_progress(source_id: int = None) -> dict:
    """Tarama ilerleme durumunu dondur (dashboard icin)."""
    if source_id and source_id in _scan_progress:
        return _scan_progress[source_id]
    return _scan_progress


def get_or_create_cancel_event(source_id: int) -> threading.Event:
    """Return (creating if needed) the ``threading.Event`` for ``source_id``.

    A single event is shared between the scan worker and the stop
    endpoint. Calling :py:meth:`Event.set` causes the scan loop to break
    on its next batch boundary.
    """
    with _cancel_events_lock:
        ev = _cancel_events.get(source_id)
        if ev is None:
            ev = threading.Event()
            _cancel_events[source_id] = ev
        return ev


def get_cancel_event(source_id: int) -> threading.Event | None:
    """Lookup-only — return the event for ``source_id`` or None."""
    with _cancel_events_lock:
        return _cancel_events.get(source_id)


def reset_cancel_event(source_id: int) -> None:
    """Drop the cancel event for ``source_id`` (next scan starts fresh)."""
    with _cancel_events_lock:
        _cancel_events.pop(source_id, None)


class FileScanner:
    """Dosya paylasim tarayicisi."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config.get("scanner", {}) if "scanner" in config else config
        self.batch_size = self.config.get("batch_size", 1000)
        self.skip_hidden = self.config.get("skip_hidden", True)
        self.skip_system = self.config.get("skip_system", True)
        self.exclude_patterns = self.config.get("exclude_patterns", [])
        self.read_owner = self.config.get("read_owner", False)
        self._ntfs_access_checked = False
        # Keep the full config dict around so backends can read their own keys.
        self._full_config = config if isinstance(config, dict) else {"scanner": self.config}
        # Issue #131 — cancellation. ``cancel_event`` is checked at every
        # batch boundary inside scan_source; setting it from another
        # thread causes the loop to break and the partial scan_run row
        # to be marked ``status='cancelled'``. Default-constructed (not
        # set) so the very first scan runs to completion.
        self.cancel_event: threading.Event = threading.Event()
        # Issue #137 — optional ``callable(stage: str, processed: int)``.
        # When set, the scanner forwards it to backends that report
        # mid-walk live counters (currently only ``NtfsMftBackend`` —
        # the MFT collection phase produces a record count well before
        # any DB row is written). The dashboard wires this to
        # ``OperationsRegistry.progress(op_id, processed=N, label=...)``
        # so the Sources page card and DOSYA KPI track the ops banner.
        self.progress_callback = None

    def _select_backend(self, path: str) -> ScannerBackend:
        """Return the walk backend to use for ``path``.

        Backend selection order (fastest first):
          1. :class:`NtfsMftBackend` — local NTFS + admin only.
          2. :class:`SmbParallelBackend` — universal fallback (UNC + local).

        Each candidate is asked whether it can serve the path; failures
        (NotImplementedError, OSError) fall through to the next backend.
        UNC paths are detected explicitly so the intent is obvious in the
        logs.
        """
        is_unc = path.startswith("\\\\")

        # Try NTFS MFT first (fastest, requires local NTFS + admin).
        # Issue #135 — pass the ops_registry + op_id through so the MFT
        # backend can emit incremental progress every 50k records during
        # enumeration. Both may be None; the backend handles that.
        try:
            mft = NtfsMftBackend(
                self._full_config,
                progress_callback=self.progress_callback,
            )
            if mft.is_supported(path):
                logger.debug("Scanner backend: ntfs_mft for %s", path)
                return mft
        except (NotImplementedError, OSError) as exc:
            logger.debug("ntfs_mft backend unavailable for %s: %s", path, exc)

        backend_name = "smb_parallel (UNC)" if is_unc else "smb_parallel (local)"
        logger.debug("Scanner backend: %s for %s", backend_name, path)
        return SmbParallelBackend(self._full_config)

    def scan_source(self, source_id: int, source_name: str, path: str) -> dict:
        """Bir kaynagi tara ve sonuclari veritabanina yaz.

        Args:
            source_id: Kaynak ID
            source_name: Kaynak adi
            path: UNC veya lokal yol

        Returns:
            {"total_files": int, "total_size": int, "errors": int, "status": str}
        """
        logger.info("Tarama basladi: %s (%s)", source_name, path)
        # Issue #175 — stash the active source_id for _run_size_enrich
        # which doesn't carry it through the orchestrator argument list.
        self._current_source_id = source_id

        # Ilerleme durumu baslat
        # Issue #135 — ``phase`` enumere edilmis tarama yasam dongusunu
        # frontend'e tasir: enumeration -> insert -> analysis -> completed.
        progress = {
            "source_id": source_id,
            "source_name": source_name,
            "status": "connecting",
            "phase": "enumeration",
            "file_count": 0,
            "total_size": 0,
            "total_size_formatted": "0 B",
            "errors": 0,
            "current_dir": "",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed": "0s",
            "files_per_second": 0,
        }
        _scan_progress[source_id] = progress

        # Baglanti kontrolu
        reachable, msg = test_connectivity(path)
        if not reachable:
            logger.error(msg)
            progress["status"] = "failed"
            progress["error"] = msg
            return {"total_files": 0, "total_size": 0, "errors": 1, "status": "failed", "error": msg}

        progress["status"] = "scanning"

        # NTFS access time uyarisi (ilk calismada bir kez)
        if not self._ntfs_access_checked:
            self._ntfs_access_checked = True
            if not check_ntfs_last_access_enabled():
                logger.warning("UYARI: NtfsDisableLastAccessUpdate aktif - erisim zamanlari guvenilmez olabilir")

        # Resume support: check for incomplete scan
        scanned_paths = set()
        incomplete = self.db.get_incomplete_scan(source_id)
        if incomplete:
            scan_id = incomplete["scan_id"]
            scanned_paths = self.db.get_scanned_paths(scan_id)
            file_count = incomplete["total_files"] or 0
            total_size = incomplete["total_size"] or 0
            logger.info("Onceki tarama devam ettiriliyor: scan_id=%d, %d dosya zaten tarandi", scan_id, len(scanned_paths))
            progress["status"] = "resuming"
        else:
            scan_id = self.db.create_scan_run(source_id)
            file_count = 0
            total_size = 0
        errors = 0
        batch = []
        start_time = time.time()
        last_log_time = start_time
        name_analyzer = FileNameAnalyzer()
        mit_analyzer = MITNamingAnalyzer()

        # Issue #135 — throttle ``scan_runs`` UPDATE to "every 10 seconds OR
        # every 100k records, whichever comes first". A short single-row
        # UPDATE plays nicely with the bulk INSERT writer lock; a long
        # transaction wrapping more would starve the dashboard reader.
        last_db_update_ts: float = start_time
        last_db_update_count: int = 0
        DB_UPDATE_EVERY_SECONDS = 10.0
        DB_UPDATE_EVERY_RECORDS = 100_000

        # Issue #139 — partial summary scheduler. While the scan runs we
        # periodically aggregate the rows already written and stash a
        # JSON snapshot on ``scan_runs.partial_summary_json`` so the
        # dashboard can show rolling KPIs instead of all-zeros. Trigger:
        # every 10 minutes OR every 100k records, whichever first. The
        # compute itself runs in a SHORT background thread on a read-only
        # cursor so it can never block the writer. If a single compute
        # exceeds 30 sec we double the interval to 20 minutes.
        partial_last_compute_ts: float = start_time
        partial_last_compute_count: int = 0
        partial_interval_seconds: float = 600.0  # 10 minutes
        PARTIAL_INTERVAL_RECORDS = 100_000
        PARTIAL_SLOW_THRESHOLD_SECONDS = 30.0
        partial_thread: threading.Thread | None = None

        # Issue #181 Track B1 — partial summary v2 builder.
        # Config-gated: ``scanner.partial_summary_schema`` defaults to 2.
        # When 2 (default) the v2 builder maintains running counters on
        # the writer thread and we flush them via ``flush_to_db`` at the
        # same cadence as v1's compute. When 1 we keep the v1 GROUP BY
        # path so legacy deployments can opt out.
        partial_schema_version = int(
            self.config.get("partial_summary_schema", 2) or 2,
        )
        v2_builder = None
        if partial_schema_version == 2:
            try:
                from src.analyzer.partial_summary_v2 import (
                    PartialSummaryV2Builder,
                )
                v2_builder = PartialSummaryV2Builder(
                    self.db, scan_id, source_id,
                )
            except Exception as e:  # pragma: no cover - defensive
                logger.warning(
                    "partial_summary_v2 init failed (falling back to v1): %s",
                    e,
                )
                v2_builder = None
                partial_schema_version = 1
        # Stash on the instance so post-scan phases (extension check,
        # size enrich) can still poke the same counters / flush state.
        self._v2_builder = v2_builder

        def _run_partial_summary(captured_scan_id: int) -> None:
            """Compute + persist a partial summary in a worker thread.

            v2 path: render + flush the in-memory builder. Cheap (no DB
            read), so the slow-threshold doubling never trips.

            v1 path (legacy): re-runs the GROUP BY queries on a
            read-only cursor. Doubles ``partial_interval_seconds`` if the
            compute takes more than 30 sec.
            """
            try:
                t0 = time.time()
                if v2_builder is not None:
                    rate = 0.0
                    elapsed_s = max(0.001, t0 - start_time)
                    rate = float(file_count) / elapsed_s
                    active_dir = progress.get("current_dir", "") or ""
                    v2_builder.flush_to_db(
                        scan_state="db_writing",
                        rate_per_sec=rate,
                        active_dir=active_dir,
                    )
                    dt = time.time() - t0
                    logger.debug(
                        "partial_summary_v2 scan=%d files=%d elapsed=%.3fs",
                        captured_scan_id, file_count, dt,
                    )
                    return

                from src.analyzer.partial_summary import compute_partial_summary
                payload = compute_partial_summary(self.db, captured_scan_id)
                self.db.save_scan_partial_summary(captured_scan_id, payload)
                dt = time.time() - t0
                if dt > PARTIAL_SLOW_THRESHOLD_SECONDS:
                    new_interval = min(
                        partial_interval_slot[0] * 2.0, 3600.0,
                    )
                    if new_interval != partial_interval_slot[0]:
                        logger.warning(
                            "partial_summary slow (%.1fs > %.1fs); doubling "
                            "interval to %.0f sec for scan_id=%d",
                            dt, PARTIAL_SLOW_THRESHOLD_SECONDS,
                            new_interval, captured_scan_id,
                        )
                        partial_interval_slot[0] = new_interval
                else:
                    logger.debug(
                        "partial_summary scan=%d files=%s elapsed=%.2fs",
                        captured_scan_id,
                        payload.get("total_files"), dt,
                    )
            except Exception as e:  # pragma: no cover - defensive
                logger.warning(
                    "partial_summary compute failed scan=%d: %s",
                    captured_scan_id, e,
                )

        # Mutable slot so the worker can adjust the interval — we're
        # already in a non-trivial closure tree, this is the cleanest
        # cross-thread channel that doesn't need a Lock.
        partial_interval_slot = [partial_interval_seconds]

        # Issue #135 — initial phase = enumeration (MFT walk in progress).
        try:
            self.db.update_scan_phase(scan_id, "enumeration")
        except Exception as e:  # pragma: no cover - defensive only
            logger.debug("update_scan_phase('enumeration') failed: %s", e)

        # Parquet staging path: when pyarrow + DuckDB are available, scan
        # rows are buffered to a Parquet file and bulk-INSERTed via DuckDB
        # (10-50x faster on 100k+ row scans). Construct once per scan; on
        # any failure the stager silently falls back to bulk_insert.
        stager = ParquetStager(self.db, self._full_config)

        # Initialise loop-exit status now so the cancel-break path inside
        # the loop can override it before the try-block's final assignment.
        status = "completed"

        # Issue #135 — track whether we already moved past the enumeration
        # phase. The first record from the backend means the MFT walk has
        # produced rows we can stage; from that point on the dashboard
        # label should read "DB'ye yaziliyor" rather than "MFT okunuyor".
        phase_transitioned_to_insert = False

        try:
            backend = self._select_backend(path)
            for record in backend.walk(path):
                file_path = record.get("file_path")
                if not file_path:
                    continue

                # Resume: skip already scanned files
                if scanned_paths and file_path in scanned_paths:
                    continue

                # Issue #135 — first usable record => transition phase to
                # ``insert``. Frontend banner switches from "MFT okunuyor"
                # to "DB'ye yaziliyor". We update DB once and the in-memory
                # progress dict so /api/scan/progress reflects it instantly.
                if not phase_transitioned_to_insert:
                    phase_transitioned_to_insert = True
                    progress["phase"] = "insert"
                    try:
                        self.db.update_scan_phase(scan_id, "insert")
                    except Exception as e:  # pragma: no cover - defensive only
                        logger.debug("update_scan_phase('insert') failed: %s", e)

                try:
                    file_name = record.get("file_name") or os.path.basename(file_path)
                    file_size = int(record.get("file_size") or 0)
                    creation_time = record.get("creation_time")
                    last_access_time = record.get("last_access_time")
                    last_modify_time = record.get("last_modify_time")
                    owner = record.get("owner")
                    attributes = int(record.get("attributes") or 0)

                    rel_path = get_relative_path(file_path, path)
                    ext = os.path.splitext(file_name)[1].lower().lstrip(".")
                    if not ext:
                        ext = None

                    row = {
                        "source_id": source_id,
                        "scan_id": scan_id,
                        "file_path": file_path,
                        "relative_path": rel_path,
                        "file_name": file_name,
                        "extension": ext,
                        "file_size": file_size,
                        "creation_time": creation_time,
                        "last_access_time": last_access_time,
                        "last_modify_time": last_modify_time,
                        "owner": owner,
                        "attributes": attributes,
                    }

                    batch.append(row)
                    file_count += 1
                    total_size += file_size

                    # Dosya adi uyumluluk analizi
                    name_analyzer.analyze(file_path, file_name)
                    mit_analyzer.analyze(file_path, file_name)

                    # Batch insert (parquet-staged when available, falls back
                    # to bulk_insert_scanned_files inside append() otherwise).
                    if len(batch) >= self.batch_size:
                        stager.append(batch)
                        # Issue #181 Track B1 — feed the v2 builder
                        # immediately after the stager accepts the
                        # batch. Same writer thread, so no lock needed.
                        # Falls back silently when v2 is disabled.
                        if v2_builder is not None:
                            try:
                                v2_builder.absorb_batch(batch)
                            except Exception as e:  # pragma: no cover
                                logger.debug(
                                    "v2_builder.absorb_batch failed: %s", e,
                                )
                        batch = []
                        # Issue #153 Lever A — signal the manual
                        # checkpointer that we just released the writer
                        # lock. Non-blocking: the daemon may pick this
                        # up on its next iteration. ``checkpointer`` may
                        # be ``None`` if init failed; treat as no-op.
                        cp = getattr(self.db, "checkpointer", None)
                        if cp is not None:
                            try:
                                cp.request()
                            except Exception as e:  # pragma: no cover
                                logger.debug(
                                    "checkpointer.request() failed: %s", e,
                                )
                        # Issue #135 — throttled scan_runs progress UPDATE:
                        # fire when EITHER 10 seconds have elapsed since the
                        # last write OR 100k records have accumulated. Old
                        # behaviour wrote every 5k records which still left
                        # the dashboard at 0 during the MFT enum phase
                        # (records weren't flushed yet) and over-wrote on
                        # tight scans. ``last_db_update_*`` trackers keep
                        # the cost off the per-record path.
                        now_db = time.time()
                        if (
                            (file_count - last_db_update_count)
                            >= DB_UPDATE_EVERY_RECORDS
                            or (now_db - last_db_update_ts)
                            >= DB_UPDATE_EVERY_SECONDS
                        ):
                            try:
                                self.db.update_scan_progress(
                                    scan_id, file_count, total_size,
                                )
                            except Exception as e:
                                # Never break a scan over a progress write.
                                logger.debug(
                                    "update_scan_progress failed: %s", e,
                                )
                            last_db_update_ts = now_db
                            last_db_update_count = file_count

                        # Issue #139 — partial summary scheduler. Same
                        # batch-boundary placement as the progress UPDATE
                        # above so the cost is amortised across batches.
                        # Fire-and-forget worker thread so a slow aggregate
                        # never stalls the insert loop. We only spawn one
                        # worker at a time — if the previous compute is
                        # still running we skip this trigger and try at
                        # the next checkpoint.
                        elapsed_since_partial = now_db - partial_last_compute_ts
                        rows_since_partial = file_count - partial_last_compute_count
                        partial_due = (
                            rows_since_partial >= PARTIAL_INTERVAL_RECORDS
                            or elapsed_since_partial >= partial_interval_slot[0]
                        )
                        if partial_due and (
                            partial_thread is None or not partial_thread.is_alive()
                        ):
                            partial_last_compute_ts = now_db
                            partial_last_compute_count = file_count
                            partial_thread = threading.Thread(
                                target=_run_partial_summary,
                                args=(scan_id,),
                                name=f"partial-summary-{scan_id}",
                                daemon=True,
                            )
                            partial_thread.start()

                        # Issue #131 — cancellation check at batch boundary.
                        # Setting cancel_event from /api/scan/{id}/stop
                        # causes us to break here; partial rows are
                        # already flushed via stager.append above so
                        # nothing is lost.
                        if self.cancel_event.is_set():
                            logger.info(
                                "Tarama iptal istegi alindi (scan_id=%d, %d dosya)",
                                scan_id, file_count,
                            )
                            status = "cancelled"
                            break

                    # Ilerleme guncelle (her 500 dosyada veya 2 saniyede bir)
                    now = time.time()
                    if file_count % 500 == 0 or (now - last_log_time) >= 2.0:
                        elapsed = now - start_time
                        fps = file_count / elapsed if elapsed > 0 else 0
                        progress["current_dir"] = os.path.dirname(file_path)
                        progress.update({
                            "file_count": file_count,
                            "total_size": total_size,
                            "total_size_formatted": format_size(total_size),
                            "errors": errors,
                            "elapsed": f"{elapsed:.0f}s",
                            "files_per_second": round(fps, 1),
                        })

                        if now - last_log_time >= 5.0:
                            logger.info(
                                "Taraniyor: %d dosya | %s | %s | %.0f dosya/sn",
                                file_count, format_size(total_size),
                                progress.get("current_dir", "")[-50:], fps
                            )
                            last_log_time = now

                except PermissionError:
                    errors += 1
                    logger.debug("Erisim reddedildi: %s", record.get("file_path"))
                except OSError as e:
                    errors += 1
                    logger.debug("Dosya hatasi: %s - %s", record.get("file_path"), e)

            # Kalan batch'i yaz + stager buffer'ini bosalt
            if batch:
                stager.append(batch)
                # Issue #181 — final batch needs to flow through v2 too.
                if v2_builder is not None:
                    try:
                        v2_builder.absorb_batch(batch)
                    except Exception as e:  # pragma: no cover
                        logger.debug(
                            "v2_builder.absorb_batch (final) failed: %s", e,
                        )
            try:
                stager.flush()
            except Exception as e:
                logger.warning("Stager final flush hatasi (kritik degil): %s", e)

            # Don't override "cancelled" set by the in-loop break above.
            if status != "cancelled":
                status = "completed"

        except Exception as e:
            status = "failed"
            errors += 1
            logger.error("Tarama basarisiz: %s", e)
            # Cancel/exception path: try to flush whatever we buffered so
            # the rows aren't lost in memory.
            try:
                if batch:
                    stager.append(batch)
                    if v2_builder is not None:
                        try:
                            v2_builder.absorb_batch(batch)
                        except Exception:  # pragma: no cover
                            pass
                stager.flush()
            except Exception as flush_err:
                logger.warning(
                    "Stager exception-path flush hatasi (kritik degil): %s",
                    flush_err,
                )

        # Tarama kaydini tamamla
        elapsed = time.time() - start_time
        self.db.complete_scan_run(scan_id, file_count, total_size, errors, status)
        self.db.update_source_last_scanned(source_id)

        fps = file_count / elapsed if elapsed > 0 else 0

        logger.info(
            "Tarama tamamlandi: %d dosya | %s | %.0f saniye | %.0f dosya/sn | %d hata",
            file_count, format_size(total_size), elapsed, fps, errors
        )

        # KPI summary + AI insights cache — Dashboard Overview + AI Onerileri
        # paneli bu JSON'lari okur, scanned_files tablosunu taramaz.
        # Issue #135 — entering the analysis phase. ``complete_scan_run``
        # above flipped scan_runs.status to ``completed``, so phase rows
        # filtered by ``status='running'`` now hit zero matches; we emit
        # the analysis label only via the in-memory progress dict (the
        # dashboard's /api/scan/progress endpoint reads this dict before
        # falling back to scan_runs).
        if status == "completed" and file_count > 0:
            progress["phase"] = "analysis"
            try:
                t0 = time.time()
                self.db.compute_scan_summary(scan_id)
                logger.info(
                    "Scan summary hesaplandi (scan_id=%d, %.1f sn)",
                    scan_id, time.time() - t0,
                )
            except Exception as e:
                logger.warning("Scan summary hesaplanamadi (scan_id=%d): %s", scan_id, e)

            try:
                from src.analyzer.ai_insights import InsightsEngine
                t0 = time.time()
                engine = InsightsEngine(self.db)
                insights_result = engine.generate_insights(source_id)
                self.db.save_scan_insights(scan_id, insights_result)
                logger.info(
                    "AI insights hesaplandi ve cache'lendi (scan_id=%d, %d insight, %.1f sn)",
                    scan_id, len(insights_result.get("insights", [])), time.time() - t0,
                )
            except Exception as e:
                logger.warning("AI insights hesaplanamadi (scan_id=%d): %s", scan_id, e)

            # Issue #144 Phase 1 — opt-in wrong-extension detection.
            # Default OFF (perf cost: libmagic.from_file() per scanned
            # file). When enabled, run as a separate post-scan phase so
            # the main MFT/SMB walk never blocks on libmagic.
            if self.config.get("detect_wrong_extensions", False):
                try:
                    ext_result = self._run_extension_check(scan_id)
                    # Issue #181 Track B1 — fold the anomaly count into
                    # the v2 builder so the dashboard's anomalies card
                    # lights up before the scan completes.
                    if v2_builder is not None and isinstance(ext_result, dict):
                        try:
                            v2_builder.increment_anomaly(
                                "extension",
                                int(ext_result.get("anomalies", 0) or 0),
                            )
                        except Exception:  # pragma: no cover
                            pass
                except Exception as e:
                    logger.warning(
                        "Extension-check pasi basarisiz (scan_id=%d): %s",
                        scan_id, e,
                    )

            # Issue #175 — post-walk size + timestamp enrich. Streams
            # scanned_files rows for this scan, fills file_size / mtime
            # via os.stat (or FSCTL on local NTFS). Gated by
            # scanner.enrich_sizes (default ON because the customer's #1
            # KPI was BOYUT showing 0 B).
            try:
                self._run_size_enrich(scan_id)
                # Issue #181 Track B1 — after enrich, size + age buckets
                # finally have data to populate. Flush the v2 dict with
                # the ``enrich`` scan_state so the dashboard knows it's
                # safe to render the BOYUT/YAS cards.
                if v2_builder is not None:
                    try:
                        elapsed_so_far = max(
                            0.001, time.time() - start_time,
                        )
                        v2_builder.flush_to_db(
                            scan_state="enrich",
                            rate_per_sec=float(file_count) / elapsed_so_far,
                            active_dir=progress.get("current_dir", "") or "",
                        )
                    except Exception as e:  # pragma: no cover
                        logger.debug(
                            "v2_builder enrich flush failed: %s", e,
                        )
            except Exception as e:
                logger.warning(
                    "Size-enrich pasi basarisiz (scan_id=%d): %s",
                    scan_id, e,
                )

        # Issue #181 Track B1 — final flush at scan completion. Always
        # runs (even on failure / cancel) so the dashboard's last
        # snapshot reflects the actual end state and scan_state moves
        # off ``db_writing`` regardless.
        if v2_builder is not None:
            try:
                final_state = "completed" if status == "completed" else "db_writing"
                v2_builder.increment_errors(int(errors))
                elapsed_final = max(0.001, time.time() - start_time)
                v2_builder.flush_to_db(
                    scan_state=final_state,
                    rate_per_sec=float(file_count) / elapsed_final,
                    active_dir=progress.get("current_dir", "") or "",
                )
            except Exception as e:  # pragma: no cover
                logger.debug(
                    "v2_builder final flush failed: %s", e,
                )

        # Son ilerleme durumunu guncelle
        # Issue #135 — phase artik ``completed`` (veya cancelled/failed). Bu
        # alan /api/scan/progress yanitinda kullanilir; in-memory progress
        # dict bittikten 30s sonra silinir, ondan once frontend "Tamamlandi"
        # state'ine gecer.
        final_phase = "completed" if status == "completed" else status
        progress.update({
            "status": status,
            "phase": final_phase,
            "file_count": file_count,
            "total_size": total_size,
            "total_size_formatted": format_size(total_size),
            "errors": errors,
            "elapsed": f"{elapsed:.0f}s",
            "files_per_second": round(fps, 1),
        })

        # Dosya adi uyumluluk raporu
        compat_report = name_analyzer.get_report()
        mit_report = mit_analyzer.get_report()

        result = {
            "scan_id": scan_id,
            "total_files": file_count,
            "total_size": total_size,
            "total_size_formatted": format_size(total_size),
            "errors": errors,
            "status": status,
            "elapsed": f"{elapsed:.0f}s",
            "files_per_second": round(fps, 1),
            "compatibility": compat_report,
            "mit_naming": mit_report,
        }

        # Ilerleme durumuna uyumluluk ozetini ekle
        progress["compatibility"] = {
            "health_score": compat_report["health_score"],
            "long_paths": compat_report["summary"]["long_paths"],
            "turkish_chars": compat_report["summary"]["turkish_chars"],
            "invalid_chars": compat_report["summary"]["invalid_chars"],
        }

        # Uyumluluk sorunlarini logla
        if compat_report["summary"]["invalid_chars"] > 0:
            logger.warning("Yasak karakter iceren dosya: %d", compat_report["summary"]["invalid_chars"])
        if compat_report["summary"]["long_paths"] > 0:
            logger.info("260+ karakter yol: %d (max: %d)", compat_report["summary"]["long_paths"], compat_report["max_path_length"])
        if compat_report["summary"]["turkish_chars"] > 0:
            logger.info("Turkce karakter iceren: %d", compat_report["summary"]["turkish_chars"])

        # Tarama basariliysa otomatik rapor uret
        if status == "completed" and file_count > 0:
            progress["status"] = "generating_report"
            result["report"] = self._generate_auto_report(source_id, source_name)
            progress["status"] = "completed"

        # Perceptual image hash phase (opt-in, issue #144 Phase 2).
        scanner_cfg = self._full_config.get("scanner", {}) or {}
        if status == "completed" and bool(scanner_cfg.get("compute_image_hashes", False)):
            try:
                progress["phase"] = "image_hashing"
                ih_stats = self._run_image_hashing(scan_id)
                result["image_hashing"] = ih_stats
                logger.info(
                    "image_hash phase tamamlandi: scan=%d hashed=%d skipped=%d errors=%d",
                    scan_id,
                    ih_stats.get("hashed", 0),
                    ih_stats.get("skipped", 0),
                    ih_stats.get("errors", 0),
                )
            except Exception as e:
                logger.warning("image_hash phase basarisiz (scan=%d): %s", scan_id, e)
            finally:
                progress["phase"] = "completed"

        return result

    def _run_extension_check(self, scan_id: int) -> dict:
        """Issue #144 Phase 1 — wrong-extension detection (Czkawka pattern).

        Iterates the freshly-inserted scanned_files for ``scan_id``,
        runs ``ExtensionChecker.check_file()`` on each path and bulk-
        inserts the mismatches into ``extension_anomalies``. Skips
        cleanly when libmagic isn't installed.

        Returns a small summary dict ({checked, anomalies, by_severity}).
        """
        from src.analyzer.extension_check import ExtensionChecker

        checker = ExtensionChecker()
        if not checker.available:
            logger.info(
                "Extension-check atlandi (libmagic yok). pip install -r "
                "requirements-accel.txt"
            )
            return {"checked": 0, "anomalies": 0, "skipped_no_libmagic": True}

        progress = _scan_progress.get(scan_id) or {}
        progress["phase"] = "extension_check"

        t0 = time.time()
        # Stream paths so we don't materialise the full file set in
        # memory on big shares.
        paths_iter = self.db.iter_scanned_paths(scan_id) \
            if hasattr(self.db, "iter_scanned_paths") \
            else self._iter_scan_paths(scan_id)

        anomalies = []
        checked = 0
        for path in paths_iter:
            checked += 1
            hit = checker.check_file(path)
            if hit is not None:
                anomalies.append(hit)
            # Flush in chunks of 500 so a multi-million file scan doesn't
            # hold all anomalies in memory.
            if len(anomalies) >= 500:
                self.db.insert_extension_anomalies(scan_id, anomalies)
                anomalies = []

        if anomalies:
            self.db.insert_extension_anomalies(scan_id, anomalies)

        elapsed = time.time() - t0
        # Aggregate by severity for the log line / summary.
        by_sev: dict[str, int] = {}
        try:
            counts = self.db.list_extension_anomalies(
                scan_id=scan_id, limit=10_000_000,
            )
            for r in counts:
                sev = r["severity"] or "?"
                by_sev[sev] = by_sev.get(sev, 0) + 1
        except Exception:
            pass

        total = sum(by_sev.values())
        logger.info(
            "Extension-check (scan_id=%d): %d dosya kontrol edildi, "
            "%d anomali (kritik=%d, yuksek=%d, orta=%d, dusuk=%d) %.1f sn",
            scan_id, checked, total,
            by_sev.get("critical", 0),
            by_sev.get("high", 0),
            by_sev.get("medium", 0),
            by_sev.get("low", 0),
            elapsed,
        )
        return {
            "scan_id": scan_id,
            "checked": checked,
            "anomalies": total,
            "by_severity": by_sev,
            "elapsed_seconds": round(elapsed, 2),
        }

    def _iter_scan_paths(self, scan_id: int):
        """Fallback iterator — stream file_path values for ``scan_id``.

        Used by ``_run_extension_check`` if Database doesn't expose a
        dedicated streaming helper.
        """
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT file_path FROM scanned_files WHERE scan_id = ?",
                (scan_id,),
            )
            for row in cur.fetchall():
                yield row["file_path"]

    def _run_size_enrich(self, scan_id: int) -> dict:
        """Issue #175 — post-walk size + timestamp enrich pass.

        Iterates ``scanned_files`` rows for ``scan_id`` whose
        ``file_size = 0`` (the MFT backend signature) and runs
        :meth:`SizeEnricher.enrich` over them. Default ON because the
        customer's BOYUT KPI was always ``0 B``; flip
        ``scanner.enrich_sizes: false`` in config to skip.

        Mirrors :meth:`_run_extension_check` in shape: callback wiring
        through ``self.progress_callback``, a single phase log line,
        chunked DB writes through the retry-protected helper.

        Returns ``{scan_id, enriched, skipped, elapsed_seconds, skipped_disabled}``.
        """
        if not self.config.get("enrich_sizes", True):
            # Silent skip — operators who consciously disabled this
            # don't need a log line on every scan.
            return {
                "scan_id": scan_id,
                "enriched": 0,
                "skipped": 0,
                "elapsed_seconds": 0.0,
                "skipped_disabled": True,
            }

        from src.scanner.size_enricher import SizeEnricher

        enricher = SizeEnricher(self._full_config, self.db)
        if not enricher.available:
            logger.info(
                "Size-enrich atlandi (os.stat erisilebilir degil) scan=%d",
                scan_id,
            )
            return {
                "scan_id": scan_id,
                "enriched": 0,
                "skipped": 0,
                "elapsed_seconds": 0.0,
                "skipped_unavailable": True,
            }

        progress = _scan_progress.get(scan_id) or {}
        progress["phase"] = "size_enrich"

        t0 = time.time()

        def _paths_iter():
            # Stream so we don't materialise millions of paths for the
            # 3M-file customer scan. ``scan_id`` is composite-indexed
            # via idx_sf_scan; the additional ``file_size = 0`` filter
            # means we only stat rows the MFT backend left empty.
            with self.db.get_cursor() as cur:
                cur.execute(
                    "SELECT file_path FROM scanned_files "
                    "WHERE scan_id = ? AND file_size = 0",
                    (scan_id,),
                )
                for r in cur.fetchall():
                    yield r["file_path"]

        # Forward the ops banner callback so the dashboard's scan
        # progress card lights up during this phase too.
        progress_cb = self.progress_callback

        try:
            enriched = enricher.enrich(
                scan_id=scan_id,
                source_id=getattr(self, "_current_source_id", 0),
                paths_iter=_paths_iter(),
                progress_cb=progress_cb,
            )
        except Exception as e:
            elapsed = time.time() - t0
            logger.warning(
                "Size-enrich basarisiz scan=%d %.1f sn: %s",
                scan_id, elapsed, e,
            )
            return {
                "scan_id": scan_id,
                "enriched": 0,
                "skipped": 0,
                "elapsed_seconds": round(elapsed, 2),
                "error": str(e),
            }

        skipped = int(getattr(enricher, "last_skipped", 0) or 0)
        elapsed = time.time() - t0
        logger.info(
            "Size-enrich (scan_id=%d): %d satir zenginlestirildi, "
            "%d atlandi, %.1f sn",
            scan_id, enriched, skipped, elapsed,
        )
        return {
            "scan_id": scan_id,
            "enriched": enriched,
            "skipped": skipped,
            "elapsed_seconds": round(elapsed, 2),
        }

    def _generate_auto_report(self, source_id: int, source_name: str) -> dict:
        """Tarama sonrasi otomatik rapor uret ve kaydet."""
        try:
            from src.analyzer.report_generator import ReportGenerator
            from src.analyzer.report_exporter import ReportExporter

            gen = ReportGenerator(self.db, self.config)
            full_config = self.config if isinstance(self.config, dict) and "reports" in self.config else {}
            exporter = ReportExporter(full_config)

            data = gen.generate_full_report(source_id)
            if "error" in data:
                logger.warning("Otomatik rapor olusturulamadi: %s", data["error"])
                return {"generated": False, "error": data["error"]}

            paths = exporter.export_full_report(data, source_name)
            logger.info("Otomatik rapor kaydedildi: %s", paths.get("html_path", "?"))

            self._print_scan_summary(data)

            return {"generated": True, **paths}

        except Exception as e:
            logger.warning("Otomatik rapor hatasi: %s", e)
            return {"generated": False, "error": str(e)}

    def _run_image_hashing(self, scan_id: int) -> dict:
        """Perceptual-hash post-scan phase (issue #144 Phase 2).

        Streams image files from the completed scan, computes pHash /
        dHash / aHash via :class:`~src.analyzer.image_hash.ImageHasher`,
        and upserts rows to ``image_hashes`` in 500-row chunks.

        Image extensions eligible: jpg, jpeg, png, gif, bmp, tiff,
        tif, webp.

        Returns a summary dict::

            {
                "scan_id": <int>,
                "hashed": <int>,
                "skipped": <int>,   # too large / unreadable
                "errors": <int>,    # hasher returned None for other reasons
                "unavailable": <bool>,  # imagehash not installed
            }
        """
        from src.analyzer.image_hash import ImageHasher, IMAGE_EXTENSIONS

        hasher = ImageHasher(self._full_config)
        if not hasher.available:
            return {
                "scan_id": scan_id,
                "hashed": 0,
                "skipped": 0,
                "errors": 0,
                "unavailable": True,
            }

        hashed = 0
        skipped = 0
        errors = 0
        chunk: list[dict] = []
        CHUNK_SIZE = 500

        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT id AS file_id, file_path, extension, file_size "
                "FROM scanned_files "
                "WHERE scan_id = ? AND extension IN ({}) "
                "ORDER BY file_path".format(
                    ",".join("?" * len(IMAGE_EXTENSIONS))
                ),
                (scan_id, *IMAGE_EXTENSIONS),
            )
            rows = cur.fetchall()

        for row in rows:
            file_id = row["file_id"]
            file_path = row["file_path"]

            result = hasher.compute(file_path)
            if result is None:
                # Either too large or unreadable.
                skipped += 1
                continue

            chunk.append(
                {
                    "file_id": file_id,
                    "scan_id": scan_id,
                    "phash": result.get("phash"),
                    "dhash": result.get("dhash"),
                    "ahash": result.get("ahash"),
                }
            )
            hashed += 1

            if len(chunk) >= CHUNK_SIZE:
                try:
                    self.db.insert_image_hashes(chunk)
                except Exception as e:
                    logger.warning("image_hash DB flush hatasi: %s", e)
                    errors += len(chunk)
                    hashed -= len(chunk)
                chunk = []

        # Flush remaining
        if chunk:
            try:
                self.db.insert_image_hashes(chunk)
            except Exception as e:
                logger.warning("image_hash DB final flush hatasi: %s", e)
                errors += len(chunk)
                hashed -= len(chunk)

        return {
            "scan_id": scan_id,
            "hashed": hashed,
            "skipped": skipped,
            "errors": errors,
            "unavailable": False,
        }

    def _print_scan_summary(self, data: dict):
        """Tarama sonrasi konsola ozet rapor yazdir."""
        summary = data.get("summary", {})
        frequency = data.get("frequency", [])
        types = data.get("types", [])
        source = data.get("source", {})

        print(f"\n{'=' * 60}")
        print(f"  TARAMA RAPORU: {source.get('name', '?')}")
        print(f"{'=' * 60}")
        print(f"  Toplam Dosya:  {summary.get('total_files', 0):,}")
        print(f"  Toplam Boyut:  {summary.get('total_size_formatted', '-')}")
        print(f"  Uzanti Sayisi: {summary.get('type_count', 0)}")
        print(f"  En Eski:       {(summary.get('oldest_file') or '-')[:10]}")
        print(f"  En Yeni:       {(summary.get('newest_file') or '-')[:10]}")

        if frequency:
            print(f"\n  {'-' * 50}")
            print(f"  Erisim Sikligi:")
            for f in frequency:
                print(f"    {f['label']:<30} {f['file_count']:>8,} dosya  {f['total_size_formatted']:>10}")

        if types:
            print(f"\n  {'-' * 50}")
            print(f"  En Buyuk 10 Dosya Turu:")
            for t_item in types[:10]:
                print(f"    .{t_item['extension']:<10} {t_item['file_count']:>8,} dosya  {t_item['total_size_formatted']:>10}")

        # Arsivleme onerisi
        for f in frequency:
            if f.get("days", 0) >= 365:
                total = summary.get("total_files", 1)
                pct = f["file_count"] / total * 100 if total else 0
                print(f"\n  {'-' * 50}")
                print(f"  ARSIVLEME ONERISI:")
                print(f"    365+ gun erisilemyen {f['file_count']:,} dosya ({f['total_size_formatted']})")
                print(f"    Arsivlenerek %{pct:.1f} alan kazanilabilir.")
                break

        print(f"{'=' * 60}\n")

    def _recursive_scandir(self, path: str, progress: dict = None):
        """Recursive os.scandir() jeneratoru - performansli dizin gezme.
        Uzun yollar (260+ karakter) otomatik desteklenir."""
        scan_path = _long_path(path) if len(path) >= 240 else path
        try:
            with os.scandir(scan_path) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if progress:
                                # Sadece ust dizin adini goster (cok uzun olmasin)
                                try:
                                    progress["current_dir"] = entry.path
                                except Exception:
                                    pass
                            yield from self._recursive_scandir(entry.path, progress)
                        else:
                            yield entry
                    except PermissionError:
                        logger.debug("Dizin erisim reddedildi: %s", entry.path)
                    except OSError as e:
                        logger.debug("Dizin hatasi: %s - %s", entry.path, e)
        except PermissionError:
            logger.debug("Dizin erisim reddedildi: %s", path)
        except OSError as e:
            logger.debug("Dizin hatasi: %s - %s", path, e)

    def _should_skip(self, entry) -> bool:
        """Dosyanin atlanip atlanmayacagini kontrol et."""
        name = entry.name

        # Exclude patterns
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(name, pattern):
                return True

        # Hidden/System kontrol
        if self.skip_hidden or self.skip_system:
            try:
                # Performans: sadece stat() ile attributes kontrol et
                if os.name == 'nt':
                    import stat
                    st = entry.stat(follow_symlinks=False)
                    attrs = st.st_file_attributes if hasattr(st, 'st_file_attributes') else 0
                    if self.skip_hidden and (attrs & 2):  # FILE_ATTRIBUTE_HIDDEN
                        return True
                    if self.skip_system and (attrs & 4):  # FILE_ATTRIBUTE_SYSTEM
                        return True
                else:
                    if self.skip_hidden and name.startswith('.'):
                        return True
            except Exception:
                pass

        return False
