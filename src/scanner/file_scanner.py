"""Ana dosya tarama modulu.

Recursive olarak dizin tarar, dosya bilgilerini toplar ve veritabanina yazar.
os.scandir() ile performansli calisir, batch insert yapar.
Hem UNC hem lokal yollari destekler.
Tarama sirasinda ilerleme bilgisi loglar.
"""

import os
import re
import time
import fnmatch
import logging
import unicodedata
from datetime import datetime

from src.scanner.win_attributes import (
    get_file_times, is_hidden, is_system, check_ntfs_last_access_enabled, _long_path
)
from src.scanner.share_resolver import get_relative_path, test_connectivity
from src.storage.database import Database
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


def get_scan_progress(source_id: int = None) -> dict:
    """Tarama ilerleme durumunu dondur (dashboard icin)."""
    if source_id and source_id in _scan_progress:
        return _scan_progress[source_id]
    return _scan_progress


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

        # Ilerleme durumu baslat
        progress = {
            "source_id": source_id,
            "source_name": source_name,
            "status": "connecting",
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

        try:
            for entry in self._recursive_scandir(path, progress):
                if not entry.is_file(follow_symlinks=False):
                    continue

                if self._should_skip(entry):
                    continue

                # Resume: skip already scanned files
                if scanned_paths and entry.path in scanned_paths:
                    continue

                try:
                    times = get_file_times(entry.path, read_owner=self.read_owner)

                    # get_file_times bos donerse entry.stat() ile boyut al
                    if times.file_size == 0 and times.creation_time is None:
                        try:
                            st = entry.stat(follow_symlinks=False)
                            times.file_size = st.st_size
                            times.creation_time = datetime.fromtimestamp(st.st_ctime).strftime("%Y-%m-%d %H:%M:%S")
                            times.last_access_time = datetime.fromtimestamp(st.st_atime).strftime("%Y-%m-%d %H:%M:%S")
                            times.last_modify_time = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            pass  # En azindan dosya adi kaydedilsin

                    rel_path = get_relative_path(entry.path, path)
                    ext = os.path.splitext(entry.name)[1].lower().lstrip(".")
                    if not ext:
                        ext = None

                    row = {
                        "source_id": source_id,
                        "scan_id": scan_id,
                        "file_path": entry.path,
                        "relative_path": rel_path,
                        "file_name": entry.name,
                        "extension": ext,
                        "file_size": times.file_size,
                        "creation_time": times.creation_time,
                        "last_access_time": times.last_access_time,
                        "last_modify_time": times.last_modify_time,
                        "owner": times.owner,
                        "attributes": times.win32_attributes,
                    }

                    batch.append(row)
                    file_count += 1
                    total_size += times.file_size

                    # Dosya adi uyumluluk analizi
                    name_analyzer.analyze(entry.path, entry.name)
                    mit_analyzer.analyze(entry.path, entry.name)

                    # Batch insert
                    if len(batch) >= self.batch_size:
                        self.db.bulk_insert_scanned_files(batch)
                        batch = []
                        # scan_runs'i guncelle (dashboard aninda gorsun)
                        if file_count % 5000 == 0:
                            self.db.update_scan_progress(scan_id, file_count, total_size)

                    # Ilerleme guncelle (her 500 dosyada veya 2 saniyede bir)
                    now = time.time()
                    if file_count % 500 == 0 or (now - last_log_time) >= 2.0:
                        elapsed = now - start_time
                        fps = file_count / elapsed if elapsed > 0 else 0
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
                    logger.debug("Erisim reddedildi: %s", entry.path)
                except OSError as e:
                    errors += 1
                    logger.debug("Dosya hatasi: %s - %s", entry.path, e)

            # Kalan batch'i yaz
            if batch:
                self.db.bulk_insert_scanned_files(batch)

            status = "completed"

        except Exception as e:
            status = "failed"
            errors += 1
            logger.error("Tarama basarisiz: %s", e)

        # Tarama kaydini tamamla
        elapsed = time.time() - start_time
        self.db.complete_scan_run(scan_id, file_count, total_size, errors, status)
        self.db.update_source_last_scanned(source_id)

        fps = file_count / elapsed if elapsed > 0 else 0

        logger.info(
            "Tarama tamamlandi: %d dosya | %s | %.0f saniye | %.0f dosya/sn | %d hata",
            file_count, format_size(total_size), elapsed, fps, errors
        )

        # Son ilerleme durumunu guncelle
        progress.update({
            "status": status,
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

        return result

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
