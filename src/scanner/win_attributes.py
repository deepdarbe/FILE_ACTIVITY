"""Windows dosya özelliklerini pywin32 ile okuma modülü.

Win32 API kullanarak dosya zamanları, boyut ve özelliklerini alır.
NtfsDisableLastAccessUpdate kontrolü yapar.
"""

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("file_activity.scanner.win_attributes")

try:
    import win32file
    import win32api
    import win32security
    import win32con
    import pywintypes
    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False
    logger.warning("pywin32 bulunamadı, os.stat() fallback kullanılacak")


@dataclass
class FileTimesInfo:
    """Dosya zaman ve boyut bilgileri."""
    creation_time: Optional[str] = None       # "YYYY-MM-DD HH:MM:SS"
    last_access_time: Optional[str] = None    # "YYYY-MM-DD HH:MM:SS"
    last_modify_time: Optional[str] = None    # "YYYY-MM-DD HH:MM:SS"
    file_size: int = 0
    win32_attributes: int = 0
    owner: Optional[str] = None


def _pywintypes_to_datetime(pytime) -> Optional[str]:
    """pywintypes.datetime'i SQLite uyumlu ISO string'e cevir (naive, no timezone).

    SQLite datetime fonksiyonlari timezone-aware string'lerle tutarsiz calisir.
    Bu yuzden timezone bilgisi kaldirilir ve 'YYYY-MM-DD HH:MM:SS' formatinda dondurulur.
    """
    if pytime is None:
        return None
    try:
        # pywintypes.datetime zaten datetime-uyumlu
        dt = datetime(
            pytime.year, pytime.month, pytime.day,
            pytime.hour, pytime.minute, pytime.second,
        )
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError, OverflowError):
        return None


def check_ntfs_last_access_enabled() -> bool:
    """NtfsDisableLastAccessUpdate registry değerini kontrol et.

    Returns:
        True: last access time güncellemesi aktif (güvenilir)
        False: devre dışı (güvenilmez)
    """
    try:
        import winreg
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\FileSystem"
        )
        value, _ = winreg.QueryValueEx(key, "NtfsDisableLastAccessUpdate")
        winreg.CloseKey(key)
        # 0 = aktif (eski davranış), 1 = devre dışı, 0x80000003 = system managed (genelde devre dışı)
        return value == 0
    except (OSError, ImportError):
        return False


def _long_path(path: str) -> str:
    """Windows uzun yol destegi (260+ karakter).

    Windows'ta 260 karakterden uzun yollar icin \\\\?\\ prefix'i gerekir.
    UNC yollar icin \\\\?\\UNC\\server\\share formatina donusturulur.
    """
    if path.startswith("\\\\?\\"):
        return path  # Zaten long path
    if len(path) < 240:
        return path  # Kisa yol, prefix gereksiz
    if path.startswith("\\\\"):
        # UNC: \\server\share -> \\?\UNC\server\share
        return "\\\\?\\UNC\\" + path[2:]
    # Lokal: C:\folder -> \\?\C:\folder
    return "\\\\?\\" + path


def get_file_times(path: str, read_owner: bool = False) -> FileTimesInfo:
    """Dosya zaman ve boyut bilgilerini al.

    Win32 API varsa GetFileAttributesEx kullanir (en hizli).
    Yoksa os.stat() fallback.
    Uzun yollar (260+ karakter) otomatik desteklenir.
    """
    lp = _long_path(path)
    if HAS_WIN32:
        return _get_times_win32(lp, read_owner)
    return _get_times_stat(lp)


def _get_times_win32(path: str, read_owner: bool = False) -> FileTimesInfo:
    """Win32 API ile dosya bilgilerini al."""
    try:
        # GetFileAttributesEx: (attributes, creation_time, access_time, write_time, file_size)
        attrs = win32file.GetFileAttributesEx(path)

        info = FileTimesInfo(
            creation_time=_pywintypes_to_datetime(attrs[1]),
            last_access_time=_pywintypes_to_datetime(attrs[2]),
            last_modify_time=_pywintypes_to_datetime(attrs[3]),
            file_size=attrs[4],
            win32_attributes=attrs[0],
        )

        if read_owner:
            info.owner = _get_file_owner(path)

        return info

    except pywintypes.error as e:
        logger.debug("Win32 API hatasi %s: %s, fallback kullaniliyor", path[:100], e)
        return _get_times_stat(path)


def _get_times_stat(path: str) -> FileTimesInfo:
    """os.stat() ile fallback bilgi toplama."""
    try:
        st = os.stat(path)
        return FileTimesInfo(
            creation_time=datetime.fromtimestamp(st.st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
            last_access_time=datetime.fromtimestamp(st.st_atime).strftime("%Y-%m-%d %H:%M:%S"),
            last_modify_time=datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            file_size=st.st_size,
            win32_attributes=0,
        )
    except OSError as e:
        # Uzun yol hatasi — sadece debug seviyesinde logla (cok fazla dosya olabilir)
        logger.debug("os.stat() hatasi: %s", path[:100])
        return FileTimesInfo()


def _get_file_owner(path: str) -> Optional[str]:
    """Dosya sahibini al (yavaş - isteğe bağlı)."""
    if not HAS_WIN32:
        return None
    try:
        sd = win32security.GetFileSecurity(
            path, win32security.OWNER_SECURITY_INFORMATION
        )
        owner_sid = sd.GetSecurityDescriptorOwner()
        name, domain, _ = win32security.LookupAccountSid(None, owner_sid)
        return f"{domain}\\{name}"
    except Exception:
        return None


def is_hidden(attributes: int) -> bool:
    """Dosya Hidden attribute'a sahip mi?"""
    if HAS_WIN32:
        return bool(attributes & win32con.FILE_ATTRIBUTE_HIDDEN)
    return False


def is_system(attributes: int) -> bool:
    """Dosya System attribute'a sahip mi?"""
    if HAS_WIN32:
        return bool(attributes & win32con.FILE_ATTRIBUTE_SYSTEM)
    return False
