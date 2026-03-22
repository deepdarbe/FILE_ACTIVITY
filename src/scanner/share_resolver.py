"""Yol dogrulama ve baglanti kontrolu modulu.

Hem UNC yollarini (\\\\server\\share) hem lokal yollari (E:\\Paylasim) destekler.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger("file_activity.scanner.share_resolver")

try:
    import win32net
    import win32netcon
    HAS_WIN32NET = True
except ImportError:
    HAS_WIN32NET = False


def is_unc_path(path: str) -> bool:
    """UNC yolu mu kontrol et."""
    return path.startswith("\\\\") and len(path.split("\\")) >= 4


def is_local_path(path: str) -> bool:
    """Lokal yol mu kontrol et (C:\\, D:\\, E:\\ vb.)."""
    if len(path) >= 2 and path[1] == ":":
        drive = path[0].upper()
        return "A" <= drive <= "Z"
    return False


def validate_path(path: str) -> bool:
    """Yolun gecerli formatini kontrol et (UNC veya lokal)."""
    return is_unc_path(path) or is_local_path(path)


def test_connectivity(path: str) -> tuple[bool, str]:
    """Yola erisimi test et.

    UNC ve lokal yollari destekler.

    Returns:
        (erisilebilir_mi, mesaj)
    """
    if not validate_path(path):
        return False, f"Gecersiz yol: {path} (UNC: \\\\server\\share veya Lokal: D:\\klasor)"

    try:
        if os.path.exists(path):
            if os.path.isdir(path):
                # Dizin icerigini okumaya calis
                try:
                    next(os.scandir(path), None)
                    return True, f"Erisilebilir: {path}"
                except PermissionError:
                    return False, f"Erisim reddedildi (okuma izni yok): {path}"
            else:
                return False, f"Yol bir dizin degil: {path}"
        return False, f"Yol bulunamadi: {path}"
    except PermissionError:
        return False, f"Erisim reddedildi: {path}"
    except OSError as e:
        return False, f"Baglanti hatasi: {path} - {e}"


def list_shares(server: str) -> list[dict]:
    """Sunucudaki paylasimlari listele (win32net gerekli)."""
    if not HAS_WIN32NET:
        logger.warning("win32net bulunamadi, paylasim listeleme devre disi")
        return []

    try:
        if not server.startswith("\\\\"):
            server = f"\\\\{server}"

        shares, _, _ = win32net.NetShareEnum(server, 1)
        result = []
        for share in shares:
            stype = share.get("type", 0)
            if stype == 0:
                result.append({
                    "name": share["netname"],
                    "path": f"{server}\\{share['netname']}",
                    "remark": share.get("remark", ""),
                })
        return result
    except Exception as e:
        logger.error("Paylasim listeleme hatasi %s: %s", server, e)
        return []


def get_relative_path(file_path: str, source_root: str) -> str:
    """Kaynak kokune gore goreceli yolu hesapla."""
    file_path = os.path.normpath(file_path)
    source_root = os.path.normpath(source_root)

    if file_path.startswith(source_root):
        rel = file_path[len(source_root):]
        return rel.lstrip(os.sep)
    return file_path
