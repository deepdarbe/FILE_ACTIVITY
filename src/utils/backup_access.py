"""Backup-semantics file access — read files/dirs whose NTFS ACL denies the
service account, by leveraging ``SeBackupPrivilege`` (held by both
``BURCU\\itwise`` and ``LocalSystem``). This is how backup/archive tools
(Acronis, Netwrix) traverse a file server regardless of per-folder ACLs, WITHOUT
modifying the customer's permissions.

Design rule — **normal-first, backup-only-on-denial**: every helper does the
ordinary ``os`` call first and only falls back to a
``FILE_FLAG_BACKUP_SEMANTICS`` handle when normal access is denied. So behaviour
is byte-for-byte identical for accessible paths (nothing existing breaks) and
only ACL-denied paths take the privileged path. Degrades to plain ``os`` on
non-Windows, when pywin32 is missing, or when the privilege can't be enabled.

Enable once per process via :func:`enable_backup_privilege` (best-effort); until
then every helper is exactly ``os`` behaviour.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger("file_activity.backup_access")

try:  # pragma: no cover - Windows-only
    import win32api
    import win32con
    import win32file
    import win32security
    _HAS_WIN32 = True
except Exception:  # ImportError on non-Windows / missing pywin32
    _HAS_WIN32 = False

# FILE_FLAG_BACKUP_SEMANTICS (also opens directory handles); FILE_ATTRIBUTE_DIRECTORY.
_FLAG_BACKUP = 0x02000000
_ATTR_DIRECTORY = 0x10

# Set True by enable_backup_privilege() once SeBackupPrivilege is enabled.
_BACKUP_READY = False


def available() -> bool:
    """True if backup-semantics reads are active (privilege enabled)."""
    return _BACKUP_READY


def enable_backup_privilege() -> bool:
    """Enable ``SeBackupPrivilege`` (+ ``SeRestorePrivilege``) in this process
    token. Idempotent, best-effort — safe to call at every startup. Returns True
    if backup-semantics reads are available afterwards. No-op (returns False) on
    non-Windows / missing pywin32 / an account that lacks the privilege.
    """
    global _BACKUP_READY
    if not _HAS_WIN32:
        return False
    try:
        th = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32security.TOKEN_ADJUST_PRIVILEGES | win32security.TOKEN_QUERY)
        privs = []
        for name in ("SeBackupPrivilege", "SeRestorePrivilege"):
            try:
                luid = win32security.LookupPrivilegeValue(None, name)
                privs.append((luid, win32security.SE_PRIVILEGE_ENABLED))
            except Exception:  # privilege name not resolvable
                pass
        if privs:
            win32security.AdjustTokenPrivileges(th, False, privs)
        # Prove the read path actually works: a backup handle on a dir we can
        # already reach must open. If it doesn't, keep normal-only behaviour.
        h = win32file.CreateFile(
            os.environ.get("SystemRoot", r"C:\Windows"), win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE
            | win32con.FILE_SHARE_DELETE,
            None, win32con.OPEN_EXISTING, _FLAG_BACKUP, None)
        h.Close()
        _BACKUP_READY = True
        logger.info("backup-semantics access ENABLED (SeBackupPrivilege) — "
                    "ACL-restricted folders are readable")
    except Exception as e:  # privilege not held / AdjustToken denied
        _BACKUP_READY = False
        logger.info("backup-semantics access unavailable (%s) — using normal "
                    "file access", e)
    return _BACKUP_READY


def _backup_handle(path: str):
    """CreateFile handle with FILE_FLAG_BACKUP_SEMANTICS (files AND dirs)."""
    return win32file.CreateFile(
        path, win32con.GENERIC_READ,
        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE
        | win32con.FILE_SHARE_DELETE,
        None, win32con.OPEN_EXISTING, _FLAG_BACKUP, None)


def _backup_attrs(path: str):
    """dwFileAttributes for ``path`` via a backup handle, or None if it can't
    be opened (genuinely missing / not even backup-openable)."""
    if not _BACKUP_READY:
        return None
    try:
        h = _backup_handle(path)
        try:
            return win32file.GetFileInformationByHandle(h)[0]
        finally:
            h.Close()
    except Exception:
        return None


def exists(path: str) -> bool:
    """``os.path.exists`` with a backup-semantics fallback on access-denied."""
    if os.path.exists(path):
        return True
    return _backup_attrs(path) is not None


def is_dir(path: str) -> bool:
    """``os.path.isdir`` with a backup-semantics fallback on access-denied."""
    if os.path.isdir(path):
        return True
    attrs = _backup_attrs(path)
    return attrs is not None and bool(attrs & _ATTR_DIRECTORY)


def is_file(path: str) -> bool:
    """``os.path.isfile`` with a backup-semantics fallback on access-denied."""
    if os.path.isfile(path):
        return True
    attrs = _backup_attrs(path)
    return attrs is not None and not (attrs & _ATTR_DIRECTORY)


def open_read(path: str):
    """Open ``path`` for binary read. Falls back to a backup-semantics handle
    only on PermissionError; a genuinely missing file still raises
    FileNotFoundError. Returns a normal binary file object (usable in a
    ``with`` block); nothing to special-case at the call site.
    """
    try:
        return open(path, "rb")
    except PermissionError:
        if not _BACKUP_READY:
            raise
    import msvcrt
    h = _backup_handle(path)
    # Hand the raw OS handle to an fd Python owns; closing the file closes it.
    fd = msvcrt.open_osfhandle(h.Detach(), os.O_RDONLY)
    return os.fdopen(fd, "rb")
