"""FindFirstFileExW + LARGE_FETCH ctypes scanner backend.

This backend walks a directory tree using the raw Win32
``FindFirstFileExW`` / ``FindNextFileW`` / ``FindClose`` family from
``kernel32.dll`` with the ``FindExInfoBasic`` info level (skips the
8.3 alternate-name lookup) and the ``FIND_FIRST_EX_LARGE_FETCH`` flag
(requests 64 KB result buffers on Windows 7+).

Compared to :func:`os.walk` / :func:`os.scandir`, which under the hood
call ``FindFirstFileW`` with default flags, this typically gives a
2-3x speedup on large local NTFS trees because it avoids the short-name
probe and reduces the number of kernel round trips.

The module is import-safe on non-Windows platforms: ctypes bindings are
loaded lazily inside :meth:`Win32FindExBackend.__init__`. Instantiating
the class on a non-Windows host raises ``RuntimeError``.

References
----------
- https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-findfirstfileexw
- https://learn.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-win32_find_dataw
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Iterator, Optional

logger = logging.getLogger("file_activity.scanner.backends.win32_find_ex")


# ----------------------------------------------------------------------
# Win32 constants
# ----------------------------------------------------------------------

# FINDEX_INFO_LEVELS
FindExInfoStandard = 0
FindExInfoBasic = 1  # Skip populating cAlternateFileName (8.3 short name)

# FINDEX_SEARCH_OPS
FindExSearchNameMatch = 0

# dwAdditionalFlags
FIND_FIRST_EX_LARGE_FETCH = 2  # 64 KB result buffers (Windows 7+)

# Return value meaning "no handle"
INVALID_HANDLE_VALUE = -1

# File attribute bits we care about
FILE_ATTRIBUTE_DIRECTORY = 0x10
FILE_ATTRIBUTE_REPARSE_POINT = 0x400

# Win32 error codes
ERROR_FILE_NOT_FOUND = 2
ERROR_PATH_NOT_FOUND = 3
ERROR_ACCESS_DENIED = 5
ERROR_NO_MORE_FILES = 18
ERROR_INVALID_PARAMETER = 87

# FILETIME (100ns intervals since 1601-01-01) → Unix epoch offset (seconds)
_FILETIME_EPOCH_OFFSET = 11644473600
_FILETIME_TICKS_PER_SEC = 10_000_000

# Long-path threshold: Windows legacy MAX_PATH is 260, but we switch to the
# \\?\ prefix a bit earlier to stay safe when joining sub-paths during recursion.
_LONG_PATH_THRESHOLD = 240


# ----------------------------------------------------------------------
# ctypes structure / prototype builders (Windows-only)
# ----------------------------------------------------------------------


def _build_ctypes_bindings():
    """Build and return ctypes bindings, structures, and kernel32 handle.

    Imported lazily so this module stays import-safe on non-Windows hosts.
    Returns a dict of symbols used by the backend.
    """
    import ctypes
    from ctypes import wintypes

    class FILETIME(ctypes.Structure):
        _fields_ = [
            ("dwLowDateTime", wintypes.DWORD),
            ("dwHighDateTime", wintypes.DWORD),
        ]

    class WIN32_FIND_DATAW(ctypes.Structure):
        _fields_ = [
            ("dwFileAttributes", wintypes.DWORD),
            ("ftCreationTime", FILETIME),
            ("ftLastAccessTime", FILETIME),
            ("ftLastWriteTime", FILETIME),
            ("nFileSizeHigh", wintypes.DWORD),
            ("nFileSizeLow", wintypes.DWORD),
            ("dwReserved0", wintypes.DWORD),
            ("dwReserved1", wintypes.DWORD),
            ("cFileName", wintypes.WCHAR * 260),
            ("cAlternateFileName", wintypes.WCHAR * 14),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    # HANDLE FindFirstFileExW(
    #   LPCWSTR lpFileName,
    #   FINDEX_INFO_LEVELS fInfoLevelId,
    #   LPVOID lpFindFileData,
    #   FINDEX_SEARCH_OPS fSearchOp,
    #   LPVOID lpSearchFilter,     // must be NULL
    #   DWORD dwAdditionalFlags);
    FindFirstFileExW = kernel32.FindFirstFileExW
    FindFirstFileExW.argtypes = [
        wintypes.LPCWSTR,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    FindFirstFileExW.restype = wintypes.HANDLE

    # BOOL FindNextFileW(HANDLE, LPWIN32_FIND_DATAW);
    FindNextFileW = kernel32.FindNextFileW
    FindNextFileW.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
    FindNextFileW.restype = wintypes.BOOL

    # BOOL FindClose(HANDLE);
    FindClose = kernel32.FindClose
    FindClose.argtypes = [wintypes.HANDLE]
    FindClose.restype = wintypes.BOOL

    return {
        "ctypes": ctypes,
        "FILETIME": FILETIME,
        "WIN32_FIND_DATAW": WIN32_FIND_DATAW,
        "FindFirstFileExW": FindFirstFileExW,
        "FindNextFileW": FindNextFileW,
        "FindClose": FindClose,
        "INVALID_HANDLE_VALUE": ctypes.c_void_p(-1).value,
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _filetime_to_str(high: int, low: int) -> Optional[str]:
    """Convert a FILETIME (high, low DWORDs) to ``"YYYY-MM-DD HH:MM:SS"``.

    Returns ``None`` if the FILETIME is zero or out of range for Python's
    datetime (e.g. corrupted or 1601 epoch placeholders).
    """
    ticks = (high << 32) | low
    if ticks == 0:
        return None
    try:
        seconds = ticks / _FILETIME_TICKS_PER_SEC - _FILETIME_EPOCH_OFFSET
        return datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return None


def _long_path(path: str) -> str:
    """Prepend ``\\\\?\\`` when the path is close to the legacy MAX_PATH limit.

    UNC paths become ``\\\\?\\UNC\\server\\share\\...``, local paths become
    ``\\\\?\\C:\\...``.
    """
    if path.startswith("\\\\?\\"):
        return path
    if len(path) < _LONG_PATH_THRESHOLD:
        return path
    if path.startswith("\\\\"):
        return "\\\\?\\UNC\\" + path[2:]
    return "\\\\?\\" + path


def _strip_long_prefix(path: str) -> str:
    """Reverse of :func:`_long_path` for yielded ``file_path`` fields."""
    if path.startswith("\\\\?\\UNC\\"):
        return "\\\\" + path[8:]
    if path.startswith("\\\\?\\"):
        return path[4:]
    return path


# ----------------------------------------------------------------------
# Backend
# ----------------------------------------------------------------------


class Win32FindExBackend:
    """Scanner backend that walks a tree via ``FindFirstFileExW``.

    Parameters
    ----------
    config:
        Project configuration dictionary. Currently unused — accepted for
        protocol compatibility with other backends.
    """

    def __init__(self, config: dict) -> None:
        if sys.platform != "win32":
            raise RuntimeError("Win32FindExBackend requires Windows")

        self.config = config or {}
        # If we hit ERROR_INVALID_PARAMETER from LARGE_FETCH once (pre-Win7),
        # disable it for the remainder of this backend's lifetime.
        self._large_fetch_supported = True

        bindings = _build_ctypes_bindings()
        self._ctypes = bindings["ctypes"]
        self._WIN32_FIND_DATAW = bindings["WIN32_FIND_DATAW"]
        self._FindFirstFileExW = bindings["FindFirstFileExW"]
        self._FindNextFileW = bindings["FindNextFileW"]
        self._FindClose = bindings["FindClose"]
        self._INVALID_HANDLE_VALUE = bindings["INVALID_HANDLE_VALUE"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def walk(self, root: str) -> Iterator[dict]:
        """Recursively yield one dict per file found under ``root``.

        Directories are recursed into but not yielded. Symlinks / reparse
        points are yielded but not followed.
        """
        # Use a work-stack instead of recursion so deep trees don't blow
        # the Python stack (Windows allows up to 32k path components).
        stack = [root]
        while stack:
            current = stack.pop()
            yield from self._walk_one(current, stack)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _walk_one(self, directory: str, stack: list) -> Iterator[dict]:
        """Iterate a single directory, pushing subdirs onto ``stack``."""
        search_root = _long_path(directory)
        # FindFirstFileExW wants a wildcard pattern, not a directory name.
        pattern = os.path.join(search_root, "*")

        handle, first_entry = self._open(pattern)
        if handle is None:
            return

        try:
            # Emit the entry that FindFirstFileExW already returned.
            entry = first_entry
            while True:
                name = entry.cFileName
                if name and name not in (".", ".."):
                    attrs = entry.dwFileAttributes
                    full = os.path.join(directory, name)
                    if attrs & FILE_ATTRIBUTE_DIRECTORY:
                        # Don't follow reparse points (symlinks / junctions).
                        if not (attrs & FILE_ATTRIBUTE_REPARSE_POINT):
                            stack.append(full)
                    else:
                        size = (entry.nFileSizeHigh << 32) | entry.nFileSizeLow
                        yield {
                            "file_path": _strip_long_prefix(full),
                            "file_size": size,
                            "creation_time": _filetime_to_str(
                                entry.ftCreationTime.dwHighDateTime,
                                entry.ftCreationTime.dwLowDateTime,
                            ),
                            "last_access_time": _filetime_to_str(
                                entry.ftLastAccessTime.dwHighDateTime,
                                entry.ftLastAccessTime.dwLowDateTime,
                            ),
                            "last_modify_time": _filetime_to_str(
                                entry.ftLastWriteTime.dwHighDateTime,
                                entry.ftLastWriteTime.dwLowDateTime,
                            ),
                            "attributes": attrs,
                        }

                # Reuse the same buffer for the next entry.
                next_entry = self._WIN32_FIND_DATAW()
                ok = self._FindNextFileW(
                    handle, self._ctypes.byref(next_entry)
                )
                if not ok:
                    err = self._ctypes.get_last_error()
                    if err == ERROR_NO_MORE_FILES:
                        break
                    self._log_win_error(err, directory, context="FindNextFileW")
                    break
                entry = next_entry
        finally:
            # Always release the handle, including on exception.
            try:
                self._FindClose(handle)
            except OSError:
                # Defensive: if the DLL itself raises, there is nothing
                # useful we can do — swallow to let the original error
                # (if any) surface.
                logger.debug("FindClose raised on %s", directory[:100])

    def _open(self, pattern: str):
        """Call ``FindFirstFileExW`` with LARGE_FETCH (falling back if needed).

        Returns a ``(handle, first_entry)`` pair, or ``(None, None)`` if
        the directory cannot be opened.
        """
        data = self._WIN32_FIND_DATAW()
        flags = FIND_FIRST_EX_LARGE_FETCH if self._large_fetch_supported else 0

        handle = self._FindFirstFileExW(
            pattern,
            FindExInfoBasic,
            self._ctypes.byref(data),
            FindExSearchNameMatch,
            None,
            flags,
        )

        if handle == self._INVALID_HANDLE_VALUE or handle is None or handle == 0:
            err = self._ctypes.get_last_error()

            # Pre-Windows 7: LARGE_FETCH is rejected. Disable permanently
            # and retry once without the flag.
            if err == ERROR_INVALID_PARAMETER and self._large_fetch_supported:
                logger.debug(
                    "FIND_FIRST_EX_LARGE_FETCH rejected (pre-Win7?), "
                    "disabling and retrying"
                )
                self._large_fetch_supported = False
                handle = self._FindFirstFileExW(
                    pattern,
                    FindExInfoBasic,
                    self._ctypes.byref(data),
                    FindExSearchNameMatch,
                    None,
                    0,
                )
                if handle == self._INVALID_HANDLE_VALUE or handle in (None, 0):
                    err = self._ctypes.get_last_error()
                    self._log_win_error(err, pattern, context="FindFirstFileExW")
                    return None, None
                return handle, data

            self._log_win_error(err, pattern, context="FindFirstFileExW")
            return None, None

        return handle, data

    @staticmethod
    def _log_win_error(err: int, path: str, context: str) -> None:
        """Log a Win32 error code at the appropriate level."""
        short = path[:160]
        if err in (ERROR_FILE_NOT_FOUND, ERROR_PATH_NOT_FOUND, ERROR_ACCESS_DENIED):
            logger.debug("%s: Win32 error %d on %s", context, err, short)
        else:
            logger.warning("%s: Win32 error %d on %s", context, err, short)


__all__ = ["Win32FindExBackend"]
