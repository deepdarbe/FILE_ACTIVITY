"""Resolve NTFS parent-directory paths from File Reference Numbers (FRN).

#340 Faz 2 (Part C). A USN journal record for a deleted file carries only the
file name plus the *parent* directory's FRN. The parent directory almost
always survives the child's deletion, so we can turn the parent FRN into a
full path via ``OpenFileById`` + ``GetFinalPathNameByHandleW`` and prepend it
to the file name — giving the deletion audit row a real ``E:\\dir\\file``
path instead of a bare name.

Design notes
------------
* **Windows-only, POSIX-import-safe.** Every ``ctypes`` / win32 import is lazy
  (inside methods), so the module imports cleanly on Linux for unit testing.
* **Best-effort with a hard floor.** Any failure (bad FRN, deleted parent,
  missing privilege, non-Windows) returns ``None``; the caller keeps its
  name-only behavior. A slow/failed resolve must never stall USN tailing, so
  results are LRU-cached and the resolver *disables itself* after a run of
  consecutive failures (a systemic failure — e.g. the volume handle can't be
  opened — trips this quickly; a single bad FRN among good ones does not).
* **Full 64-bit FRN required.** ``OpenFileById(FileIdType)`` validates the
  NTFS *sequence number* in the high 16 bits of the file reference. The USN
  parser (:mod:`_ntfs_records`) masks that off for its parent-chain dict keys,
  so callers must pass the RAW parent FRN (``event["parent_frn_raw"]``); the
  masked value would make almost every ``OpenFileById`` fail.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Optional, Union

logger = logging.getLogger("file_activity.scanner.frn_resolver")

# Disable the resolver after this many consecutive failures. Resets to 0 on
# any success, so it only trips on a *systemic* failure (every resolve fails),
# not on the odd deleted-parent FRN interleaved with good ones.
_MAX_CONSECUTIVE_FAILURES = 20

# ── Win32 constants (referenced lazily inside methods) ────────────────────
_GENERIC_READ = 0x80000000
_FILE_READ_ATTRIBUTES = 0x0080
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_FILE_SHARE_DELETE = 0x00000004
_OPEN_EXISTING = 3
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000  # needed to open a directory handle
_FILE_ID_TYPE = 0  # FILE_ID_TYPE.FileIdType (64-bit NTFS file reference)


class FrnResolver:
    """Resolve NTFS parent-directory FRNs to full paths, with an LRU cache.

    Cheap to construct; opens volume handles lazily on first use and caches
    them per volume key. Call :meth:`close` when finished to release handles.
    """

    def __init__(self, cache_size: int = 4096):
        self._cache_size = max(1, cache_size)
        # parent_frn -> resolved path (OrderedDict = simple LRU).
        self._path_cache: "OrderedDict[int, str]" = OrderedDict()
        # volume key (path str) -> open volume handle we own and must close.
        self._owned_handles: dict = {}
        self._consecutive_failures = 0
        self._disabled = False

    # ── Public API ────────────────────────────────────────────────────────

    def resolve(self, volume: Union[int, str, None],
                parent_frn: Optional[int]) -> Optional[str]:
        """Return the full path of the directory identified by ``parent_frn``
        on ``volume`` (an open volume HANDLE int, or a ``\\\\.\\X:`` path str),
        or ``None`` on any failure.

        Cached hits skip all win32 calls. Consecutive failures eventually
        disable the resolver (short-circuit to ``None``) so a broken volume
        never keeps hammering ``OpenFileById`` on the USN callback thread.
        """
        if self._disabled or parent_frn is None or volume is None:
            return None

        cached = self._path_cache.get(parent_frn)
        if cached is not None:
            self._path_cache.move_to_end(parent_frn)
            return cached

        try:
            path = self._resolve_impl(volume, parent_frn)
        except Exception as e:  # never let a win32 quirk escape to the caller
            logger.debug("FrnResolver.resolve hata (frn=%s): %s", parent_frn, e)
            path = None

        if path is None:
            self._consecutive_failures += 1
            if (self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES
                    and not self._disabled):
                self._disabled = True
                logger.info(
                    "FrnResolver devre dışı bırakıldı (%d ardışık başarısız "
                    "çözümleme) — USN olayları yalnız dosya adıyla kaydedilecek",
                    self._consecutive_failures)
            return None

        self._consecutive_failures = 0
        self._path_cache[parent_frn] = path
        self._path_cache.move_to_end(parent_frn)
        while len(self._path_cache) > self._cache_size:
            self._path_cache.popitem(last=False)
        return path

    def close(self) -> None:
        """Close any volume handles this resolver opened. Idempotent."""
        if not self._owned_handles:
            return
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
        except Exception:
            self._owned_handles.clear()
            return
        for h in list(self._owned_handles.values()):
            try:
                kernel32.CloseHandle(h)
            except Exception:
                pass
        self._owned_handles.clear()

    # ── Internals (Windows-only; imports are lazy) ────────────────────────

    def _get_volume_handle(self, volume: Union[int, str]):
        """Return a usable volume-hint handle for ``OpenFileById``.

        * ``int`` → an already-open volume handle owned by the caller; used
          directly (we never close it).
        * ``str`` → a volume path like ``\\\\.\\E:``; opened once and cached
          (owned by us, closed in :meth:`close`).
        """
        if isinstance(volume, int):
            return volume

        h = self._owned_handles.get(volume)
        if h is not None:
            return h

        import ctypes
        kernel32 = ctypes.windll.kernel32
        # Bare-int handles, default restype — mirrors the proven pattern in
        # ntfs_usn_tail.py (Windows HANDLEs always fit in 32 bits, so no
        # truncation). FILE_FLAG_BACKUP_SEMANTICS lets us later open a
        # directory handle by id.
        h = kernel32.CreateFileW(
            volume,
            _GENERIC_READ,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
            None,
            _OPEN_EXISTING,
            _FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
        if not self._handle_ok(h):
            return None
        self._owned_handles[volume] = h
        return h

    @staticmethod
    def _handle_ok(h) -> bool:
        """True if a win32 HANDLE return value is valid (not NULL / INVALID)."""
        if h is None:
            return False
        try:
            hv = int(h)
        except (TypeError, ValueError):
            return False
        # INVALID_HANDLE_VALUE is -1 (0xFFFFFFFFFFFFFFFF as unsigned).
        return hv not in (0, -1, 0xFFFFFFFFFFFFFFFF)

    def _resolve_impl(self, volume: Union[int, str],
                      parent_frn: int) -> Optional[str]:
        import ctypes

        kernel32 = ctypes.windll.kernel32

        vol_handle = self._get_volume_handle(volume)
        if not self._handle_ok(vol_handle):
            return None

        # FILE_ID_DESCRIPTOR with a 16-byte union (largest member is
        # FILE_ID_128); for FileIdType only the first 8 bytes (LARGE_INTEGER
        # FileId) are read.
        class _FileIdUnion(ctypes.Union):
            _fields_ = [
                ("FileId", ctypes.c_longlong),
                ("ExtendedFileId", ctypes.c_ubyte * 16),
            ]

        class FILE_ID_DESCRIPTOR(ctypes.Structure):
            _fields_ = [
                ("dwSize", ctypes.c_uint32),
                ("Type", ctypes.c_uint32),
                ("u", _FileIdUnion),
            ]

        desc = FILE_ID_DESCRIPTOR()
        desc.dwSize = ctypes.sizeof(FILE_ID_DESCRIPTOR)
        desc.Type = _FILE_ID_TYPE
        # ctypes.c_longlong is signed; NTFS FRNs fit and round-trip fine.
        desc.u.FileId = ctypes.c_longlong(parent_frn & 0xFFFFFFFFFFFFFFFF).value

        file_handle = kernel32.OpenFileById(
            vol_handle,
            ctypes.byref(desc),
            _FILE_READ_ATTRIBUTES,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
            None,
            _FILE_FLAG_BACKUP_SEMANTICS,
        )
        if not self._handle_ok(file_handle):
            return None

        try:
            path = self._final_path(kernel32, file_handle)
        finally:
            try:
                kernel32.CloseHandle(file_handle)
            except Exception:
                pass
        return path

    @staticmethod
    def _final_path(kernel32, file_handle) -> Optional[str]:
        """GetFinalPathNameByHandleW → cleaned directory path (or None)."""
        import ctypes

        buf_len = 4096
        buf = ctypes.create_unicode_buffer(buf_len)
        needed = kernel32.GetFinalPathNameByHandleW(file_handle, buf, buf_len, 0)
        if needed == 0:
            return None
        if needed >= buf_len:
            # Buffer too small — retry with the exact size it reported.
            buf = ctypes.create_unicode_buffer(needed + 1)
            needed = kernel32.GetFinalPathNameByHandleW(
                file_handle, buf, needed + 1, 0)
            if needed == 0:
                return None

        path = buf.value
        # Strip the \\?\ (or \\?\UNC\) extended-length prefixes that
        # GetFinalPathNameByHandleW returns.
        if path.startswith("\\\\?\\UNC\\"):
            path = "\\\\" + path[len("\\\\?\\UNC\\"):]
        elif path.startswith("\\\\?\\"):
            path = path[len("\\\\?\\"):]
        return path or None


__all__ = ["FrnResolver"]
