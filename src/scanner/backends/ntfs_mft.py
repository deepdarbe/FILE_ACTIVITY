"""NTFS Master File Table (MFT) scanner backend.

This backend enumerates every file on a local NTFS volume by issuing
``DeviceIoControl(FSCTL_ENUM_USN_DATA, ...)`` against a raw volume handle.
It is dramatically faster than directory walks (``FindFirstFileEx``,
``os.scandir``) on large volumes because the kernel streams MFT entries
directly without per-directory open / close overhead.

Requirements
------------
* Local NTFS volume (UNC paths are explicitly rejected — the IOCTL only
  works against local volumes).
* Administrator privileges (raw volume access requires it).
* Windows. The module is import-safe on Linux/macOS because all ctypes
  bindings are loaded lazily inside methods.

Limitations (documented for callers)
------------------------------------
* **No timestamps.** USN_RECORD_V2 does not carry creation / modify /
  access times. Fetching them would require one
  ``FSCTL_GET_NTFS_FILE_RECORD`` per FRN, which would erase the speed
  advantage. Callers receive ``None`` for all three timestamp fields.
* **No file sizes.** Same reason as above; ``file_size`` is reported as 0.
* **No hardlink expansion.** Each FRN appears once. Files with multiple
  hardlinks are emitted under one of their names only.
* **Root path prefix.** Yielded ``file_path`` values are joined with the
  volume root (e.g. ``C:\\``) using backslashes.

References
----------
* https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_enum_usn_data
* https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-mft_enum_data_v0
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Iterator

from src.scanner.backends._ntfs_records import (
    FILE_ATTRIBUTE_DIRECTORY,
    NTFS_ROOT_FRN,
    parse_usn_records,
    reconstruct_paths,
)

logger = logging.getLogger("file_activity.scanner.backends.ntfs_mft")


# ---------------------------------------------------------------------------
# Win32 constants used by walk()
# ---------------------------------------------------------------------------

# DeviceIoControl code: FSCTL_ENUM_USN_DATA.
FSCTL_ENUM_USN_DATA = 0x000900B3

# CreateFileW flags.
GENERIC_READ = 0x80000000
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
OPEN_EXISTING = 3
INVALID_HANDLE_VALUE = -1

# Output buffer size for FSCTL_ENUM_USN_DATA. 64 KB is the canonical value
# used by Microsoft samples; large enough to amortize syscall overhead but
# small enough that a single allocation is cheap.
_OUT_BUF_SIZE = 65536


def _build_ctypes_bindings():
    """Lazy ctypes import — keeps this module importable on non-Windows."""
    import ctypes
    from ctypes import wintypes

    class MFT_ENUM_DATA_V0(ctypes.Structure):
        """Input struct for FSCTL_ENUM_USN_DATA (V0 layout)."""

        _fields_ = [
            ("StartFileReferenceNumber", ctypes.c_ulonglong),
            ("LowUsn", ctypes.c_longlong),
            ("HighUsn", ctypes.c_longlong),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    CreateFileW = kernel32.CreateFileW
    CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    CreateFileW.restype = wintypes.HANDLE

    DeviceIoControl = kernel32.DeviceIoControl
    DeviceIoControl.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.c_void_p,
    ]
    DeviceIoControl.restype = wintypes.BOOL

    CloseHandle = kernel32.CloseHandle
    CloseHandle.argtypes = [wintypes.HANDLE]
    CloseHandle.restype = wintypes.BOOL

    return {
        "ctypes": ctypes,
        "wintypes": wintypes,
        "kernel32": kernel32,
        "MFT_ENUM_DATA_V0": MFT_ENUM_DATA_V0,
        "CreateFileW": CreateFileW,
        "DeviceIoControl": DeviceIoControl,
        "CloseHandle": CloseHandle,
        "INVALID_HANDLE_VALUE": ctypes.c_void_p(-1).value,
    }


def _volume_root(path: str) -> str:
    """Return the volume root (e.g. ``C:\\``) for the given absolute path."""
    drive = os.path.splitdrive(os.path.abspath(path))[0]
    return drive + "\\"


class NtfsMftBackend:
    """Scanner backend that enumerates files via FSCTL_ENUM_USN_DATA.

    Parameters
    ----------
    config:
        Project configuration dictionary. Currently unused beyond storage —
        accepted for protocol compatibility with other backends.
    """

    def __init__(self, config: dict) -> None:
        self.config = config or {}

    # ------------------------------------------------------------------
    # Capability check
    # ------------------------------------------------------------------

    def is_supported(self, path: str) -> bool:
        """Return True iff ``path`` is on a local NTFS volume and we are admin.

        UNC paths are rejected outright because FSCTL_ENUM_USN_DATA only
        works against local volume handles. On non-Windows hosts, this
        always returns False without touching ctypes.
        """
        if sys.platform != "win32":
            return False
        if not path:
            return False
        if path.startswith("\\\\"):
            # UNC — not addressable via raw volume handle.
            return False

        try:
            import ctypes  # noqa: F401  (lazy import guard)

            volume_root = _volume_root(path)
            fs_buf = ctypes.create_unicode_buffer(256)
            ok = ctypes.windll.kernel32.GetVolumeInformationW(
                ctypes.c_wchar_p(volume_root),
                None,
                0,
                None,
                None,
                None,
                fs_buf,
                256,
            )
            if not ok:
                return False
            if fs_buf.value.upper() != "NTFS":
                return False

            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception as exc:  # pragma: no cover — defensive on Windows
            logger.debug("is_supported probe failed for %s: %s", path, exc)
            return False

    # ------------------------------------------------------------------
    # Walk
    # ------------------------------------------------------------------

    def walk(self, root: str) -> Iterator[dict]:
        """Yield one dict per file on the volume containing ``root``.

        Raises
        ------
        NotImplementedError
            If the backend cannot run (non-Windows, non-NTFS, no admin,
            UNC path).
        OSError
            If a Win32 call fails unexpectedly (volume open, IOCTL).
        """
        if not self.is_supported(root):
            raise NotImplementedError(
                "NtfsMftBackend requires local NTFS volume + admin"
            )

        # All Windows-only types are loaded inside the method to keep the
        # module importable on Linux for unit tests.
        bindings = _build_ctypes_bindings()
        ctypes_mod = bindings["ctypes"]
        wintypes = bindings["wintypes"]
        MFT_ENUM_DATA_V0 = bindings["MFT_ENUM_DATA_V0"]
        CreateFileW = bindings["CreateFileW"]
        DeviceIoControl = bindings["DeviceIoControl"]
        CloseHandle = bindings["CloseHandle"]
        INVALID_HANDLE = bindings["INVALID_HANDLE_VALUE"]

        volume_root = _volume_root(root)
        # CreateFileW path for raw volume access: \\.\C:
        drive_letter = volume_root[0]
        raw_volume = "\\\\.\\" + drive_letter + ":"

        logger.info("MFT taramasi basliyor: %s", raw_volume)

        handle = CreateFileW(
            raw_volume,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            0,
            None,
        )
        if handle == INVALID_HANDLE or handle in (None, 0):
            err = ctypes_mod.get_last_error()
            raise OSError(err, "CreateFileW failed for %s" % raw_volume)

        try:
            records = self._collect_records(
                handle,
                ctypes_mod,
                wintypes,
                MFT_ENUM_DATA_V0,
                DeviceIoControl,
            )
        finally:
            try:
                CloseHandle(handle)
            except Exception:  # pragma: no cover — defensive
                logger.debug("CloseHandle raised on %s", raw_volume)

        logger.info("MFT taramasi: %d kayit toplandi, yollar olusturuluyor", len(records))

        # Path reconstruction is pure-Python; isolated from ctypes for
        # easy testing.
        paths = reconstruct_paths(records, root_frn=NTFS_ROOT_FRN)

        emitted = 0
        for frn, rec in records.items():
            if rec["attributes"] & FILE_ATTRIBUTE_DIRECTORY:
                continue
            rel_path = paths.get(frn)
            if rel_path is None:
                # Cycle / orphaned chain — skip silently.
                continue

            full_path = volume_root + rel_path if rel_path else volume_root
            yield {
                "file_path": full_path,
                "file_name": rec["file_name"],
                # TODO: enrich via FSCTL_GET_NTFS_FILE_RECORD when timestamps
                # / sizes are required. Skipped here to preserve the speed
                # advantage of bulk MFT enumeration.
                "file_size": 0,
                "last_modify_time": None,
                "creation_time": None,
                "last_access_time": None,
                "attributes": rec["attributes"],
            }
            emitted += 1

        logger.info("MFT taramasi tamamlandi: %d dosya yayildi", emitted)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _collect_records(
        self,
        handle,
        ctypes_mod,
        wintypes,
        MFT_ENUM_DATA_V0,
        DeviceIoControl,
    ) -> dict:
        """Run the FSCTL_ENUM_USN_DATA loop and return ``{frn: record}``."""
        in_buf = MFT_ENUM_DATA_V0()
        in_buf.StartFileReferenceNumber = 0
        in_buf.LowUsn = 0
        # MaxUsn must be high enough to cover every record currently on disk.
        # (1 << 63) - 1 is the practical maximum signed-64-bit USN.
        in_buf.HighUsn = (1 << 63) - 1

        out_buf = ctypes_mod.create_string_buffer(_OUT_BUF_SIZE)
        returned = wintypes.DWORD(0)

        records: dict = {}
        iterations = 0

        while True:
            ok = DeviceIoControl(
                handle,
                FSCTL_ENUM_USN_DATA,
                ctypes_mod.byref(in_buf),
                ctypes_mod.sizeof(in_buf),
                out_buf,
                _OUT_BUF_SIZE,
                ctypes_mod.byref(returned),
                None,
            )
            if not ok:
                err = ctypes_mod.get_last_error()
                # ERROR_HANDLE_EOF (38) marks normal end-of-enumeration.
                if err == 38:
                    break
                raise OSError(err, "DeviceIoControl(FSCTL_ENUM_USN_DATA) failed")

            n = returned.value
            if n <= 8:
                # Only the next-FRN header was returned — no more records.
                break

            raw = bytes(out_buf.raw[:n])
            for rec in parse_usn_records(raw, offset=8):
                records[rec["frn"]] = rec

            # The first 8 bytes of the output buffer are the next-FRN to
            # resume enumeration from.
            next_frn = int.from_bytes(raw[:8], "little", signed=False)
            in_buf.StartFileReferenceNumber = next_frn

            iterations += 1
            if iterations % 100 == 0:
                logger.debug(
                    "MFT enum: %d iterasyon, %d kayit", iterations, len(records)
                )

        return records


__all__ = ["NtfsMftBackend"]
