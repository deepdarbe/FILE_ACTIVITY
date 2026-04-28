"""Pure-Python USN_RECORD_V2 parser for FSCTL_ENUM_USN_DATA output buffers.

This module is intentionally free of any Windows-only imports so it can be
unit-tested on any platform. The Windows-specific :mod:`ntfs_mft` backend
calls into these helpers after retrieving raw bytes from
``DeviceIoControl(FSCTL_ENUM_USN_DATA, ...)``.

USN_RECORD_V2 layout (see MS docs:
https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-usn_record_v2)::

    +0   DWORD  RecordLength
    +4   WORD   MajorVersion (must be 2)
    +6   WORD   MinorVersion
    +8   ULL    FileReferenceNumber
    +16  ULL    ParentFileReferenceNumber
    +24  ULL    Usn
    +32  LARGE_INTEGER TimeStamp
    +40  DWORD  Reason
    +44  DWORD  SourceInfo
    +48  DWORD  SecurityId
    +52  DWORD  FileAttributes
    +56  WORD   FileNameLength (bytes)
    +58  WORD   FileNameOffset
    +60+ WCHAR  FileName

The first 8 bytes of an FSCTL_ENUM_USN_DATA output buffer contain the
"next" file reference number (used to advance the enumeration); records
themselves start at offset 8.
"""

from __future__ import annotations

import struct
from typing import Dict, Iterator, Optional


# Header field offsets within a single USN_RECORD_V2.
_OFF_RECORD_LENGTH = 0
_OFF_MAJOR_VERSION = 4
_OFF_MINOR_VERSION = 6
_OFF_FRN = 8
_OFF_PARENT_FRN = 16
_OFF_USN = 24
_OFF_FILE_ATTRIBUTES = 52
_OFF_FILE_NAME_LENGTH = 56
_OFF_FILE_NAME_OFFSET = 58

# Minimum size of a fixed USN_RECORD_V2 header (filename starts at +60).
_MIN_RECORD_SIZE = 60

# NTFS volume root always has FRN segment number 5.
NTFS_ROOT_FRN = 5

# NTFS FRN encoding: lower 48 bits = MFT segment number (the actual entry
# index), upper 16 bits = sequence number (incremented when an entry is
# reused after deletion). For path reconstruction we ONLY care about the
# segment number — sequence numbers cause parent-chain lookups to miss
# whenever the volume has had any churn, which manifested in prod as
# "3M kayit toplandi → 0 dosya yayildi" (issue #144 follow-up).
_FRN_SEGMENT_MASK = (1 << 48) - 1

# FILE_ATTRIBUTE_DIRECTORY bit — used by callers to filter directories.
FILE_ATTRIBUTE_DIRECTORY = 0x10


def parse_usn_records(buf: bytes, offset: int = 8) -> Iterator[dict]:
    """Yield one dict per USN_RECORD_V2 found in ``buf`` starting at ``offset``.

    Each yielded dict contains: ``frn``, ``parent_frn``, ``usn``,
    ``attributes``, ``file_name``, ``_record_length``.

    Iteration stops cleanly when:
      * ``offset`` would exceed ``len(buf)``,
      * remaining buffer is smaller than the fixed header,
      * ``RecordLength == 0`` (sentinel),
      * the parsed record overruns the buffer.

    Records with ``MajorVersion != 2`` are skipped (advanced past) because
    other versions have a different layout.
    """
    buf_len = len(buf)

    while offset + _MIN_RECORD_SIZE <= buf_len:
        # RecordLength is always the very first DWORD.
        (record_length,) = struct.unpack_from("<I", buf, offset + _OFF_RECORD_LENGTH)
        if record_length == 0:
            return
        if record_length < _MIN_RECORD_SIZE:
            # Corrupt / truncated record — bail rather than infinite-loop.
            return
        if offset + record_length > buf_len:
            return

        (major_version,) = struct.unpack_from(
            "<H", buf, offset + _OFF_MAJOR_VERSION
        )
        if major_version != 2:
            # Unknown version: skip but keep walking the buffer.
            offset += record_length
            continue

        (frn_raw,) = struct.unpack_from("<Q", buf, offset + _OFF_FRN)
        (parent_frn_raw,) = struct.unpack_from("<Q", buf, offset + _OFF_PARENT_FRN)
        # Mask off the sequence number (upper 16 bits) so children whose
        # parent_frn carries a non-zero sequence number still match the
        # parent record's key in the {frn: rec} dict. See _FRN_SEGMENT_MASK
        # comment above for the prod failure mode.
        frn = frn_raw & _FRN_SEGMENT_MASK
        parent_frn = parent_frn_raw & _FRN_SEGMENT_MASK
        (usn,) = struct.unpack_from("<q", buf, offset + _OFF_USN)
        (attributes,) = struct.unpack_from(
            "<I", buf, offset + _OFF_FILE_ATTRIBUTES
        )
        (name_len_bytes,) = struct.unpack_from(
            "<H", buf, offset + _OFF_FILE_NAME_LENGTH
        )
        (name_off,) = struct.unpack_from(
            "<H", buf, offset + _OFF_FILE_NAME_OFFSET
        )

        name_start = offset + name_off
        name_end = name_start + name_len_bytes
        if name_end > offset + record_length or name_end > buf_len:
            # Malformed record — skip it.
            offset += record_length
            continue

        try:
            file_name = buf[name_start:name_end].decode("utf-16-le")
        except UnicodeDecodeError:
            file_name = buf[name_start:name_end].decode("utf-16-le", errors="replace")

        yield {
            "frn": frn,
            "parent_frn": parent_frn,
            "usn": usn,
            "attributes": attributes,
            "file_name": file_name,
            "_record_length": record_length,
        }

        offset += record_length


def reconstruct_paths(
    records: Dict[int, dict], root_frn: int = NTFS_ROOT_FRN
) -> Dict[int, Optional[str]]:
    """Build a ``{frn: full_path}`` mapping by walking parent_frn chains.

    Parameters
    ----------
    records:
        Mapping ``{frn: record_dict}`` where each record dict contains at
        least ``parent_frn`` and ``file_name`` (as produced by
        :func:`parse_usn_records`).
    root_frn:
        File reference number of the volume root (5 on every NTFS volume).
        The root maps to the empty string and acts as the recursion base.

    Returns
    -------
    Mapping ``{frn: path}`` where ``path`` uses ``\\`` separators and is
    relative to the volume root. Entries whose chain leads to a missing
    parent or a cycle map to ``None`` and are skipped by the caller.
    """
    cache: Dict[int, Optional[str]] = {root_frn: ""}
    # Always pin the root so a synthetic record for FRN 5 isn't required.
    if root_frn in records and not cache[root_frn]:
        cache[root_frn] = ""

    def _resolve(frn: int, visiting: set) -> Optional[str]:
        if frn in cache:
            return cache[frn]
        if frn not in records:
            cache[frn] = None
            return None
        if frn in visiting:
            # Cycle: refuse to recurse — mark all visited as unresolved.
            return None
        visiting.add(frn)

        rec = records[frn]
        parent = rec.get("parent_frn")
        name = rec.get("file_name", "")

        # Self-parent (other than the root) is a degenerate cycle.
        if parent == frn and frn != root_frn:
            visiting.discard(frn)
            cache[frn] = None
            return None

        parent_path = _resolve(parent, visiting) if parent is not None else None
        visiting.discard(frn)

        if parent_path is None and parent != root_frn:
            cache[frn] = None
            return None
        if parent_path is None:
            # Root parent yields empty prefix.
            parent_path = ""

        if parent_path:
            full = parent_path + "\\" + name
        else:
            full = name
        cache[frn] = full
        return full

    for frn in list(records.keys()):
        if frn not in cache:
            _resolve(frn, set())

    return cache


__all__ = [
    "parse_usn_records",
    "reconstruct_paths",
    "NTFS_ROOT_FRN",
    "FILE_ATTRIBUTE_DIRECTORY",
]
