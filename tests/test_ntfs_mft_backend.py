"""Tests for the NTFS MFT scanner backend.

All tests in this file MUST run on Linux (the parser module is pure
Python; the backend module is import-safe because ctypes is loaded
lazily). Real volume access cannot be exercised off-Windows, so the
backend's ``walk()`` is only tested for its NotImplementedError guard.
"""

from __future__ import annotations

import struct
import sys

import pytest


# ---------------------------------------------------------------------------
# Module import safety
# ---------------------------------------------------------------------------


def test_modules_import_cross_platform() -> None:
    """Both modules must import cleanly on any platform."""
    import src.scanner.backends._ntfs_records as records_mod
    import src.scanner.backends.ntfs_mft as backend_mod

    assert hasattr(records_mod, "parse_usn_records")
    assert hasattr(records_mod, "reconstruct_paths")
    assert hasattr(backend_mod, "NtfsMftBackend")


# ---------------------------------------------------------------------------
# Synthetic USN_RECORD_V2 fixture builder
# ---------------------------------------------------------------------------


def _build_record(
    frn: int,
    parent_frn: int,
    file_name: str,
    attributes: int = 0x20,  # FILE_ATTRIBUTE_ARCHIVE
    usn: int = 0,
    major_version: int = 2,
) -> bytes:
    """Construct a single USN_RECORD_V2 byte string for parser tests.

    Layout matches the official MS struct exactly so the parser sees a
    byte-for-byte realistic record.
    """
    name_bytes = file_name.encode("utf-16-le")
    name_offset = 60
    # Pad record length to a multiple of 8 (kernel actually does this).
    raw_len = name_offset + len(name_bytes)
    record_length = (raw_len + 7) & ~7

    buf = bytearray(record_length)
    struct.pack_into("<I", buf, 0, record_length)        # RecordLength
    struct.pack_into("<H", buf, 4, major_version)         # MajorVersion
    struct.pack_into("<H", buf, 6, 0)                     # MinorVersion
    struct.pack_into("<Q", buf, 8, frn)                   # FileReferenceNumber
    struct.pack_into("<Q", buf, 16, parent_frn)           # ParentFileReferenceNumber
    struct.pack_into("<q", buf, 24, usn)                  # Usn
    struct.pack_into("<q", buf, 32, 0)                    # TimeStamp
    struct.pack_into("<I", buf, 40, 0)                    # Reason
    struct.pack_into("<I", buf, 44, 0)                    # SourceInfo
    struct.pack_into("<I", buf, 48, 0)                    # SecurityId
    struct.pack_into("<I", buf, 52, attributes)           # FileAttributes
    struct.pack_into("<H", buf, 56, len(name_bytes))      # FileNameLength
    struct.pack_into("<H", buf, 58, name_offset)          # FileNameOffset
    buf[name_offset:name_offset + len(name_bytes)] = name_bytes
    return bytes(buf)


def _wrap_buffer(records: bytes, next_frn: int = 0) -> bytes:
    """Prepend the 8-byte next-FRN header to one or more concatenated records."""
    return next_frn.to_bytes(8, "little", signed=False) + records


# ---------------------------------------------------------------------------
# parse_usn_records
# ---------------------------------------------------------------------------


def test_parse_single_record() -> None:
    from src.scanner.backends._ntfs_records import parse_usn_records

    rec = _build_record(frn=100, parent_frn=5, file_name="hello.txt")
    buf = _wrap_buffer(rec)

    parsed = list(parse_usn_records(buf, offset=8))

    assert len(parsed) == 1
    p = parsed[0]
    assert p["frn"] == 100
    assert p["parent_frn"] == 5
    assert p["file_name"] == "hello.txt"
    assert p["attributes"] == 0x20
    assert p["_record_length"] == len(rec)


def test_parse_multiple_records() -> None:
    from src.scanner.backends._ntfs_records import parse_usn_records

    rec1 = _build_record(frn=100, parent_frn=5, file_name="a.txt")
    rec2 = _build_record(frn=101, parent_frn=5, file_name="b.txt")
    buf = _wrap_buffer(rec1 + rec2)

    parsed = list(parse_usn_records(buf, offset=8))

    assert len(parsed) == 2
    assert parsed[0]["file_name"] == "a.txt"
    assert parsed[0]["frn"] == 100
    assert parsed[1]["file_name"] == "b.txt"
    assert parsed[1]["frn"] == 101


def test_parse_stops_on_zero_length() -> None:
    from src.scanner.backends._ntfs_records import parse_usn_records

    rec = _build_record(frn=100, parent_frn=5, file_name="x.txt")
    # Append at least 64 bytes of zeros so the parser actually inspects
    # the fixed header and finds RecordLength == 0.
    buf = _wrap_buffer(rec) + b"\x00" * 64

    parsed = list(parse_usn_records(buf, offset=8))

    # Only the real record should be yielded; zero-length sentinel halts.
    assert len(parsed) == 1
    assert parsed[0]["file_name"] == "x.txt"


def test_parse_handles_unicode_name() -> None:
    """Turkish characters (Unicode) must round-trip via utf-16-le."""
    from src.scanner.backends._ntfs_records import parse_usn_records

    rec = _build_record(frn=200, parent_frn=5, file_name="rapor_calisma.txt")
    buf = _wrap_buffer(rec)

    parsed = list(parse_usn_records(buf, offset=8))
    assert parsed[0]["file_name"] == "rapor_calisma.txt"


def test_parse_skips_unknown_major_version() -> None:
    from src.scanner.backends._ntfs_records import parse_usn_records

    bad = _build_record(frn=300, parent_frn=5, file_name="v3.txt", major_version=3)
    good = _build_record(frn=301, parent_frn=5, file_name="v2.txt")
    buf = _wrap_buffer(bad + good)

    parsed = list(parse_usn_records(buf, offset=8))
    assert len(parsed) == 1
    assert parsed[0]["file_name"] == "v2.txt"


def test_parse_empty_buffer() -> None:
    from src.scanner.backends._ntfs_records import parse_usn_records

    # Just the 8-byte next-FRN header, no records.
    buf = _wrap_buffer(b"")
    assert list(parse_usn_records(buf, offset=8)) == []


def test_parse_masks_frn_sequence_number() -> None:
    """Real-world FRNs carry a 16-bit sequence number in the upper bits.
    The parser must mask it off so child.parent_frn matches parent.frn
    after dict-keyed lookup. Customer prod regression: 3.1M kayit → 0
    dosya yayildi, traced to a sequence-number mismatch.
    """
    from src.scanner.backends._ntfs_records import parse_usn_records

    # Sequence=0x0005 in upper 16 bits, segment=5 in lower 48 bits.
    root_full_frn = (0x0005 << 48) | 5
    rec = _build_record(
        frn=(0x0010 << 48) | 100,
        parent_frn=root_full_frn,
        file_name="x.txt",
    )
    buf = _wrap_buffer(rec)
    parsed = list(parse_usn_records(buf, offset=8))
    assert len(parsed) == 1
    # Both FRN and parent_frn must be masked to lower 48 bits.
    assert parsed[0]["frn"] == 100
    assert parsed[0]["parent_frn"] == 5


# ---------------------------------------------------------------------------
# reconstruct_paths
# ---------------------------------------------------------------------------


def test_reconstruct_paths_simple() -> None:
    from src.scanner.backends._ntfs_records import reconstruct_paths

    records = {
        5: {"parent_frn": 5, "file_name": "", "attributes": 0x10},
        10: {"parent_frn": 5, "file_name": "docs", "attributes": 0x10},
        20: {"parent_frn": 10, "file_name": "a.txt", "attributes": 0x20},
    }

    paths = reconstruct_paths(records, root_frn=5)

    assert paths[5] == ""
    assert paths[10] == "docs"
    assert paths[20] == "docs\\a.txt"


def test_reconstruct_paths_deep_nesting() -> None:
    from src.scanner.backends._ntfs_records import reconstruct_paths

    records = {
        10: {"parent_frn": 5, "file_name": "a"},
        20: {"parent_frn": 10, "file_name": "b"},
        30: {"parent_frn": 20, "file_name": "c"},
        40: {"parent_frn": 30, "file_name": "d.txt"},
    }
    paths = reconstruct_paths(records, root_frn=5)
    assert paths[40] == "a\\b\\c\\d.txt"


def test_reconstruct_paths_cycle_detection() -> None:
    from src.scanner.backends._ntfs_records import reconstruct_paths

    records = {
        10: {"parent_frn": 20, "file_name": "x"},
        20: {"parent_frn": 10, "file_name": "y"},
    }

    paths = reconstruct_paths(records, root_frn=5)

    # Both cyclic entries must resolve to None (i.e. unresolvable).
    assert paths.get(10) is None
    assert paths.get(20) is None


def test_reconstruct_paths_orphan_parent() -> None:
    """Records pointing at a missing parent FRN must resolve to None."""
    from src.scanner.backends._ntfs_records import reconstruct_paths

    records = {
        99: {"parent_frn": 12345, "file_name": "ghost.txt"},
    }

    paths = reconstruct_paths(records, root_frn=5)
    assert paths.get(99) is None


def test_reconstruct_paths_real_world_frns_via_parser() -> None:
    """End-to-end regression for the prod '0 dosya yayildi' bug:
    feed records with non-zero sequence numbers through the parser into
    reconstruct_paths and assert paths resolve. Without the FRN mask,
    the children's parent chain misses the root and every path is None.
    """
    from src.scanner.backends._ntfs_records import (
        parse_usn_records,
        reconstruct_paths,
    )

    # All three records carry non-zero sequence numbers, mirroring an
    # NTFS volume that has been through allocate/free cycles.
    rec_root = _build_record(
        frn=(0x0005 << 48) | 5,
        parent_frn=(0x0005 << 48) | 5,
        file_name="",
        attributes=0x10,  # directory
    )
    rec_dir = _build_record(
        frn=(0x0010 << 48) | 100,
        parent_frn=(0x0005 << 48) | 5,
        file_name="docs",
        attributes=0x10,
    )
    rec_file = _build_record(
        frn=(0x0020 << 48) | 200,
        parent_frn=(0x0010 << 48) | 100,
        file_name="report.txt",
    )
    buf = _wrap_buffer(rec_root + rec_dir + rec_file)

    records = {r["frn"]: r for r in parse_usn_records(buf, offset=8)}
    paths = reconstruct_paths(records, root_frn=5)

    assert paths.get(100) == "docs"
    assert paths.get(200) == "docs\\report.txt"


# ---------------------------------------------------------------------------
# NtfsMftBackend platform guards
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Non-Windows guard only checked on non-Windows hosts",
)
def test_backend_not_supported_on_linux() -> None:
    """is_supported() must short-circuit to False on Linux/macOS."""
    from src.scanner.backends.ntfs_mft import NtfsMftBackend

    backend = NtfsMftBackend(config={})
    assert backend.is_supported("/tmp") is False
    assert backend.is_supported("/home/user") is False


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Non-Windows guard only checked on non-Windows hosts",
)
def test_backend_walk_raises_not_implemented_on_linux() -> None:
    """walk() must raise NotImplementedError when is_supported is False."""
    from src.scanner.backends.ntfs_mft import NtfsMftBackend

    backend = NtfsMftBackend(config={})
    with pytest.raises(NotImplementedError):
        list(backend.walk("/tmp"))


def test_backend_rejects_unc_paths() -> None:
    """UNC paths are never supported, regardless of platform."""
    from src.scanner.backends.ntfs_mft import NtfsMftBackend

    backend = NtfsMftBackend(config={})
    assert backend.is_supported("\\\\server\\share") is False
