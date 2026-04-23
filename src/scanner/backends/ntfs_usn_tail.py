"""NTFS USN Change Journal tailer for incremental file change detection.

Reads pending entries from the volume's $UsnJrnl:$J via
``FSCTL_READ_USN_JOURNAL`` and reports them through a callback. Local
NTFS only (admin required); UNC / SMB paths are explicitly unsupported.

State persistence
-----------------

Last seen USN + journal id are persisted to the ``usn_tail_state``
table so the tailer can resume across restarts. Three startup paths:

* No persisted state -> start at the journal head (only new events).
* Persisted ``journal_id`` differs from the live journal -> the
  journal has been recreated; full rescan needed (gap_detected=True),
  reset to head.
* Persisted ``last_seen_usn`` < live ``FirstUsn`` -> the entry has
  been overwritten by the circular journal; full rescan needed
  (gap_detected=True), reset to head.

The caller is responsible for triggering the rescan. We just signal it.

Reuses :func:`src.scanner.backends._ntfs_records.parse_usn_records`
for the on-the-wire byte parsing — same record format as MFT enum.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import Callable, Optional

from src.scanner.backends._ntfs_records import parse_usn_records

logger = logging.getLogger("file_activity.scanner.usn_tail")


# ─────────────────────────────────────────────────────────────────────
# USN_REASON_* flag decode table
# ─────────────────────────────────────────────────────────────────────

USN_REASON_FLAGS = {
    0x00000001: "DATA_OVERWRITE",
    0x00000002: "DATA_EXTEND",
    0x00000004: "DATA_TRUNCATION",
    0x00000010: "NAMED_DATA_OVERWRITE",
    0x00000020: "NAMED_DATA_EXTEND",
    0x00000040: "NAMED_DATA_TRUNCATION",
    0x00000100: "FILE_CREATE",
    0x00000200: "FILE_DELETE",
    0x00000400: "EA_CHANGE",
    0x00000800: "SECURITY_CHANGE",
    0x00001000: "RENAME_OLD_NAME",
    0x00002000: "RENAME_NEW_NAME",
    0x00004000: "INDEXABLE_CHANGE",
    0x00008000: "BASIC_INFO_CHANGE",
    0x00010000: "HARD_LINK_CHANGE",
    0x00020000: "COMPRESSION_CHANGE",
    0x00040000: "ENCRYPTION_CHANGE",
    0x00080000: "OBJECT_ID_CHANGE",
    0x00100000: "REPARSE_POINT_CHANGE",
    0x00200000: "STREAM_CHANGE",
    0x00400000: "TRANSACTED_CHANGE",
    0x80000000: "CLOSE",
}


def reason_to_list(reason: int) -> list:
    """Decode a USN reason bitmask into a list of human flag names."""
    return [name for bit, name in USN_REASON_FLAGS.items() if reason & bit]


# ─────────────────────────────────────────────────────────────────────
# State persistence — usn_tail_state table
# ─────────────────────────────────────────────────────────────────────

def ensure_state_table(db) -> None:
    """Idempotent CREATE for usn_tail_state. Safe to call repeatedly."""
    with db.get_cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS usn_tail_state (
                source_id INTEGER PRIMARY KEY,
                volume_letter TEXT NOT NULL,
                journal_id INTEGER NOT NULL,
                last_seen_usn INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def _load_state(db, source_id: int) -> Optional[dict]:
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT volume_letter, journal_id, last_seen_usn FROM usn_tail_state "
            "WHERE source_id = ?",
            (source_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    # row may be sqlite3.Row or dict_factory dict; both support index access
    return {
        "volume_letter": row["volume_letter"],
        "journal_id": row["journal_id"],
        "last_seen_usn": row["last_seen_usn"],
    }


def _save_state(db, source_id: int, volume_letter: str,
                 journal_id: int, last_seen_usn: int) -> None:
    with db.get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO usn_tail_state (source_id, volume_letter, journal_id, last_seen_usn)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                volume_letter = excluded.volume_letter,
                journal_id = excluded.journal_id,
                last_seen_usn = excluded.last_seen_usn,
                updated_at = CURRENT_TIMESTAMP
            """,
            (source_id, volume_letter, journal_id, last_seen_usn),
        )


# ─────────────────────────────────────────────────────────────────────
# NtfsUsnTailer
# ─────────────────────────────────────────────────────────────────────

# Constants used by the Windows DeviceIoControl path. Defined at module
# level so tests on Linux can reference them without importing ctypes.
FSCTL_QUERY_USN_JOURNAL = 0x000900F4
FSCTL_READ_USN_JOURNAL = 0x000900BB
GENERIC_READ = 0x80000000
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
OPEN_EXISTING = 3
INVALID_HANDLE_VALUE = -1
USN_BUFFER_SIZE = 65536


class NtfsUsnTailer:
    """Tail a volume's USN change journal and forward events to a callback.

    Construction is cheap; ``initialize()`` opens the volume handle and
    queries the journal. Call :meth:`poll_once` from your loop or
    :meth:`run_loop` for a blocking polling thread.

    Always call :meth:`close` when finished — it persists ``last_seen_usn``
    and releases the volume handle.

    On non-Windows ``initialize()`` raises ``NotImplementedError``.
    """

    def __init__(self, db, config: dict, source_id: int, volume_letter: str):
        self.db = db
        self.config = config or {}
        self.source_id = source_id
        # Normalize "C:" / "C:\" / "C:\\" -> "C"
        self.volume_letter = volume_letter.rstrip("\\:").upper()[:1]
        self._handle = None
        self._journal_id = None
        self._last_seen_usn = None
        self._gap_detected = False
        ensure_state_table(self.db)

    # ── Detection ──────────────────────────────────────────────────────

    @staticmethod
    def is_supported(path: str) -> bool:
        """True if path is on a local NTFS volume and we have admin rights.

        Cross-platform safe — returns False on non-Windows.
        """
        if sys.platform != "win32":
            return False
        if not path or path.startswith("\\\\"):
            return False
        try:
            import ctypes
            volume_root = os.path.splitdrive(os.path.abspath(path))[0] + "\\"
            fs_buf = ctypes.create_unicode_buffer(256)
            ok = ctypes.windll.kernel32.GetVolumeInformationW(
                volume_root, None, 0, None, None, None, fs_buf, 256
            )
            if not ok or fs_buf.value.upper() != "NTFS":
                return False
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:
            return False

    # ── Lifecycle ──────────────────────────────────────────────────────

    def initialize(self) -> dict:
        """Open volume, query journal, decide start USN. Returns init result.

        Result keys: journal_id, first_usn, next_usn, last_seen_usn,
        gap_detected (bool), reason (str | None).
        """
        if sys.platform != "win32":
            raise NotImplementedError(
                "NtfsUsnTailer requires Windows (sys.platform=%s)" % sys.platform
            )
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.windll.kernel32
        volume_path = "\\\\.\\%s:" % self.volume_letter
        h = kernel32.CreateFileW(
            volume_path,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            0,
            None,
        )
        if h in (INVALID_HANDLE_VALUE, 0xFFFFFFFFFFFFFFFF):
            err = ctypes.get_last_error()
            raise OSError(err, "CreateFileW(%s) basarisiz" % volume_path)
        self._handle = h

        # USN_JOURNAL_DATA_V0 — first 24 bytes give us journal_id, first_usn, next_usn
        # Layout (V0):
        #   ULL UsnJournalID, USN FirstUsn, USN NextUsn, USN LowestValidUsn,
        #   USN MaxUsn, ULL MaximumSize, ULL AllocationDelta
        out_buf = (ctypes.c_ubyte * 80)()
        returned = wintypes.DWORD(0)
        ok = kernel32.DeviceIoControl(
            h, FSCTL_QUERY_USN_JOURNAL,
            None, 0,
            ctypes.byref(out_buf), ctypes.sizeof(out_buf),
            ctypes.byref(returned),
            None,
        )
        if not ok:
            err = ctypes.get_last_error()
            raise OSError(err, "FSCTL_QUERY_USN_JOURNAL basarisiz")

        import struct
        journal_id, first_usn, next_usn = struct.unpack_from("<QqQ", bytes(out_buf), 0)

        # Decide start USN based on persisted state
        prev = _load_state(self.db, self.source_id)
        gap = False
        reason = None
        if prev is None:
            start_usn = next_usn
            reason = "no_state"
        elif prev["journal_id"] != journal_id:
            start_usn = next_usn
            gap = True
            reason = "journal_recreated"
            logger.warning(
                "USN journal id degisti (kaynak %d): %d -> %d, full rescan gerekli",
                self.source_id, prev["journal_id"], journal_id,
            )
        elif prev["last_seen_usn"] < first_usn:
            start_usn = next_usn
            gap = True
            reason = "usn_overwritten"
            logger.warning(
                "USN journal gap (kaynak %d): last_seen=%d < first=%d, full rescan gerekli",
                self.source_id, prev["last_seen_usn"], first_usn,
            )
        else:
            start_usn = prev["last_seen_usn"]
            reason = "resume"

        self._journal_id = journal_id
        self._last_seen_usn = start_usn
        self._gap_detected = gap

        logger.info(
            "USN tail hazir (kaynak %d, vol=%s, jid=%d, first=%d, next=%d, start=%d, gap=%s)",
            self.source_id, self.volume_letter, journal_id, first_usn,
            next_usn, start_usn, gap,
        )
        return {
            "journal_id": journal_id,
            "first_usn": first_usn,
            "next_usn": next_usn,
            "last_seen_usn": start_usn,
            "gap_detected": gap,
            "reason": reason,
        }

    def close(self) -> None:
        """Persist state and close volume handle. Idempotent."""
        try:
            if self._journal_id is not None and self._last_seen_usn is not None:
                _save_state(
                    self.db, self.source_id, self.volume_letter,
                    self._journal_id, self._last_seen_usn,
                )
        except Exception as e:
            logger.warning("USN state persist hatasi: %s", e)
        if self._handle is not None:
            try:
                import ctypes
                ctypes.windll.kernel32.CloseHandle(self._handle)
            except Exception:
                pass
            self._handle = None

    # ── Polling ────────────────────────────────────────────────────────

    def poll_once(self, callback: Callable[[dict], None]) -> int:
        """Read all pending USN entries, invoke callback per record.

        Returns the number of records dispatched. Non-blocking
        (BytesToWaitFor=0). Safe to call repeatedly.
        """
        if self._handle is None or self._journal_id is None:
            raise RuntimeError("NtfsUsnTailer: initialize() once before poll_once()")

        import ctypes
        import struct
        from ctypes import wintypes

        kernel32 = ctypes.windll.kernel32

        class READ_USN_JOURNAL_DATA_V1(ctypes.Structure):
            _fields_ = [
                ("StartUsn", ctypes.c_int64),
                ("ReasonMask", ctypes.c_uint32),
                ("ReturnOnlyOnClose", ctypes.c_uint32),
                ("Timeout", ctypes.c_uint64),
                ("BytesToWaitFor", ctypes.c_uint64),
                ("UsnJournalID", ctypes.c_uint64),
                ("MinMajorVersion", ctypes.c_uint16),
                ("MaxMajorVersion", ctypes.c_uint16),
            ]

        in_struct = READ_USN_JOURNAL_DATA_V1(
            StartUsn=self._last_seen_usn,
            ReasonMask=0xFFFFFFFF,
            ReturnOnlyOnClose=0,
            Timeout=0,
            BytesToWaitFor=0,
            UsnJournalID=self._journal_id,
            MinMajorVersion=2,
            MaxMajorVersion=3,
        )
        out_buf = (ctypes.c_ubyte * USN_BUFFER_SIZE)()
        returned = wintypes.DWORD(0)

        count = 0
        while True:
            ok = kernel32.DeviceIoControl(
                self._handle, FSCTL_READ_USN_JOURNAL,
                ctypes.byref(in_struct), ctypes.sizeof(in_struct),
                ctypes.byref(out_buf), ctypes.sizeof(out_buf),
                ctypes.byref(returned),
                None,
            )
            if not ok:
                err = ctypes.get_last_error()
                logger.warning("FSCTL_READ_USN_JOURNAL hatasi: %d", err)
                break

            n = returned.value
            if n <= 8:
                # Only the 8-byte next-USN header; no records
                break

            buf_bytes = bytes(out_buf)[:n]
            next_usn = struct.unpack_from("<q", buf_bytes, 0)[0]

            for rec in parse_usn_records(buf_bytes, offset=8):
                event = {
                    "timestamp": time.time(),
                    "source_id": self.source_id,
                    "frn": rec.get("frn"),
                    "parent_frn": rec.get("parent_frn"),
                    "usn": rec.get("usn"),
                    "file_name": rec.get("file_name"),
                    "attributes": rec.get("attributes"),
                    "reason_raw": rec.get("reason", 0),
                    "reason": reason_to_list(rec.get("reason", 0)),
                }
                try:
                    callback(event)
                except Exception as e:
                    logger.debug("USN callback hatasi (%s): %s",
                                 event.get("file_name", "?"), e)
                count += 1

            self._last_seen_usn = next_usn
            in_struct.StartUsn = next_usn

            # If buffer wasn't full, no more pending entries this round.
            if n < USN_BUFFER_SIZE - 1024:
                break

        # Periodic state persist after a busy round
        if count > 0:
            try:
                _save_state(
                    self.db, self.source_id, self.volume_letter,
                    self._journal_id, self._last_seen_usn,
                )
            except Exception as e:
                logger.debug("Periyodik state persist hatasi: %s", e)

        return count

    def run_loop(self, callback: Callable[[dict], None],
                  poll_interval_seconds: float = 1.0,
                  stop_event: Optional[threading.Event] = None) -> None:
        """Continuous tail loop. Blocks until ``stop_event`` is set."""
        if stop_event is None:
            stop_event = threading.Event()
        logger.info("USN tail dongu baslatildi (kaynak %d, interval=%.1fs)",
                    self.source_id, poll_interval_seconds)
        while not stop_event.is_set():
            try:
                self.poll_once(callback)
            except Exception as e:
                logger.error("USN poll hatasi: %s", e)
            stop_event.wait(timeout=poll_interval_seconds)
        logger.info("USN tail dongu durduruldu (kaynak %d)", self.source_id)


__all__ = [
    "NtfsUsnTailer",
    "USN_REASON_FLAGS",
    "reason_to_list",
    "ensure_state_table",
    "FSCTL_QUERY_USN_JOURNAL",
    "FSCTL_READ_USN_JOURNAL",
]
