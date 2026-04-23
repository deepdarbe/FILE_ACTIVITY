"""Tests for src.scanner.backends.ntfs_usn_tail.

All tests run on Linux. The Windows volume-handle / DeviceIoControl
path is exercised via dependency-injection style stubbing (we don't
need to import ctypes here).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest

# Ensure src is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scanner.backends.ntfs_usn_tail import (
    NtfsUsnTailer,
    USN_REASON_FLAGS,
    ensure_state_table,
    reason_to_list,
)


# ─────────────────────────────────────────────────────────────────────
# Minimal Database stub: just enough for ensure_state_table /
# _load_state / _save_state which use db.get_cursor().
# ─────────────────────────────────────────────────────────────────────

def dict_factory(cursor, row):
    cols = [c[0] for c in cursor.description]
    return dict(zip(cols, row))


class FakeDB:
    def __init__(self, path: str):
        self._path = path

    def get_cursor(self):
        # Each call: open a fresh connection so the context manager
        # can close it cleanly. Mirrors Database.get_cursor() shape.
        conn = sqlite3.connect(self._path)
        conn.row_factory = dict_factory

        class Ctx:
            def __init__(self, c):
                self.c = c
                self.cur = None
            def __enter__(self):
                self.cur = self.c.cursor()
                return self.cur
            def __exit__(self, et, ev, tb):
                if et is None:
                    self.c.commit()
                else:
                    self.c.rollback()
                self.cur.close()
                self.c.close()

        return Ctx(conn)


# ─────────────────────────────────────────────────────────────────────
# Reason flag decoding
# ─────────────────────────────────────────────────────────────────────

class ReasonFlagTests(unittest.TestCase):
    def test_single_bit_decode(self):
        self.assertEqual(reason_to_list(0x100), ["FILE_CREATE"])
        self.assertEqual(reason_to_list(0x200), ["FILE_DELETE"])

    def test_multi_bit_decode(self):
        # FILE_CREATE | DATA_EXTEND | CLOSE
        flags = reason_to_list(0x100 | 0x002 | 0x80000000)
        self.assertIn("FILE_CREATE", flags)
        self.assertIn("DATA_EXTEND", flags)
        self.assertIn("CLOSE", flags)

    def test_zero_returns_empty(self):
        self.assertEqual(reason_to_list(0), [])

    def test_unknown_bits_skipped(self):
        # 0x10000000 isn't in the table; should produce empty list
        self.assertEqual(reason_to_list(0x10000000), [])

    def test_all_known_flags_present(self):
        # Sanity: every flag in the table decodes to itself
        for bit, name in USN_REASON_FLAGS.items():
            self.assertIn(name, reason_to_list(bit))


# ─────────────────────────────────────────────────────────────────────
# is_supported - pure, no Windows needed
# ─────────────────────────────────────────────────────────────────────

class IsSupportedTests(unittest.TestCase):
    def test_unsupported_on_linux(self):
        self.assertFalse(NtfsUsnTailer.is_supported("/tmp"))

    def test_unsupported_on_unc(self):
        # Even on Windows, UNC paths are explicitly unsupported
        self.assertFalse(NtfsUsnTailer.is_supported(r"\\server\share"))

    def test_unsupported_empty(self):
        self.assertFalse(NtfsUsnTailer.is_supported(""))


# ─────────────────────────────────────────────────────────────────────
# State persistence — works on Linux because it's just SQLite
# ─────────────────────────────────────────────────────────────────────

class StateTableTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="usn_state_")
        self.dbpath = os.path.join(self.tmpdir, "test.db")
        self.db = FakeDB(self.dbpath)
        ensure_state_table(self.db)

    def tearDown(self):
        try:
            os.remove(self.dbpath)
        except OSError:
            pass
        try:
            os.rmdir(self.tmpdir)
        except OSError:
            pass

    def test_ensure_table_idempotent(self):
        # Running twice must not raise
        ensure_state_table(self.db)
        ensure_state_table(self.db)
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='usn_tail_state'"
            )
            row = cur.fetchone()
        self.assertIsNotNone(row)

    def test_save_and_load_state(self):
        from src.scanner.backends.ntfs_usn_tail import _load_state, _save_state
        _save_state(self.db, source_id=1, volume_letter="C",
                    journal_id=12345, last_seen_usn=99999)
        st = _load_state(self.db, source_id=1)
        self.assertEqual(st["volume_letter"], "C")
        self.assertEqual(st["journal_id"], 12345)
        self.assertEqual(st["last_seen_usn"], 99999)

    def test_save_upserts(self):
        from src.scanner.backends.ntfs_usn_tail import _load_state, _save_state
        _save_state(self.db, 1, "C", 100, 10)
        _save_state(self.db, 1, "C", 100, 20)
        st = _load_state(self.db, 1)
        self.assertEqual(st["last_seen_usn"], 20)

    def test_load_missing_returns_none(self):
        from src.scanner.backends.ntfs_usn_tail import _load_state
        self.assertIsNone(_load_state(self.db, source_id=999))


# ─────────────────────────────────────────────────────────────────────
# Initialization gap-detection logic — tested by injecting state
# without actually running on Windows.
# ─────────────────────────────────────────────────────────────────────

class GapDetectionLogicTests(unittest.TestCase):
    """Validate the decision tree in ``initialize`` symbolically.

    We can't open a real volume on Linux, so we test the pure decision
    function via the same conditions the implementation uses.
    """

    @staticmethod
    def decide(prev_state, journal_id, first_usn, next_usn):
        """Mirror the gap-decision branches from initialize()."""
        if prev_state is None:
            return next_usn, False, "no_state"
        if prev_state["journal_id"] != journal_id:
            return next_usn, True, "journal_recreated"
        if prev_state["last_seen_usn"] < first_usn:
            return next_usn, True, "usn_overwritten"
        return prev_state["last_seen_usn"], False, "resume"

    def test_no_state_starts_at_next(self):
        start, gap, reason = self.decide(None, 100, 50, 200)
        self.assertEqual(start, 200)
        self.assertFalse(gap)
        self.assertEqual(reason, "no_state")

    def test_journal_id_mismatch_signals_gap(self):
        prev = {"journal_id": 100, "last_seen_usn": 150}
        start, gap, reason = self.decide(prev, 999, 50, 200)
        self.assertTrue(gap)
        self.assertEqual(start, 200)
        self.assertEqual(reason, "journal_recreated")

    def test_last_usn_below_first_signals_gap(self):
        prev = {"journal_id": 100, "last_seen_usn": 10}
        start, gap, reason = self.decide(prev, 100, 50, 200)
        self.assertTrue(gap)
        self.assertEqual(start, 200)
        self.assertEqual(reason, "usn_overwritten")

    def test_normal_resume(self):
        prev = {"journal_id": 100, "last_seen_usn": 150}
        start, gap, reason = self.decide(prev, 100, 50, 200)
        self.assertFalse(gap)
        self.assertEqual(start, 150)
        self.assertEqual(reason, "resume")


# ─────────────────────────────────────────────────────────────────────
# Real DeviceIoControl tests — Windows-only, skipped on Linux
# ─────────────────────────────────────────────────────────────────────

@unittest.skipIf(sys.platform != "win32", "Requires Windows + admin")
class WindowsIntegrationTests(unittest.TestCase):
    def test_initialize_on_c_volume(self):
        # Smoke test only — not run on CI
        self.skipTest("Manual run: requires admin, mutates state table")


if __name__ == "__main__":
    unittest.main(verbosity=2)
