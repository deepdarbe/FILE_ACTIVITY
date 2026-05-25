"""Issue #1 — owner enrichment plumbing.

The actual win32 owner resolution (LookupAccountSid) is Windows-only and
not exercised here; these cover the cross-platform plumbing that makes the
MFT path-only backend stop reporting every owner as "(Bilinmiyor)":
  * bulk_update_file_sizes writes ``owner`` with COALESCE semantics
    (a real owner is set; a None never clobbers an existing one),
  * SizeEnricher honours ``scanner.read_owner`` and only emits an ``owner``
    key on the enrich row when it is enabled.
"""
from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.scanner.size_enricher import SizeEnricher  # noqa: E402
from src.storage.database import Database  # noqa: E402


class _FakeDB:
    """SizeEnricher.__init__/_stat_path never touch the db."""


def _seed(tmp_path):
    db = Database({"path": str(tmp_path / "t.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('s','/x')")
        sid = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'completed')",
            (sid,),
        )
        scan = cur.lastrowid
    return db, sid, scan


def _insert(db, sid, scan, path, owner=None):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scanned_files "
            "(source_id, scan_id, file_path, relative_path, file_name, file_size, owner) "
            "VALUES (?,?,?,?,?,0,?)",
            (sid, scan, path, os.path.basename(path), os.path.basename(path), owner),
        )


def _owner_of(db, scan, path):
    with db.get_read_cursor() as cur:
        cur.execute(
            "SELECT owner FROM scanned_files WHERE scan_id=? AND file_path=?",
            (scan, path),
        )
        return cur.fetchone()["owner"]


def test_bulk_update_sets_owner(tmp_path):
    db, sid, scan = _seed(tmp_path)
    _insert(db, sid, scan, "/x/a.txt", owner=None)
    n = db.bulk_update_file_sizes(
        [{"scan_id": scan, "file_path": "/x/a.txt", "file_size": 10,
          "owner": "DOMAIN\\bob"}]
    )
    assert n == 1
    assert _owner_of(db, scan, "/x/a.txt") == "DOMAIN\\bob"


def test_bulk_update_owner_none_preserves_existing(tmp_path):
    db, sid, scan = _seed(tmp_path)
    _insert(db, sid, scan, "/x/b.txt", owner="ACME\\alice")
    db.bulk_update_file_sizes(
        [{"scan_id": scan, "file_path": "/x/b.txt", "file_size": 20,
          "owner": None}]
    )
    # COALESCE(NULL, owner) keeps the existing owner — a size-only enrich
    # row must never wipe an owner a richer backend already set.
    assert _owner_of(db, scan, "/x/b.txt") == "ACME\\alice"


def test_size_enricher_honours_read_owner_flag():
    on = SizeEnricher({"scanner": {"read_owner": True}}, _FakeDB())
    off = SizeEnricher({"scanner": {"read_owner": False}}, _FakeDB())
    assert on.read_owner is True
    assert off.read_owner is False


def test_stat_path_owner_key_gated_on_read_owner(tmp_path):
    f = tmp_path / "c.txt"
    f.write_text("hi")
    on = SizeEnricher({"scanner": {"read_owner": True}}, _FakeDB())
    row = on._stat_path(str(f))
    # Key present when enabled (value is None off-Windows; win32 fills it
    # on the customer's box).
    assert "owner" in row
    off = SizeEnricher({"scanner": {"read_owner": False}}, _FakeDB())
    assert "owner" not in off._stat_path(str(f))
