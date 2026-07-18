"""CodeQL py/path-injection (#24/#25/#26): AuditExporter.export_range builds its
output filename from the raw start_date/end_date query params.

Before the fix, ``_tag(d) = d[:10]`` kept path separators and ``..`` so a crafted
date could write the .jsonl (and its .sig) outside ``worm_export_dir``. The tag
is now whitelisted to date characters and a realpath containment check guards the
write. These tests pin: (1) traversal input stays contained, (2) normal dates
still produce the intended filename.

Pure-python (no fastapi); exercises the sink directly.
"""

from __future__ import annotations

import os

import pytest

from src.storage.audit_export import AuditExporter
from src.storage.database import Database


@pytest.fixture
def db(tmp_path):
    d = Database({"path": str(tmp_path / "ae.db"),
                  "retention": {"auto_cleanup_on_startup": False}})
    d.connect()
    yield d
    d.close()


def _exporter(db, tmp_path):
    out = tmp_path / "worm"
    return AuditExporter(db, {"audit": {"worm_export_dir": str(out)}}), out


def test_traversal_in_dates_stays_inside_export_dir(db, tmp_path):
    exp, out = _exporter(db, tmp_path)
    # Windows-style and posix-style traversal payloads in both bounds.
    res = exp.export_range(r"..\..\..\evil", "../../../../etc")
    written = os.path.realpath(res["file"])
    root = os.path.realpath(str(out))
    assert written == root or written.startswith(root + os.sep), (
        f"export escaped worm dir: {written} not under {root}")
    # The separators/dots are stripped from the tag entirely.
    assert ".." not in os.path.basename(written)
    assert os.sep not in os.path.basename(written).replace(".jsonl", "")


def test_normal_dates_produce_expected_filename(db, tmp_path):
    exp, out = _exporter(db, tmp_path)
    res = exp.export_range("2026-07-01", "2026-07-19")
    assert os.path.basename(res["file"]) == "audit-2026-07-01-to-2026-07-19.jsonl"
    assert os.path.exists(res["file"])


def test_missing_bounds_tag_as_all(db, tmp_path):
    exp, out = _exporter(db, tmp_path)
    res = exp.export_range(None, None)
    assert os.path.basename(res["file"]) == "audit-all-to-all.jsonl"
