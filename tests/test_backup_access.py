"""backup_access: normal-first file access with a SeBackupPrivilege fallback.

The point of these tests is the safety guarantee — for ACCESSIBLE paths the
helpers are byte-for-byte ``os`` behaviour, so wiring them into open_folder /
readers can't break anything. The privileged backup-semantics path is
Windows-only (needs pywin32 + the privilege + an ACL-locked folder) and is
validated on the box; here (Linux CI) ``_HAS_WIN32`` is False so every helper
degrades to plain ``os``. No fastapi needed.
"""

from __future__ import annotations

from src.utils import backup_access as ba


def test_enable_is_safe_and_idempotent():
    # On a box without the privilege / pywin32 this is a no-op returning False;
    # it must never raise and must be safe to call repeatedly.
    r1 = ba.enable_backup_privilege()
    r2 = ba.enable_backup_privilege()
    assert r1 in (True, False)
    assert r2 == r1
    assert ba.available() == r1


def test_accessible_paths_match_os(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    f = d / "a.bin"
    f.write_bytes(b"hello-backup")

    assert ba.exists(str(d)) is True
    assert ba.is_dir(str(d)) is True
    assert ba.is_file(str(d)) is False

    assert ba.exists(str(f)) is True
    assert ba.is_file(str(f)) is True
    assert ba.is_dir(str(f)) is False


def test_missing_path_is_false(tmp_path):
    missing = str(tmp_path / "nope" / "gone")
    assert ba.exists(missing) is False
    assert ba.is_dir(missing) is False
    assert ba.is_file(missing) is False


def test_open_read_returns_content(tmp_path):
    f = tmp_path / "b.bin"
    f.write_bytes(b"0123456789")
    with ba.open_read(str(f)) as fh:
        assert fh.read() == b"0123456789"


def test_open_read_missing_raises(tmp_path):
    import pytest
    with pytest.raises(FileNotFoundError):
        ba.open_read(str(tmp_path / "does_not_exist.bin"))
