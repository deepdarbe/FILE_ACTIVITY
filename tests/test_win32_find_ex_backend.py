"""Tests for Win32FindExBackend.

The backend is only exercised on Windows (ctypes calls kernel32).
On other platforms the module is still import-safe, and we verify
that instantiation raises RuntimeError with a clear message.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Cross-platform import safety
# ---------------------------------------------------------------------------


def test_module_import_is_cross_platform() -> None:
    """Module must import cleanly on any platform (ctypes loaded lazily)."""
    import src.scanner.backends.win32_find_ex as mod  # noqa: F401

    assert hasattr(mod, "Win32FindExBackend")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Non-Windows guard only checked on non-Windows hosts",
)
def test_non_windows_init_raises() -> None:
    """On non-Windows, constructor must raise RuntimeError."""
    from src.scanner.backends.win32_find_ex import Win32FindExBackend

    with pytest.raises(RuntimeError, match="Windows"):
        Win32FindExBackend({})


# ---------------------------------------------------------------------------
# Windows-only functional tests
# ---------------------------------------------------------------------------

pytestmark_win = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Win32FindExBackend only runs on Windows",
)


@pytest.fixture
def populated_tree(tmp_path: Path) -> tuple[Path, set[str], Path]:
    """Create ~50 files at 3 depth levels, plus one long-path directory.

    Returns
    -------
    (root, expected_file_paths, long_dir)
    """
    root = tmp_path / "scan_root"
    root.mkdir()

    expected: set[str] = set()

    # Level 1: 10 files directly under root
    for i in range(10):
        p = root / f"l1_file_{i:02d}.txt"
        p.write_text(f"l1-{i}")
        expected.add(str(p))

    # Level 2: 4 subdirs × 5 files = 20
    for d in range(4):
        sub = root / f"sub_{d}"
        sub.mkdir()
        for i in range(5):
            p = sub / f"l2_file_{i}.txt"
            p.write_text(f"l2-{d}-{i}")
            expected.add(str(p))

        # Level 3 under each sub: 5 files
        sub3 = sub / "nested"
        sub3.mkdir()
        for i in range(5):
            p = sub3 / f"l3_file_{i}.txt"
            p.write_text(f"l3-{d}-{i}")
            expected.add(str(p))

    # Long-path directory (component name pushes total path near MAX_PATH)
    long_name = "L" * 200
    long_dir = root / long_name
    try:
        long_dir.mkdir()
        long_file = long_dir / "deep.txt"
        long_file.write_text("long")
        expected.add(str(long_file))
    except OSError:
        # Filesystem may still reject; not fatal for the rest of the test.
        long_dir = root

    return root, expected, long_dir


@pytestmark_win
def test_walk_yields_all_files(populated_tree) -> None:
    from src.scanner.backends.win32_find_ex import Win32FindExBackend

    root, expected, _ = populated_tree
    backend = Win32FindExBackend({})

    found = {os.path.normpath(r["file_path"]) for r in backend.walk(str(root))}
    expected_norm = {os.path.normpath(p) for p in expected}

    missing = expected_norm - found
    assert not missing, f"Backend missed {len(missing)} files, e.g. {list(missing)[:3]}"


@pytestmark_win
def test_walk_parses_attributes(tmp_path: Path) -> None:
    """Regular files must have DIRECTORY bit clear; backend never yields dirs."""
    from src.scanner.backends.win32_find_ex import (
        FILE_ATTRIBUTE_DIRECTORY,
        Win32FindExBackend,
    )

    root = tmp_path / "attr_root"
    root.mkdir()
    (root / "plain.txt").write_text("hi")
    (root / "nested_dir").mkdir()
    (root / "nested_dir" / "inner.txt").write_text("x")

    backend = Win32FindExBackend({})
    records = list(backend.walk(str(root)))

    # Backend yields files only, never the directory entry itself.
    names = {os.path.basename(r["file_path"]) for r in records}
    assert "plain.txt" in names
    assert "inner.txt" in names
    assert "nested_dir" not in names

    for r in records:
        assert r["attributes"] & FILE_ATTRIBUTE_DIRECTORY == 0, (
            f"File yielded with DIRECTORY bit set: {r}"
        )
        assert isinstance(r["file_size"], int)
        assert r["file_size"] >= 0


@pytestmark_win
def test_walk_parses_timestamps(tmp_path: Path) -> None:
    """FILETIME conversion should produce parseable YYYY-MM-DD HH:MM:SS."""
    import re

    from src.scanner.backends.win32_find_ex import Win32FindExBackend

    root = tmp_path / "ts_root"
    root.mkdir()
    (root / "a.txt").write_text("a")

    backend = Win32FindExBackend({})
    records = list(backend.walk(str(root)))
    assert records, "expected at least one record"

    pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    r = records[0]
    for key in ("creation_time", "last_access_time", "last_modify_time"):
        val = r[key]
        assert val is None or pattern.match(val), (
            f"{key} not in expected format: {val!r}"
        )


@pytestmark_win
def test_walk_empty_directory(tmp_path: Path) -> None:
    from src.scanner.backends.win32_find_ex import Win32FindExBackend

    empty = tmp_path / "empty"
    empty.mkdir()

    backend = Win32FindExBackend({})
    records = list(backend.walk(str(empty)))
    assert records == []


@pytestmark_win
def test_walk_missing_directory_is_silent(tmp_path: Path) -> None:
    """Missing path should log + continue, not raise."""
    from src.scanner.backends.win32_find_ex import Win32FindExBackend

    missing = tmp_path / "does_not_exist"
    backend = Win32FindExBackend({})
    # Should simply yield nothing.
    assert list(backend.walk(str(missing))) == []
