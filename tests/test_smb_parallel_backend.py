"""Tests for :class:`src.scanner.backends.smb_parallel.SmbParallelBackend`."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

# Allow running `python -m unittest tests.test_smb_parallel_backend` from repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.scanner.backends.smb_parallel import SmbParallelBackend  # noqa: E402


def _make_tree(root: Path) -> set[str]:
    """Create a tree with files at varying depths. Returns the set of
    absolute paths that should be yielded by a full walk."""
    expected: set[str] = set()

    # Depth 0: file directly under root.
    (root / "top.txt").write_text("top")
    expected.add(str(root / "top.txt"))

    # Depth 1: files under subdirectories.
    for i in range(3):
        sub = root / f"sub_{i}"
        sub.mkdir()
        for j in range(4):
            f = sub / f"file_{j}.txt"
            f.write_text(f"{i}-{j}")
            expected.add(str(f))

        # Depth 2: files in nested subdirectory.
        nested = sub / "nested"
        nested.mkdir()
        for k in range(2):
            f = nested / f"deep_{k}.txt"
            f.write_text(f"{i}-{k}")
            expected.add(str(f))

        # Depth 3: even deeper, verifies we don't cap recursion.
        deeper = nested / "x" / "y"
        deeper.mkdir(parents=True)
        f = deeper / "bottom.txt"
        f.write_text("bottom")
        expected.add(str(f))

    return expected


class SmbParallelBackendTests(unittest.TestCase):
    def test_yields_every_file(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            expected = _make_tree(root)

            backend = SmbParallelBackend({"scanner": {
                "smb_workers": 4,
                "exclude_patterns": [],
                "skip_hidden": False,
                "skip_system": False,
                "read_owner": False,
            }})

            got = {rec["file_path"] for rec in backend.walk(str(root))}
            # On macOS/WSL os.scandir may surface paths with or without
            # trailing normalization quirks — compare as sets of realpaths.
            self.assertEqual(
                {os.path.realpath(p) for p in got},
                {os.path.realpath(p) for p in expected},
            )

    def test_skips_hidden_files_when_flag_set(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)

            visible = root / "visible.txt"
            visible.write_text("v")

            # POSIX hidden convention: leading dot. The backend honors this
            # on non-Windows (on Windows the check reads FILE_ATTRIBUTE_HIDDEN
            # from st_file_attributes, which isn't meaningfully settable in a
            # portable test).
            hidden = root / ".hidden.txt"
            hidden.write_text("h")

            backend = SmbParallelBackend({"scanner": {
                "smb_workers": 2,
                "exclude_patterns": [],
                "skip_hidden": True,
                "skip_system": True,
                "read_owner": False,
            }})

            got = {rec["file_name"] for rec in backend.walk(str(root))}
            self.assertIn("visible.txt", got)
            self.assertNotIn(".hidden.txt", got)

    def test_exclude_patterns(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            (root / "keep.txt").write_text("k")
            (root / "drop.tmp").write_text("d")
            sub = root / "sub"
            sub.mkdir()
            (sub / "nested.tmp").write_text("n")
            (sub / "nested.keep").write_text("n")

            backend = SmbParallelBackend({"scanner": {
                "smb_workers": 2,
                "exclude_patterns": ["*.tmp"],
                "skip_hidden": False,
                "skip_system": False,
                "read_owner": False,
            }})

            names = {rec["file_name"] for rec in backend.walk(str(root))}
            self.assertEqual(names, {"keep.txt", "nested.keep"})


if __name__ == "__main__":
    unittest.main()
