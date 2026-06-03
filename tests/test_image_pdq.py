"""Tests for optional PDQ image near-duplicate hashing."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.analyzer.image_hash import IMAGE_EXTENSIONS  # noqa: E402
from src.analyzer.image_pdq import (  # noqa: E402
    ImagePdqHasher,
    find_pdq_duplicate_groups,
    hamming_distance,
)


class TestPdqHammingDistance:
    def test_identical(self):
        h = "f" * 64
        assert hamming_distance(h, h) == 0

    def test_single_bit_diff(self):
        h1 = "0" * 64
        h2 = "0" * 63 + "1"
        assert hamming_distance(h1, h2) == 1

    def test_invalid_lengths(self):
        assert hamming_distance("abcd", "abc") == -1
        assert hamming_distance("", "abcd") == -1


class TestImagePdqHasher:
    def test_graceful_noop_when_unavailable(self):
        hasher = ImagePdqHasher({"scanner": {"image_hash_max_mb": 1}})
        hasher._available = False
        assert hasher.available is False
        assert hasher.compute("/tmp/does-not-exist.jpg") is None

    def test_extensions_reused(self):
        assert "jpg" in IMAGE_EXTENSIONS
        assert "png" in IMAGE_EXTENSIONS

    def test_compute_end_to_end_optional(self, tmp_path):
        pytest.importorskip("pdqhash")
        pytest.importorskip("numpy")
        PIL = pytest.importorskip("PIL.Image")

        p = tmp_path / "tiny.png"
        img = PIL.new("RGB", (16, 16), (100, 20, 30))
        img.save(p)

        hasher = ImagePdqHasher({"scanner": {"image_hash_max_mb": 5}})
        out = hasher.compute(str(p))
        assert out is not None
        assert len(out["pdq_hash"]) == 64


class TestFindPdqDuplicateGroups:
    def test_groups_simple(self):
        rows = [
            {"file_path": "a.jpg", "pdq_hash": "0" * 64},
            {"file_path": "b.jpg", "pdq_hash": "0" * 63 + "1"},
            {"file_path": "c.jpg", "pdq_hash": "f" * 64},
        ]
        groups = find_pdq_duplicate_groups(rows, max_distance=1)
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_transitive_union(self):
        rows = [
            {"file_path": "a.jpg", "pdq_hash": "0" * 64},
            {"file_path": "b.jpg", "pdq_hash": "0" * 63 + "1"},
            {"file_path": "c.jpg", "pdq_hash": "0" * 63 + "3"},
            {"file_path": "d.jpg", "pdq_hash": "f" * 64},
        ]
        groups = find_pdq_duplicate_groups(rows, max_distance=1)
        assert len(groups) == 1
        assert len(groups[0]) == 3
