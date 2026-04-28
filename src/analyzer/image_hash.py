"""Perceptual hash duplicate detection for images (issue #144 Phase 2).

Supports pHash, dHash, and aHash via the optional `imagehash` package
(PyPI, MIT). When `imagehash` is not installed the module degrades
gracefully: `available` is set to False, `compute()` returns None, and
a single WARNING is logged.

Usage::

    hasher = ImageHasher(config)
    if hasher.available:
        result = hasher.compute("/path/to/image.jpg")
        # result = {"phash": "abc123...", "dhash": "...", "ahash": "..."}

Hamming distance helper works without imagehash (pure Python):

    dist = hamming_distance("aabbccdd11223344", "aabbccdd11223355")
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger("file_activity.analyzer.image_hash")

# Image extensions eligible for perceptual hashing.
IMAGE_EXTENSIONS = frozenset(
    {"jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp"}
)

# Default max file size for hashing (200 MB). pHash on a 4 GB TIFF can
# take 5+ minutes; skip gracefully.
_DEFAULT_MAX_MB = 200

# Hamming distance threshold below which two images are "near-duplicate".
_DEFAULT_THRESHOLD = 5


def hamming_distance(hex1: str, hex2: str) -> int:
    """Compute Hamming distance between two hex-encoded perceptual hashes.

    Pure Python — does NOT depend on the `imagehash` package.

    Each hex character encodes 4 bits, so we convert both strings to
    integers, XOR, and count set bits.

    Returns -1 if either string is None/empty or they have different
    lengths (graceful no-match signal rather than an exception).
    """
    if not hex1 or not hex2:
        return -1
    if len(hex1) != len(hex2):
        return -1
    try:
        diff = int(hex1, 16) ^ int(hex2, 16)
    except ValueError:
        return -1
    return bin(diff).count("1")


class ImageHasher:
    """Compute perceptual hashes for a single image file.

    Instantiate once per scan (or share across scans — it is stateless).
    The object lazily probes for `imagehash` on the first `compute()`
    call; repeated failures do NOT spam the log (only one WARNING).

    Args:
        config: Full application config dict (reads
            ``scanner.image_hash_max_mb``).
    """

    def __init__(self, config: Optional[dict] = None):
        cfg = (config or {})
        scanner_cfg = cfg.get("scanner", {}) or {}
        self.max_bytes: int = (
            int(scanner_cfg.get("image_hash_max_mb", _DEFAULT_MAX_MB)) * 1_048_576
        )
        # Tri-state: None = not yet probed, True/False = result.
        self._available: Optional[bool] = None

    @property
    def available(self) -> bool:
        """True if imagehash + PIL are importable."""
        if self._available is None:
            self._probe()
        return bool(self._available)

    def _probe(self) -> None:
        """Lazily check whether imagehash is importable."""
        try:
            import imagehash  # noqa: F401
            from PIL import Image  # noqa: F401
            self._available = True
        except ImportError as e:
            self._available = False
            logger.warning(
                "imagehash/Pillow yuklu degil — perceptual hash devre disi. "
                "Yuklemek icin: pip install imagehash>=4.3. Hata: %s", e,
            )

    def compute(self, file_path: str) -> Optional[dict]:
        """Compute pHash, dHash, and aHash for one image file.

        Returns:
            ``{"phash": "<16-char hex>", "dhash": "<16-char hex>",
               "ahash": "<16-char hex>"}`` on success, or ``None`` if
            imagehash is unavailable, the file is too large, or any
            error occurs.
        """
        if not self.available:
            return None

        # Size guard (skip huge TIFFs etc.)
        try:
            fsize = os.path.getsize(file_path)
        except OSError:
            return None
        if fsize > self.max_bytes:
            logger.debug(
                "image_hash: skip (too large %.1f MB > %.0f MB): %s",
                fsize / 1_048_576, self.max_bytes / 1_048_576, file_path,
            )
            return None

        try:
            import imagehash
            from PIL import Image

            with Image.open(file_path) as img:
                ph = str(imagehash.phash(img))
                dh = str(imagehash.dhash(img))
                ah = str(imagehash.average_hash(img))

            return {"phash": ph, "dhash": dh, "ahash": ah}

        except Exception as e:
            logger.debug("image_hash compute error %s: %s", file_path, e)
            return None


# ---------------------------------------------------------------------------
# Clustering helper
# ---------------------------------------------------------------------------

def find_duplicate_groups(
    rows: list[dict],
    hash_type: str = "phash",
    max_distance: int = _DEFAULT_THRESHOLD,
) -> list[list[dict]]:
    """Cluster image hash rows into near-duplicate groups.

    Args:
        rows: List of dicts with at least ``{"file_id": ...,
            "file_path": ..., "<hash_type>": "<hex>", ...}`` keys.
        hash_type: Which hash column to compare (``phash``, ``dhash``,
            or ``ahash``).
        max_distance: Maximum Hamming distance to consider a match
            (inclusive). Default 5.

    Returns:
        List of groups, each group being a list of row-dicts whose
        mutual Hamming distance is ≤ ``max_distance``. Singletons are
        excluded.

    Algorithm:
        Naïve O(n²) — sufficient for production image counts up to
        ~10k. BK-tree optimisation is deferred (Phase 3).
    """
    if not rows:
        return []

    max_distance = max(0, int(max_distance))

    # Filter out rows that lack the requested hash.
    valid = [r for r in rows if r.get(hash_type)]
    n = len(valid)

    # Union-Find for grouping
    parent = list(range(n))

    def _find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(a: int, b: int) -> None:
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        hi = valid[i][hash_type]
        for j in range(i + 1, n):
            hj = valid[j][hash_type]
            dist = hamming_distance(hi, hj)
            if 0 <= dist <= max_distance:
                _union(i, j)

    # Collect groups
    buckets: dict[int, list[dict]] = {}
    for i in range(n):
        root = _find(i)
        buckets.setdefault(root, []).append(valid[i])

    return [members for members in buckets.values() if len(members) >= 2]
