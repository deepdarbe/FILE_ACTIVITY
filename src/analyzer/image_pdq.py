"""PDQ perceptual hash duplicate detection for images (near-dup upgrade).

Supports Facebook PDQ via optional `pdqhash` package. When `pdqhash`
is not installed the module degrades gracefully: `available` is False,
`compute()` returns None, and a single WARNING is logged.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from src.analyzer.image_hash import IMAGE_EXTENSIONS

logger = logging.getLogger("file_activity.analyzer.image_pdq")

_DEFAULT_MAX_MB = 200
_DEFAULT_THRESHOLD = 64


def hamming_distance(hex1: str, hex2: str) -> int:
    """Compute Hamming distance between two hex-encoded PDQ hashes."""
    if not hex1 or not hex2:
        return -1
    if len(hex1) != len(hex2):
        return -1
    try:
        diff = int(hex1, 16) ^ int(hex2, 16)
    except ValueError:
        return -1
    return bin(diff).count("1")


class ImagePdqHasher:
    """Compute PDQ perceptual hash for a single image file."""

    def __init__(self, config: Optional[dict] = None):
        cfg = (config or {})
        if hasattr(cfg, "get") and not isinstance(cfg, dict):
            max_mb = cfg.get("scanner", "image_hash_max_mb", _DEFAULT_MAX_MB)
        else:
            scanner_cfg = cfg.get("scanner", {}) or {}
            max_mb = scanner_cfg.get("image_hash_max_mb", _DEFAULT_MAX_MB)
        self.max_bytes: int = int(max_mb) * 1_048_576
        self._available: Optional[bool] = None

    @property
    def available(self) -> bool:
        """True if pdqhash + PIL + numpy are importable."""
        if self._available is None:
            self._probe()
        return bool(self._available)

    def _probe(self) -> None:
        """Lazily check whether pdqhash is importable."""
        try:
            import pdqhash  # type: ignore
            from PIL import Image  # type: ignore
            import numpy as np  # type: ignore # noqa: F401

            self._available = True
        except ImportError as e:
            self._available = False
            logger.warning(
                "pdqhash/Pillow yuklu degil — PDQ hash devre disi. "
                "Yuklemek icin: pip install pdqhash>=0.2. Hata: %s", e,
            )

    def compute(self, file_path: str) -> Optional[dict]:
        """Compute PDQ hash for one image file."""
        if not self.available:
            return None

        try:
            fsize = os.path.getsize(file_path)
        except OSError:
            return None
        if fsize > self.max_bytes:
            logger.debug(
                "image_pdq: skip (too large %.1f MB > %.0f MB): %s",
                fsize / 1_048_576, self.max_bytes / 1_048_576, file_path,
            )
            return None

        try:
            import numpy as np
            import pdqhash
            from PIL import Image

            with Image.open(file_path) as img:
                bits, _quality = pdqhash.compute(np.array(img.convert("RGB")))
                bit_string = "".join(str(int(b)) for b in bits)
                pdq_hex = f"{int(bit_string, 2):064x}"
            return {"pdq_hash": pdq_hex}
        except Exception as e:
            logger.debug("image_pdq compute error %s: %s", file_path, e)
            return None


def find_pdq_duplicate_groups(
    rows: list[dict],
    max_distance: int = _DEFAULT_THRESHOLD,
) -> list[list[dict]]:
    """Cluster PDQ rows into near-duplicate groups via union-find."""
    if not rows:
        return []

    max_distance = max(0, int(max_distance))
    valid = [r for r in rows if r.get("pdq_hash")]
    n = len(valid)

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
        hi = valid[i]["pdq_hash"]
        for j in range(i + 1, n):
            hj = valid[j]["pdq_hash"]
            dist = hamming_distance(hi, hj)
            if 0 <= dist <= max_distance:
                _union(i, j)

    buckets: dict[int, list[dict]] = {}
    for i in range(n):
        root = _find(i)
        buckets.setdefault(root, []).append(valid[i])

    return [members for members in buckets.values() if len(members) >= 2]
