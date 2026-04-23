"""Pytest configuration: ensure the project root is on sys.path.

Tests import the project as ``src.<module>`` rather than relying on an
installed package, so we prepend the repository root to ``sys.path``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
