"""Pluggable scanner backend package.

A scanner backend is any object implementing :class:`ScannerBackend`.
Backends abstract the low level directory walk so ``FileScanner`` can
stay focused on DB writes, progress reporting and analyzer integration
while the raw walk strategy (parallel scandir, MFT, FindFirstFileEx)
varies per target.

Every yielded dict MUST contain at minimum the keys consumed by
``FileScanner.scan_source``:

* ``file_path``       (str)           absolute path
* ``file_name``       (str)           basename
* ``file_size``       (int)           size in bytes
* ``last_modify_time`` (Optional[str]) "YYYY-MM-DD HH:MM:SS"
* ``creation_time``    (Optional[str])
* ``last_access_time`` (Optional[str])
* ``attributes``       (int)           Win32 attribute bitmask (0 on non-Windows)
* ``owner``            (Optional[str]) only populated when ``read_owner`` is set
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Protocol, runtime_checkable


@runtime_checkable
class ScannerBackend(Protocol):
    """Protocol implemented by every scanner backend.

    A backend is initialized with the project configuration dict (typically
    ``config.yaml`` loaded) and exposes ``walk(root)`` which yields one
    metadata dict per file. Implementations should be resilient:
    permission / OS errors on a subtree must be swallowed (ideally logged
    at ``DEBUG``) so a single bad folder cannot abort the whole scan.
    """

    def __init__(self, config: dict) -> None: ...

    def walk(self, root: str) -> Iterator[Dict[str, Any]]:
        """Yield one metadata dict per file under ``root``."""
        ...


__all__ = ["ScannerBackend"]
