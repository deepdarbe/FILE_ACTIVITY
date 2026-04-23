"""Scanner backends package.

Backends implement the :class:`ScannerBackend` protocol and provide
alternate walk strategies for the file scanner (e.g. ctypes-based Win32
``FindFirstFileExW`` for faster local Windows scans, SMB parallel
backend for multi-threaded UNC scans, etc.).

The :class:`FileScanner` can dispatch to any backend that conforms to
this protocol. The protocol intentionally stays minimal: a backend is
just something that, given a root path, yields file records as dicts.
"""

from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable


@runtime_checkable
class ScannerBackend(Protocol):
    """Protocol all scanner backends implement.

    A backend is initialized with a configuration dictionary (typically
    the project ``config.yaml`` loaded as a dict) and exposes a single
    :meth:`walk` method that yields one dict per file encountered under
    ``root``.

    Yielded dicts should at minimum contain:

    - ``file_path``: absolute path as ``str``
    - ``file_size``: size in bytes as ``int``
    - ``creation_time``: ``"YYYY-MM-DD HH:MM:SS"`` or ``None``
    - ``last_access_time``: ``"YYYY-MM-DD HH:MM:SS"`` or ``None``
    - ``last_modify_time``: ``"YYYY-MM-DD HH:MM:SS"`` or ``None``
    - ``attributes``: raw Win32 attribute bitmask as ``int``
    """

    def __init__(self, config: dict) -> None: ...

    def walk(self, root: str) -> Iterator[dict]: ...


__all__ = ["ScannerBackend"]
