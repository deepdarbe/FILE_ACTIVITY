"""Storage backend package (issue #114).

Phase 1 introduced the :class:`StorageBackend` Protocol and the
:class:`SqliteBackend` shim; Phase 2 adds the optional
:class:`ElasticsearchBackend`. The ES backend is exposed via a lazy
factory (``get_elasticsearch_backend``) so importing this package
never pulls in the ``elasticsearch`` client — SQLite-only deployments
must stay deployable without the new dependency.
"""

from __future__ import annotations

from .manager import StorageManager
from .sqlite_backend import SqliteBackend


def get_elasticsearch_backend():  # pragma: no cover - thin lazy loader
    """Lazily import + return the ``ElasticsearchBackend`` class.

    Kept as a function so ``import src.storage.backends`` never
    triggers the ``elasticsearch`` client import. Callers that want
    the class do ``cls = get_elasticsearch_backend(); cls(db, cfg)``;
    the more common path is via :class:`StorageManager`, which lazy-
    imports it itself.
    """
    from .elasticsearch_backend import ElasticsearchBackend

    return ElasticsearchBackend


__all__ = [
    "StorageManager",
    "SqliteBackend",
    "get_elasticsearch_backend",
]
