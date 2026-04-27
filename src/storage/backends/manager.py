"""Storage backend factory + holder.

Phase 1 of issue #114: introduce ``StorageManager`` so the dashboard
has a single attach point (``app.state.storage``) for the active
backend. Phase 3 will rewire dashboard endpoints to call backend
methods through this manager instead of issuing SQL against ``db``
directly.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("file_activity.storage.manager")


class StorageManager:
    """Factory + holder for the active storage backend.

    Owned by ``app.state.storage`` in the dashboard. Phase 3 will wire
    dashboard endpoints to read from this instead of ``db.execute(...)``.

    The active backend is selected from ``config.storage.backend``;
    default is ``"sqlite"``. Unknown backends raise ``ValueError``;
    Phase 2's ``"elasticsearch"`` raises ``NotImplementedError`` (the
    contract is here, the impl isn't).
    """

    def __init__(self, db: Any, config: dict) -> None:
        storage_cfg = (config or {}).get("storage") or {}
        backend_name = storage_cfg.get("backend", "sqlite")

        if backend_name == "sqlite":
            # Local import keeps Phase 2's ES dependency optional —
            # importing ``manager`` must never pull in elasticsearch.
            from .sqlite_backend import SqliteBackend

            self.backend = SqliteBackend(db, config or {})
        elif backend_name == "elasticsearch":
            raise NotImplementedError(
                "Elasticsearch backend lands in #114 Phase 2"
            )
        else:
            raise ValueError(
                f"Unknown storage.backend: {backend_name!r}"
            )

        self.name = self.backend.name
        logger.info("StorageManager: backend=%s", self.name)

    def __getattr__(self, name: str) -> Any:
        # Delegate everything else to the backend so callers can do
        # ``app.state.storage.query_files(...)`` without unwrapping.
        # ``__getattr__`` is only consulted for missing attributes, so
        # ``self.backend`` / ``self.name`` resolve normally.
        return getattr(self.backend, name)
