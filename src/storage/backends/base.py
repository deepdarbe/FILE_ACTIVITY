"""Storage backend protocol.

Phase 1 of issue #114: defines the abstraction that the dashboard query
layer will eventually call instead of hand-rolled SQL. This module is
intentionally tiny — it exists to lock in the contract before Phase 2
introduces the Elasticsearch implementation.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional, Protocol


class StorageBackend(Protocol):
    """Storage abstraction for scanned-file metadata.

    Phase 1 (issue #114): contract definition only. SqliteBackend is the
    sole implementation; Phase 2 will add ElasticsearchBackend; Phase 3+
    refactors the dashboard query layer to call these methods instead of
    raw SQL.
    """

    name: str  # 'sqlite', 'elasticsearch', etc.

    def insert_scanned_files(self, scan_id: int, rows: list[dict]) -> int:
        """Bulk insert. Returns inserted count."""
        ...

    def query_files(
        self,
        scan_id: int,
        filter_dsl: dict,
        limit: int = 1000,
    ) -> list[dict]:
        """Filter on scan; filter_dsl: simple dict like
        {'extension': 'pdf', 'min_size': 1048576}.
        """
        ...

    def aggregate(
        self,
        scan_id: int,
        group_by: str,
        metric: str = "count",
    ) -> list[dict]:
        """Aggregation: group_by in {'extension', 'owner', 'directory_path'},
        metric in {'count', 'sum_size'}.
        """
        ...

    def search_text(
        self,
        scan_id: int,
        query: str,
        limit: int = 100,
    ) -> list[dict]:
        """Full-text search on file_path. Phase 1: LIKE-based. Phase 2 ES
        will use proper text analysis.
        """
        ...

    def delete_scan(self, scan_id: int) -> int:
        """Delete all rows for a scan. Returns deleted count."""
        ...

    def count_scanned_files(self, scan_id: int) -> int:
        ...

    def iterate_scan(
        self,
        scan_id: int,
        batch_size: int = 1000,
    ) -> Iterator[list[dict]]:
        """Yields batches — for migrations + heavy reports."""
        ...

    def health_check(self) -> dict:
        """Returns {'name', 'available', 'details'}."""
        ...
