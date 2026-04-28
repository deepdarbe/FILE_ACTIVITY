"""Elasticsearch-backed implementation of :class:`StorageBackend`.

Phase 2 of issue #114. Mirrors the validation discipline of
:mod:`sqlite_backend` and shares its whitelists by importing them —
same DSL, same validation, two backends. Phase 3 will rewire the
dashboard query layer to call the active backend through
:class:`StorageManager`; Phase 4 ships the SQLite -> ES backfill
tool. This module is intentionally narrow: it does not touch the
dashboard, does not auto-migrate, and does not refresh indices in
production paths (refresh is a per-shard cluster-load anti-pattern at
scale — tests can call ``client.indices.refresh(...)`` explicitly).

Index layout (see :mod:`es_mapping`):

    scanned_files-{source_id}-{scan_id}

The Protocol scopes by ``scan_id`` only, so the backend resolves
``source_id`` through the SQLite :class:`Database` instance passed to
the constructor. That keeps the Protocol surface unchanged across
backends; Phase 3 callers continue to pass a single ``scan_id``.
"""

from __future__ import annotations

import copy
import logging
import time
from typing import Any, Iterator

from .es_mapping import INDEX_BODY, index_name
from .sqlite_backend import (
    _ALLOWED_FILTER_KEYS,
    _ALLOWED_GROUP_BY,
    _ALLOWED_METRICS,
)

logger = logging.getLogger("file_activity.storage.elasticsearch_backend")


# Lazy-imported at construction time so importing this module on a
# host without the ``elasticsearch`` client doesn't blow up at import
# (e.g. tooling that only needs ``index_name``). The constructor will
# raise a clean ImportError if the dep is missing.
_ES_IMPORT_ERROR: Exception | None = None
try:  # pragma: no cover - import guard
    from elasticsearch import Elasticsearch  # type: ignore
    from elasticsearch import helpers as es_helpers  # type: ignore
except Exception as _e:  # pragma: no cover - import guard
    Elasticsearch = None  # type: ignore[assignment]
    es_helpers = None  # type: ignore[assignment]
    _ES_IMPORT_ERROR = _e


class ElasticsearchBackend:
    """Elasticsearch implementation of :class:`StorageBackend`.

    Constructor takes the same SQLite ``Database`` the SqliteBackend
    takes — used for ``source_id`` lookup so the Protocol can keep
    its ``scan_id``-only signature. This is intentional: callers must
    not be forced to thread ``source_id`` through every read.
    """

    name = "elasticsearch"

    def __init__(self, db: Any, cfg: dict) -> None:
        if Elasticsearch is None:
            raise ImportError(
                "elasticsearch client not installed. "
                "pip install -r requirements-elastic.txt "
                f"(underlying error: {_ES_IMPORT_ERROR!r})"
            )
        self.db = db
        self.config = cfg or {}
        es_cfg = ((self.config.get("storage") or {}).get("elasticsearch")) or {}

        hosts = es_cfg.get("hosts") or ["http://localhost:9200"]
        api_key = es_cfg.get("api_key")
        verify_certs = bool(es_cfg.get("verify_certs", True))
        request_timeout = int(es_cfg.get("request_timeout", 30))

        client_kwargs: dict[str, Any] = {
            "hosts": hosts,
            "verify_certs": verify_certs,
            "request_timeout": request_timeout,
        }
        if api_key:
            client_kwargs["api_key"] = api_key

        self.client = Elasticsearch(**client_kwargs)
        # Cache resolved source_ids per scan_id so we don't hit SQLite
        # on every read. scan_id -> source_id is immutable for a given
        # scan, so the cache never goes stale.
        self._source_id_cache: dict[int, int] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_source_id(self, scan_id: int) -> int:
        """Resolve ``source_id`` for ``scan_id`` via the SQLite catalog.

        Callers pass only ``scan_id`` (Protocol contract); ES indices
        are partitioned by ``(source_id, scan_id)``, so we look up the
        owning source via ``scan_runs``.
        """
        cached = self._source_id_cache.get(scan_id)
        if cached is not None:
            return cached
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT source_id FROM scan_runs WHERE id = ?",
                (scan_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"ElasticsearchBackend: unknown scan_id={scan_id} "
                    f"(no row in scan_runs)"
                )
            source_id = int(row["source_id"])
        self._source_id_cache[scan_id] = source_id
        return source_id

    def _index_for(self, scan_id: int) -> str:
        return index_name(self._resolve_source_id(scan_id), scan_id)

    def _ensure_index(self, index: str) -> None:
        """Create the index with the canonical mapping if it doesn't
        exist. Cheap idempotent call — ES will 400 if it already
        exists, which we swallow."""
        try:
            if not self.client.indices.exists(index=index):
                body = copy.deepcopy(INDEX_BODY)
                self.client.indices.create(index=index, body=body)
        except Exception as e:  # pragma: no cover - race-conditional
            # Race: another writer created it between exists() and
            # create(). Idempotent — log and continue.
            logger.debug("index create race for %s: %s", index, e)

    def _build_filter_query(
        self, scan_id: int, filter_dsl: dict
    ) -> dict:
        """Translate a whitelisted SQLite-style filter DSL to an ES
        ``bool/filter`` query. Validation matches the SqliteBackend
        exactly — same allowlist, same error message style."""
        if filter_dsl is None:
            filter_dsl = {}
        unknown = set(filter_dsl) - _ALLOWED_FILTER_KEYS
        if unknown:
            raise ValueError(
                f"query_files: unsupported filter keys: {sorted(unknown)}. "
                f"Allowed: {sorted(_ALLOWED_FILTER_KEYS)}"
            )

        filters: list[dict] = [{"term": {"scan_id": int(scan_id)}}]

        if "extension" in filter_dsl:
            filters.append({"term": {"extension": filter_dsl["extension"]}})
        if "owner" in filter_dsl:
            filters.append({"term": {"owner": filter_dsl["owner"]}})
        size_range: dict[str, Any] = {}
        if "min_size" in filter_dsl:
            size_range["gte"] = filter_dsl["min_size"]
        if "max_size" in filter_dsl:
            size_range["lte"] = filter_dsl["max_size"]
        if size_range:
            filters.append({"range": {"size_bytes": size_range}})
        mtime_range: dict[str, Any] = {}
        if "min_mtime" in filter_dsl:
            mtime_range["gte"] = filter_dsl["min_mtime"]
        if "max_mtime" in filter_dsl:
            mtime_range["lte"] = filter_dsl["max_mtime"]
        if mtime_range:
            filters.append({"range": {"mtime": mtime_range}})
        if "directory_prefix" in filter_dsl:
            # Use prefix on the keyword sub-field so we don't get
            # token-level prefix surprises.
            filters.append(
                {
                    "prefix": {
                        "file_path.keyword": str(filter_dsl["directory_prefix"])
                    }
                }
            )

        return {"bool": {"filter": filters}}

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def insert_scanned_files(self, scan_id: int, rows: list[dict]) -> int:
        """Bulk insert via ``helpers.bulk`` with retry + exponential
        backoff. Returns successful insert count.

        ``raise_on_error=False`` — partial failures don't poison the
        whole batch; the caller gets the success count and we log
        the rest.
        """
        if not rows:
            return 0
        index = self._index_for(scan_id)
        self._ensure_index(index)

        actions = []
        for r in rows:
            doc = {
                "scan_id": int(scan_id),
                "source_id": int(
                    r.get("source_id") or self._resolve_source_id(scan_id)
                ),
                "file_path": r.get("file_path"),
                "extension": r.get("extension"),
                "owner": r.get("owner"),
                "size_bytes": r.get("file_size") or r.get("size_bytes") or 0,
                "mtime": r.get("last_modify_time") or r.get("mtime"),
                "directory_path": r.get("directory_path")
                or self._derive_directory(r),
            }
            actions.append({"_index": index, "_source": doc})

        # Retry with backoff: 1s, 2s, 4s. ``helpers.bulk`` already
        # batches; this loop wraps the whole call for transient
        # connectivity hiccups.
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                success, errors = es_helpers.bulk(
                    self.client,
                    actions,
                    refresh="false",
                    raise_on_error=False,
                    raise_on_exception=False,
                )
                if errors:
                    logger.warning(
                        "bulk insert had %d errors (sample: %s)",
                        len(errors),
                        errors[:1],
                    )
                return int(success)
            except Exception as e:  # pragma: no cover - network path
                last_err = e
                logger.warning(
                    "bulk insert attempt %d failed: %s", attempt + 1, e
                )
                time.sleep(2 ** attempt)
        # All retries exhausted.
        raise RuntimeError(
            f"ElasticsearchBackend.insert_scanned_files failed: {last_err!r}"
        )

    @staticmethod
    def _derive_directory(row: dict) -> str | None:
        """Best-effort directory_path derivation when the producer
        didn't supply it. SQLite shim does the same trick at query
        time; we precompute on write so ES aggregations can group_by
        ``directory_path`` as a keyword."""
        path = row.get("file_path")
        name = row.get("file_name")
        if not path:
            return None
        if name and path.endswith(name):
            return path[: len(path) - len(name)]
        # Fallback: trim after the last separator.
        for sep in ("\\", "/"):
            idx = path.rfind(sep)
            if idx >= 0:
                return path[: idx + 1]
        return path

    def delete_scan(self, scan_id: int) -> int:
        """Delete the scan's index outright. Cheaper and cleaner than
        delete-by-query, and the index is single-purpose. Returns the
        approximate doc count that was deleted (best-effort)."""
        index = self._index_for(scan_id)
        try:
            count = int(
                self.client.count(index=index).get("count", 0)
            )
        except Exception:
            count = 0
        try:
            # delete-by-query keeps the index around for
            # subsequent inserts; that matches SqliteBackend, which
            # keeps the table.
            res = self.client.delete_by_query(
                index=index,
                body={"query": {"term": {"scan_id": int(scan_id)}}},
                refresh=False,
                conflicts="proceed",
            )
            return int(res.get("deleted", count) or count)
        except Exception as e:
            logger.warning("delete_by_query failed for %s: %s", index, e)
            return 0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def count_scanned_files(self, scan_id: int) -> int:
        index = self._index_for(scan_id)
        try:
            res = self.client.count(
                index=index,
                body={"query": {"term": {"scan_id": int(scan_id)}}},
            )
            return int(res.get("count", 0))
        except Exception as e:
            logger.debug("count failed for %s: %s", index, e)
            return 0

    def query_files(
        self,
        scan_id: int,
        filter_dsl: dict,
        limit: int = 1000,
    ) -> list[dict]:
        """Filter-DSL search. Validates against the shared whitelist
        before constructing the ES query; same error message style as
        the SQLite shim."""
        query = self._build_filter_query(scan_id, filter_dsl or {})
        index = self._index_for(scan_id)
        res = self.client.search(
            index=index,
            body={"query": query, "size": int(limit)},
        )
        return [hit.get("_source", {}) for hit in res["hits"]["hits"]]

    def aggregate(
        self,
        scan_id: int,
        group_by: str,
        metric: str = "count",
    ) -> list[dict]:
        """Terms aggregation. Validates ``group_by`` and ``metric``
        against the shared whitelist before issuing the query."""
        if group_by not in _ALLOWED_GROUP_BY:
            raise ValueError(
                f"aggregate: unsupported group_by={group_by!r}. "
                f"Allowed: {sorted(_ALLOWED_GROUP_BY)}"
            )
        if metric not in _ALLOWED_METRICS:
            raise ValueError(
                f"aggregate: unsupported metric={metric!r}. "
                f"Allowed: {sorted(_ALLOWED_METRICS)}"
            )

        index = self._index_for(scan_id)
        # All aggregation fields are indexed as keyword (or are
        # already keyword type), so terms agg is direct.
        terms_field = group_by  # extension/owner/directory_path are all keyword.

        body: dict[str, Any] = {
            "size": 0,
            "query": {"term": {"scan_id": int(scan_id)}},
            "aggs": {
                "g": {
                    "terms": {"field": terms_field, "size": 10_000},
                }
            },
        }
        if metric == "sum_size":
            body["aggs"]["g"]["aggs"] = {
                "m": {"sum": {"field": "size_bytes"}}
            }

        res = self.client.search(index=index, body=body)
        buckets = res.get("aggregations", {}).get("g", {}).get("buckets", [])

        out: list[dict] = []
        if metric == "count":
            for b in buckets:
                out.append({group_by: b["key"], "count": int(b["doc_count"])})
        else:
            for b in buckets:
                out.append(
                    {
                        group_by: b["key"],
                        "sum_size": int(b.get("m", {}).get("value") or 0),
                    }
                )
        # Sort matches the SqliteBackend convention (ORDER BY metric DESC).
        sort_key = "count" if metric == "count" else "sum_size"
        out.sort(key=lambda r: r[sort_key], reverse=True)
        return out

    def search_text(
        self,
        scan_id: int,
        query: str,
        limit: int = 100,
    ) -> list[dict]:
        """Full-text search on the analyzed ``file_path`` field.
        This is the place ES outshines SQLite's LIKE — proper
        tokenisation + relevance scoring."""
        index = self._index_for(scan_id)
        body = {
            "size": int(limit),
            "query": {
                "bool": {
                    "filter": [{"term": {"scan_id": int(scan_id)}}],
                    "must": [{"match": {"file_path": query or ""}}],
                }
            },
        }
        res = self.client.search(index=index, body=body)
        return [hit.get("_source", {}) for hit in res["hits"]["hits"]]

    def iterate_scan(
        self,
        scan_id: int,
        batch_size: int = 1000,
    ) -> Iterator[list[dict]]:
        """Stream the whole scan via the ``scan`` helper. Used by
        Phase 4 backfill / migration tooling and by heavy reports."""
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        index = self._index_for(scan_id)
        gen = es_helpers.scan(
            self.client,
            index=index,
            query={"query": {"term": {"scan_id": int(scan_id)}}},
            size=int(batch_size),
            preserve_order=False,
        )
        batch: list[dict] = []
        for hit in gen:
            batch.append(hit.get("_source", {}))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def health_check(self) -> dict:
        """Cluster ping + basic info. NEVER raises — the dashboard
        ops banner depends on this returning a usable dict even when
        ES is down."""
        try:
            ping_ok = bool(self.client.ping())
            details: dict[str, Any] = {"ping": ping_ok}
            if ping_ok:
                try:
                    info = self.client.info()
                    details["cluster_name"] = info.get("cluster_name")
                    details["version"] = (info.get("version") or {}).get(
                        "number"
                    )
                except Exception as e:  # pragma: no cover - defensive
                    details["info_error"] = str(e)
            return {
                "name": self.name,
                "available": ping_ok,
                "details": details,
            }
        except Exception as e:
            return {
                "name": self.name,
                "available": False,
                "details": {"error": str(e)},
            }
