"""Elasticsearch index mapping + settings for ``scanned_files`` docs.

Phase 2 of issue #114. Centralised so the backend, the migration tool
(Phase 4), and any ad-hoc tooling all use the exact same mapping.

Index naming convention (defined in ``elasticsearch_backend``):

    scanned_files-{source_id}-{scan_id}

Defaults below assume a single-node dev/staging cluster
(``number_of_replicas: 0``). Production clusters should override
``number_of_shards`` / ``number_of_replicas`` via index templates —
this module's literal is the conservative default. The backend does
NOT mutate this dict; copy with ``copy.deepcopy`` before patching.
"""

from __future__ import annotations

# Single source of truth for the mapping. Kept as a plain dict so it
# round-trips through ``json.dumps`` without surprises and so other
# tools (Phase 4 migrator, ops scripts) can ``from es_mapping import
# INDEX_BODY`` without pulling the ES client.
INDEX_BODY: dict = {
    "settings": {
        "number_of_shards": 1,
        # Single-node default. Multi-node clusters should set this to
        # at least 1 via an index template before pointing the backend
        # at them.
        "number_of_replicas": 0,
        "refresh_interval": "1s",
    },
    "mappings": {
        # Strict so a typo in the doc producer surfaces at index time
        # rather than as a silent ignored field at query time.
        "dynamic": "strict",
        "properties": {
            "scan_id": {"type": "long"},
            "source_id": {"type": "long"},
            # ``file_path`` is the workhorse field for full-text search
            # (search_text) and exact-match / prefix queries (filter
            # DSL ``directory_prefix``). Multi-field gives us both.
            "file_path": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 1024},
                },
            },
            "extension": {"type": "keyword"},
            "owner": {"type": "keyword"},
            "size_bytes": {"type": "long"},
            "mtime": {"type": "date"},
            "directory_path": {"type": "keyword"},
        },
    },
}


def index_name(source_id: int, scan_id: int) -> str:
    """Return the canonical index name for a (source, scan) pair.

    Kept here so the backend and any future tooling never disagree on
    the format. A drift between producer and reader silently loses
    data.
    """
    return f"scanned_files-{int(source_id)}-{int(scan_id)}"
