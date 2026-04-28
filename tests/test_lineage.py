"""Tests for issue #145: W3C PROV-O JSON-LD lineage builder.

Coverage:
  * ``build_for_file`` returns a structurally-valid PROV-O document.
  * Every audit event for the file produces an Activity in the graph.
  * ``created`` events are linked via ``prov:wasGeneratedBy``;
    other event types use ``prov:wasInfluencedBy``.
  * Username is rendered as a ``prov:Agent`` and attached via
    ``prov:wasAssociatedWith``.
  * ``build_for_scan`` returns a Collection with ``hadMember``
    references for every distinct file touched during the scan.
  * Document validates against a basic JSON-LD shape (no full RDF
    parse; this is a structure-level smoke check).
"""

from __future__ import annotations

import json
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.compliance.lineage import LineageBuilder, serialize  # noqa: E402


# ── Fixtures ───────────────────────────────────────────────


def _make_db(tmp_path) -> Database:
    db = Database({"path": str(tmp_path / "lineage.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("test_src", "/share"),
        )
    return db


def _seed_event(db, *, event_time, event_type, username, file_path,
                details="", source_id=1):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO file_audit_events "
            "(source_id, event_time, event_type, username, file_path, "
            " file_name, details, detected_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'test')",
            (source_id, event_time, event_type, username, file_path,
             os.path.basename(file_path), details),
        )


# ── build_for_file ─────────────────────────────────────────


def test_lineage_for_file_returns_valid_provo(tmp_path):
    db = _make_db(tmp_path)
    _seed_event(
        db,
        event_time="2026-04-28 10:00:00",
        event_type="created",
        username="alice",
        file_path="/share/report.docx",
        details="initial creation",
    )
    builder = LineageBuilder(db, organization_uri="https://example.org")
    doc = builder.build_for_file("/share/report.docx")

    assert "@context" in doc
    assert "@graph" in doc
    # Context references the canonical PROV-O context URL.
    assert any(
        ctx == "http://www.w3.org/ns/prov.jsonld" if isinstance(ctx, str)
        else False
        for ctx in (doc["@context"] if isinstance(doc["@context"], list) else [doc["@context"]])
    )
    graph = doc["@graph"]
    assert isinstance(graph, list) and len(graph) >= 3

    # The file's Entity must be present.
    entities = [n for n in graph if "prov:Entity" in (
        n.get("@type") if isinstance(n.get("@type"), list)
        else [n.get("@type")]
    )]
    assert any(n.get("prov:value") == "/share/report.docx" for n in entities)

    # An Activity for the create event.
    activities = [n for n in graph if n.get("@type") == "prov:Activity"]
    assert any(n.get("prov:value") == "created" for n in activities)

    # An Agent for the user.
    agents = [n for n in graph if n.get("@type") == "prov:Agent"]
    assert any(n.get("prov:value") == "alice" for n in agents)


def test_lineage_includes_all_audit_events(tmp_path):
    db = _make_db(tmp_path)
    fp = "/share/big.docx"
    _seed_event(db, event_time="2026-04-01 09:00:00",
                event_type="created", username="alice", file_path=fp)
    _seed_event(db, event_time="2026-04-10 11:00:00",
                event_type="modified", username="alice", file_path=fp)
    _seed_event(db, event_time="2026-04-20 14:00:00",
                event_type="archived", username="bob", file_path=fp)

    builder = LineageBuilder(db)
    doc = builder.build_for_file(fp)
    graph = doc["@graph"]

    # Three Activities, one per audit event.
    activities = [n for n in graph if n.get("@type") == "prov:Activity"]
    activity_values = sorted(n.get("prov:value") for n in activities)
    assert activity_values == ["archived", "created", "modified"]

    # Two unique agents (alice + bob).
    agents = [n for n in graph if n.get("@type") == "prov:Agent"]
    agent_values = sorted({n.get("prov:value") for n in agents})
    assert agent_values == ["alice", "bob"]

    # The created event uses wasGeneratedBy on the entity addendum;
    # the other events use wasInfluencedBy.
    addenda = [
        n for n in graph
        if n.get("@type") == "prov:Entity"
        and ("prov:wasGeneratedBy" in n or "prov:wasInfluencedBy" in n)
    ]
    has_generated = any("prov:wasGeneratedBy" in n for n in addenda)
    has_influenced = any("prov:wasInfluencedBy" in n for n in addenda)
    assert has_generated, "created event must produce wasGeneratedBy"
    assert has_influenced, "non-create events must produce wasInfluencedBy"


def test_lineage_jsonld_validates(tmp_path):
    """Basic structural validation: the document round-trips through
    json.dumps + json.loads, every node has an ``@id`` and ``@type``,
    and every IRI reference (``{"@id": ...}``) targets a string."""
    db = _make_db(tmp_path)
    _seed_event(db, event_time="2026-04-01 09:00:00",
                event_type="created", username="alice",
                file_path="/share/x.txt")
    _seed_event(db, event_time="2026-04-02 10:00:00",
                event_type="modified", username="bob",
                file_path="/share/x.txt")

    builder = LineageBuilder(db, organization_uri="https://example.org")
    doc = builder.build_for_file("/share/x.txt")
    text = serialize(doc)
    reparsed = json.loads(text)
    assert reparsed["@graph"]

    # Every node must have an @id and @type.
    for node in reparsed["@graph"]:
        assert "@id" in node, node
        assert "@type" in node, node
        assert isinstance(node["@id"], str)

    # @context must include the PROV-O URL.
    ctx = reparsed["@context"]
    if isinstance(ctx, list):
        assert any(
            c == "http://www.w3.org/ns/prov.jsonld"
            for c in ctx if isinstance(c, str)
        )
    else:
        assert ctx == "http://www.w3.org/ns/prov.jsonld"


def test_lineage_for_file_handles_no_events(tmp_path):
    """A file with no audit history still produces a valid document
    with a single Entity node."""
    db = _make_db(tmp_path)
    builder = LineageBuilder(db)
    doc = builder.build_for_file("/share/never-touched.txt")
    assert doc["@graph"]
    entities = [
        n for n in doc["@graph"]
        if n.get("@type") == "prov:Entity"
    ]
    assert len(entities) == 1
    assert entities[0]["prov:value"] == "/share/never-touched.txt"


def test_lineage_for_file_rejects_empty_path(tmp_path):
    db = _make_db(tmp_path)
    builder = LineageBuilder(db)
    with pytest.raises(ValueError):
        builder.build_for_file("")


# ── build_for_scan ─────────────────────────────────────────


def test_lineage_for_scan_collects_unique_files(tmp_path):
    db = _make_db(tmp_path)
    # Seed a completed scan and audit events that fall inside its
    # window. We touch two distinct files so the Collection should
    # have two hadMember edges.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scan_runs (id, source_id, started_at, "
            "completed_at, status, total_files, total_size) "
            "VALUES (1, 1, '2026-04-01 00:00:00', "
            "'2026-04-30 00:00:00', 'completed', 2, 0)"
        )
    _seed_event(db, event_time="2026-04-05 10:00:00",
                event_type="created", username="alice",
                file_path="/share/a.txt")
    _seed_event(db, event_time="2026-04-06 11:00:00",
                event_type="modified", username="alice",
                file_path="/share/a.txt")
    _seed_event(db, event_time="2026-04-10 11:00:00",
                event_type="created", username="bob",
                file_path="/share/b.txt")

    builder = LineageBuilder(db)
    doc = builder.build_for_scan(1)
    graph = doc["@graph"]

    # Find the Collection node.
    collection = None
    for n in graph:
        types = n.get("@type")
        if isinstance(types, list) and "prov:Collection" in types:
            collection = n
            break
    assert collection is not None, "scan must produce a Collection"
    members = collection.get("prov:hadMember") or []
    assert len(members) == 2
    member_ids = {m["@id"] for m in members}
    assert any("a.txt" in iri for iri in member_ids)
    assert any("b.txt" in iri for iri in member_ids)


def test_lineage_for_scan_rejects_bad_scan_id(tmp_path):
    db = _make_db(tmp_path)
    builder = LineageBuilder(db)
    with pytest.raises(ValueError):
        builder.build_for_scan(None)
    with pytest.raises(ValueError):
        builder.build_for_scan("not-an-int")
