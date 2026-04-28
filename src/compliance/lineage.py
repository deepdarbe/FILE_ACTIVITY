"""W3C PROV-O JSON-LD lineage builder (issue #145).

Maps the FILE ACTIVITY ``file_audit_events`` table onto the W3C PROV
data model so downstream lineage / governance tooling (Apache Atlas,
LinkedIn DataHub, Collibra Data Catalog, ...) can ingest our audit
trail without bespoke adapters.

PROV mapping
============

================================  =================================
file_audit_events column          PROV term
================================  =================================
``file_path``                     ``prov:Entity``
``event_type`` (one row)          ``prov:Activity``
``username`` (or SID)             ``prov:Agent``
``event_type='created'``          ``prov:wasGeneratedBy`` link
``event_type='modified'``         ``prov:wasInfluencedBy`` link
``event_time``                    ``prov:atTime`` /
                                  ``prov:generatedAtTime``
``details`` blob                  attached as ``prov:value``
``source_id`` (scan owner)        ``prov:wasAttributedTo`` link
================================  =================================

Notes
-----

* This module is **read-only**. It never inserts/updates audit rows.
* JSON-LD is emitted with ``@context`` pointing at the canonical
  PROV-O context document so consumers can resolve term IRIs without
  additional lookup.
* Stdlib only — ``json``, ``datetime``, no ``rdflib`` dependency.
  Building a PROV document is just structured dict assembly; we don't
  need a triple store to *write* it.
* ``LineageBuilder`` accepts an optional ``organization_uri`` which is
  used as the IRI prefix for activities/agents/entities when assigned
  via :meth:`__init__`. Defaults to ``urn:fileactivity:`` so the
  document is self-consistent even if the operator has not configured
  a public URI.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Iterable, Optional
from urllib.parse import quote

logger = logging.getLogger("file_activity.compliance.lineage")


# Canonical PROV-O JSON-LD context. Pinned to the W3C published doc.
# Consumers (Atlas, DataHub, Collibra) all resolve this URL when they
# load a JSON-LD lineage document.
PROV_CONTEXT_URL = "http://www.w3.org/ns/prov.jsonld"


def _iso(dt: Any) -> Optional[str]:
    """Coerce a SQLite TEXT timestamp into an ISO-8601 string.

    SQLite stores ``event_time`` / ``created_at`` as plain strings
    such as ``"2026-04-28 10:11:12"``. Returning the value verbatim
    would still parse for most JSON-LD consumers, but PROV-O's
    ``xsd:dateTime`` slots strictly require the ``T`` separator. We
    therefore swap a single space for ``T`` (no full datetime parse,
    no timezone synthesis — those would be a guess).
    """
    if dt is None:
        return None
    s = str(dt).strip()
    if not s:
        return None
    if " " in s and "T" not in s:
        # First space is the date/time separator in SQLite TEXT format.
        s = s.replace(" ", "T", 1)
    return s


def _make_iri(prefix: str, kind: str, ident: str) -> str:
    """Build a stable IRI for a PROV node.

    ``prefix`` is either the operator-provided ``organization_uri``
    or the fallback ``urn:fileactivity:``. ``kind`` is one of
    ``entity`` / ``activity`` / ``agent`` / ``collection``. ``ident``
    is URL-quoted so paths with spaces/Unicode produce valid IRIs.
    """
    base = prefix.rstrip("/")
    safe = quote(str(ident), safe="")
    if base.startswith("urn:"):
        # urn-style: keep the colon-delimited form (no slashes).
        return f"{base}:{kind}:{safe}"
    return f"{base}/{kind}/{safe}"


class LineageBuilder:
    """W3C PROV-O JSON-LD lineage for a file path or scan.

    Maps our audit events to PROV terms:
      file_path → prov:Entity
      scan/archive/quarantine event → prov:Activity
      user (SID/username) → prov:Agent
      created → prov:wasGeneratedBy
      modified → prov:wasInfluencedBy
    """

    def __init__(self, db, organization_uri: str = "urn:fileactivity:") -> None:
        self.db = db
        self.organization_uri = organization_uri or "urn:fileactivity:"

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def build_for_file(self, file_path: str) -> dict:
        """Returns PROV-O JSON-LD doc for a single file's full history."""
        if not file_path or not str(file_path).strip():
            raise ValueError("file_path is required")

        events = self._fetch_events_for_file(file_path)
        graph: list[dict] = []

        # The file itself is one Entity. Even with zero audit events
        # we emit it so consumers see the path was queried.
        entity_iri = _make_iri(
            self.organization_uri, "entity", file_path,
        )
        graph.append({
            "@id": entity_iri,
            "@type": "prov:Entity",
            "prov:value": file_path,
        })

        # Each row becomes one Activity, with edges back to the
        # Entity and (optionally) to the Agent.
        for ev in events:
            graph.extend(self._render_event(ev, entity_iri))

        return self._wrap(graph, primary=entity_iri)

    def build_for_scan(self, scan_id: int) -> dict:
        """Returns PROV-O JSON-LD doc for the entire scan as a Collection."""
        if scan_id is None:
            raise ValueError("scan_id is required")
        try:
            scan_id_int = int(scan_id)
        except (TypeError, ValueError) as e:
            raise ValueError("scan_id must be an integer") from e

        # Scan metadata — used both as the Collection's properties and
        # as the run's Activity. Empty dict if the scan does not exist
        # so the caller still gets a structurally valid document.
        scan_row = self._fetch_scan(scan_id_int)
        events = self._fetch_events_for_scan(scan_id_int, scan_row)

        collection_iri = _make_iri(
            self.organization_uri, "collection", f"scan-{scan_id_int}",
        )
        scan_activity_iri = _make_iri(
            self.organization_uri, "activity", f"scan-{scan_id_int}",
        )

        graph: list[dict] = []

        # The scan itself: a Collection (PROV subclass of Entity) plus
        # an Activity that produced it. wasGeneratedBy ties them
        # together so consumers can render "this catalogue was the
        # output of *this* scan run".
        coll_node: dict[str, Any] = {
            "@id": collection_iri,
            "@type": ["prov:Entity", "prov:Collection"],
            "prov:value": f"scan-{scan_id_int}",
            "prov:wasGeneratedBy": {"@id": scan_activity_iri},
        }
        if scan_row:
            ts = _iso(scan_row.get("completed_at") or scan_row.get("started_at"))
            if ts:
                coll_node["prov:generatedAtTime"] = {
                    "@type": "xsd:dateTime",
                    "@value": ts,
                }
        graph.append(coll_node)

        scan_activity: dict[str, Any] = {
            "@id": scan_activity_iri,
            "@type": "prov:Activity",
            "prov:value": "scan",
        }
        if scan_row:
            started = _iso(scan_row.get("started_at"))
            ended = _iso(scan_row.get("completed_at"))
            if started:
                scan_activity["prov:startedAtTime"] = {
                    "@type": "xsd:dateTime", "@value": started,
                }
            if ended:
                scan_activity["prov:endedAtTime"] = {
                    "@type": "xsd:dateTime", "@value": ended,
                }
        graph.append(scan_activity)

        # Each unique file in the scan becomes an Entity that
        # ``hadMember`` attaches to the Collection. The audit events
        # for those files become Activities with the same edges as
        # ``build_for_file``.
        seen_entities: dict[str, str] = {}
        for ev in events:
            fp = ev.get("file_path")
            if not fp:
                continue
            entity_iri = seen_entities.get(fp)
            if entity_iri is None:
                entity_iri = _make_iri(
                    self.organization_uri, "entity", fp,
                )
                seen_entities[fp] = entity_iri
                graph.append({
                    "@id": entity_iri,
                    "@type": "prov:Entity",
                    "prov:value": fp,
                    # The entity is a member of the scan collection
                    # and was derived from the scan activity.
                    "prov:wasDerivedFrom": {"@id": collection_iri},
                })
            graph.extend(self._render_event(ev, entity_iri))

        # hadMember edges live on the Collection; PROV permits the
        # property to carry an array of IRI references.
        if seen_entities:
            coll_node["prov:hadMember"] = [
                {"@id": iri} for iri in seen_entities.values()
            ]

        return self._wrap(graph, primary=collection_iri)

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    def _wrap(self, graph: list[dict], primary: Optional[str] = None) -> dict:
        """Wrap a node list in the standard JSON-LD envelope."""
        doc: dict[str, Any] = {
            "@context": [
                PROV_CONTEXT_URL,
                {"xsd": "http://www.w3.org/2001/XMLSchema#"},
            ],
            "@graph": graph,
        }
        # ``generated`` metadata is not part of PROV-O proper but is
        # useful for downstream consumers ("when was this snapshot
        # produced?"). We attach it as a top-level annotation.
        doc["meta:generatedAt"] = {
            "@type": "xsd:dateTime",
            "@value": datetime.utcnow().isoformat() + "Z",
        }
        if primary:
            doc["meta:primary"] = primary
        return doc

    def _render_event(self, ev: dict, entity_iri: str) -> list[dict]:
        """Render one audit event as a list of PROV-O JSON-LD nodes.

        Yields the Activity node, optional Agent node, and links the
        Entity (already in the graph) to both via the appropriate
        PROV property based on ``event_type``.
        """
        out: list[dict] = []
        event_id = ev.get("id")
        event_type = ev.get("event_type") or "unknown"
        username = ev.get("username") or ""
        details = ev.get("details") or ""
        event_time = _iso(ev.get("event_time"))

        activity_iri = _make_iri(
            self.organization_uri, "activity", f"event-{event_id}",
        )
        activity_node: dict[str, Any] = {
            "@id": activity_iri,
            "@type": "prov:Activity",
            "prov:value": event_type,
        }
        if event_time:
            activity_node["prov:atTime"] = {
                "@type": "xsd:dateTime",
                "@value": event_time,
            }
        if details:
            activity_node["prov:hadDescription"] = details
        out.append(activity_node)

        # Agent (the user / service account that performed the
        # action). Only emitted when we actually know who acted.
        if username:
            agent_iri = _make_iri(
                self.organization_uri, "agent", username,
            )
            out.append({
                "@id": agent_iri,
                "@type": "prov:Agent",
                "prov:value": username,
            })
            activity_node["prov:wasAssociatedWith"] = {"@id": agent_iri}

        # Edge from the file Entity back to this Activity. We pick the
        # property based on event_type: ``created`` becomes
        # ``wasGeneratedBy`` (matches PROV semantics for "produced by"),
        # everything else (modify, archive, quarantine, legal_hold_*)
        # becomes ``wasInfluencedBy`` so consumers still see a chain
        # without overloading the more specific ``wasDerivedFrom``.
        et_lower = event_type.lower()
        if et_lower in ("created", "create"):
            edge_property = "prov:wasGeneratedBy"
            time_property = "prov:generatedAtTime"
        else:
            edge_property = "prov:wasInfluencedBy"
            time_property = None

        # We mutate the entity node in place via a follow-up
        # annotation node. Adding properties to the existing entity
        # node is cleaner, but the entity was emitted earlier; the
        # JSON-LD spec lets us emit the same @id twice with
        # complementary properties and consumers merge them.
        entity_addendum: dict[str, Any] = {
            "@id": entity_iri,
            "@type": "prov:Entity",
            edge_property: {"@id": activity_iri},
        }
        if username:
            entity_addendum["prov:wasAttributedTo"] = {
                "@id": _make_iri(self.organization_uri, "agent", username),
            }
        if time_property and event_time:
            entity_addendum[time_property] = {
                "@type": "xsd:dateTime",
                "@value": event_time,
            }
        out.append(entity_addendum)

        return out

    def _fetch_events_for_file(self, file_path: str) -> list[dict]:
        """All audit rows that touch ``file_path``, oldest-first."""
        try:
            cur_ctx = self.db.get_read_cursor
        except AttributeError:
            cur_ctx = self.db.get_cursor
        with cur_ctx() as cur:
            cur.execute(
                "SELECT id, source_id, event_time, event_type, username, "
                "       file_path, file_name, details "
                "FROM file_audit_events "
                "WHERE file_path = ? "
                "ORDER BY event_time ASC, id ASC",
                (file_path,),
            )
            return [dict(r) for r in cur.fetchall()]

    def _fetch_scan(self, scan_id: int) -> Optional[dict]:
        try:
            cur_ctx = self.db.get_read_cursor
        except AttributeError:
            cur_ctx = self.db.get_cursor
        with cur_ctx() as cur:
            cur.execute(
                "SELECT id, source_id, started_at, completed_at, status, "
                "       total_files, total_size "
                "FROM scan_runs WHERE id = ?",
                (scan_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _fetch_events_for_scan(
        self, scan_id: int, scan_row: Optional[dict],
    ) -> list[dict]:
        """All audit events whose timestamp falls inside the scan
        window. The audit table is scan-agnostic (no ``scan_id`` FK)
        so we use the scan's started_at..completed_at envelope as a
        proxy. When the scan is still running ``completed_at`` is
        NULL — we leave the upper bound open in that case.
        """
        if not scan_row:
            return []
        started = scan_row.get("started_at")
        ended = scan_row.get("completed_at")
        try:
            cur_ctx = self.db.get_read_cursor
        except AttributeError:
            cur_ctx = self.db.get_cursor
        with cur_ctx() as cur:
            if started and ended:
                cur.execute(
                    "SELECT id, source_id, event_time, event_type, username, "
                    "       file_path, file_name, details "
                    "FROM file_audit_events "
                    "WHERE event_time >= ? AND event_time <= ? "
                    "AND (source_id IS NULL OR source_id = ?) "
                    "ORDER BY event_time ASC, id ASC",
                    (started, ended, scan_row.get("source_id")),
                )
            elif started:
                cur.execute(
                    "SELECT id, source_id, event_time, event_type, username, "
                    "       file_path, file_name, details "
                    "FROM file_audit_events "
                    "WHERE event_time >= ? "
                    "AND (source_id IS NULL OR source_id = ?) "
                    "ORDER BY event_time ASC, id ASC",
                    (started, scan_row.get("source_id")),
                )
            else:
                cur.execute(
                    "SELECT id, source_id, event_time, event_type, username, "
                    "       file_path, file_name, details "
                    "FROM file_audit_events "
                    "WHERE source_id = ? "
                    "ORDER BY event_time ASC, id ASC",
                    (scan_row.get("source_id"),),
                )
            return [dict(r) for r in cur.fetchall()]


def serialize(doc: dict) -> str:
    """Pretty-print a PROV-O JSON-LD document. Stdlib ``json`` only."""
    return json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=False)
