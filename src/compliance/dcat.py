"""DCAT v3 catalog builder (issue #145).

DCAT v3 (W3C Data Catalog Vocabulary, 2026-03 final) JSON-LD catalog
of every configured scan source. The output is consumable by data
governance platforms (Apache Atlas, LinkedIn DataHub, Collibra, OKFN
CKAN, Magda) without bespoke adapters.

Mapping
=======

================================  =================================
FILE ACTIVITY concept             DCAT term
================================  =================================
``sources`` table (top level)     ``dcat:Catalog``
one row in ``sources``            ``dcat:Dataset``
``scan_runs`` per source          ``dcat:Distribution`` (snapshot)
``scanned_files`` aggregates      ``dcat:byteSize``,
                                  ``dcat:keyword``,
                                  ``dct:modified``
================================  =================================

Notes
-----

* Read-only. No state mutation.
* Stdlib only — ``json`` + ``datetime``.
* The license URI is configurable per deployment via
  ``compliance.standards.license_uri``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote

logger = logging.getLogger("file_activity.compliance.dcat")


# DCAT v3 + Dublin Core Terms context. The official W3C DCAT context
# document is hosted at the URL below; keeping it pinned makes the
# JSON-LD self-describing for downstream catalog ingest.
DCAT_CONTEXT = {
    "dcat": "http://www.w3.org/ns/dcat#",
    "dct": "http://purl.org/dc/terms/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "spdx": "http://spdx.org/rdf/terms#",
}


def _iri(prefix: str, kind: str, ident: str) -> str:
    """Build an IRI for a DCAT node. See ``lineage._make_iri``."""
    base = (prefix or "urn:fileactivity:").rstrip("/")
    safe = quote(str(ident), safe="")
    if base.startswith("urn:"):
        return f"{base}:{kind}:{safe}"
    return f"{base}/{kind}/{safe}"


def _iso(dt: Any) -> Optional[str]:
    """Same SQLite-TEXT → ISO-8601 coercion as the lineage module."""
    if dt is None:
        return None
    s = str(dt).strip()
    if not s:
        return None
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    return s


class CatalogBuilder:
    """DCAT v3 (W3C 2026-03 final) catalog of scan sources.

    Maps:
      sources → dcat:Catalog (top-level) + dcat:Dataset (per source)
      scan_runs → dcat:Distribution (snapshots in time)
      scanned_files aggregates → dcat properties (byteSize, modified, etc.)
    """

    def __init__(self, db, config: Optional[dict] = None) -> None:
        self.db = db
        self.config = config or {}
        std_cfg = (
            (self.config.get("compliance") or {}).get("standards") or {}
        )
        self.organization_uri = (
            std_cfg.get("organization_uri") or "urn:fileactivity:"
        )
        self.license_uri = std_cfg.get("license_uri") or "internal-use"

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def build_catalog(self) -> dict:
        """Full catalog of all sources as DCAT JSON-LD."""
        sources = self._fetch_sources()

        catalog_iri = _iri(self.organization_uri, "catalog", "all-sources")
        graph: list[dict] = []

        # The Catalog node lists every Dataset by IRI. ``dct:title``
        # / ``dct:description`` give human-readable labels for catalog
        # browsers.
        catalog_node: dict[str, Any] = {
            "@id": catalog_iri,
            "@type": "dcat:Catalog",
            "dct:title": "FILE ACTIVITY scan catalog",
            "dct:description": (
                "DCAT v3 catalog of every configured scan source, with one "
                "Dataset per source and one Distribution per completed scan."
            ),
            "dct:modified": {
                "@type": "xsd:dateTime",
                "@value": datetime.utcnow().isoformat() + "Z",
            },
            "dct:license": self.license_uri,
            "dcat:dataset": [],
        }
        graph.append(catalog_node)

        for src in sources:
            dataset_node, dist_nodes, ext_kw = self._build_dataset(src)
            catalog_node["dcat:dataset"].append({"@id": dataset_node["@id"]})
            graph.append(dataset_node)
            graph.extend(dist_nodes)
            # Hoist a flat keyword list (file extensions) onto the
            # Catalog so a catalog-level keyword search lights up
            # all datasets that share an extension. DCAT permits
            # ``dcat:keyword`` on both Catalog and Dataset.
            if ext_kw:
                cur_kw = catalog_node.setdefault("dcat:keyword", [])
                for k in ext_kw:
                    if k not in cur_kw:
                        cur_kw.append(k)

        return self._wrap(graph, primary=catalog_iri)

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    def _wrap(self, graph: list[dict], primary: Optional[str] = None) -> dict:
        doc: dict[str, Any] = {
            "@context": DCAT_CONTEXT,
            "@graph": graph,
        }
        doc["meta:generatedAt"] = {
            "@type": "xsd:dateTime",
            "@value": datetime.utcnow().isoformat() + "Z",
        }
        if primary:
            doc["meta:primary"] = primary
        return doc

    def _build_dataset(self, src: dict) -> tuple[dict, list[dict], list[str]]:
        """Render one source row as a DCAT Dataset + its Distributions.

        Returns ``(dataset_node, distribution_nodes, extension_keywords)``.
        """
        sid = src.get("id")
        name = src.get("name") or f"source-{sid}"
        unc = src.get("unc_path") or ""

        dataset_iri = _iri(self.organization_uri, "dataset", name)
        dataset_node: dict[str, Any] = {
            "@id": dataset_iri,
            "@type": "dcat:Dataset",
            "dct:title": name,
            "dct:description": f"Scan source rooted at {unc}",
            "dct:identifier": str(sid),
            "dct:license": self.license_uri,
            "dcat:landingPage": unc,
        }
        last_scanned = _iso(src.get("last_scanned_at"))
        if last_scanned:
            dataset_node["dct:modified"] = {
                "@type": "xsd:dateTime",
                "@value": last_scanned,
            }

        # Aggregates from the latest completed scan: byte size, file
        # count, top extensions. Used to populate ``dcat:byteSize``
        # and ``dcat:keyword`` so catalog browsers can sort/filter
        # without fetching the full distribution.
        latest_scan = self._fetch_latest_scan(sid)
        ext_keywords: list[str] = []
        if latest_scan:
            agg = self._fetch_scan_aggregates(latest_scan["id"])
            if agg.get("byte_size") is not None:
                dataset_node["dcat:byteSize"] = {
                    "@type": "xsd:nonNegativeInteger",
                    "@value": str(int(agg["byte_size"])),
                }
            if agg.get("file_count") is not None:
                # DCAT does not have a "file count" property, but
                # ``dct:extent`` is the documented escape hatch.
                dataset_node["dct:extent"] = {
                    "@type": "xsd:nonNegativeInteger",
                    "@value": str(int(agg["file_count"])),
                }
            ext_keywords = list(agg.get("top_extensions") or [])
            if ext_keywords:
                dataset_node["dcat:keyword"] = ext_keywords

        # One Distribution per completed scan_run. Even when there
        # is no completed scan we still emit an empty list so the
        # property is present in the JSON-LD shape.
        distributions: list[dict] = []
        scans = self._fetch_completed_scans(sid)
        dist_refs: list[dict] = []
        for scan in scans:
            dist_node = self._build_distribution(scan, dataset_iri, src)
            dist_refs.append({"@id": dist_node["@id"]})
            distributions.append(dist_node)
        if dist_refs:
            dataset_node["dcat:distribution"] = dist_refs

        return dataset_node, distributions, ext_keywords

    def _build_distribution(
        self, scan: dict, dataset_iri: str, src: dict,
    ) -> dict:
        """Render one ``scan_runs`` row as a ``dcat:Distribution``."""
        scan_id = scan.get("id")
        dist_iri = _iri(
            self.organization_uri, "distribution", f"scan-{scan_id}",
        )
        dist: dict[str, Any] = {
            "@id": dist_iri,
            "@type": "dcat:Distribution",
            "dct:title": f"Scan run #{scan_id}",
            "dct:identifier": str(scan_id),
            "dct:isPartOf": {"@id": dataset_iri},
            "dcat:mediaType": "application/octet-stream",
        }

        started = _iso(scan.get("started_at"))
        ended = _iso(scan.get("completed_at"))
        if ended:
            dist["dct:issued"] = {
                "@type": "xsd:dateTime", "@value": ended,
            }
        if started:
            dist["dcat:startDate"] = {
                "@type": "xsd:dateTime", "@value": started,
            }

        if scan.get("total_size") is not None:
            dist["dcat:byteSize"] = {
                "@type": "xsd:nonNegativeInteger",
                "@value": str(int(scan["total_size"] or 0)),
            }

        # DCAT v3 ``dcat:checksum`` carries an ``spdx:Checksum`` node
        # with an ``algorithm`` and a ``checksumValue``. We have no
        # per-distribution hash stored today (a future enhancement is
        # to roll up the per-file content hashes), so we fingerprint
        # the canonical scan tuple as a stable identifier — operators
        # can rely on it to detect "same scan re-emitted vs. new
        # snapshot" without loading the file list.
        fp_payload = json.dumps(
            {
                "scan_id": scan_id,
                "source_id": src.get("id"),
                "started_at": started,
                "completed_at": ended,
                "total_files": scan.get("total_files") or 0,
                "total_size": scan.get("total_size") or 0,
            },
            sort_keys=True,
        ).encode("utf-8")
        checksum_value = hashlib.sha256(fp_payload).hexdigest()
        dist["dcat:checksum"] = {
            "@type": "spdx:Checksum",
            "spdx:algorithm": "sha256",
            "spdx:checksumValue": checksum_value,
        }

        return dist

    # ──────────────────────────────────────────────
    # DB fetches
    # ──────────────────────────────────────────────

    def _cur(self):
        try:
            return self.db.get_read_cursor
        except AttributeError:
            return self.db.get_cursor

    def _fetch_sources(self) -> list[dict]:
        with self._cur()() as cur:
            cur.execute(
                "SELECT id, name, unc_path, archive_dest, enabled, "
                "       created_at, last_scanned_at "
                "FROM sources ORDER BY id ASC"
            )
            return [dict(r) for r in cur.fetchall()]

    def _fetch_completed_scans(self, source_id: int) -> list[dict]:
        with self._cur()() as cur:
            cur.execute(
                "SELECT id, source_id, started_at, completed_at, status, "
                "       total_files, total_size "
                "FROM scan_runs "
                "WHERE source_id = ? AND status = 'completed' "
                "ORDER BY completed_at DESC, id DESC",
                (source_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def _fetch_latest_scan(self, source_id: int) -> Optional[dict]:
        with self._cur()() as cur:
            cur.execute(
                "SELECT id, source_id, started_at, completed_at, status, "
                "       total_files, total_size "
                "FROM scan_runs "
                "WHERE source_id = ? AND status = 'completed' "
                "ORDER BY completed_at DESC, id DESC LIMIT 1",
                (source_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _fetch_scan_aggregates(self, scan_id: int) -> dict:
        """Count + size + top extensions for one scan.

        Top extensions are limited to the 10 most common to keep the
        keyword list tractable.
        """
        out: dict[str, Any] = {
            "file_count": 0, "byte_size": 0, "top_extensions": [],
        }
        with self._cur()() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(file_size), 0) AS sz "
                "FROM scanned_files WHERE scan_id = ?",
                (scan_id,),
            )
            row = cur.fetchone()
            if row:
                out["file_count"] = int(row["cnt"] or 0)
                out["byte_size"] = int(row["sz"] or 0)
            cur.execute(
                "SELECT extension, COUNT(*) AS cnt FROM scanned_files "
                "WHERE scan_id = ? AND extension IS NOT NULL "
                "AND extension != '' "
                "GROUP BY extension ORDER BY cnt DESC LIMIT 10",
                (scan_id,),
            )
            out["top_extensions"] = [
                str(r["extension"]).lstrip(".")
                for r in cur.fetchall()
                if r["extension"]
            ]
        return out


def serialize(doc: dict) -> str:
    """Pretty-print a DCAT JSON-LD document. Stdlib ``json`` only."""
    return json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=False)
