"""Tests for issue #145: DCAT v3 catalog builder.

Coverage:
  * ``build_catalog`` includes a Dataset for every source.
  * Every Dataset has the required DCAT properties (``dct:title``,
    ``dct:identifier``, ``dct:license``).
  * Every completed scan becomes a Distribution attached to its
    Dataset.
  * ``dcat:byteSize`` aggregates the latest scan's total size.
  * ``dcat:keyword`` is populated from the top file extensions.
  * License URI is configurable per deployment.
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
from src.compliance.dcat import CatalogBuilder, serialize  # noqa: E402


# ── Fixtures ───────────────────────────────────────────────


def _make_db(tmp_path) -> Database:
    db = Database({"path": str(tmp_path / "dcat.db")})
    db.connect()
    return db


def _seed_source(db, sid: int, name: str, unc: str = "/share/x"):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path, last_scanned_at) "
            "VALUES (?, ?, ?, '2026-04-28 12:00:00')",
            (sid, name, unc),
        )


def _seed_completed_scan(
    db, scan_id: int, source_id: int,
    started: str = "2026-04-28 10:00:00",
    completed: str = "2026-04-28 11:00:00",
    total_files: int = 0, total_size: int = 0,
):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scan_runs "
            "(id, source_id, started_at, completed_at, status, "
            " total_files, total_size) "
            "VALUES (?, ?, ?, ?, 'completed', ?, ?)",
            (scan_id, source_id, started, completed, total_files, total_size),
        )


def _seed_scanned_file(
    db, source_id: int, scan_id: int, path: str,
    file_size: int = 100, ext: str = "txt",
):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scanned_files "
            "(source_id, scan_id, file_path, relative_path, file_name, "
            " extension, file_size) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (source_id, scan_id, path, os.path.basename(path),
             os.path.basename(path), ext, file_size),
        )


# ── Tests ──────────────────────────────────────────────────


def test_catalog_includes_all_sources(tmp_path):
    db = _make_db(tmp_path)
    _seed_source(db, 1, "finance", "/share/finance")
    _seed_source(db, 2, "hr", "/share/hr")

    builder = CatalogBuilder(db, {})
    doc = builder.build_catalog()

    assert "@graph" in doc
    datasets = [n for n in doc["@graph"] if n.get("@type") == "dcat:Dataset"]
    titles = sorted(d.get("dct:title") for d in datasets)
    assert titles == ["finance", "hr"]

    # The Catalog node references both Datasets.
    catalogs = [n for n in doc["@graph"] if n.get("@type") == "dcat:Catalog"]
    assert len(catalogs) == 1
    refs = catalogs[0].get("dcat:dataset") or []
    assert len(refs) == 2


def test_catalog_dataset_has_required_dcat_properties(tmp_path):
    db = _make_db(tmp_path)
    _seed_source(db, 1, "finance", "/share/finance")
    _seed_completed_scan(
        db, scan_id=10, source_id=1, total_files=5, total_size=4096,
    )
    _seed_scanned_file(db, 1, 10, "/share/finance/a.docx",
                       file_size=2048, ext="docx")
    _seed_scanned_file(db, 1, 10, "/share/finance/b.docx",
                       file_size=1024, ext="docx")
    _seed_scanned_file(db, 1, 10, "/share/finance/c.txt",
                       file_size=1024, ext="txt")

    builder = CatalogBuilder(
        db,
        {"compliance": {"standards": {
            "organization_uri": "https://example.org",
            "license_uri": "https://example.org/license/internal",
        }}},
    )
    doc = builder.build_catalog()

    datasets = [n for n in doc["@graph"] if n.get("@type") == "dcat:Dataset"]
    assert len(datasets) == 1
    ds = datasets[0]

    assert ds.get("dct:title") == "finance"
    assert ds.get("dct:identifier") == "1"
    assert ds.get("dct:license") == "https://example.org/license/internal"
    # byte_size from the aggregate.
    bs = ds.get("dcat:byteSize")
    assert bs and bs.get("@value") == "4096"
    # extent = file count.
    ext = ds.get("dct:extent")
    assert ext and ext.get("@value") == "3"
    # keyword from top extensions (docx is more frequent than txt).
    kw = ds.get("dcat:keyword") or []
    assert "docx" in kw and "txt" in kw


def test_catalog_distribution_per_completed_scan(tmp_path):
    db = _make_db(tmp_path)
    _seed_source(db, 1, "finance")
    _seed_completed_scan(
        db, scan_id=11, source_id=1,
        started="2026-04-01 09:00:00", completed="2026-04-01 10:00:00",
        total_files=2, total_size=200,
    )
    _seed_completed_scan(
        db, scan_id=12, source_id=1,
        started="2026-04-15 09:00:00", completed="2026-04-15 10:00:00",
        total_files=4, total_size=400,
    )

    builder = CatalogBuilder(db, {})
    doc = builder.build_catalog()
    distributions = [
        n for n in doc["@graph"]
        if n.get("@type") == "dcat:Distribution"
    ]
    assert len(distributions) == 2
    titles = sorted(d.get("dct:title") for d in distributions)
    assert titles == ["Scan run #11", "Scan run #12"]
    # Each distribution carries an spdx:Checksum block.
    for d in distributions:
        cks = d.get("dcat:checksum")
        assert cks and cks.get("spdx:algorithm") == "sha256"
        assert isinstance(cks.get("spdx:checksumValue"), str)
        assert len(cks["spdx:checksumValue"]) == 64  # sha256 hex


def test_catalog_round_trips_through_json(tmp_path):
    db = _make_db(tmp_path)
    _seed_source(db, 1, "finance")
    _seed_completed_scan(db, 10, 1, total_files=1, total_size=10)
    _seed_scanned_file(db, 1, 10, "/share/finance/x.txt",
                       file_size=10, ext="txt")
    builder = CatalogBuilder(db, {})
    doc = builder.build_catalog()
    text = serialize(doc)
    reparsed = json.loads(text)
    assert reparsed["@graph"]
    # @context must include both DCAT and DCT prefixes.
    ctx = reparsed["@context"]
    assert "dcat" in ctx and "dct" in ctx


def test_catalog_handles_empty_db(tmp_path):
    db = _make_db(tmp_path)
    builder = CatalogBuilder(db, {})
    doc = builder.build_catalog()
    catalogs = [n for n in doc["@graph"] if n.get("@type") == "dcat:Catalog"]
    assert len(catalogs) == 1
    # No datasets is fine; catalog still has empty list.
    assert catalogs[0].get("dcat:dataset") == []


def test_catalog_license_default(tmp_path):
    db = _make_db(tmp_path)
    _seed_source(db, 1, "finance")
    builder = CatalogBuilder(db, {})  # no compliance.standards block
    doc = builder.build_catalog()
    datasets = [n for n in doc["@graph"] if n.get("@type") == "dcat:Dataset"]
    assert datasets[0]["dct:license"] == "internal-use"
