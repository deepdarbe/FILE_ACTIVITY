"""Compliance subpackage.

Houses regulatory / legal-hold features that gate destructive
operations on file_path patterns. Currently:

* :mod:`legal_hold` — issue #59 — glob-based path freeze registry
  (blocks archive, retention purge, and scan-retention cleanup).
* :mod:`retention` — issue #58 — retention policy engine.
* :mod:`pii_engine` — issue #58 — GDPR PII scanner.
* :mod:`lineage` — issue #145 — W3C PROV-O JSON-LD lineage builder
  (read-only, stdlib-only).
* :mod:`dcat` — issue #145 — DCAT v3 catalog builder
  (read-only, stdlib-only).

The package is intentionally small and dependency-free so the rest
of the codebase can import it cheaply.
"""
