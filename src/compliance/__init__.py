"""Compliance subpackage.

Houses regulatory / legal-hold features that gate destructive
operations on file_path patterns. Currently:

* :mod:`legal_hold` — issue #59 — glob-based path freeze registry
  (blocks archive, retention purge, and scan-retention cleanup).

The package is intentionally small and dependency-free so the rest
of the codebase can import it cheaply.
"""
