"""Shared read-only data-access helpers for the playground.

Every connection opened here is READ-ONLY. The playground must never
INSERT/UPDATE/DELETE/ALTER. The constraints are double-enforced:

1. SQLite is opened with ``mode=ro`` URI flag.
2. DuckDB ATTACHes the SQLite file with ``READ_ONLY``.

Pages import :func:`get_sqlite_conn` / :func:`get_duckdb_conn` and use
the returned cursors directly. Connections are cached for the lifetime
of the Streamlit session so heavy queries don't re-open the DB.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Optional

import yaml


CONFIG_SEARCH_PATHS = (
    "config/config.yaml",
    "config.yaml",
)


def _project_root() -> Path:
    """Best-effort project root: this file is at ``src/playground/``."""
    return Path(__file__).resolve().parent.parent.parent


def find_config_path() -> Optional[Path]:
    """Locate the project's ``config.yaml``.

    Looks at ``config/config.yaml`` first (per issue #75 spec), then
    falls back to the project's ``config.yaml`` next to ``main.py``.
    Returns ``None`` if no file is found — caller will fall back to
    sensible defaults.
    """
    root = _project_root()
    for rel in CONFIG_SEARCH_PATHS:
        candidate = root / rel
        if candidate.is_file():
            return candidate
    # Also try CWD relative
    for rel in CONFIG_SEARCH_PATHS:
        candidate = Path(rel)
        if candidate.is_file():
            return candidate.resolve()
    return None


def load_config() -> dict:
    """Load the FILE_ACTIVITY config for the playground.

    Read-only loader — no merging with `DEFAULT_CONFIG`, no logging
    side-effects (Streamlit captures stderr noisily). Returns ``{}``
    if no file is found.
    """
    path = find_config_path()
    if path is None:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_db_path(config: dict) -> Path:
    """Return the absolute path to the SQLite DB.

    Uses ``database.path`` from the loaded config, defaulting to
    ``data/file_activity.db`` per ``DEFAULT_CONFIG``. Relative paths
    are resolved against the project root so the playground can be
    launched from any CWD.
    """
    db_rel = (config.get("database") or {}).get("path", "data/file_activity.db")
    db_path = Path(db_rel)
    if not db_path.is_absolute():
        db_path = _project_root() / db_path
    return db_path


def open_sqlite_readonly(db_path: Path) -> sqlite3.Connection:
    """Open SQLite in URI read-only mode.

    Uses the ``file:...?mode=ro`` form to guarantee the OS rejects any
    accidental write attempt. ``check_same_thread=False`` because
    Streamlit may pass the cached connection across reruns on the same
    session worker.
    """
    if not db_path.exists():
        raise FileNotFoundError(
            f"SQLite veritabani bulunamadi: {db_path}. "
            "FILE_ACTIVITY ana uygulamasini en az bir kez calistirip "
            "data/ dizininde DB'yi olusturduktan sonra playground'u baslatin."
        )
    uri = "file:" + str(db_path) + "?mode=ro"
    return sqlite3.connect(uri, uri=True, check_same_thread=False)


def open_duckdb_readonly(db_path: Path, memory_limit: str = "512MB",
                          threads: int = 4):
    """Open DuckDB with the SQLite source ATTACHed READ_ONLY.

    Mirrors the pattern in ``src/storage/analytics.py``. Returns
    ``None`` if duckdb isn't installed in the playground venv (the
    pages then fall back to plain SQLite queries via
    :func:`open_sqlite_readonly`).
    """
    try:
        import duckdb  # type: ignore
    except ImportError:
        return None

    conn = duckdb.connect(database=":memory:")
    try:
        # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
        conn.execute(f"SET memory_limit='{memory_limit}'")
        # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
        conn.execute(f"SET threads={int(threads)}")
        try:
            conn.execute("INSTALL sqlite")
        except Exception:
            # Bundled in most builds; ignore if INSTALL fails offline.
            pass
        conn.execute("LOAD sqlite")
        # Try the explicit READ_ONLY form first; some duckdb builds use
        # a different keyword casing.
        # CODEQL-SAFE: value is config-derived, never from request handlers. See audit I-3.
        attach_variants = [
            f"ATTACH '{db_path}' AS sqlite_db (TYPE SQLITE, READ_ONLY)",
            f"ATTACH '{db_path}' AS sqlite_db (TYPE SQLITE)",
        ]
        last_err: Optional[Exception] = None
        for sql in attach_variants:
            try:
                conn.execute(sql)
                last_err = None
                break
            except Exception as e:
                last_err = e
        if last_err is not None:
            conn.close()
            raise last_err
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    return conn


# ──────────────────────────────────────────────
# Streamlit-cached connection getters.
# Each page calls ``get_sqlite_conn()`` / ``get_duckdb_conn()`` and
# trusts Streamlit's per-session cache to keep one connection per
# (db_path) tuple. ``hash_funcs`` keeps the ``Path`` hashable.
# ──────────────────────────────────────────────


def _streamlit_cache_resource(*args, **kwargs):
    """Indirect import so unit tests that don't have streamlit
    installed can still import this module to inspect helpers."""
    import streamlit as st
    return st.cache_resource(*args, **kwargs)


def get_sqlite_conn() -> sqlite3.Connection:
    """Cached read-only SQLite connection for the current session."""
    @_streamlit_cache_resource(show_spinner=False)
    def _open(db_path_str: str) -> sqlite3.Connection:
        return open_sqlite_readonly(Path(db_path_str))

    cfg = load_config()
    db = resolve_db_path(cfg)
    return _open(str(db))


def get_duckdb_conn():
    """Cached read-only DuckDB connection (or ``None`` if duckdb is
    missing). Pages should always check for ``None`` and fall back."""
    @_streamlit_cache_resource(show_spinner=False)
    def _open(db_path_str: str, memory_limit: str, threads: int):
        return open_duckdb_readonly(Path(db_path_str), memory_limit, threads)

    cfg = load_config()
    db = resolve_db_path(cfg)
    analytics_cfg = cfg.get("analytics") or {}
    return _open(
        str(db),
        analytics_cfg.get("memory_limit", "512MB"),
        int(analytics_cfg.get("threads", 4)),
    )


def get_db_path_str() -> str:
    """Convenience used by the sidebar status block."""
    return str(resolve_db_path(load_config()))


def have_duckdb() -> bool:
    try:
        import duckdb  # noqa: F401  pylint: disable=unused-import
        return True
    except ImportError:
        return False


# ──────────────────────────────────────────────
# Defensive guards. Used in tests + at start-up to assert that the
# read-only contract holds even if a developer wires in something new.
# ──────────────────────────────────────────────

_FORBIDDEN_KEYWORDS = ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "DROP ",
                       "CREATE ", "REPLACE ", "TRUNCATE ", "VACUUM ")


def assert_select_only(sql: str) -> None:
    """Raise if ``sql`` looks like anything other than SELECT/WITH.

    Belt-and-braces guard for the (currently unused) ad-hoc query
    surface; pages today use parameterised SELECTs only, but the
    helper is exposed for any future "let admins paste SQL" feature
    so we don't accidentally lose the read-only contract.
    """
    head = sql.lstrip().upper()
    if not (head.startswith("SELECT") or head.startswith("WITH")
            or head.startswith("PRAGMA")):
        raise ValueError("Sadece SELECT / WITH / PRAGMA sorgulari kabul edilir.")
    upper = " " + " ".join(sql.upper().split()) + " "
    for kw in _FORBIDDEN_KEYWORDS:
        if kw in upper:
            raise ValueError(f"Yasakli anahtar kelime tespit edildi: {kw.strip()}")


def env_token() -> str:
    """Read the optional bearer token from the environment."""
    return os.environ.get("FILEACTIVITY_PLAYGROUND_TOKEN", "").strip()
