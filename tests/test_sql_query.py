"""Tests for the admin ad-hoc SQL query guard (issue #48).

The guard's job is to keep the panel safely read-only and bounded;
that contract is what these tests pin down — every mutating keyword
is rejected, every non-allow-listed table is rejected, and a missing
``LIMIT`` is injected so a runaway ``SELECT *`` cannot drain memory.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from src.dashboard.sql_query import SqlQueryGuard


@pytest.fixture
def guard():
    return SqlQueryGuard(max_rows=100, timeout_seconds=5)


@pytest.mark.parametrize("sql", [
    "SELECT id, file_name FROM scanned_files WHERE file_size > 1000",
    "SELECT COUNT(*) FROM scan_runs",
    "WITH agg AS (SELECT owner, COUNT(*) c FROM scanned_files GROUP BY owner) "
    "SELECT * FROM scanned_files WHERE 1=1 LIMIT 10",
    "SELECT 1",
    "  select * from sources  ",
])
def test_validate_accepts_valid_select(guard, sql):
    ok, reason = guard.validate(sql)
    assert ok, reason


@pytest.mark.parametrize("kw,sql", [
    ("INSERT", "INSERT INTO scanned_files (id) VALUES (1)"),
    ("UPDATE", "UPDATE scanned_files SET file_name='x' WHERE id=1"),
    ("DELETE", "DELETE FROM scanned_files WHERE id=1"),
    ("DROP", "DROP TABLE scanned_files"),
    ("ATTACH", "ATTACH 'foo.db' AS bar"),
    ("PRAGMA", "PRAGMA table_info(scanned_files)"),
    ("ALTER", "ALTER TABLE scanned_files ADD COLUMN x INT"),
    ("VACUUM", "VACUUM"),
    ("TRUNCATE", "TRUNCATE scanned_files"),
])
def test_validate_rejects_blocked_keywords(guard, kw, sql):
    ok, reason = guard.validate(sql)
    assert not ok
    assert kw in (reason or "")


def test_validate_rejects_unknown_table(guard):
    ok, reason = guard.validate("SELECT * FROM secrets_table")
    assert not ok
    assert "secrets_table" in (reason or "")


def test_validate_rejects_blocked_table(guard):
    ok, reason = guard.validate("SELECT * FROM notification_log")
    assert not ok
    assert "notification_log" in (reason or "")


def test_validate_rejects_non_select_start(guard):
    ok, reason = guard.validate("SHOW TABLES")
    assert not ok


def test_validate_rejects_oversized_query(guard):
    long_sql = "SELECT * FROM scanned_files WHERE id=1 OR " + ("id=1 OR " * 1000)
    ok, _ = guard.validate(long_sql)
    assert not ok


def test_validate_strips_comments_before_keyword_check(guard):
    # The DELETE here is inside a comment and should not trigger a reject.
    ok, _ = guard.validate("SELECT id FROM scanned_files -- DELETE me later")
    assert ok
    ok, _ = guard.validate("/* DROP TABLE x */ SELECT 1")
    assert ok


def test_prepare_appends_limit_when_missing(guard):
    prepared = guard._prepare("SELECT id FROM scanned_files")
    assert "LIMIT 100" in prepared
    # Already-present LIMIT is not duplicated.
    prepared2 = guard._prepare("SELECT id FROM scanned_files LIMIT 5")
    assert "LIMIT 5" in prepared2
    assert prepared2.count("LIMIT") == 1


def test_prepare_normalises_table_prefix(guard):
    prepared = guard._prepare("SELECT * FROM sqlite_db.scanned_files")
    assert "sqlite_db.scanned_files" in prepared
    prepared2 = guard._prepare("SELECT * FROM scanned_files")
    assert "sqlite_db.scanned_files" in prepared2


def test_execute_round_trips_basic_select():
    duckdb = pytest.importorskip("duckdb")
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE scanned_files (id INTEGER, file_name TEXT)")
        conn.execute("INSERT INTO scanned_files VALUES (1, 'hello.txt')")
        conn.commit()
        conn.close()

        class _DB:
            db_path = path

        guard = SqlQueryGuard(max_rows=10, timeout_seconds=10)
        result = guard.execute(_DB(), "SELECT id, file_name FROM scanned_files")
        assert result["columns"] == ["id", "file_name"]
        assert result["rows"] == [[1, "hello.txt"]]
        assert result["row_count"] == 1
        assert result["truncated"] is False
        assert result["elapsed_ms"] >= 0
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
