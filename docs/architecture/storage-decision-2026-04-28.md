---
status: Accepted
date: 2026-04-28
authors: research subagent + main thread
context: v1.9.0-rc1, customer prod test feedback
supersedes: none
---

# Storage Backend Decision — Stay on SQLite, Finish the Tuning

## Decision (TL;DR)

**Stay on SQLite WAL + DuckDB read-only ATTACH. Do not migrate to PostgreSQL,
ClickHouse or DuckDB-as-primary in v1.9.x.**

The reported "slow during scan" pain has a known mechanical cause (WAL
checkpoint starvation) and a config-level fix. Migration to a server-class
database would solve a problem we don't have at the cost of operational
discipline we don't need yet.

The pluggable backend abstraction (issue #114 Phase 1) stays warm — when a
customer shows up with 500 M+ rows or a sub-minute RTO requirement, we
revisit.

## Context

Customer prod test (28 Apr): 3.5 GB SQLite, 2.5 M rows. Complaints:
- Dashboard slow during active scan (Overview / Reports show 0)
- "Askeri seviyede kararlılık + hızlı yanıt" goal
- Asked: should we move off SQLite?

## Decision Matrix

| Dimension | SQLite tuned | PostgreSQL 16 | ClickHouse 24 | DuckDB 1.5+ persistent |
|---|---|---|---|---|
| Bulk insert (k/s) | 50–150 | 100–500 | 500–2000 | 200–800 |
| GROUP BY 50 M rows | 5–60 s | 1–10 s | 50–500 ms | 100–800 ms |
| Concurrent read while writing | OK; checkpoint can stall | MVCC, no blocking | non-blocking | **single writer process** |
| Audit chain (per-row ACID) | ACID, single-file | ACID + FK | ❌ eventual merges | ACID within process |
| Backup story | file copy / VACUUM INTO | pg_basebackup, PITR | painful | EXPORT only |
| Replication / HA | ❌ Litestream async | ✅ logical + physical | ⚠️ Keeper, manual resync | ❌ none |
| Operational cost | zero install | DBA-class | Keeper, MergeTree tuning | embedded |
| License | Public domain | PostgreSQL (BSD-like) | Apache 2.0 | MIT |
| TCO (eng-hrs/yr/customer) | ~10 | ~60 | ~120 | ~25 |
| Migration cost | 0 (current) | 3–5 wk | 6–10 wk | 2–4 wk |

## Why we're staying

1. **The pain has a config fix.** WAL checkpoint starvation is well-documented
   ([Loke.dev "20 GB WAL"](https://loke.dev/blog/sqlite-checkpoint-starvation-wal-growth),
   [SQLite forum](https://sqlite.org/forum/info/6a66501e4df030ae)). We've fixed
   two of three causes (read-only cursor #134, live_count sync #138). The
   remaining lever is a manual checkpointer thread + tuned pragmas. That's
   ~3–5 days of work, not a 5-week migration.

2. **Customer profile is "Windows file servers, no DBA."** Adding PostgreSQL
   means adding a Windows service, `pg_hba.conf`, role management, vacuum
   tuning. ~40 hours of customer onboarding per site versus zero for SQLite.

3. **"Military-grade discipline" maps to ACID + verifiable backups + hash-chained
   audit.** SQLite gives all three with a single-file copy. PostgreSQL adds
   rigor at high op-cost. ClickHouse weakens per-row ACID (ReplacingMergeTree
   eventual semantics).

4. **Our pluggable abstraction (#114 Phase 1) is the insurance.** We're not
   burning the bridge — Phase 2 lights up Postgres if a customer ever shows up
   with 500 M+ rows.

## Concrete v1.9.x action items

In priority order:

1. **Manual checkpointer thread** + `wal_autocheckpoint=0`. The scanner pauses
   between batches; that's our checkpoint window.
2. **Default pragmas**: `mmap_size=2GB`, `page_size=8192`,
   `cache_size=-262144` (256 MB), `synchronous=NORMAL`, `temp_store=MEMORY`.
3. **Partition very-hot tables by `scan_id`** — separate file per scan, ATTACH
   on demand. Old scans become read-only files.
4. **Document VACUUM INTO + hash-verify backup** procedure — already shipped
   (#77), just needs operator docs (`docs/operator-runbook.md` updated).
5. **Expose DuckDB ATTACH more aggressively** to dashboard reads —
   `ReportGenerator` should prefer DuckDB for any GROUP BY > 100k rows.

## Honest pushbacks against the alternatives

### "PostHog says ClickHouse is 20× faster than Postgres"
True for hot-cache aggregates over hundreds of millions of rows where the
working set didn't fit shared_buffers. At 50 M rows / 30 GB on a server with
32 GB RAM, the gap shrinks to 3–8×.

### "100× faster than Postgres" (ClickHouse marketing)
That's on 1.1 B taxi rides where columnar+vectorization compounds. On 50 M rows
where the table fits in OS page cache, the multiplier collapses to 5–15× — not
enough to justify the ops burden (Keeper, MergeTree tuning, async merges,
eventual consistency on dedupes, Linux-preferred).

### "Just use DuckDB for everything"
Dealbreaker: DuckDB **does not support concurrent writes from multiple
processes** ([DuckDB FAQ](https://duckdb.org/docs/current/connect/concurrency),
[#1119](https://github.com/duckdb/duckdb/issues/1119),
[#4899](https://github.com/duckdb/duckdb/discussions/4899)). Our deployment has
FastAPI workers (5–10 procs) + scanner + scheduler. With DuckDB-as-primary
we'd need to funnel every write through a single writer process and IPC the
rest — rearchitecture, not swap. The DuckDB team itself recommends DuckLake +
Postgres for shared-write workloads.

### "SQLite can't scale"
False at this size. SQLite handles tens of GB and millions of rows daily in
production (Expensify, Fossil, every browser). What it can't do is multi-writer
or built-in HA — neither of which the file-scanner workload needs (1 writer by
design).

## When to reconsider this decision

Reopen if **three or more** of these flip:

- Multi-writer required (multiple scanners, multiple ingest sources)
- Single customer DB > 500 GB or > 200 M rows
- Customer has dedicated DBA on-call
- Sub-minute RTO requirement (file-copy backup not enough)
- Sustained insert rate > 500 k records/hour over 4+ hours
- Top-5 dashboard queries are full-table aggregates DuckDB-attach can't help with

## What we cannot tell without prod data

Customer should measure (and send) before any migration commitment:

1. p99 of `sqlite3_wal_checkpoint_v2` duration during scan
2. WAL file peak size (if > 500 MB, checkpoint starvation confirmed)
3. Sustained vs peak insert rate over 4+ hours (not 60-sec burst)
4. Top-5 slow dashboard queries with `EXPLAIN QUERY PLAN`
5. Disk IOPS + queue depth during slow window
6. Number of concurrent reader processes during scan
7. Memory headroom (mmap_size=2GB only free if host has it)
8. Backup window tolerance (5 min file copy OK, or sub-minute online?)

Without items 1–3, "slow during scan" is unfalsifiable; engine swap is a guess.

## References

- [phiresky — SQLite performance tuning, 100k SELECT/s](https://phiresky.github.io/blog/2020/sqlite-performance-tuning/)
- [Loke.dev — The 20 GB WAL file](https://loke.dev/blog/sqlite-checkpoint-starvation-wal-growth)
- [SQLite forum — wal_checkpoint(RESTART) BUSY](https://sqlite.org/forum/info/6a66501e4df030ae)
- [SQLite WAL spec](https://sqlite.org/wal.html)
- [PostHog — ClickHouse vs PostgreSQL](https://posthog.com/blog/clickhouse-vs-postgres)
- [Tinybird — ClickHouse vs PostgreSQL 2026](https://www.tinybird.co/blog/clickhouse-vs-postgresql-with-extensions)
- [Mark Litwintschik — 1.1 B taxi rides on ClickHouse](https://tech.marksblogg.com/clickhouse-14900k-1b-taxi-rides.html)
- [ClickBench](https://benchmark.clickhouse.com/)
- [HN — ClickHouse in production](https://news.ycombinator.com/item?id=27181471)
- [DuckDB — Concurrency docs](https://duckdb.org/docs/current/connect/concurrency)
- [DuckDB #1119 — MVCC concurrency](https://github.com/duckdb/duckdb/issues/1119)
- [DuckDB #4899 — concurrent RW](https://github.com/duckdb/duckdb/discussions/4899)
- [Lukas Barth — DuckDB vs SQLite](https://www.lukas-barth.net/blog/sqlite-duckdb-benchmark/)
- [marvelousmlops — DB comparison repo](https://github.com/marvelousmlops/database_comparison)
- [Tiger Data — Postgres ingest INSERT vs COPY](https://www.tigerdata.com/learn/testing-postgres-ingest-insert-vs-batch-insert-vs-copy)

---

**Bottom line**: ship the SQLite tuning work in v1.9.x. Reopen this document
when a customer brings 500 M rows or a hard sub-minute RTO.
