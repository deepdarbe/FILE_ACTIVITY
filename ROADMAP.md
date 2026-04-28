# FILE ACTIVITY — Roadmap

> **Project management memory** — single source of truth for where the project
> is, where it's going, and how the work is organised. Mirrored by a pinned
> GitHub issue that tracks the live backlog.

**Last updated**: 2026-04-28 — post customer prod-test wave, v1.9.0 (post-rc1).
See "Recent wave (2026-04-28)" below for what shipped this round.

---

## Recent wave (2026-04-28) — customer prod-test bug fixes

Customer ran prod test on a 3.1M-file `E:\` NTFS volume. The session
delivered **24 PRs** that took the product from "scan returns 0 files"
to "scan completes end-to-end with full size data + UI clarity":

### Critical bug fixes
- **#164** — MFT FRN sequence-number mask. NTFS FRNs carry a sequence
  number in the upper 16 bits; the parser keyed records by full FRN
  but `reconstruct_paths` seeded `cache = {root_frn: ""}` with the
  bare integer 5, so on any volume with churn every parent chain
  failed and **0 files emitted from 3.1M MFT entries**. Now masks to
  the lower-48 segment number on parse.
- **#174 / PR #176** — Scan abort + WAL leak. After #164 unblocked
  emit, the DuckDB ATTACH(READ_WRITE) ingest path raced the dashboard
  reader for the writer lock and aborted scans at ~100k of 3.1M rows
  with `database is locked`. Three fixes: `parquet_staging.enabled`
  flipped to `false` by default, `busy_timeout` 5 s → 60 s,
  `bulk_insert_scanned_files` retries 5× with 1/2/4/8/16 s backoff.

### Customer-visible features
- **#175 / PR #179** — Post-walk size + timestamp enrich. MFT
  enumeration is path-only by design; this pass `os.stat()`s every
  row and bulk-UPDATEs `file_size` / `last_modify_time`. Default ON.
  Customer's BOYUT KPI no longer stuck at "0 B".
- **#177 / PR #178** — Persistent scan summary banner + edge-triggered
  completion toasts. Banner no longer unmounts when scan ends; eight
  named scan states map to coloured banners; source cards show
  delta-vs-previous counts.
- **#172 / PR #173** — `setup-source.ps1` is now service-aware:
  `Stop-Service` before cleanup so a running NSSM supervisor doesn't
  hold `bin\nssm.exe`. Operators no longer have to manually stop the
  service before `update.cmd`.

### Earlier in the same wave
PR #163 (NSSM stderr noise), #167 (#114 Phase 2 ElasticsearchBackend
MVP), #168 (#144 Phase 1 wrong-extension via libmagic), #169 (#145
W3C PROV + DCAT v3 endpoints), #170 (#133 db cleanup endpoint M-3),
#171 (#144 Phase 2 perceptual image hash via imagehash).

### Issues opened retrospectively for traceability
#165, #166 (covering #164 + #163), #172, #174, #175, #177.

### What changed in delegation
- **GitHub Copilot coding-agent** delivered #170 + #171 + #178
  autonomously — first wave where Copilot + Claude split the queue
  reliably.
- **Worktree-isolated subagents** delivered #167 (Phase 2 ES backend),
  #168 (extension check), #169 (PROV+DCAT), #179 (size enrich).
- Main thread stayed on critical fixes (#164, #173, #176) and the
  merge / rebase / conflict-resolve loop.

---

## Vision

An **enterprise-grade Windows / Linux file share analysis, auditing, and
archiving platform** that scales to multi-million-file shares with
dashboards that never hang.

**Core value propositions**:
- **Visibility** — know what's on your shares, who owns it, how stale
- **Compliance** — audit trail, retention policies, per-user reports
- **Reclamation** — duplicate detection, stale archiving, oversized
  file alerts
- **Automation** — policy-driven archiving, scheduled notifications,
  SMTP + Active Directory integration
- **Performance** — scan millions of files without tying up the host,
  serve dashboards in milliseconds

---

## Current state (as of v1.9.0, post 2026-04-28 customer prod-test wave)

### What works
- **Scanner**: NTFS MFT backend (issue #32, FRN-mask hardened in #164)
  plus parallel `os.scandir` for SMB shares. Wrong-extension detection
  (#168) + perceptual image-hash dedup (#171) + post-walk size enrich
  (#179). Cross-platform `os.walk` fallback for non-NTFS hosts.
- **Storage**: SQLite (OLTP) + DuckDB read-only ATTACH (OLAP) +
  `scan_runs.summary_json` / `insights_json` cache for instant
  Overview rendering
- **Dashboard**: FastAPI + static HTML/Chart.js/D3. Sidebar with
  GitHub update banner, WAL size warning, AD + SMTP status
- **Scheduler**: APScheduler with scan / archive / `notify_users`
  task types
- **User activity**: LDAP lookup (AD email resolution),
  per-user efficiency score, HTML email notifications with admin CC
- **Deployment**: One-command PowerShell installer
  (`setup-source.ps1`) that auto-installs Python 3.11 if missing,
  auto-handles corporate TLS-inspection proxies, and writes an
  `update.cmd` launcher

### Known limitations
- `os.walk` is slow on Windows vs native MFT/USN reads
- No real-time change monitoring on network shares (polling only)
- Heavy endpoints still run per-scan aggregates on cache miss
- No horizontal scaling (single-host)
- Minimal test coverage

### Recent architectural wins
- Pre-computed scan summaries (PR #24): Overview loads in <1s
  regardless of scan size
- Cached AI insights (PR #25)
- Auto-cleanup of orphan `scanned_files` rows (PR #23, #26)
- Non-blocking Overview with lazy heavy endpoints (PR #27)
- WAL bloat prevention (PR #21)

---

## Performance benchmarks

Goal: match or beat best-in-class file indexers on the Windows host
scenario. On network shares we accept the SMB penalty but still want
to be faster than `os.walk`.

### Current (os.walk, baseline)

| Scenario | Time | Notes |
|---|---|---|
| 2.5M files on SMB share | ~45 min | Customer-observed |
| 2.5M files on local NTFS | TBD | Need measurement |
| Dashboard Overview (cached) | <1s | ✅ post-PR #24 |
| Dashboard Overview (uncached) | 1-5 min | First load after scan |

### Target (post-roadmap)

| Scenario | Time | Approach |
|---|---|---|
| 2.5M files on local NTFS | <10 s | MFT enumeration |
| 2.5M files on SMB | TBD | SMB server-side if available |
| Incremental scan | <5 s | USN journal |
| Any dashboard page | <500ms | All endpoints cached |

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Dashboard (FastAPI + static HTML)                        │
│  - Reads from summary_json / insights_json                │
│  - Never aggregates files table on the request path       │
├──────────────────────────────────────────────────────────┤
│  Storage layer                                             │
│  - SQLite (source of truth, OLTP writes)                  │
│  - DuckDB (read-only ATTACH, columnar analytics)          │
│  - scan_runs: summary_json, insights_json caches          │
├──────────────────────────────────────────────────────────┤
│  Scanner / watcher                                         │
│  - os.walk (today)                                        │
│  - MFT enumeration (planned)                              │
│  - USN journal delta (planned)                            │
├──────────────────────────────────────────────────────────┤
│  Services                                                  │
│  - APScheduler (cron tasks)                               │
│  - LDAP lookup (AD email resolution, cached)              │
│  - SMTP notifier (per-user reports, admin CC)             │
└──────────────────────────────────────────────────────────┘
```

---

## Roadmap

### Phase 1 — Performance (current priority)

The customer has 2.5M files and waits minutes on first cache fill.
Scanning and aggregation must be an order of magnitude faster.

1. **Fast file scanner** — MFT enumeration via Windows API
   (`FSCTL_ENUM_USN_DATA`), fallback to `os.walk` on non-NTFS targets
2. **Incremental scans** via USN journal
3. **Extend scan_runs caches** — include top extensions, top owners,
   age distribution, size distribution (eliminates `/api/reports/*`
   aggregate queries on Overview path)
4. **Indexed query audit** — review `src/storage/database.py` queries,
   add composite indexes where missing

### Phase 2 — Database interactivity

> "Her türlü interaktif çalışma" — richer analytics, faster feedback.

1. **DuckDB-backed ad-hoc query panel** — admin runs `SELECT … FROM
   scanned_files` via dashboard (read-only, whitelist-guarded)
2. **Saved views** — store named filters in DB, one-click apply on
   file tables
3. **Aggregation drill-through** — click a KPI card → drill into
   the underlying rows with pagination
4. **Real-time change feed** — watcher pushes changes to a UI panel
   via SSE (not just polling the `/api/watcher/status` endpoint)

### Phase 3 — Enterprise features

Details to be refined after enterprise-use-case research completes.

1. Anomaly detection (ransomware-style rapid deletes, unusual access
   patterns)
2. Orphaned-user cleanup (files owned by deleted AD accounts)
3. Chargeback reports (storage per department)
4. Approval workflows for archive operations
5. SIEM export (syslog / CEF / Splunk HEC)

### Phase 4 — Packaging + quality

1. Automated EXE release via GitHub Actions (already have workflow
   in PR #6, needs real build tested)
2. pytest suite (scanner, archiver, db queries)
3. Benchmark harness + CI guardrails (no perf regressions)
4. Documentation site (`docs/`)

---

## Gap analysis

### Competitive landscape — closest analogues

| Project | Language | Scan approach | Why it matters |
|---|---|---|---|
| [diskover-community](https://github.com/diskoverdata/diskover-community) | Python | Parallel walk → Elasticsearch | **Closest direct competitor**. Elasticsearch tier for 100M+ files. |
| [fclones](https://github.com/pkolaczk/fclones) | Rust | Rayon parallel | Best-in-class dedupe; size → prefix-hash → full-hash pipeline (10-100× faster than naive) |
| [Czkawka](https://github.com/qarmin/czkawka) | Rust | `jwalk` parallel | Dup + empty + large + stale; UI separate from engine |
| [rmlint](https://github.com/sahib/rmlint) | C | Multi-threaded | BTRFS reflink dedupe, lint mode |
| [Velociraptor](https://github.com/Velocidex/velociraptor) | Go | MFT + USN direct | Enterprise audit with raw NTFS parser |
| [osquery](https://github.com/osquery/osquery) | C++ | OS-native (USN/FSEvents/inotify) | SQL-over-filesystem virtual tables |
| [FSearch](https://github.com/cboxdoerfer/fsearch) | C/GTK | In-memory trie + inotify | Linux Everything clone |

### Everything (voidtools) — why it's fast

**MFT enumeration** via `FSCTL_ENUM_USN_DATA` on the raw volume handle
`\\.\C:`. Despite the name, this call walks the MFT (not the journal)
and returns `USN_RECORD_V2/V3` entries with name, parent FRN, and
attributes in a single sequential I/O pass. Typical numbers on a
2.5M-file volume:

| Approach | First scan | Incremental | RAM |
|---|---|---|---|
| `os.walk` (Python) | 3-8 min | full rescan | 500 MB - 1.5 GB |
| `os.scandir` recursive | 90 s - 3 min | full rescan | ~400 MB |
| `FindFirstFileExW` + `LARGE_FETCH` (C/ctypes) | 30-90 s | full rescan | ~300 MB |
| `FSCTL_ENUM_USN_DATA` (MFT scan) | **3-10 s** | USN tail | 150-200 MB |
| Raw MFT cluster read (Everything 1.5, WizTree) | **1-5 s** on NVMe | USN tail | 75 MB / 1M |
| USN journal tail (incremental) | N/A | **<50 ms latency** | negligible |

**Runtime flow**:
1. `FSCTL_GET_NTFS_VOLUME_DATA` → cluster size, MFT start LCN
2. `FSCTL_ENUM_USN_DATA` with `StartFRN=0` → walk MFT, build in-mem index
3. `FSCTL_QUERY_USN_JOURNAL` → get current USN
4. Tail `FSCTL_READ_USN_JOURNAL` for deltas — sub-second latency per batch

**Hard limits — why we can't just copy this**:
- **NTFS only** (ReFS has partial `MFT_ENUM_DATA_V1` support, inconsistent)
- **Does not work on SMB/network drives** — `DeviceIoControl` fails with
  `ERROR_INVALID_FUNCTION`. SMB2 has no remote-MFT op.
- Requires `SeManageVolumePrivilege` (admin token)
- No exFAT / FAT32 / CDFS / BitLocker-locked
- Hardlinks need explicit handling (same MFT record, multiple
  `$FILE_NAME` attrs)

**On SMB — our main case — the honest ceiling**:
- Everything itself: ~3 min per 1M files (voidtools' own figure)
- `os.walk` today: 15-40 min per 1M files over 1 Gb link
- Parallel `os.scandir` (32 threads): **2-5 min per 1M files**
- Realistic speedup: **5-8×**, not 60×. The ceiling is SMB
  directory-enumeration latency, not Python.
- Only way to hit seconds: run an agent on the file server itself
  (voidtools' `etp_server` is OSS — github.com/voidtools/etp_server).

### Top customer pain points (from r/sysadmin, ServerFault, vendor research)

1. **"Who is eating my storage?"** — #1 emergency ask at 2am. Needs
   fast per-user/per-folder breakdown, not 8-hour rescan.
2. **Orphaned SIDs after AD cleanup** — files owned by unresolvable
   SIDs. Bulk re-ACL workflow expected.
3. **Ransomware detection via rename/write velocity** — inline
   detection, not next-morning reports. Kill SMB session + quarantine.
4. **MAX_PATH / long-path errors** blocking backup and migration.
5. **Stale data with dead owners** — need to email the **manager**,
   not just the owner. Escalation timer.
6. **Permission sprawl** — "everyone" shares, nested-group effective
   permissions. Varonis's main wedge.
7. **Duplicate sprawl across departments** — scoped dedupe reports.
8. **Scan performance on 10 TB+ shares** — USN-based incrementals
   are table stakes.
9. **Open files blocking archive** — retry queues + session kill.
10. **Executive-friendly reports** — auto-emailed PDF with charts,
    CSVs are ignored.

### What we're missing (prioritized)

**Tier 1 — table stakes for enterprise**
- USN journal / ReFS change feed → incremental scans instead of full
  re-walk (unblocks 10 TB+ shares)
- NTFS ACL / effective-permissions analysis (Varonis's moat)
- Ransomware canary + rename-velocity detector with auto SMB kill
- Tamper-evident audit log (hash-chained, WORM-storable)
- PowerShell module (`Import-Module FileActivity` + cmdlets)
- REST API → Syslog/CEF → Splunk/Elastic/Sentinel integration
- Orphaned-SID report with bulk reassignment

**Tier 2 — compliance unlocks budget**
- GDPR Art. 17/30: PII search, per-subject export, retention engine
  with attestation
- HIPAA §164.312(b): tamper-evident access log, 6-year retention
- SOX §404: change auditing on financial paths, segregation-of-duties
  alerts
- Legal hold: freeze a path from policy/archive with audit trail
- Data classification tagging (Public / Internal / Confidential /
  Restricted)

**Tier 3 — growth features**
- Chargeback / showback: GB-months per OU with configurable rate
- Two-person approval on destructive ops
- Quota + trend forecasting ("fills in 47 days at current growth")
- Pre-archive stub/symlink so old paths redirect to "restore"
- Elasticsearch backend option for > 100M files

### Skip / deprioritise

- AI insight narratives (admins distrust; keep minimal)
- Per-user efficiency scores (HR-sensitive, rarely deployed in
  practice — we may scale ours down)
- Self-service "what did I delete" portal (Veeam/Commvault handle it)
- Mobile app
- Native Mac/Linux agent (SMB scan from Windows is sufficient)
- Auto-generated org chart visualisations

### Quick wins — achievable in 1-2 PRs each, ranked by ROI

1. **Parallel `os.scandir` + thread pool** (for SMB) — 5-8× on network
   shares, zero new deps. `scandir` returns `stat` in the `DirEntry`
   (one syscall not two). 32-thread executor over subtrees fills 1 Gb
   BDP. **Biggest single win for customer's SMB scenario**.
2. **`FindFirstFileExW` + `FIND_FIRST_EX_LARGE_FETCH`** on Windows via
   pywin32/ctypes — 2-3× on large directories, skips 8.3 alt-name
   lookup, 64 KB result buffers vs one-entry-per-round-trip.
3. **MFT / USN backend for local NTFS** — `FSCTL_ENUM_USN_DATA` via
   `ctypes`, feature-detected and fallback to parallel scandir on
   SMB. **20-60×** on direct-attached drives.
4. **USN journal tail for incremental scans** — drops rescans from
   hours to sub-second on local NTFS volumes. Local-only.
5. **fdupes / fclones hash pipeline**: size → 4 KB prefix hash → full
   hash. Typically 10-100× fewer bytes hashed for same dup set.
6. **Parquet staging + DuckDB `COPY`** for bulk ingest — 10-50×
   faster than row-by-row SQLite inserts.
7. **`fclones` as a subprocess** for dedupe — Apache-2.0, parallel,
   JSON output. Ship rather than reimplement.
8. **`watchdog` + `ReadDirectoryChangesW`** for local shares — swap
   polling for event-driven change feed.

### Scanner backend architecture

Design `Scanner` as a protocol with pluggable backends, auto-detected
by volume type:

```
Scanner (protocol)
├── NtfsMftBackend        # local NTFS, admin → FSCTL_ENUM_USN_DATA
├── Win32FindExBackend    # local non-NTFS or no admin → FindFirstFileEx
├── SmbParallelBackend    # UNC path → ThreadPoolExecutor(scandir)
└── LinuxStatxBackend     # Linux → statx + io_uring (future)
```

Detection: `GetVolumeInformationW` + `GetDriveTypeW`. Graceful
downgrade on privilege / filesystem mismatch.

### Architecture borrows

- **restic-style content-addressable archive**: dedupe blobs at chunk
  level, not whole-file. Saves bandwidth + storage on archive.
- **osquery virtual-table pattern**: expose our data via SQL over
  stdin/out for admin ad-hoc queries.

---

## Tracked work

All open work is tracked as GitHub issues labelled `roadmap`.
See the [pinned master tracking issue](#) for the live ordered
backlog. Each roadmap item corresponds to one or more issues, which
are then worked as branches / PRs.

### Labels in use

| Label | Meaning |
|---|---|
| `roadmap` | Part of the strategic roadmap above |
| `bug` | Customer-visible defect |
| `enhancement` | New feature (non-critical) |
| `performance` | Speed / memory improvement |
| `database` | DB schema / query change |
| `scanner` | File enumeration / watching |
| `dashboard` | UI / frontend |
| `deployment` | Installer / packaging |
| `security` | Auth, authz, input validation |
| `documentation` | Docs-only change |

---

## References

Research links will be added by the research phase commits. For now:

- [voidtools Everything](https://www.voidtools.com/)
- [Microsoft — NTFS MFT](https://learn.microsoft.com/en-us/windows/win32/fileio/master-file-table)
- [Microsoft — USN Change Journal](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/fsutil-usn)
- [SQLite performance hints](https://www.sqlite.org/queryplanner.html)
- [DuckDB over SQLite](https://duckdb.org/docs/extensions/sqlite_scanner.html)

---

## Skill / MCP / hardware-acceleration research (2026-04-23)

### Adoptable upstream skills (anthropics/skills, wshobson/agents)
- **xlsx skill** — formula-based compliance reports + post-write `recalc.py`
  verifier. Replace today's hardcoded computed totals in CSV/JSON exports
  so auditors can trust the numbers and edit assumptions in-place.
- **mcp-builder skill** — methodology for building our own
  `file-activity-mcp` server (Pydantic schemas, action-prefixed tools,
  10+ eval questions). High-impact: lets Claude Code users query their
  own shares in natural language.
- **skill-creator** — progressive-disclosure pattern for shipping
  per-customer retention/PII playbooks without bloating system prompts.
- **wshobson/agents/gdpr-data-handling** — drop-in DSAR / consent
  drafting helper from our PII engine.

### MCP servers worth wiring
- `motherduckdb/mcp-server-motherduck` — exposes our DuckDB analytics in
  natural language; reuses existing Parquet staging layer.
- `modelcontextprotocol/servers/Filesystem` — scoped read access for
  Claude to inspect actual files flagged by the PII engine.
- `grafana/mcp-grafana` — Claude can pull metrics/alerts during
  ransomware-detector triage (Prometheus + Loki + ES via one server).
- `WayStation-ai/mcp` — Slack + Notion + Jira + Monday in one MCP for
  "notify on detection" + "open ticket on legal hold" workflows.
- `elastic/mcp-server-elasticsearch` (target the new Agent Builder MCP,
  ES 9.2+) — wires into the planned ES backend.

### Hardware acceleration opportunities (NPU / NVIDIA / CPU SIMD)
The customer typically runs FILE_ACTIVITY on file servers without
discrete GPUs, but admin workstations often have NVIDIA cards or Intel
NPU (Core Ultra). Three tiers, ordered by realism:

1. **CPU SIMD (always-on, no detection needed on modern x86)**
   - **Intel Hyperscan / Vectorscan for PiiEngine regex** — AVX2/AVX-512
     accelerated multi-pattern regex; 10-100× faster than `re` for
     scanning multi-MB text corpora. Drop-in via `python-hyperscan` with
     graceful fallback to stdlib `re` on non-x86. Biggest practical win.
   - **SHA-NI for content_duplicates hashing** — stdlib `hashlib` already
     uses SHA-NI when present; verify by logging `hashlib.algorithms_guaranteed`
     and add a startup capability probe to surface this in `/api/health`.
2. **GPU/NPU (opt-in, capability-gated)**
   - **NER-based PII detection** — small DistilBERT on CUDA / OpenVINO on
     NPU catches name + address + ID-card patterns regex misses. Off by
     default; run as a follow-up pass on text files PII-flagged by regex.
   - **Embedding-based semantic dedup** — sentence-transformers on CUDA
     to detect "same content, different format" (e.g. .docx vs .pdf of
     the same contract). Complements byte-level hash dedup.
3. **Don't bother**
   - GPU SHA-256 for dedup — disk I/O is the bottleneck on SMB shares,
     not hash compute. SHA-NI on CPU already saturates a 1 GbE link.
   - GPU-accelerated SQL — DuckDB has experimental GPU support but it's
     unstable and our queries are not compute-bound.

### Patterns to borrow (not whole skills)
- xlsx skill's "formulas, never hardcoded" pattern → compliance Excel exports.
- mcp-builder's "10+ eval questions per server" → bake into PR template
  for any new dashboard endpoint or PowerShell cmdlet.
- skill-creator's progressive disclosure → retention-policy packs.
- obra/superpowers `systematic-debugging` 4-phase root-cause method →
  standard runbook template for incident response docs.

---

## External ecosystem research (2026-04-28, v1.9.0-rc1)

### Top adoptable GitHub repos
1. **Microsoft Presidio** (MIT) — `EntityRecognizer` ABC; wrap our Hyperscan
   as a recognizer + bolt on Turkish NER without rewriting (#143).
2. **Diskover Community** (Apache 2.0) — production ES schema + JSON export
   format for cross-tool compatibility (file-tagged for future #114 Phase 2).
3. **Czkawka** (MIT core) — perceptual-hash similarity, wrong-extension,
   broken-file detection. Customer-expected dedup features beyond byte hash (#144).
4. **Mayan EDMS** (Apache 2.0) — workflow state-machine concept for legal-hold
   exception routing beyond the existing two-person framework. Mirror schema only.
5. **Elastic Common Schema (ECS)** (Apache 2.0) — `file.*` field set; rename
   our syslog/CEF JSON keys to ECS dotted paths so Wazuh/Elastic SIEM customers
   get zero-config ingestion (#142).

### HuggingFace models (CPU-friendly)
- **`sentence-transformers/all-MiniLM-L6-v2`** (Apache 2.0, 22.7M params,
  ~90 MB) — CPU-first; embed file snippets/headers for semantic dedup.
- **`savasy/bert-base-turkish-ner-cased`** (license verify before use,
  110M params; quantize to ~110 MB INT8) — Turkish PII NER, second-pass
  verifier on Hyperscan candidates (~50-200 ms/sentence on CPU; cheap
  if scoped to flagged files).
- **`Isotonic/distilbert_finetuned_ai4privacy_v2`** (CC-BY-NC-4.0,
  non-commercial) — 54-class English PII; on-prem self-host OK, NOT
  bundleable in commercial SKU.

### Standards alignment (#145 umbrella)
- **ECS** (`file.*` fields) — forwarder JSON keys; ~2 days.
- **W3C PROV** (Entity/Activity/Agent) — `prov:wasDerivedFrom` between
  file versions; satisfies regulator lineage requests; ~3 days.
- **DCAT v3** (Catalog/Dataset/Distribution) — `/dcat/catalog.jsonld`
  endpoint; data-governance teams ingest our inventory into their existing
  catalogs; ~1 week.

### Don't bother
- Paperless-ngx, Wazuh/OSSEC, Cyberduck — GPL viral.
- GovReady-Q, OSCAL — narrative/control catalogs, not data classification.
- Davlan multilingual NER — no Turkish, awkward license.
- LayoutLMv3 — CC-BY-NC-SA-4.0 blocks commercial bundling.
- Elastic detection-rules — source-available, not OSI; reference ECS only.

---

*This document is mirrored by a pinned GitHub issue for operational
visibility. Changes here should be reflected in that issue and
vice-versa.*
