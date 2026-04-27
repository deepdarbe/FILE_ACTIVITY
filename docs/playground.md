# Analytics Playground

A standalone Streamlit app for ad-hoc admin data exploration over the
FILE_ACTIVITY SQLite database. **Read-only.** Runs as its own process
on a separate port from the FastAPI dashboard. Does **not** replace the
dashboard — the REST API that the PowerShell module and the MCP server
depend on is untouched.

> **Sadece admin/yonetici icin. Internal network only. Asla public
> expose etme.** Bu arac auth varsayilan olarak kapali baslar; uzerinde
> bearer token gate'i vardir ama gercek bir reverse proxy + IP
> restriction olmadan internet'e cikarmayin.

## What it is

* Streamlit native multi-page app under `src/playground/`.
* SQLite is opened with `mode=ro` (URI flag) — the OS rejects any
  accidental write attempt.
* Heavy aggregates use DuckDB's `sqlite_scanner` extension with
  `READ_ONLY` ATTACH (mirrors `src/storage/analytics.py`).
* Pages auto-discovered from `src/playground/pages/` by Streamlit:

  | File                              | Purpose                               |
  |-----------------------------------|---------------------------------------|
  | `01_cold_data.py`                 | Yas + boyut esikli sogumus dosyalar   |
  | `02_duplicate_walker.py`          | Icerik kopyalari drill-down           |
  | `03_audit_timeline.py`            | Audit olaylari gunluk dagilim         |
  | `04_retention_what_if.py`         | fnmatch + yas onizleme (preview only) |
  | `05_pii_pivot.py`                 | PII pattern × scan heatmap            |

## What it ISN'T

* **Not a dashboard replacement.** All production operations
  (scan/archive/retention apply, etc.) still go through the FastAPI
  dashboard at `http://localhost:8085`.
* **Not for non-admins.** No row-level access control. Anyone who has
  the URL + token can read the entire DB.
* **Not for the public internet.** No HTTPS, no CSRF protection
  beyond the bearer token, no rate limiting.

## Install

The playground deps are kept out of the base install so the dashboard
deployment stays lean:

```bash
pip install -r requirements-playground.txt
```

This installs `streamlit`, `plotly`, and `pandas`. DuckDB is reused
from the base `requirements.txt`.

## Run

```bash
streamlit run src/playground/app.py --server.port 8086
```

Opens the app at <http://localhost:8086>. The dashboard at
<http://localhost:8085> is unaffected.

The app reads `config/config.yaml` (with `config.yaml` as a fallback)
to find the SQLite DB path under `database.path`. Relative paths
resolve against the project root.

## Auth

Set a bearer token in the environment before launching:

```bash
export FILEACTIVITY_PLAYGROUND_TOKEN="$(openssl rand -hex 32)"
streamlit run src/playground/app.py --server.port 8086
```

Then access via either:

* `http://localhost:8086/?token=<X>` (query string), or
* `Authorization: Bearer <X>` header (Streamlit >= 1.37 only).

If `FILEACTIVITY_PLAYGROUND_TOKEN` is unset, the app boots in **dev
mode** — no auth, but a red banner at the top warns that production
deployments must set the token. Token comparison uses
`hmac.compare_digest` so it is constant-time.

## Read-only contract

Three layers enforce the read-only invariant:

1. SQLite URI: `sqlite3.connect("file:" + path + "?mode=ro", uri=True)`
   — the OS rejects writes at the syscall level.
2. DuckDB ATTACH: `ATTACH '...' AS sqlite_db (TYPE SQLITE, READ_ONLY)`.
3. `data_access.assert_select_only()` helper rejects any SQL string
   not starting with `SELECT` / `WITH` / `PRAGMA`. Reserved for any
   future ad-hoc query surface; the current pages use parameterised
   SELECTs only.

If a developer ever wires in a write path the layered defences
guarantee the app can't actually mutate the DB even if step (3) is
forgotten.

## Performance

The 2.5M-row `scanned_files` cold-data page renders in under 2
seconds on a typical workstation. Heavy lifting happens in DuckDB's
columnar engine via the read-only ATTACH; pages fall back to
straight SQLite if DuckDB isn't available.

## Tests

`tests/test_playground_imports.py` uses `pytest.importorskip` so the
suite passes on CI runners that haven't installed
`requirements-playground.txt`. When Streamlit *is* installed, every
page module must import cleanly.
