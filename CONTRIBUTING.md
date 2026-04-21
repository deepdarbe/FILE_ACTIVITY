# Contributing

Short guide for making changes to FILE ACTIVITY.

## Development setup

Full integration requires Windows 10/11 or Windows Server 2016+ because the
scanner and archiver modules use `pywin32` APIs (`win32security`,
`win32api`, etc.). Linux works for most of the code except those modules.

```powershell
git clone https://github.com/deepdarbe/FILE_ACTIVITY.git
cd FILE_ACTIVITY
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py dashboard
```

Dashboard: http://localhost:8085

On Linux (for docs / analytics / db layer work):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # pywin32 will be skipped via environment markers
python -m compileall -q src/ main.py
```

## Branch naming

- `feat/<short-desc>` — new features
- `fix/<short-desc>` — bug fixes
- `chore/<short-desc>` — maintenance (deps, CI, docs)
- `claude/<short-desc>` — branches produced by Claude Code sessions

## Commit messages

- Subject ≤ 72 chars, imperative mood ("Add X", not "Added X")
- Blank line, then body explaining **why**, not **what**
- Reference issues like `Closes #42` in the body when relevant

Example:

```
Fix pip SSL failure behind corporate TLS-inspection proxies

Customer install on an AD-joined Windows Server failed at pip install
with SSL CERTIFICATE_VERIFY_FAILED. The corporate proxy MITMs HTTPS...
```

## Pull requests

1. Branch off `master`
2. Open PR against `master` — fill in the template (Summary + Test plan)
3. Wait for CI green (syntax + PowerShell parse)
4. Merge with a merge commit (not squash) to preserve logical units

## Code style

- Stdlib preferred — don't pull in a dep for trivial helpers
- Match the style of the file you're editing; there's no strict formatter
- Type hints welcome on new code, not mandatory for existing code
- Comments explain **why** non-obvious decisions were made; skip obvious
  **what** (that's what identifiers are for)
- No dead code. No `TODO: ...` for planned work — file an issue instead.

## Storage layer

- SQLite is the source of truth (`src/storage/database.py`)
- DuckDB (`src/storage/analytics.py`) is an optional read-only layer for
  heavy aggregates. Every caller must have a SQLite fallback.
- When you add a new analytical query, write it first in SQLite, then
  optionally add a DuckDB fast-path.

## Testing

No automated test suite yet. For now:

- `python -m compileall -q src/ main.py` passes
- Manual smoke: scan → dashboard → drill-down → archive → restore
- If you touch `src/storage/analytics.py`, verify duplicate / drill-down /
  growth against a small in-memory SQLite fixture (pattern in the commit
  history on PRs #1, #2)

## Release process

1. Merge changes to `master`
2. Tag a semver release: `git tag v1.x.y && git push --tags`
3. GitHub Actions (`.github/workflows/release.yml`) builds the EXE on
   `windows-latest`, creates the release, and attaches
   `FileActivity-Deploy.zip`
4. Existing installs pick up the new release via `setup.ps1` or via
   `C:\FileActivity\update.cmd` (source installs)
