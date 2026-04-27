# Docker test infrastructure

Local + CI-friendly container for running the FILE_ACTIVITY pytest suite on
Linux. Avoids polluting the developer workstation with system C/C++
toolchain, libpcre3, libhyperscan, etc.

This is **Phase 1 of issue #91** — only the Python unit-test runner. SMB
fixtures (Phase 2) and CI integration (Phase 3) live in separate PRs.

## Quick reference

```bash
# Build the image (one-time, or after dep changes)
docker compose -f docker/docker-compose.test.yml build

# Run the full suite (rebuilds if needed)
./scripts/run-tests.sh

# Run a specific test file with verbose output
./scripts/run-tests.sh tests/test_pii_engine.py -v

# Drop into an interactive shell inside the test container
./scripts/run-tests.sh --shell

# Re-use the cached image (skip rebuild for fast iteration)
./scripts/run-tests.sh --no-build tests/test_pii_engine.py
```

The wrapper passes any extra arguments straight to `pytest`.

## Image details

* Base: `python:3.11-slim` (Debian bookworm).
* User: runs as `root` — the default for the upstream Python image. The
  test suite writes only into `tmp_path` fixtures so this is safe; switch
  to a non-root UID later if we add a CI hardening pass.
* System packages: `build-essential`, `libpcre3-dev`, `libhyperscan-dev`,
  `git`. Hyperscan is preferred via the Debian `libhyperscan-dev` package
  (Hyperscan 5.4) so the `hyperscan` Python wheel can link against the
  system library; if the wheel still has to build from source, the C++
  toolchain and PCRE headers are present.
* Optional dependencies (`requirements-accel.txt`, `requirements-mcp.txt`)
  are installed best-effort. Tests gate the affected paths with
  `pytest.importorskip`, so a partial install just produces extra skips
  rather than failures.
* `pywin32` is filtered out of `requirements.txt` at install time — it
  publishes Windows-only wheels and the Windows-only tests already gate
  themselves with `sys.platform == "win32"`.

## Files

| Path | Purpose |
| ---- | ------- |
| `docker/Dockerfile.test` | Single-stage test image. |
| `docker/docker-compose.test.yml` | `pytest` service with bind-mount for live edits. |
| `scripts/run-tests.sh` | Thin wrapper around `docker compose run`. |
| `.dockerignore` | Build-context filter (mirrors `.gitignore`). |

## Prerequisites

* Docker Engine 20.10+
* Docker Compose v2 (`docker compose`, not legacy `docker-compose`)
* ~1 GB free disk for the built image and dependency layers

The wrapper script is POSIX-bash compatible and works unchanged on Linux
and macOS.
