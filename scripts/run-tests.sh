#!/usr/bin/env bash
# FILE_ACTIVITY — Docker test runner (issue #91, Phase 1).
#
# Usage:
#   ./scripts/run-tests.sh                                # full suite, fresh build
#   ./scripts/run-tests.sh tests/test_pii_engine.py -v    # subset, fresh build
#   ./scripts/run-tests.sh --no-build tests/              # reuse cached image
#   ./scripts/run-tests.sh --shell                        # interactive bash
#
# Prerequisites:
#   * Docker Engine 20.10+ and Docker Compose v2 (`docker compose`, not
#     legacy `docker-compose`).
#   * No special permissions inside the container; tests run as the default
#     `root` user from `python:3.11-slim`.
#
# The exit code from pytest is propagated back unchanged so CI / local
# scripts can rely on `$?`.
set -euo pipefail

# Resolve repo root regardless of where the script is invoked from. macOS
# ships with bash 3.2 and lacks GNU `readlink -f`, so we hop directories
# instead — works on both Linux and macOS without coreutils.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.test.yml"

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker not found on PATH" >&2
    exit 127
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "error: 'docker compose' v2 plugin not available (legacy 'docker-compose' is not supported)" >&2
    exit 127
fi

BUILD_FLAG="--build"
SHELL_MODE=0
PYTEST_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --no-build)
            BUILD_FLAG=""
            shift
            ;;
        --shell)
            SHELL_MODE=1
            shift
            ;;
        --help|-h)
            sed -n '2,18p' "$0"
            exit 0
            ;;
        --)
            shift
            PYTEST_ARGS+=("$@")
            break
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

if [ "${SHELL_MODE}" -eq 1 ]; then
    # Override the entrypoint so users can poke around inside the image.
    exec docker compose -f "${COMPOSE_FILE}" run --rm ${BUILD_FLAG} \
        --entrypoint /bin/bash pytest
fi

# `run --rm` removes the container on exit; pytest's exit code surfaces as
# the compose exit code, which `exec` then propagates as the script's.
if [ "${#PYTEST_ARGS[@]}" -eq 0 ]; then
    exec docker compose -f "${COMPOSE_FILE}" run --rm ${BUILD_FLAG} pytest
else
    exec docker compose -f "${COMPOSE_FILE}" run --rm ${BUILD_FLAG} pytest "${PYTEST_ARGS[@]}"
fi
