"""Pytest fixtures for the synthetic corpus."""

from __future__ import annotations

import pytest

from .generate_corpus import generate_corpus
from .manifest import CorpusManifest


@pytest.fixture(scope="session")
def fixture_corpus_quick(tmp_path_factory) -> CorpusManifest:
    """Session-scoped quick corpus (~1 000 files, < 5 s build)."""
    out_dir = tmp_path_factory.mktemp("corpus_quick")
    return generate_corpus(out_dir, quick=True)
