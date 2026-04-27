"""Synthetic corpus generator for issue #91 phase 2.

Produces a deterministic, on-disk file tree whose shape matches the
contract in :mod:`tests.fixtures.manifest`. Same ``seed`` ⇒ byte-
identical output (including mtimes and content). Sparse ``truncate()``
is used for the > 100 MB / > 1 GB buckets so the real disk usage stays
under ~100 MB even for the full 10 000-file corpus.

Run as a module:

    python -m tests.fixtures.generate_corpus --out /tmp/corpus
    python -m tests.fixtures.generate_corpus --out /tmp/corpus --quick
    python -m tests.fixtures.generate_corpus --out /tmp/corpus \
        --seed 42 --json-manifest /tmp/manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from pathlib import Path

from .manifest import (
    EXPECTED_DUPLICATE_FILES,
    EXPECTED_DUPLICATE_GROUPS,
    EXPECTED_EMPTY_FILES,
    EXPECTED_HIDDEN_FILES,
    EXPECTED_LARGE_FILES,
    EXPECTED_NAMING_VIOLATIONS,
    EXPECTED_PII_FINDINGS,
    EXPECTED_STALE_180,
    EXPECTED_STALE_1YEAR,
    EXPECTED_STALE_3YEAR,
    EXPECTED_TEMP_FILES,
    EXPECTED_VERY_LARGE,
    TOTAL_FILES,
    CorpusManifest,
)

# ---------------------------------------------------------------------------
# Synthetic, non-real PII strings. These deliberately use the well-known
# "obviously fake" values so a real secret scanner won't flag them as
# leaks while the project's own pattern engine still matches.
# ---------------------------------------------------------------------------
PII_VALUES = {
    "email": "noone@example.invalid",
    "iban_tr": "TR12 0006 1005 1978 6457 8413 26",
    "phone_tr": "+90 555 111 22 33",
    "tckn": "11111111110",
    "credit_card": "4111-1111-1111-1111",
}

DIR_NAMES = [
    "projects", "archive", "shared", "reports", "drafts", "finance",
    "hr", "legal", "engineering", "marketing", "sales", "ops",
    "2021", "2022", "2023", "2024", "q1", "q2", "q3", "q4",
    "incoming", "outgoing", "team", "personal", "review",
]
EXTS = [".txt", ".csv", ".log", ".md", ".json", ".xml", ".html", ".py"]
TEMP_PATTERNS = [".tmp", ".temp", ".bak", ".old", ".cache", ".log"]
AD_OWNERS = [
    "CONTOSO\\jdoe", "CONTOSO\\asmith", "CONTOSO\\bwilson", "CONTOSO\\mlee",
    "CONTOSO\\rgupta", "FABRIKAM\\nkim", "FABRIKAM\\pchen",
    "CONTOSO\\service-batch", "CONTOSO\\admin",
]


def _now() -> float:
    # Frozen "now" so that mtimes are stable across runs given the same
    # seed (we offset deterministically from this anchor).
    return 1_700_000_000.0


def _set_mtime(path: Path, days_old: float) -> None:
    ts = _now() - days_old * 86400.0
    os.utime(path, (ts, ts))


def _mkparents(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _pick_dir(rng: random.Random, root: Path) -> Path:
    depth = rng.randint(3, 6)
    parts = [rng.choice(DIR_NAMES) for _ in range(depth)]
    return root.joinpath(*parts)


def _write_text(path: Path, body: str, days_old: float) -> None:
    _mkparents(path)
    path.write_text(body, encoding="utf-8")
    _set_mtime(path, days_old)


def _write_sparse(path: Path, size_bytes: int, days_old: float) -> None:
    _mkparents(path)
    with open(path, "wb") as f:
        f.truncate(size_bytes)
        # Single trailing byte so the file size is exact and tools that
        # stat() rather than seek to EOF read the right value.
        f.seek(size_bytes - 1)
        f.write(b"\0")
    _set_mtime(path, days_old)


def _record_owner(owners: dict, path: Path, owner: str) -> None:
    d = str(path.parent)
    owners.setdefault(d, {})[path.name] = owner


def _flush_owners(owners: dict) -> None:
    for d, mapping in owners.items():
        sidecar = Path(d) / "_owners.json"
        sidecar.write_text(
            json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8"
        )


# ---------------------------------------------------------------------------
# Per-bucket emitters. Each returns the number of files it produced.
# ---------------------------------------------------------------------------

def _emit_duplicates(
    rng: random.Random, root: Path, owners: dict,
    groups: int, total_files: int,
) -> int:
    # Distribute total_files across groups, each group >= 2 members.
    sizes = [2] * groups
    remaining = total_files - 2 * groups
    while remaining > 0:
        i = rng.randrange(groups)
        if sizes[i] < 5:
            sizes[i] += 1
            remaining -= 1
        elif all(s >= 5 for s in sizes):
            sizes[rng.randrange(groups)] += 1
            remaining -= 1
    written = 0
    for gi, count in enumerate(sizes):
        # >1 MB payload, deterministic per group.
        seed_bytes = f"dupe-group-{gi}".encode()
        block = hashlib.sha256(seed_bytes).digest() * (1024 * 32 + 1)  # ~1 MB
        block = block[: 1024 * 1024 + 64]
        for mi in range(count):
            d = _pick_dir(rng, root)
            p = d / f"dupe_{gi:02d}_{mi}.bin"
            _mkparents(p)
            p.write_bytes(block)
            _set_mtime(p, rng.uniform(1, 90))
            _record_owner(owners, p, rng.choice(AD_OWNERS))
            written += 1
    return written


def _emit_pii(rng: random.Random, root: Path, owners: dict) -> int:
    written = 0
    for pattern, count in EXPECTED_PII_FINDINGS.items():
        for i in range(count):
            d = _pick_dir(rng, root)
            p = d / f"pii_{pattern}_{i}.txt"
            body = (
                f"Synthetic test record (do not flag).\n"
                f"value: {PII_VALUES[pattern]}\n"
                f"id: {pattern}-{i}\n"
            )
            _write_text(p, body, rng.uniform(10, 200))
            _record_owner(owners, p, rng.choice(AD_OWNERS))
            written += 1
    return written


def _emit_naming_violations(
    rng: random.Random, root: Path, owners: dict, count: int,
) -> int:
    reserved = ["CON.txt", "PRN.txt", "AUX.txt", "NUL.txt", "COM1.txt"]
    written = 0
    for i in range(count):
        d = _pick_dir(rng, root)
        kind = i % 5
        if kind == 0:
            name = f"trailing space {i} .txt"
        elif kind == 1:
            name = f"double..dot.{i}.txt"
        elif kind == 2:
            name = reserved[i % len(reserved)].replace(".txt", f"_{i}.txt")
            # Real reserved on first few only:
            if i < len(reserved):
                name = reserved[i]
        elif kind == 3:
            name = f"name_with_trailing_dot_{i}."
        else:
            name = f"weird#char${i}.txt"
        p = d / name
        _write_text(p, f"naming violation {i}\n", rng.uniform(1, 60))
        _record_owner(owners, p, rng.choice(AD_OWNERS))
        written += 1
    return written


def _emit_empty(rng: random.Random, root: Path, owners: dict, count: int) -> int:
    for i in range(count):
        d = _pick_dir(rng, root)
        p = d / f"empty_{i}.dat"
        _mkparents(p)
        p.touch()
        _set_mtime(p, rng.uniform(1, 365))
        _record_owner(owners, p, rng.choice(AD_OWNERS))
    return count


def _emit_temp(rng: random.Random, root: Path, owners: dict, count: int) -> int:
    for i in range(count):
        d = _pick_dir(rng, root)
        if i % 7 == 0:
            p = d / f"~$Doc{i}.docx"
        else:
            ext = TEMP_PATTERNS[i % len(TEMP_PATTERNS)]
            p = d / f"scratch_{i}{ext}"
        _write_text(p, f"temp file {i}\n", rng.uniform(1, 30))
        _record_owner(owners, p, rng.choice(AD_OWNERS))
    return count


def _emit_hidden(rng: random.Random, root: Path, owners: dict, count: int) -> int:
    for i in range(count):
        d = _pick_dir(rng, root)
        p = d / f".hidden_{i}.cfg"
        _write_text(p, f"hidden={i}\n", rng.uniform(1, 200))
        _record_owner(owners, p, rng.choice(AD_OWNERS))
    return count


def _emit_large(
    rng: random.Random, root: Path, owners: dict,
    count: int, size_bytes: int, prefix: str,
) -> int:
    for i in range(count):
        d = _pick_dir(rng, root)
        p = d / f"{prefix}_{i:04d}.bin"
        _write_sparse(p, size_bytes, rng.uniform(1, 90))
        _record_owner(owners, p, rng.choice(AD_OWNERS))
    return count


def _emit_filler(
    rng: random.Random, root: Path, owners: dict,
    count: int, stale_180: int, stale_1y: int, stale_3y: int,
) -> int:
    """Plain files that fill the corpus and carry the staleness buckets."""
    # Build an age list. stale_3y ⊂ stale_1y ⊂ stale_180 per spec.
    ages: list[float] = []
    ages += [rng.uniform(1095 + 1, 1095 + 1000) for _ in range(stale_3y)]
    ages += [
        rng.uniform(365 + 1, 1095 - 1)
        for _ in range(stale_1y - stale_3y)
    ]
    ages += [
        rng.uniform(180 + 1, 365 - 1)
        for _ in range(stale_180 - stale_1y)
    ]
    fresh_count = count - len(ages)
    ages += [rng.uniform(0, 179) for _ in range(fresh_count)]
    rng.shuffle(ages)

    for i, age in enumerate(ages):
        d = _pick_dir(rng, root)
        ext = EXTS[i % len(EXTS)]
        p = d / f"file_{i:05d}{ext}"
        # Tiny body, deterministic.
        body = f"id={i}\nseed={i * 7 % 9973}\n"
        _write_text(p, body, age)
        _record_owner(owners, p, rng.choice(AD_OWNERS))
    return count


# ---------------------------------------------------------------------------
# Public entrypoint.
# ---------------------------------------------------------------------------

def _scaled(quick: bool, full: int, ratio: float) -> int:
    return max(1, int(round(full * ratio))) if quick else full


def generate_corpus(
    out_dir: Path | str, seed: int = 42, quick: bool = False,
) -> CorpusManifest:
    """Generate a synthetic corpus rooted at ``out_dir``.

    ``quick=True`` shrinks the corpus to ~1 000 files keeping the same
    ratios so unit tests stay fast (< 5 s). The returned manifest
    reflects the *quick* counts in that case so assertions remain valid.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    owners: dict = {}

    ratio = 0.1 if quick else 1.0
    total = _scaled(quick, TOTAL_FILES, ratio)
    dup_groups = _scaled(quick, EXPECTED_DUPLICATE_GROUPS, ratio)
    dup_files = _scaled(quick, EXPECTED_DUPLICATE_FILES, ratio)
    if dup_files < dup_groups * 2:
        dup_files = dup_groups * 2
    naming = _scaled(quick, EXPECTED_NAMING_VIOLATIONS, ratio)
    empty = _scaled(quick, EXPECTED_EMPTY_FILES, ratio)
    temp = _scaled(quick, EXPECTED_TEMP_FILES, ratio)
    hidden = _scaled(quick, EXPECTED_HIDDEN_FILES, ratio)
    large = _scaled(quick, EXPECTED_LARGE_FILES, ratio)
    very_large = _scaled(quick, EXPECTED_VERY_LARGE, ratio)
    stale_180 = _scaled(quick, EXPECTED_STALE_180, ratio)
    stale_1y = _scaled(quick, EXPECTED_STALE_1YEAR, ratio)
    stale_3y = _scaled(quick, EXPECTED_STALE_3YEAR, ratio)
    pii_total = sum(EXPECTED_PII_FINDINGS.values())  # PII counts unscaled

    written = 0
    written += _emit_duplicates(rng, out, owners, dup_groups, dup_files)
    written += _emit_pii(rng, out, owners)
    written += _emit_naming_violations(rng, out, owners, naming)
    written += _emit_empty(rng, out, owners, empty)
    written += _emit_temp(rng, out, owners, temp)
    written += _emit_hidden(rng, out, owners, hidden)
    written += _emit_large(rng, out, owners, large, 101 * 1024 * 1024, "large")
    written += _emit_large(
        rng, out, owners, very_large, 1024 * 1024 * 1024 + 1, "vlarge",
    )

    filler = max(0, total - written)
    # Ensure the staleness buckets fit inside filler.
    if filler < stale_180:
        stale_180 = filler
        stale_1y = min(stale_1y, stale_180)
        stale_3y = min(stale_3y, stale_1y)
    _emit_filler(rng, out, owners, filler, stale_180, stale_1y, stale_3y)

    _flush_owners(owners)

    if quick:
        return CorpusManifest(
            out_dir=out,
            seed=seed,
            total_files=total,
            expected_duplicates=dup_groups,
            expected_duplicate_files=dup_files,
            expected_pii_findings=dict(EXPECTED_PII_FINDINGS),
            expected_naming_violations=naming,
            expected_empty_files=empty,
            expected_temp_files=temp,
            expected_hidden_files=hidden,
            expected_stale_1year=stale_1y,
            expected_stale_3year=stale_3y,
            expected_stale_180=stale_180,
            expected_large_files=large,
            expected_very_large=very_large,
        )
    return CorpusManifest(out_dir=out, seed=seed)


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------

def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--json-manifest", type=Path, default=None)
    args = parser.parse_args(argv)

    t0 = time.time()
    manifest = generate_corpus(args.out, seed=args.seed, quick=args.quick)
    elapsed = time.time() - t0
    print(f"corpus written to {args.out} in {elapsed:.2f}s "
          f"({manifest.total_files} files)")
    if args.json_manifest:
        manifest.write_json(args.json_manifest)
        print(f"manifest -> {args.json_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
