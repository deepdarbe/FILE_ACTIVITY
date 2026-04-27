# Synthetic test corpus (`tests/fixtures/`)

Deterministic, on-disk file tree used by integration tests in lieu of
real customer data. Issue #91 phase 2 / issue #94.

## What's in it

A tree of ~10 000 files (or ~1 000 in `quick` mode) covering every
detector in the analyzer:

| bucket            | full | notes                                     |
| ----------------- | ---- | ----------------------------------------- |
| duplicates        | 121  | 47 groups, each member > 1 MB             |
| PII (5 patterns)  | 12   | synthetic, e.g. `4111-1111-1111-1111`     |
| naming violations | 50   | `CON.txt`, `double..dot`, trailing space  |
| empty             | 8    | zero-byte                                 |
| temp              | 40   | `.tmp` `.bak` `~$Doc.docx` ...            |
| hidden            | 30   | leading dot                               |
| > 100 MB          | 200  | sparse (real disk ≈ 1 byte each)          |
| > 1 GB            | 50   | sparse                                    |
| stale 180/365/3y  | 2500 / 1500 / 500 | nested supersets               |

Each directory gets a sidecar `_owners.json` mapping filename to a
fake AD owner (`CONTOSO\\jdoe` etc).

## Regenerate

```sh
python -m tests.fixtures.generate_corpus --out /tmp/corpus            # full
python -m tests.fixtures.generate_corpus --out /tmp/corpus --quick    # 1 000 files
python -m tests.fixtures.generate_corpus --out /tmp/c --json-manifest /tmp/m.json
```

## Determinism guarantee

Same `seed` (default 42) ⇒ byte-identical tree, identical mtimes,
identical content. `diff -r` between two runs produces zero output.
The generator uses `random.Random(seed)` instances and a frozen
"now" anchor for mtime computation; never the global `random` module.

## Disk usage

Total real disk usage stays under ~100 MB even for the full 10 000-
file corpus thanks to sparse `truncate()` for the > 100 MB / > 1 GB
buckets.

## Use from a test

```python
def test_scanner(fixture_corpus_quick):
    manifest = fixture_corpus_quick
    # scan manifest.out_dir, assert against manifest.expected_*
```

The `fixture_corpus_quick` fixture is session-scoped — built once per
test session.
