# Standards alignment — W3C PROV + DCAT v3

- date: 2026-04-28
- issue: #145 (umbrella) — PR for `lineage` + `dcat`
- author: standards-alignment subagent

## TL;DR

FILE ACTIVITY now exposes two read-only JSON-LD endpoints so customer
data-governance teams can ingest our scan inventory + audit trail into
their existing catalogs (Apache Atlas, LinkedIn DataHub, Collibra,
OKFN CKAN, Magda) without bespoke adapters.

| Endpoint                                      | Standard         | Use case                        |
|-----------------------------------------------|------------------|---------------------------------|
| `/api/compliance/lineage/file.jsonld?path=…`  | W3C PROV-O       | Per-file regulator lineage ask  |
| `/api/compliance/lineage/scan.jsonld?scan_id=…` | W3C PROV-O     | "What did this scan touch"      |
| `/api/compliance/dcat/catalog.jsonld`         | W3C DCAT v3      | Bulk catalog ingest             |

Both are gated by the bearer-token middleware (issue #158) just like
the rest of the API; no separate auth path.

The forwarder portion of the standards umbrella (ECS-keyed JSON
log records) shipped earlier in PR #146 — see
`src/integrations/syslog_forwarder.py`.

## Why these three (and not OSCAL, ECS-as-API, RDF/SHACL)?

ROADMAP entry "Standards alignment" surveyed the candidate space
and landed on the three that customer data-governance teams already
ingest natively:

- **ECS** — for SIEM forwarding. Already done in PR #146.
- **W3C PROV** — every regulator who has ever asked "show me how
  this file got there" is happy to receive a PROV graph; both
  Apache Atlas and DataHub render it natively.
- **DCAT v3** — Collibra / CKAN / Magda catalog browsers ingest
  DCAT JSON-LD as a first-class citizen. We get bulk-discoverability
  for free.

Skipped:
- OSCAL — narrative/control catalog, not data classification.
- RDF/SHACL — would require a triple store; doesn't add ingest
  parity over JSON-LD.
- Generic Linked Data — too vague to provide value.

## PROV mapping

The `LineageBuilder` in `src/compliance/lineage.py` walks the
`file_audit_events` table for a single `file_path` (or for the
window of a single `scan_id`) and renders one PROV-O JSON-LD
document. Mapping:

| `file_audit_events` column         | PROV term                         |
|------------------------------------|-----------------------------------|
| `file_path`                        | `prov:Entity`                     |
| One row in the table               | `prov:Activity`                   |
| `username` (or SID)                | `prov:Agent`                      |
| `event_type='created'`             | `prov:wasGeneratedBy` link        |
| Other event types                  | `prov:wasInfluencedBy` link       |
| `event_time`                       | `prov:atTime` / `prov:generatedAtTime` |
| `details`                          | `prov:hadDescription`             |

The scan-level builder additionally:
- Wraps every distinct `file_path` Entity in a `prov:Collection`.
- Emits a top-level `prov:Activity` for the scan run itself.
- Links the Collection to the Activity via `prov:wasGeneratedBy`.

### Customer ingest examples

**Apache Atlas** (HTTP POST to its REST endpoint):
```bash
curl -u admin:admin -X POST http://atlas:21000/api/atlas/v2/lineage/jsonld \
  -H "Content-Type: application/ld+json" \
  --data-binary @lineage.jsonld
```

**LinkedIn DataHub** (via the REST emitter):
```bash
datahub put --urn "urn:li:dataset:..." --aspect-name "lineage" \
  --aspect-value "$(cat lineage.jsonld)"
```

**Collibra Data Catalog** (REST API, JSON-LD endpoint):
```bash
curl -X POST https://collibra.example.org/rest/2.0/import/json-ld \
  -H "Authorization: Bearer $COLLIBRA_TOKEN" \
  -H "Content-Type: application/ld+json" \
  --data-binary @lineage.jsonld
```

## DCAT mapping

The `CatalogBuilder` in `src/compliance/dcat.py` builds a DCAT v3
catalog of every `sources` row, with one `dcat:Distribution` per
completed `scan_runs` row. Mapping:

| FILE ACTIVITY concept              | DCAT term              |
|------------------------------------|------------------------|
| `sources` table (top level)        | `dcat:Catalog`         |
| One row in `sources`               | `dcat:Dataset`         |
| `scan_runs` per source             | `dcat:Distribution`    |
| `scanned_files` aggregates         | `dcat:byteSize`, `dcat:keyword`, `dct:modified` |
| Configurable per-deployment value  | `dct:license`          |
| Per-distribution stable identifier | `spdx:Checksum` (sha256 over scan tuple) |

### Customer ingest examples

**OKFN CKAN** (DCAT JSON-LD harvester is built-in):
```bash
ckan --config=/etc/ckan/ckan.ini harvester gather \
  --source-id $SOURCE --jsonld /tmp/dcat_catalog.jsonld
```

**Magda** (DCAT-AP JSON-LD ingest is supported via the connector
SDK):
```bash
magda-connector-csw-static \
  --jsonld /tmp/dcat_catalog.jsonld \
  --tenant-id $TENANT
```

**LinkedIn DataHub** has a DCAT ingestion source in the recipe
schema; point its `path` config at the downloaded file.

## Configuration

```yaml
compliance:
  standards:
    enabled: true                       # default true (read-only)
    organization_uri: "urn:fileactivity:"  # base IRI for PROV/DCAT nodes
    license_uri: "internal-use"         # default dct:license value
```

Set `organization_uri` to a public HTTPS prefix when distributing
the catalog externally; the URN form is fine for internal use.

## Implementation notes

- **Stdlib only**. No `rdflib`, no `pyld`. JSON-LD is just
  structured dicts when you only need to *write*; consumers handle
  the round-trip into RDF on their end.
- **Read-only**. Neither builder mutates state; the endpoints are
  idempotent.
- **Bearer auth respected**. Both endpoints sit behind the same
  middleware as `/api/compliance/legal-holds/...`; no separate
  auth path.
- **No file streaming** — JSON-LD documents are small enough to
  fit comfortably in a single HTTP response. If a customer's
  scan grows past ~100k files in one go, we can revisit a
  streaming variant.

## References

- W3C PROV-O: https://www.w3.org/TR/prov-o/
- W3C PROV-JSON: https://www.w3.org/Submission/prov-json/
- W3C DCAT v3 (2026-03 final): https://www.w3.org/TR/vocab-dcat-3/
- Apache Atlas lineage REST: https://atlas.apache.org/api/v2/
- LinkedIn DataHub: https://datahubproject.io/
- Collibra REST: https://developer.collibra.com/rest/
