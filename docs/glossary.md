# Glossary — identifier names

The codebase juggles four kinds of "id" that look interchangeable but refer to
distinct entities. The refactor preserves all four; do not collapse them.

| Name | Owner | Used in | Lifetime |
|---|---|---|---|
| `task_id` | our queue (SQLite `tasks` table or in-process queue) | `/api/worker/poll`, `Worker.run_one` | one scrape run |
| `tracked_dataset_id` | over.org.il | `over_worker.py` requests; `OverOrgPublisher` only | persistent across versions |
| `collection_id` | our storage layer | `storage.collections.save`, `/api/collections/<id>` | persistent |
| `parcel_id` | nadlan distributed queue (gush-helka-subhelka) | `/api/nadlan/bulk-*` | one parcel scrape |

# Naming convention

- **Internal Python:** `snake_case` for variables, functions, methods.
  Class names: `PascalCase` (`GovIlScraper`, not `GovILScraper` — `Il` is a
  word, not an acronym).
- **External API field names** (`From`, `officeId`, `DynamicTemplateID`,
  `LayerName`, `tracked_dataset_id`): preserved verbatim as constants in the
  scraper's `_fields.py`, never renamed.
- **over.org.il contract:** locked in `worker/publishers/_contract.py`. No
  field there may be renamed; the contract test enforces byte-identity.

# Scraper IDs (registry keys)

`govil` · `datagovil` · `nadlan` · `govmap`

# External-API field constants (`_fields.py`)

Each scraper has a `_fields.py` module that pins the wire-format strings
exposed by its remote API. These are camelCase / PascalCase / lowercase
mixed because that's what the upstream services emit. They are **not**
Python identifiers we're free to rename; only the constant *holding* them
is snake_case.

| Scraper | File | Examples |
|---|---|---|
| govil | [govscraper/scrapers/govil/_fields.py](../govscraper/scrapers/govil/_fields.py) | `DynamicTemplateID`, `From`, `officeId`, `CollectorType` |
| datagovil | [govscraper/scrapers/datagovil/_fields.py](../govscraper/scrapers/datagovil/_fields.py) | `q`, `rows`, `start`, `resource_id`, `offset`, `limit`, `filters` |
| nadlan | [govscraper/scrapers/nadlan/_fields.py](../govscraper/scrapers/nadlan/_fields.py) | `dealDate`, `assetId`, `settlementCode`, `priceSM` |
| govmap | [govscraper/scrapers/govmap/_fields.py](../govscraper/scrapers/govmap/_fields.py) | `typeNames`, `outputFormat`, `bbox`, `srsName` |
| over.org.il | [govscraper/worker/publishers/_contract.py](../govscraper/worker/publishers/_contract.py) | `tracked_dataset_id`, `metadata_modified`, `version_number`, `"נתוני הסורק"` |

# snake_case aliases for legacy class names

`scraper_engine.py` historically used the all-caps `IL` suffix
(`GovILScraper`, `GovILSession`). PEP-8 treats two-letter
abbreviations as acronyms — the correct casing is `Il`.

Both spellings are importable from `scraper_engine`:

| Legacy | Canonical (phase F+) |
|---|---|
| `GovILSession` | `GovIlSession` |
| `GovILScraper` | `GovIlScraper` |
| `GovILScraperError` | `GovIlScraperError` |

The old spellings will be removed in phase G's deletion of
`scraper_engine.py` (it becomes a re-export shim from
`govscraper.scrapers.govil`).
