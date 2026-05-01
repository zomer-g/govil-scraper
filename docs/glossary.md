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
