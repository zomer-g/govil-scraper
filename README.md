# govil-scraper

A multi-component scraping platform for Israeli government open data.

The repo holds these cooperating subsystems:

1. **gov.il collector scraper** — extracts structured data from `gov.il`
   DynamicCollectors, traditional Collectors, and React `/he/pages/...`
   content pages. Web UI for ad-hoc admin scrapes, CSV/Excel export,
   file-attachment downloads.
2. **OVER integration** — a long-running local worker
   (`over_worker.py`) that polls `over.org.il` for scrape tasks, runs
   them locally, and pushes versioned results back as `tracked_dataset`
   updates. Supports incremental archive mode.
3. **nadlan.gov.il real-estate scraper** — Playwright-driven extraction
   of Israel Tax Authority transaction data, both single-parcel and
   nation-wide settlement-level. Daily incremental updates supported.
4. **Distributed bulk-nadlan worker pool** — server-coordinated queue
   so multiple machines can scrape the 1M+ parcels in parallel. Atomic
   task claims, central CSV aggregation, automatic stale-task recovery.
5. **GovMap GIS layer scraper** — pulls geospatial features from
   `govmap.gov.il`'s public OGC WFS at `/api/geoserver/wfs`. Outputs
   GeoJSON (WGS84) + CSV/Excel + manifest. Supports country-wide and
   BBOX-narrowed scrapes; layer-name and URL-based triggers; Leaflet UI
   for drawing the BBOX visually. See
   [GovMap GIS layers](#govmap-gis-layers) below.

The Flask app is deployed on Render at
[`https://govil-scraper.onrender.com`](https://govil-scraper.onrender.com).
Workers run on the operator's local machine (Windows / macOS / Linux
desktop).

---

## Table of contents

- [Architecture](#architecture)
- [Supported sources](#supported-sources)
- [Quick start (local dev)](#quick-start-local-dev)
- [Running the workers locally](#running-the-workers-locally)
  - [govil-scraper worker](#1-govil-scraper-worker-runs-flask-tasks)
  - [OVER worker](#2-over-worker-tracked-datasets-on-overorgil)
  - [nadlan tools](#3-nadlan-tools-real-estate-deals)
  - [Distributed bulk-nadlan worker pool](#4-distributed-bulk-nadlan-worker-pool)
- [GovMap GIS layers](#govmap-gis-layers)
- [HTTP API reference](#http-api-reference)
- [Environment variables](#environment-variables)
- [Deployment](#deployment-render)
- [Daily nadlan trigger (GitHub Actions)](#daily-nadlan-trigger-github-actions)
- [Tests](#tests)
- [Project layout](#project-layout)

---

## Architecture

```
                                      ┌──────────────────────────────────────┐
                                      │  Flask app (Render)                  │
   admin browser ──── HTTPS ─────────▶│  govil-scraper.onrender.com          │
                                      │  • UI + admin API                    │
                                      │  • collections / archives store      │
                                      │  • worker queue (delegated scrapes)  │
                                      │  • nadlan helper endpoints           │
                                      └────────┬─────────────────────────────┘
                                               │
                          /api/worker/* (X-Worker-Key)
                                               │
                                ┌──────────────┴──────────────┐
                                │                             │
                                ▼                             ▼
                  ┌──────────────────────┐       ┌───────────────────────┐
                  │ govil-scraper worker │       │  OVER worker          │
                  │ worker.py            │       │  over_worker.py       │
                  │ (run_worker.bat)     │       │  (run_over_worker.bat)│
                  │                      │       │                       │
                  │ polls Flask          │       │ polls over.org.il     │
                  │ → scraper_engine     │       │ → archive_engine      │
                  │ → uploads ZIP back   │       │   or nadlan_engine    │
                  └──────────────────────┘       │ → push_version()      │
                                                 └───────────────────────┘

                  Optional standalone tools (no server):
                  ┌──────────────────────────────────────┐
                  │ run_nadlan.bat → run_single_parcel,  │
                  │   bulk_nadlan, incremental_nadlan    │
                  └──────────────────────────────────────┘
```

## Supported sources

The scraper auto-detects what to do based on the URL host:

| Source | URL pattern | Scraper class |
|---|---|---|
| **gov.il DynamicCollectors** | `/he/departments/dynamiccollectors/<name>` | `govscraper.scrapers.govil.GovIlScraper` (DynamicCollector path) |
| **gov.il Collectors** (traditional) | `/he/collectors/<name>` | `govscraper.scrapers.govil.GovIlScraper` (Traditional path) |
| **gov.il Pages** (React SPA) | `/he/pages/<name>` | `govscraper.scrapers.govil.GovIlScraper` (ContentPage path) |
| **nadlan.gov.il parcel deals** | `?view=kparcel_all&id=<gush>-<chelka>` | `govscraper.scrapers.nadlan.NadlanScraper` (Playwright) |
| **govmap.gov.il GIS layers** | `?lay=<layerId>` | `govscraper.scrapers.govmap.GovMapScraper` (OGC WFS) |
| **data.gov.il datasets** (CKAN) | `/dataset[/<name>[/resource/<uuid>]]` | `govscraper.scrapers.datagovil.DataGovIlScraper` |

All flow through the same `/api/scrape` endpoint and produce one of three
result shapes (`TabularResult`, `CkanCatalogResult`, `GeoFeatureResult`)
under a single `ScrapeResult` union; see [GovMap GIS layers](#govmap-gis-layers)
below for what's special about geospatial output (additional `.geojson` file).

### Package layout (post-refactor)

The codebase was reorganised into a single `govscraper/` package. Top-level
modules (`scraper_engine.py`, `over_worker.py`, `file_handler.py`, …) are
now thin re-export shims that forward to the canonical implementations
under the package — every existing `from <module> import …` keeps working.
New code should import from `govscraper.*` directly.

| Canonical path | Purpose |
|---|---|
| [`govscraper/cli.py`](govscraper/cli.py) | Unified entry: `python -m govscraper.cli {serve,worker,scrape,bulk-nadlan}` |
| [`govscraper/scrapers/base.py`](govscraper/scrapers/base.py) | `BaseScraper`, `ScrapeResult` union, `ParsedURL`, `Checkpoint` |
| [`govscraper/scrapers/registry.py`](govscraper/scrapers/registry.py) | URL → scraper dispatch (auto-registers all 4 scrapers on import) |
| [`govscraper/scrapers/govil/`](govscraper/scrapers/govil/) | gov.il (Dynamic + Traditional + ContentPage). Engine at `legacy_engine.py`; facade at `scraper.py`. |
| [`govscraper/scrapers/datagovil/`](govscraper/scrapers/datagovil/) | data.gov.il CKAN: `package_search`, `package_show`, `datastore_search`. |
| [`govscraper/scrapers/nadlan/`](govscraper/scrapers/nadlan/) | nadlan.gov.il parcel + settlement engines. Playwright-driven. |
| [`govscraper/scrapers/govmap/`](govscraper/scrapers/govmap/) | govmap.gov.il OGC WFS + `layers.json`. |
| [`govscraper/worker/`](govscraper/worker/) | Unified `Worker` + 3 `TaskSource`s (`local_server`, `over_org`, `nadlan_queue`) + 3 `ResultPublisher`s. |
| [`govscraper/worker/publishers/_contract.py`](govscraper/worker/publishers/_contract.py) | **Frozen** over.org.il wire-format constants — pinned by `tests/contract/test_over_org.py`. |
| [`govscraper/io/`](govscraper/io/) | CSV / Excel / GeoJSON / ZIP writers, attachment downloader, `sanitize_filename`. |
| [`govscraper/geo/`](govscraper/geo/) | ITM ↔ WGS84 ↔ Web Mercator transforms; OGC WFS 2.0 client. |
| [`govscraper/legacy/`](govscraper/legacy/) | Legacy `worker.py`, `over_worker.py`, `nadlan_worker.py` clients (kept until the unified CLI worker has run for one release cycle). |

| Top-level (still here, unchanged) | Purpose |
|------|---------|
| [`app.py`](app.py) | Flask web app: UI, admin API, worker queue, archives. Hosted on Render. |
| [`auth.py`](auth.py) | Google OAuth2 SSO + admin/worker auth decorators. |
| [`storage.py`](storage.py) | SQLite-backed collection store. |
| [`govmap_api_routes.py`](govmap_api_routes.py) | Flask blueprint: `/api/govmap/layers`, `/api/govmap/preview-bbox`. |
| [`nadlan_api_routes.py`](nadlan_api_routes.py) | Flask blueprint: `/parcel-info`, `/settlements`, `/notify-trigger`, distributed bulk queue. |
| [`bulk_nadlan.py`](bulk_nadlan.py), [`incremental_nadlan_daily.py`](incremental_nadlan_daily.py) | Standalone CLIs for nadlan batch + daily delta (will fold into `cli.py bulk-nadlan`). |
| [`catalog/parcels_shapefile.py`](catalog/parcels_shapefile.py) | Builds `parcels.csv` from the Israel Mapping Center helkot shapefile. |
| [`scraper_engine.py`, `over_worker.py`, `worker.py`, `nadlan_worker.py`, `file_handler.py`, `archive_engine.py`, `govmap_engine.py`, `nadlan_api.py`, `nadlan_incremental_engine.py`, `coords.py`, `wfs_client.py`] | **Re-export shims only** — point at the canonical govscraper/ paths. |

---

## Quick start (local dev)

```bash
git clone https://github.com/zomer-g/govil-scraper.git
cd govil-scraper

pip install -r requirements.txt
python -m playwright install chromium     # required for nadlan + JS pages

cp .env.example .env  # if present; otherwise create per the table below
python app.py         # http://127.0.0.1:5000
```

Without an `.env` file the Flask app still boots, but admin features
(login, scraping) won't work. See
[Environment variables](#environment-variables) for the keys to set.

---

## Running the workers locally

There are **three independent local entry points**, each backed by a
small `.bat` wrapper that loads `.env` and dispatches to the right
Python script. All three are designed to run on a typical
desktop/laptop with internet access.

### 1. `govil-scraper` worker — runs Flask tasks

Polls the Render Flask app's task queue (`/api/worker/poll`) for
scrape jobs that admins enqueued via `POST /api/tasks`. Useful when the
Render web dyno can't run a long Playwright scrape itself.

**Setup (one-off):**
```bash
# .env in this directory must contain:
WORKER_API_KEY=<copy from Render env>
RENDER_SERVER_URL=https://govil-scraper.onrender.com   # optional; default
```

**Run:**
```bat
run_worker.bat        :: Windows
```
or directly:
```bash
python worker.py --server https://govil-scraper.onrender.com \
                 --key "$WORKER_API_KEY" --worker-id mymachine
```

Verify the server sees it: open
`https://govil-scraper.onrender.com/api/worker/status` in a browser.

### 2. OVER worker — tracked datasets on `over.org.il`

Polls `https://www.over.org.il/api/worker/poll` for tasks attached to
the operator's account. Each task has a `scraper_config` dict telling
the worker how to behave:

| `scraper_config` | Engine used | Behavior |
|------------------|-------------|----------|
| `{}` (no flags) | `scraper_engine` | One-shot scrape, push as new version. |
| `{"archive": true}` | `archive_engine` | Incremental: bootstrap once, then daily delta against checkpoint. |
| `{"archive": true, "archive_type": "nadlan_settlements"}` | `nadlan_incremental_engine` | Bootstrap or daily settlement-level nadlan scan. |

Checkpoint state is round-tripped through OVER as opaque JSON in
`scraper_config.checkpoint`, so re-runs are stateless from the
worker's perspective.

**Setup:**
```bash
# .env (UTF-8 without BOM!) must contain:
OVER_API_KEY=<personal token from over.org.il>
```

**Run:**
```bat
run_over_worker.bat   :: Windows
```
or directly:
```bash
python over_worker.py --poll-interval 30
```

While running, `worker_status.txt` in the project root holds a
one-line status that you can `cat` from another shell.

### 3. nadlan tools — real-estate deals

A unified menu launcher with three modes:

```bat
run_nadlan.bat
```

```
[1] Single parcel       Enter gush + chelka, get one CSV
[2] Bulk                Read parcels.csv, append deals to deals.csv
[3] Daily incremental   Settlement-level scan, append new deals only
```

Direct CLIs (everything `run_nadlan.bat` calls):

```bash
# 1. Single parcel
python run_single_parcel.py 31314 2 jerusalem-31314-2.csv

# 2. Bulk (parcels.csv → deals.csv with checkpoint)
python bulk_nadlan.py parcels.csv downloads/deals.csv --filter-status מוסדר
python bulk_nadlan.py parcels.csv downloads/deals.csv --limit 100   # smoke

# 3. Daily incremental against an existing master archive
python incremental_nadlan_daily.py --archive-dir D:\nadlan_archive
python incremental_nadlan_daily.py --archive-dir test_archive --settlements 3000,4000
```

**Building the parcels catalog** (one-off, before any bulk run):

```bash
# Download from data.gov.il via browser (requires manual click — files are large):
#   gushim.zip — https://data.gov.il/he/datasets/israel_mapping_center/subgushallshape  (~64 MB, optional)
#   helkot.zip — https://data.gov.il/he/datasets/israel_mapping_center/shape           (~668 MB)

python catalog/parcels_shapefile.py helkot.zip parcels.csv
python catalog/parcels_shapefile.py --inspect helkot.zip   # preview field names
```

Output `parcels.csv` columns:
`gush, chelka, locality, municipality, parcel_type, status,
legal_area_sqm, area_sqm, centroid_lat, centroid_lon`.

**Visible Chromium required.** nadlan.gov.il blocks headless Chromium
via reCAPTCHA Enterprise, so a real browser window pops up during the
scrape. Set `NADLAN_HEADLESS=1` only with a stealth-patched browser or
`xvfb` on Linux.

### 4. Distributed bulk-nadlan worker pool

For nation-wide scraping (~748k urban parcels), `bulk_nadlan.py` on a
single machine takes weeks. The distributed pool lets any number of
helper machines pull tasks from the central server in parallel.

**Architecture**

```
   ┌────────────────────────────────────────────────────┐
   │ Render server (govil-scraper.onrender.com)         │
   │   • SQLite task queue (nadlan_tasks table)         │
   │   • central deals CSV (nadlan_deals_master.csv)    │
   │   • atomic claim / stale-task recovery             │
   └────────┬────────────────────┬──────────────────────┘
            │                    │
        bulk-claim            bulk-result
            │                    │
   ┌────────▼─────┐    ┌─────────▼─────┐    ┌───────────────┐
   │  worker #1   │    │   worker #2   │ …  │   worker #N   │
   │ nadlan_worker│    │ nadlan_worker │    │ nadlan_worker │
   │ Playwright   │    │ Playwright    │    │ Playwright    │
   └──────────────┘    └───────────────┘    └───────────────┘
```

**One-time setup (admin, on the server side)**

```bash
# Upload parcels.csv and have the server enqueue every status=מוסדר row
# as a pending task. Idempotent — safe to re-upload.
curl -F file=@parcels.csv -F filter_status=מוסדר \
     https://govil-scraper.onrender.com/api/nadlan/bulk-queue
# → {"inserted": 748500, "skipped": 0, "filter_status": "מוסדר", ...}
```

**On every helper machine** — double-click `run_nadlan_worker.bat`
(Windows) or run the Python script directly. The launcher checks
Python is installed, auto-installs `requests` + Playwright +
Chromium on first run, then connects to the queue:

```bash
# Minimal — defaults to https://govil-scraper.onrender.com
python nadlan_worker.py

# Or specify
python nadlan_worker.py --server https://my-server --worker-id home-pc
```

**Monitoring** — public counts:

```bash
curl https://govil-scraper.onrender.com/api/nadlan/bulk-status
# → {"pending": 600000, "claimed": 12, "done": 148488,
#    "failed": 0, "total": 748500, "deals_collected": 1187420}
```

**Download the merged CSV** (admin only):

```bash
# Returns the central nadlan_deals_master.csv
curl -O -b cookie.jar https://govil-scraper.onrender.com/api/nadlan/bulk-deals.csv
```

**Failure semantics** (matches the standalone `bulk_nadlan.py`):

- A worker hitting `ERR_INTERNET_DISCONNECTED` / `ERR_TIMED_OUT` /
  `Timeout exceeded` reports the task as **transient** — server returns
  it to `pending` so another worker (or the same one after backoff)
  retries it.
- Real errors (parsing crashes, `ValueError`, etc.) are reported as
  **permanent** — the task moves to `failed` and won't be retried.
- A worker that crashes mid-task leaves its claim in `claimed` state.
  After 10 minutes the server's stale-reset releases it back to
  `pending` — no operator intervention needed.

---

## GovMap GIS layers

govmap.gov.il is the public GIS portal of the Survey of Israel. It
exposes a public OGC WFS 2.0 endpoint at `/api/geoserver/wfs` that we
hit directly — no auth, no API key, no third-party SDK.

### URL pattern

Paste any `https://www.govmap.gov.il/?lay=<id>` URL into the URL textbox
on the scrape tab — the dispatcher routes it to `govmap_engine`. The UI
also offers a dedicated **מפה (GovMap)** tab with a Leaflet map for
drawing a BBOX visually.

Examples:

```
# Whole country (199 features)
https://www.govmap.gov.il/?lay=220826

# Same layer, narrowed to a Tel Aviv area BBOX (auto-converted to ITM):
https://www.govmap.gov.il/?lay=220826&bbox_wgs84=34.7,32.0,34.9,32.2

# Or with explicit ITM bbox (xmin,ymin,xmax,ymax):
https://www.govmap.gov.il/?lay=220826&bbox=170000,640000,200000,670000
```

### Output

Each GovMap scrape produces three files in the collection (in addition
to the existing CSV/Excel):

- **`<name>.geojson`** — `FeatureCollection` in WGS84 (the canonical
  spatial artefact; opens in QGIS/Leaflet/Mapbox).
- **`<name>.csv`** — flat attribute table; geometry encoded as ITM WKT
  in the `_geometry_wkt` column for fidelity.
- **`<name>.xlsx`** — same as CSV with RTL formatting.
- **`manifest.json`** — sidecar with `layer_id`, `bbox_itm`, `bbox_wgs84`,
  `geometry_type`, `feature_count`, `srs`. Read by the upload endpoint
  to populate the new geo columns of the `collections` table.

Download via:
```
GET /api/collections/<id>/geojson    # → application/geo+json
GET /api/collections/<id>/csv
GET /api/collections/<id>/excel
GET /api/collections/<id>/download   # → ZIP with all of the above
```

### Layer catalog

Curated layers live in [`layers.json`](layers.json) at the repo root.
Override at deploy time with `GOVMAP_LAYERS_FILE=/path/to/custom.json`.
Adding a new layer:

```python
from scraper_engine import GovILSession
from wfs_client import WFSClient
s = GovILSession(); s.warm()
c = WFSClient(s)
print(c.hits("govmap:layer_<numeric_id>"))           # confirm > 0
print(c.describe_feature_type("govmap:layer_<id>"))  # confirm fields
```

Then add an entry to `layers.json`:
```json
{
  "id": "MY_LAYER", "label_he": "...", "label_en": "...",
  "type_name": "govmap:layer_<id>", "endpoint_kind": "wfs",
  "geometry_type": "Polygon", "out_fields": ["*"], "notes": "..."
}
```

### Quirks

- WFS 2.0 `startIndex` parameter is **rejected** by GovMap's GeoServer
  (HTTP 400). `wfs_client.iter_features` paginates via
  `CQL_FILTER=objectid > <last_id> ORDER BY objectid` instead.
- Default WFS GeoJSON output is in EPSG:3857; we explicitly request
  `srsName=EPSG:4326` to get WGS84.
- Layer `220826` (`גזרי שטחי אש` — IDF firing-zone orders) returns
  199 `MultiPolygon` features country-wide as of 2026-05.
- **Privacy:** several GovMap layers expose personal contact info of
  public officials in their `contacts` field. Use `Layer.out_fields`
  in `layers.json` to whitelist only what you want before publishing
  to OVER.

See [`docs/govmap_api.md`](docs/govmap_api.md) for the full
reverse-engineering runbook.

---

## HTTP API reference

Base URL (production): `https://govil-scraper.onrender.com`

Auth is enforced via three decorators ([`auth.py`](auth.py)):

- **public** — open, optionally rate-limited
- **`@admin_required`** — Google OAuth2 session, email in `ADMIN_EMAILS`
- **`@worker_auth_required`** — `X-Worker-Key: $WORKER_API_KEY` header
- **`@admin_or_worker`** — either of the above

### Public endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | UI dashboard. |
| `GET` | `/auth/login` | Start Google OAuth2 flow. |
| `GET` | `/auth/callback` | OAuth2 callback — sets session. |
| `GET` | `/auth/logout` | Clear session. |
| `GET` | `/auth/me` | Current user JSON, or `null`. |
| `GET` | `/api/collections` | List saved collections (60/min). |
| `GET` | `/api/collections/<cid>` | Collection metadata + records. |
| `GET` | `/api/collections/<cid>/csv` | Download CSV. |
| `GET` | `/api/collections/<cid>/excel` | Download Excel. |
| `GET` | `/api/collections/<cid>/geojson` | Download GeoJSON (WGS84) for GovMap layer collections. |
| `GET` | `/api/collections/<cid>/files` | Manifest of attachments. |
| `GET` | `/api/collections/<cid>/files/<filename>` | Single attachment. |
| `GET` | `/api/collections/<cid>/download` | Full ZIP (CSV + attachments). |
| `GET` | `/api/files` | Search across all collections (60/min). |

### GovMap GIS layer endpoints

Built-in helpers for scraping geospatial data from `govmap.gov.il`.
The catalog and preview are public and rate-limited; the layer-name
scrape trigger is admin-only.

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/govmap/layers` | public | List the curated layer catalog (`layers.json`). 60/min. |
| `POST` | `/api/govmap/preview-bbox` | public | Fast count probe before a real scrape. 30/min. |
| `POST` | `/api/govmap/scrape` | admin | **Trigger a scrape by layer name**. 5/min. |
| `GET` | `/api/collections/<cid>/geojson` | public | Download the GeoJSON file (WGS84) of a collection. |

#### `POST /api/govmap/scrape` — scrape by layer name

The simplest way to trigger a GovMap scrape: send the layer's `id`
(or numeric WFS id like `"220826"`) and the system handles everything.
No URL construction, no BBOX math.

**Request:**
```http
POST /api/govmap/scrape
Cookie: session=...        # admin Google SSO
Content-Type: application/json

{
  "layer":      "FIRE_AREAS_ORDERS",
  "bbox_wgs84": [34.7, 32.0, 34.9, 32.2],   // optional
  "mode":       "auto"                      // "auto" | "server" | "worker"
}
```

**Response:**
```http
202 Accepted
{
  "job_id": "a1b2c3d4e5f6",
  "url":    "https://www.govmap.gov.il/?lay=220826&bbox_wgs84=34.7,32.0,34.9,32.2",
  "layer":  "FIRE_AREAS_ORDERS",
  "mode":   "server"
}
```

Then stream progress via `GET /api/progress/{job_id}` (SSE).

**curl example:**
```bash
# 1. Get the layer catalog
curl https://govil-scraper.onrender.com/api/govmap/layers \
  | jq '.layers[] | {id, label_he, geometry_type}'

# 2. Probe the count (no auth needed)
curl -X POST https://govil-scraper.onrender.com/api/govmap/preview-bbox \
  -H 'Content-Type: application/json' \
  -d '{"layer":"FIRE_AREAS_ORDERS"}'
# → {"layer":"FIRE_AREAS_ORDERS", "count": 199}

# 3. Trigger the scrape (admin session cookie required)
curl -X POST https://govil-scraper.onrender.com/api/govmap/scrape \
  -b "session=$ADMIN_COOKIE" \
  -H 'Content-Type: application/json' \
  -d '{"layer":"FIRE_AREAS_ORDERS"}'
# → {"job_id":"a1b2c3d4e5f6", "url":"...", "mode":"server"}

# 4. Stream progress
curl -N https://govil-scraper.onrender.com/api/progress/a1b2c3d4e5f6

# 5. When complete, download the GeoJSON
curl https://govil-scraper.onrender.com/api/collections/a1b2c3d4e5f6/geojson \
  > fire_areas.geojson
```

**Python example:**
```python
import requests

s = requests.Session()
s.cookies.update({"session": ADMIN_SESSION_COOKIE})

# Whole-country scrape of fire training zones
r = s.post("https://govil-scraper.onrender.com/api/govmap/scrape",
           json={"layer": "FIRE_AREAS_ORDERS"})
job_id = r.json()["job_id"]

# Wait for completion (poll the status endpoint)
import time
while True:
    status = s.get(f"https://govil-scraper.onrender.com/api/status/{job_id}").json()
    print(status.get("phase"), status.get("message"))
    if status.get("phase") in ("complete", "error"):
        break
    time.sleep(2)

# Download the GeoJSON
gj = s.get(f"https://govil-scraper.onrender.com/api/collections/{job_id}/geojson")
with open("fire_areas.geojson", "wb") as f:
    f.write(gj.content)
```

#### Body schema for `/api/govmap/scrape`

| Field | Type | Required | Notes |
|---|---|---|---|
| `layer` | string | yes | Either an `id` from `/api/govmap/layers` (e.g. `"FIRE_AREAS_ORDERS"`), or a numeric WFS id (e.g. `"220826"`). Unknown numeric ids are auto-synthesized — useful for layers you found via DevTools but haven't added to `layers.json` yet. |
| `bbox` | `[xmin, ymin, xmax, ymax]` | no | ITM coordinates (EPSG:2039). Leave out for whole-country scrape (capped by `GOVMAP_MAX_FEATURES`). |
| `bbox_wgs84` | `[lon_min, lat_min, lon_max, lat_max]` | no | WGS84 — auto-converted server-side. Mutually exclusive with `bbox`. |
| `mode` | `"auto" \| "server" \| "worker"` | no | Default `auto` — picks `worker` if any worker has heart-beated within 60s, else `server`. |

#### Response codes for `/api/govmap/scrape`

| Code | Meaning |
|---|---|
| `202` | Job accepted; poll `/api/progress/{job_id}` for status. |
| `400` | Missing/invalid `layer`, bad numeric values in `bbox`, or unknown `mode`. |
| `403` | Not signed in as an admin. |
| `429` | Server-mode concurrent-job cap reached, or rate limit (5/min). |

### Public nadlan helpers

Lightweight Render-friendly endpoints (no Playwright on the server):

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/nadlan/parcel-info/<gush>/<chelka>` | Proxy to nadlan `/deal-info`. Returns parcel-level metadata `{base_level, neigh_id, neigh_name, parcel_id, setl_id, setl_name}`. No reCAPTCHA needed. |
| `GET` | `/api/nadlan/settlements` | Cached catalog of all 1,509 Israeli settlements (`setl_types.json`). 24-hour TTL, falls back to stale on upstream error. |
| `POST` | `/api/nadlan/notify-trigger` | Idempotent webhook (rate-limited 60/h/IP). Records a trigger event in an in-memory ring buffer. Used by the daily GitHub Actions cron. |
| `GET` | `/api/nadlan/bulk-status` | Aggregate counts of the distributed queue: `{pending, claimed, done, failed, total, deals_collected}`. |

### Nadlan distributed bulk endpoints

Used by `nadlan_worker.py` to coordinate parallel scraping. Worker
endpoints are unauthenticated (each request carries `worker_id`); admin
endpoints require Google OAuth2 session.

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/api/nadlan/bulk-queue` | admin | Multipart upload of `parcels.csv`. Inserts each row as a pending task. Optional `filter_status` form field (e.g. `מוסדר`). Idempotent. |
| `POST` | `/api/nadlan/bulk-claim` | worker | Body `{worker_id, count}` → `{tasks: [...]}`. Atomic; never returns a task already claimed by another worker. |
| `POST` | `/api/nadlan/bulk-result/<parcel_id>` | worker | Multipart deals CSV + `worker_id` form field. Server appends rows to `nadlan_deals_master.csv` and marks the task done. |
| `POST` | `/api/nadlan/bulk-fail/<parcel_id>` | worker | Form `{worker_id, error, transient}`. Transient → returns to `pending`; permanent → marked `failed`. |
| `GET` | `/api/nadlan/bulk-deals.csv` | admin | Download the central deals CSV. |
| `POST` | `/api/nadlan/bulk-reset-stale` | admin | Force-recover claimed-but-stuck tasks (default >10min). |
| `POST` | `/api/nadlan/bulk-clear` | admin | Drop all queued tasks. Optional `clear_deals=true` also wipes the CSV. |

### Admin endpoints (Google OAuth2 session)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/scrape` | Start a scrape job. Body: `{"url": "...", "download_files": true}`. 5/min. |
| `GET` | `/api/progress/<job_id>` | Server-Sent Events stream of progress. |
| `GET` | `/api/status/<job_id>` | One-shot job status. |
| `GET` | `/api/preview/<job_id>` | Preview rows before saving. |
| `GET` | `/api/download/<job_id>` | Full result (CSV + ZIP). |
| `DELETE` | `/api/collections/<cid>` | Delete a saved collection. |
| `POST` | `/api/tasks` | Enqueue a scrape task for a worker. 5/min. |
| `GET` | `/api/tasks` | List all tasks. |
| `GET` | `/api/tasks/<task_id>` | Task detail. |
| `DELETE` | `/api/tasks/<task_id>` | Cancel a queued task. |
| `GET` | `/api/archives` | List incremental archives. |
| `POST` | `/api/archives` | Create archive (bootstrap). |
| `GET` | `/api/archives/<id>` | Archive detail. |
| `DELETE` | `/api/archives/<id>` | Delete archive. |
| `POST` | `/api/archives/<id>/run` | Trigger incremental update. |
| `GET` | `/api/nadlan/trigger-log` | Last 50 trigger events (for cron debugging). |

### Worker endpoints (`X-Worker-Key` header)

Used only by `worker.py`. The OVER worker does **not** call any of
these — it talks to `over.org.il` directly.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/worker/poll` | Claim the next pending task, or 204. |
| `POST` | `/api/worker/progress/<task_id>` | Stream progress. |
| `POST` | `/api/worker/heartbeat` | Keepalive; updates worker-status panel. |
| `POST` | `/api/worker/complete/<task_id>` | Upload result ZIP, finalize task. |
| `POST` | `/api/worker/fail/<task_id>` | Report a failed task. |
| `GET` | `/api/worker/status` | Public — last-seen worker timestamp + label. |
| `POST` | `/api/collections/upload` | `admin_or_worker` — upload a CSV/ZIP from outside the worker queue. |

OVER server endpoints called by `over_worker.py` follow the same
shape (`/api/worker/poll`, `/upload-csv`, `/push-version`, `/progress`,
`/fail`) but live at `https://www.over.org.il`. They're **not**
implemented in this repo.

---

## Environment variables

Set these in `.env` for local dev, in the Render dashboard for
production, and in `.env` next to `over_worker.py` on each worker
host.

| Variable | Where | Required | Purpose |
|----------|-------|----------|---------|
| `GOOGLE_CLIENT_ID` | Flask app | yes (for admin login) | OAuth2 client. |
| `GOOGLE_CLIENT_SECRET` | Flask app | yes | OAuth2 secret. |
| `ADMIN_EMAILS` | Flask app | yes | Comma-separated allowlist. |
| `FLASK_SECRET_KEY` | Flask app | yes | Session signing. |
| `WORKER_API_KEY` | Flask app + `worker.py` host | yes (for worker) | Validates `X-Worker-Key`. |
| `OVER_API_KEY` | `over_worker.py` host | yes (for OVER worker) | Auth to `over.org.il`. |
| `RENDER_SERVER_URL` | `worker.py` host | optional | Defaults to `https://govil-scraper.onrender.com`. |
| `TEMP_DIR` | Flask app | optional | Where to stage CSV/ZIP. Defaults to system temp. |
| `DISABLE_PLAYWRIGHT` | Flask app | optional | `1` to skip Playwright fallback in `scraper_engine`. |
| `NADLAN_HEADLESS` | nadlan tools | optional | `1` to force headless Chromium (only works with stealth/xvfb). |
| `OVER_WORKER_STATUS_FILE` | `over_worker.py` host | optional | Path of the live-status one-liner. |

`.env` is gitignored. **Never commit it.** The Render dashboard
generates `WORKER_API_KEY` and `FLASK_SECRET_KEY` automatically on
first deploy (see [`render.yaml`](render.yaml)).

---

## Deployment (Render)

[`render.yaml`](render.yaml) declares one web service:

- Build: `pip install -r requirements.txt && playwright install --with-deps chromium`
- Start: `gunicorn app:app --workers 2 --threads 4 --timeout 600`
- Plan: Starter
- `WORKER_API_KEY`, `FLASK_SECRET_KEY` auto-generated on first deploy

[`Dockerfile`](Dockerfile) is provided for non-Render hosts. Render's
filesystem is ephemeral, so collections and archives created on the
server are **temporary** — the long-term store of record is OVER (for
versioned datasets) and the local worker's `over_archives/` directory
(for in-flight master CSVs).

---

## Daily nadlan trigger (GitHub Actions)

[`.github/workflows/nadlan-daily.yml`](.github/workflows/nadlan-daily.yml)
runs once daily (`03:00 UTC`) and POSTs to the public
`/api/nadlan/notify-trigger` endpoint. **No GitHub secrets are
required** — the trigger URL is public and the endpoint is
intentionally unauthenticated (idempotent ring buffer + per-IP rate
limit).

To run it manually: GitHub → **Actions** → **Nadlan daily trigger** →
**Run workflow**.

The trigger is a hook that records the event. Today the actual scrape
still runs on a local worker that polls OVER. When/if the trigger
endpoint evolves to enqueue real OVER tasks server-side, harden it
with GitHub OIDC verification (no secrets needed even then — Render
verifies a JWT from GitHub's public JWKS).

---

## Tests

[`tests/nadlan/`](tests/nadlan):

- **`test_validation.py`** — Phase A–E suite. Covers in-memory logic,
  single-parcel scrape, bulk-run smoke, resume correctness, and edge
  cases. Designed to run before a nationwide scrape.
- **`test_distributed.py`** — Integration tests for the distributed
  queue. Proves: (a) `nadlan_create_tasks` is idempotent, (b) two
  workers claiming concurrently never get overlapping tasks, (c)
  transient/permanent fail-state transitions are correct, (d)
  `reset_stale` recovers crashed-worker claims, (e) full HTTP
  round-trip via Flask test client.
- **`quality_check.py`** — post-run quality report on a completed
  batch. Coverage, hit rate, schema, anomalies, geographic and time
  distribution.

```bash
python tests/nadlan/test_validation.py
python tests/nadlan/test_distributed.py
python tests/nadlan/quality_check.py downloads/batch1.csv parcels_batch1.csv
```

[`benchmark.py`](benchmark.py) and [`test_urls.py`](test_urls.py) are
older smoke tools for the gov.il scraper itself.

---

## Project layout

```
.
├── govscraper/                     Canonical package — everything authoritative lives here
│   ├── cli.py                      Unified entry: serve | worker | scrape | bulk-nadlan
│   ├── types.py                    Task, Progress, ParsedURL, FileAttachment, Checkpoint
│   ├── config.py                   Env-var helpers (TEMP_DIR, OVER_API_KEY, …)
│   ├── scrapers/
│   │   ├── base.py                 BaseScraper + ScrapeResult union
│   │   ├── registry.py             URL → scraper dispatch
│   │   ├── govil/                  www.gov.il (legacy_engine.py + scraper.py + _fields.py)
│   │   ├── datagovil/              data.gov.il CKAN (ckan_client.py + scraper.py + _fields.py)
│   │   ├── nadlan/                 nadlan.gov.il parcel + settlement engines
│   │   └── govmap/                 govmap.gov.il OGC WFS + layers.json
│   ├── worker/                     Worker + 3 sources + 3 publishers
│   │   ├── runner.py               Main loop, throttled progress, heartbeat thread
│   │   ├── sources/                local_server / over_org / nadlan_queue
│   │   └── publishers/             local_collections / over_org / nadlan_queue + _contract.py
│   ├── io/                         CSV / Excel / GeoJSON / ZIP / attachments / sanitize
│   ├── geo/                        coords (ITM ↔ WGS84 ↔ WM) + WFSClient
│   ├── http/                       Shared HTTP plumbing
│   ├── storage/                    SQLite store entry point
│   ├── web/                        Flask web layer entry point
│   └── legacy/                     worker.py / over_worker.py / nadlan_worker.py clients
├── app.py                          Flask app — UI, admin API, worker queue (still here)
├── auth.py                         Google OAuth2 + auth decorators
├── storage.py                      SQLite store
├── govmap_api_routes.py            Flask blueprint: /api/govmap/*
├── nadlan_api_routes.py            Flask blueprint: /parcel-info, /settlements, /bulk-*
├── bulk_nadlan.py                  Bulk parcel scrape CLI (single-machine, with checkpoint)
├── incremental_nadlan_daily.py     Daily nadlan incremental CLI
├── run_single_parcel.py            One-shot single-parcel CLI
├── catalog/parcels_shapefile.py    Build parcels.csv from IMC shapefile
├── benchmark.py                    Throughput benchmarks
│
├── scraper_engine.py · over_worker.py · worker.py · nadlan_worker.py
├── file_handler.py · archive_engine.py · govmap_engine.py · nadlan_api.py
├── nadlan_incremental_engine.py · coords.py · wfs_client.py
│      ↑↑↑ all of these are re-export shims pointing into govscraper/  ↑↑↑
│
├── layers.json                     GovMap layer catalog (loaded by govscraper/scrapers/govmap)
├── tests/
│   ├── test_coords.py              Geo transforms
│   ├── contract/test_over_org.py   Pins over.org.il wire-format byte-for-byte
│   ├── scrapers/                   URL-parser tests per scraper (govil, nadlan, govmap, datagovil)
│   └── nadlan/                     Live nadlan validation + distributed-flow checks
├── templates/index.html            Single-page admin UI
├── static/                         CSS / JS / icons
├── docs/glossary.md                Identifier conventions + _fields.py reference
├── .github/workflows/
│   └── nadlan-daily.yml            Daily trigger ping
├── run_worker.bat                  Windows launcher — Flask worker
├── run_over_worker.bat             Windows launcher — OVER worker
├── run_nadlan.bat                  Windows launcher — nadlan tools menu (single/bulk/daily)
├── run_nadlan_worker.bat           Windows launcher — distributed worker
├── render.yaml                     Render service config
├── Dockerfile                      Container build
├── requirements.txt                Python deps
└── README.md                       This file
```

---

## License & ownership

Internal scraping infrastructure for Israeli government open-data
work. The scraped data itself is published under the open-data terms
of the originating ministries (mostly CC-BY 3.0 IL).
