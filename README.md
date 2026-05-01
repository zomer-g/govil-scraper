# govil-scraper

A multi-component scraping platform for Israeli government open data.

The repo holds **three cooperating subsystems**:

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

The Flask app is deployed on Render at
[`https://govil-scraper.onrender.com`](https://govil-scraper.onrender.com).
Workers run on the operator's local machine (Windows / macOS / Linux
desktop).

---

## Table of contents

- [Architecture](#architecture)
- [Quick start (local dev)](#quick-start-local-dev)
- [Running the workers locally](#running-the-workers-locally)
  - [govil-scraper worker](#1-govil-scraper-worker-runs-flask-tasks)
  - [OVER worker](#2-over-worker-tracked-datasets-on-overorgil)
  - [nadlan tools](#3-nadlan-tools-real-estate-deals)
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

**Key abstractions** (file ↦ purpose):

| File | Purpose |
|------|---------|
| [`app.py`](app.py) | Flask web app: UI, admin API, worker queue, archives. Hosted on Render. |
| [`scraper_engine.py`](scraper_engine.py) | Core gov.il scraper. URL parsing, page-type dispatch, pagination. |
| [`archive_engine.py`](archive_engine.py) | Incremental archive (bootstrap + delta) for traditional collectors. |
| [`nadlan_api.py`](nadlan_api.py) | Playwright-driven nadlan single-parcel scraper. |
| [`nadlan_incremental_engine.py`](nadlan_incremental_engine.py) | Settlement-level nadlan bootstrap + daily incremental. |
| [`nadlan_api_routes.py`](nadlan_api_routes.py) | Public HTTP helpers: `/parcel-info`, `/settlements`, `/notify-trigger`. |
| [`worker.py`](worker.py) | Worker that polls the Render Flask app for delegated scrapes. |
| [`over_worker.py`](over_worker.py) | Worker that polls `over.org.il` for tracked-dataset tasks. |
| [`bulk_nadlan.py`](bulk_nadlan.py) | Standalone CLI: scrape every parcel in a CSV (with checkpoint + circuit breaker). |
| [`incremental_nadlan_daily.py`](incremental_nadlan_daily.py) | Standalone CLI for the daily nadlan delta. |
| [`catalog/parcels_shapefile.py`](catalog/parcels_shapefile.py) | Builds `parcels.csv` from the Israel Mapping Center helkot shapefile. |
| [`auth.py`](auth.py) | Google OAuth2 SSO + admin/worker auth decorators. |
| [`storage.py`](storage.py) | SQLite-backed collection store. |
| [`file_handler.py`](file_handler.py) | CSV/Excel export, attachment downloads. |

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
| `GET` | `/api/collections/<cid>/files` | Manifest of attachments. |
| `GET` | `/api/collections/<cid>/files/<filename>` | Single attachment. |
| `GET` | `/api/collections/<cid>/download` | Full ZIP (CSV + attachments). |
| `GET` | `/api/files` | Search across all collections (60/min). |

### Public nadlan helpers

Lightweight Render-friendly endpoints (no Playwright on the server):

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/nadlan/parcel-info/<gush>/<chelka>` | Proxy to nadlan `/deal-info`. Returns parcel-level metadata `{base_level, neigh_id, neigh_name, parcel_id, setl_id, setl_name}`. No reCAPTCHA needed. |
| `GET` | `/api/nadlan/settlements` | Cached catalog of all 1,509 Israeli settlements (`setl_types.json`). 24-hour TTL, falls back to stale on upstream error. |
| `POST` | `/api/nadlan/notify-trigger` | Idempotent webhook (rate-limited 60/h/IP). Records a trigger event in an in-memory ring buffer. Used by the daily GitHub Actions cron. |

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
- **`quality_check.py`** — post-run quality report on a completed
  batch. Coverage, hit rate, schema, anomalies, geographic and time
  distribution.

```bash
python tests/nadlan/test_validation.py
python tests/nadlan/quality_check.py downloads/batch1.csv parcels_batch1.csv
```

[`benchmark.py`](benchmark.py) and [`test_urls.py`](test_urls.py) are
older smoke tools for the gov.il scraper itself.

---

## Project layout

```
.
├── app.py                          Flask app — UI, admin API, worker queue
├── auth.py                         Google OAuth2 + auth decorators
├── scraper_engine.py               Core gov.il scraper engine
├── archive_engine.py               Incremental archive (gov.il)
├── nadlan_api.py                   Single-parcel nadlan scraper (Playwright)
├── nadlan_incremental_engine.py    Settlement-level nadlan bootstrap + delta
├── nadlan_api_routes.py            Public HTTP helpers (parcel-info, settlements, trigger)
├── bulk_nadlan.py                  Bulk parcel scrape CLI (with checkpoint)
├── incremental_nadlan_daily.py     Daily nadlan incremental CLI
├── run_single_parcel.py            One-shot single-parcel CLI
├── catalog/
│   └── parcels_shapefile.py        Build parcels.csv from IMC shapefile
├── worker.py                       Local worker for the Flask task queue
├── over_worker.py                  Local worker for over.org.il
├── storage.py                      SQLite store
├── file_handler.py                 CSV/Excel/ZIP export helpers
├── incremental_update.py           CLI scheduler wrapper for archive_engine
├── local_scrape.py                 Standalone scrape CLI (no Flask)
├── benchmark.py                    Throughput benchmarks
├── tests/nadlan/                   Validation suite + post-run quality check
├── templates/index.html            Single-page admin UI
├── static/                         CSS / JS / icons
├── .github/workflows/
│   └── nadlan-daily.yml            Daily trigger ping
├── run_worker.bat                  Windows launcher — Flask worker
├── run_over_worker.bat             Windows launcher — OVER worker
├── run_nadlan.bat                  Windows launcher — nadlan tools menu
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
