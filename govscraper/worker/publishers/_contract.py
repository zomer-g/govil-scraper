"""Frozen contract with over.org.il — DO NOT RENAME ANY OF THESE.

Every value here mirrors a string the running over_worker.py sends to
over.org.il today. The contract test pins these — if you must change one,
update the test golden in the same commit.

Source of truth: over_worker.py:31, 107-111, 122, 137, 156, 180-187,
234-243, 283-331. See docs/glossary.md for naming conventions.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Server / endpoints
# ---------------------------------------------------------------------------

OVER_BASE_URL = "https://www.over.org.il"

PATH_POLL = "/api/worker/poll"
PATH_PROGRESS = "/api/worker/progress/{task_id}"
PATH_FAIL = "/api/worker/fail/{task_id}"
PATH_UPLOAD_ZIP = "/api/worker/upload-zip/{tracked_dataset_id}"
PATH_UPLOAD_CSV = "/api/worker/upload-csv/{tracked_dataset_id}"
PATH_UPLOAD_GEOJSON = "/api/worker/upload-geojson/{tracked_dataset_id}"
PATH_PUSH_VERSION = "/api/worker/push-version"

# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

HEADER_AUTH = "Authorization"
AUTH_BEARER_FMT = "Bearer {api_key}"
HEADER_CONTENT_TYPE_JSON = "application/json"

# ---------------------------------------------------------------------------
# Multipart fields — upload-zip
# ---------------------------------------------------------------------------

ZIP_FILE_FIELD = "file"
ZIP_MIME = "application/zip"
ZIP_FORM_VERSION_NUMBER = "version_number"
ZIP_FORM_ATTACHMENT_COUNT = "attachment_count"
ZIP_FORM_PART = "part"
ZIP_FORM_TOTAL_PARTS = "total_parts"

# ---------------------------------------------------------------------------
# Multipart fields — upload-csv
# ---------------------------------------------------------------------------

CSV_FILE_FIELD = "file"
CSV_MIME = "application/gzip"
CSV_FORM_VERSION_NUMBER = "version_number"
CSV_FORM_RESOURCE_NAME = "resource_name"
CSV_FORM_ROW_COUNT = "row_count"
CSV_FORM_COMPRESSION = "compression"
CSV_FORM_FIELDS_JSON = "fields_json"
CSV_COMPRESSION_VALUE = "gzip"
CSV_GZIP_LEVEL = 6

# ---------------------------------------------------------------------------
# Multipart fields — upload-geojson  (added 2026-05-02)
# ---------------------------------------------------------------------------

GEOJSON_FILE_FIELD = "file"
GEOJSON_MIME = "application/geo+json"
GEOJSON_FORM_VERSION_NUMBER = "version_number"
GEOJSON_FORM_RESOURCE_NAME = "resource_name"
TIMEOUT_UPLOAD_GEOJSON = 600

# ---------------------------------------------------------------------------
# push-version JSON body
# ---------------------------------------------------------------------------

# Top-level keys
PV_TRACKED_DATASET_ID = "tracked_dataset_id"
PV_METADATA_MODIFIED = "metadata_modified"
PV_RESOURCES = "resources"
PV_ATTACHMENTS = "attachments"
PV_SCRAPE_METADATA = "scrape_metadata"
PV_ZIP_RESOURCE_ID = "zip_resource_id"
PV_ZIP_RESOURCE_IDS = "zip_resource_ids"
PV_CSV_RESOURCE_IDS = "csv_resource_ids"
PV_GEOJSON_RESOURCE_IDS = "geojson_resource_ids"  # added 2026-05-02
PV_SCRAPER_CONFIG_PATCH = "scraper_config_patch"
PV_SKIP_VERSION = "skip_version"

# Resource entry keys
RES_NAME = "name"
RES_FORMAT = "format"
RES_RECORDS = "records"
RES_FIELDS = "fields"
RES_ROW_COUNT = "row_count"

# scrape_metadata keys
SM_SOURCE_URL = "source_url"
SM_SCRAPE_DURATION_SECONDS = "scrape_duration_seconds"
SM_TOTAL_ITEMS = "total_items"
SM_TOTAL_FILES = "total_files"
SM_SCRAPER_VERSION = "scraper_version"
SM_DATASET_TITLE_HE = "dataset_title_he"  # added 2026-05-02 — over.org.il updates tracked_dataset.title
SCRAPER_VERSION_VALUE = "1.0.0"

# Frozen Hebrew resource name. The push-version body always emits exactly
# one resource named this string. Do not transliterate or rename.
PRIMARY_RESOURCE_NAME = "נתוני הסורק"
PRIMARY_RESOURCE_FORMAT = "CSV"

# Duration is rounded to 1 decimal place (over_worker.py:316).
DURATION_DECIMALS = 1

# ---------------------------------------------------------------------------
# Progress payload keys (over_worker.py:139-145)
# ---------------------------------------------------------------------------

PROG_PHASE = "phase"
PROG_CURRENT = "current"
PROG_TOTAL = "total"
PROG_PERCENTAGE = "percentage"
PROG_MESSAGE = "message"

# ---------------------------------------------------------------------------
# Failure payload keys (over_worker.py:158)
# ---------------------------------------------------------------------------

FAIL_ERROR = "error"
FAIL_PHASE = "phase"
FAIL_DEFAULT_PHASE = "scraping"

# ---------------------------------------------------------------------------
# Timeouts (seconds)
# ---------------------------------------------------------------------------

TIMEOUT_POLL = 15
TIMEOUT_PROGRESS = 10
TIMEOUT_FAIL = 15
TIMEOUT_UPLOAD_ZIP = 600
TIMEOUT_UPLOAD_CSV = 1800
TIMEOUT_PUSH_VERSION = 300

# ---------------------------------------------------------------------------
# Retry policy (mirrors over_worker.py exactly)
# ---------------------------------------------------------------------------

FAIL_MAX_ATTEMPTS = 3
FAIL_RETRY_SLEEP_S = 5

CSV_UPLOAD_MAX_ATTEMPTS = 3
CSV_UPLOAD_HTTP_BACKOFF = lambda attempt: 15 * attempt  # noqa: E731
CSV_UPLOAD_EXC_BACKOFF = lambda attempt: 10 * attempt   # noqa: E731

# ---------------------------------------------------------------------------
# Worker loop intervals
# ---------------------------------------------------------------------------

HEARTBEAT_EVERY_S = 30
PROGRESS_THROTTLE_S = 5

# ---------------------------------------------------------------------------
# Multi-part ZIP threshold (over_worker.py:49)
# ---------------------------------------------------------------------------

MAX_ZIP_BYTES = 80 * 1024 * 1024


def auth_header(api_key: str) -> dict[str, str]:
    return {HEADER_AUTH: AUTH_BEARER_FMT.format(api_key=api_key)}
