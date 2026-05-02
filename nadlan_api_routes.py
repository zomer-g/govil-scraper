"""
Lightweight nadlan helper endpoints — designed to run on Render with no extra
cost. Nothing here launches Playwright or holds long-running connections.

Endpoints:
    GET  /api/nadlan/parcel-info/<gush>/<chelka>
        Public proxy to https://api.nadlan.gov.il/deal-info — returns parcel
        metadata (settlement, neighborhood). reCAPTCHA-free upstream.

    GET  /api/nadlan/settlements
        Public cached copy of https://data.nadlan.gov.il/api/index/setl_types.json
        (1,509 entries, ~147 KB). 24-hour in-process TTL. Useful for UI dropdowns.

    POST /api/nadlan/notify-trigger
        Webhook for an external scheduler (e.g. GitHub Actions cron). Records
        a trigger event in an in-memory ring buffer.

        Intentionally unauthenticated to keep the public repo's CI free of
        secrets. The handler is idempotent (writes to a 100-entry ring buffer
        only) and rate-limited per-IP. When this endpoint evolves to enqueue
        real tasks on OVER, harden it with GitHub OIDC verification then.

    GET  /api/nadlan/trigger-log
        Admin-only — last 50 trigger events (for debugging the cron pipeline).

    --- Distributed bulk-scrape queue (multi-machine) ---

    POST /api/nadlan/bulk-queue
        Admin only. Body is multipart with a parcels CSV (output of
        catalog/parcels_shapefile.py). Server inserts each row as a pending
        task. Idempotent (existing parcel_ids skipped). Returns counts.

    POST /api/nadlan/bulk-claim
        Worker requests up to N tasks atomically. Body JSON:
            {"worker_id": "host-1", "count": 5}
        Returns: {"tasks": [{parcel_id, gush, chelka, locality, ...}, ...]}

    POST /api/nadlan/bulk-result/<parcel_id>
        Worker uploads the deals it collected for one parcel. Multipart with
        a CSV (one row per deal). Server appends rows to deals_master.csv
        and marks the task done.

        Form fields:
            worker_id   = caller identity
            deals_count = N (must match CSV rows)

    POST /api/nadlan/bulk-fail/<parcel_id>
        Worker reports a failure. Form fields:
            worker_id, error, transient (true|false)
        transient=true → task returns to pending (network glitch).
        transient=false → task marked permanently failed.

    GET  /api/nadlan/bulk-status
        Public. Aggregate counts:
            {pending, claimed, done, failed, total, deals_collected}

    POST /api/nadlan/bulk-reset-stale
        Admin. Returns claimed-but-stuck tasks (>10min) to pending.

    POST /api/nadlan/bulk-clear
        Admin. Drops all queued tasks. (Testing / restart.)
"""

import collections
import csv as _csv
import io
import logging
import os
import threading
import time

import requests
from flask import Blueprint, jsonify, request, send_file

from auth import is_admin, _check_worker_key


def _admin_or_worker() -> bool:
    """Admin auth check that also accepts the worker key — convenient for
    CLI flows where an operator can't easily get an OAuth session cookie."""
    return is_admin() or _check_worker_key()

logger = logging.getLogger(__name__)

nadlan_api_bp = Blueprint("nadlan_api", __name__, url_prefix="/api/nadlan")

# Upstream endpoints we proxy.
DEAL_INFO_URL = "https://api.nadlan.gov.il/deal-info"
SETTLEMENTS_URL = "https://data.nadlan.gov.il/api/index/setl_types.json"

# Settlements catalog cache — small, infrequently changing, fine in-process.
_SETTLEMENTS_CACHE: dict = {"data": None, "fetched_at": 0.0}
_SETTLEMENTS_TTL_S = 24 * 3600

# Trigger event log — ring buffer in memory. Resets on app restart, which is
# fine because the GitHub Actions workflow is the source of truth.
_TRIGGER_LOG: list[dict] = []
_TRIGGER_LOG_MAX = 100

# Per-IP rate limit for the public trigger endpoint.
_RATE_BUCKETS: dict[str, collections.deque] = {}
_RATE_LOCK = threading.Lock()
_RATE_WINDOW_S = 3600   # 1-hour sliding window
_RATE_MAX = 60          # 60 calls/hour/IP — generous for cron, throttles abuse


def _client_ip() -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _rate_check(ip: str) -> bool:
    """Sliding-window rate check. Returns True if request is allowed."""
    now = time.time()
    cutoff = now - _RATE_WINDOW_S
    with _RATE_LOCK:
        bucket = _RATE_BUCKETS.setdefault(ip, collections.deque())
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= _RATE_MAX:
            return False
        bucket.append(now)
        # Periodic GC: drop empty buckets to bound memory.
        if len(_RATE_BUCKETS) > 1024:
            for k in [k for k, v in _RATE_BUCKETS.items() if not v]:
                del _RATE_BUCKETS[k]
        return True


def _proxy_headers() -> dict:
    """Browser-like headers so the upstream nadlan API doesn't reject us."""
    return {
        "Origin": "https://www.nadlan.gov.il",
        "Referer": "https://www.nadlan.gov.il/",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/145.0.0.0 Safari/537.36"),
    }


@nadlan_api_bp.route("/parcel-info/<gush>/<chelka>", methods=["GET"])
def parcel_info(gush: str, chelka: str):
    """Return parcel metadata (settlement + neighborhood) for one (gush, chelka).

    Body returned by upstream looks like::

        {"base_level": "neighborhood",
         "neigh_id": "65209641", "neigh_name": "פסגת זאב",
         "parcel_id": "31314-2",
         "setl_id": "3000", "setl_name": "ירושלים"}

    On unknown parcels upstream returns ``{"message":"not found"}`` with 404 —
    we propagate that as-is.
    """
    try:
        int(gush)
        int(chelka)
    except ValueError:
        return jsonify({"error": "gush and chelka must be integers"}), 400

    base_id = f"{gush}-{chelka}"
    try:
        r = requests.post(
            DEAL_INFO_URL,
            json={"base_name": "parcel_id", "base_id": base_id},
            headers=_proxy_headers(),
            timeout=15,
        )
    except requests.RequestException as e:
        logger.warning("deal-info upstream error for %s: %s", base_id, e)
        return jsonify({"error": f"upstream error: {e}"}), 502

    # Pass through 404 cleanly so callers can distinguish "no data" from
    # genuine errors.
    if r.status_code == 404:
        return jsonify({"error": "not found", "base_id": base_id}), 404
    if r.status_code != 200:
        return jsonify({"error": f"upstream {r.status_code}",
                        "body": r.text[:200]}), 502

    try:
        return jsonify(r.json())
    except ValueError:
        return jsonify({"error": "upstream returned non-JSON"}), 502


@nadlan_api_bp.route("/settlements", methods=["GET"])
def settlements():
    """Cached settlements catalog. 24-hour TTL; falls back to stale on upstream error."""
    now = time.time()
    cache = _SETTLEMENTS_CACHE
    fresh = (cache["data"] is not None
             and (now - cache["fetched_at"]) < _SETTLEMENTS_TTL_S)
    if not fresh:
        try:
            r = requests.get(SETTLEMENTS_URL, timeout=20)
            r.raise_for_status()
            cache["data"] = r.json()
            cache["fetched_at"] = now
        except (requests.RequestException, ValueError) as e:
            logger.warning("settlements upstream error: %s", e)
            if cache["data"] is None:
                return jsonify({"error": f"upstream error: {e}"}), 502
            # Serve stale.
            return jsonify({
                "data": cache["data"],
                "stale": True,
                "fetched_at": cache["fetched_at"],
                "count": len(cache["data"]),
            })

    return jsonify({
        "data": cache["data"],
        "stale": False,
        "fetched_at": cache["fetched_at"],
        "count": len(cache["data"]),
    })


@nadlan_api_bp.route("/notify-trigger", methods=["POST"])
def notify_trigger():
    """Webhook for an external scheduler. Records the trigger event.

    Public + rate-limited (60/hour/IP). The handler is idempotent — it only
    appends to a 100-entry in-memory log. Abuse is harmless.
    """
    ip = _client_ip()
    if not _rate_check(ip):
        return jsonify({"error": "rate limit exceeded"}), 429

    body = request.get_json(silent=True) or {}
    entry = {
        "ts": time.time(),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": str(body.get("source", "unknown"))[:80],
        "note": str(body.get("note", ""))[:500],
        "remote": ip,
    }
    _TRIGGER_LOG.append(entry)
    if len(_TRIGGER_LOG) > _TRIGGER_LOG_MAX:
        del _TRIGGER_LOG[: len(_TRIGGER_LOG) - _TRIGGER_LOG_MAX]

    logger.info("nadlan trigger recorded: %s", entry)
    return jsonify({"recorded": True, "log_len": len(_TRIGGER_LOG)})


@nadlan_api_bp.route("/trigger-log", methods=["GET"])
def trigger_log():
    """Recent trigger events. Admin only."""
    if not is_admin():
        return jsonify({"error": "admin required"}), 403
    return jsonify({"entries": _TRIGGER_LOG[-50:],
                    "total": len(_TRIGGER_LOG)})


# ===========================================================================
# Distributed bulk-scrape queue
# ===========================================================================

# Central deals CSV that all workers append to. Stored under the same TEMP_DIR
# the rest of the app uses, so it lives alongside the SQLite store.
_DEALS_CSV_LOCK = threading.Lock()
_DEALS_CSV_NAME = "nadlan_deals_master.csv"

# Header — must match nadlan_worker.py's row layout exactly. PARCEL columns
# come from the input parcels.csv, META + DEAL columns from the worker's API
# call. See bulk_nadlan.py for the field-by-field rationale.
_DEALS_CSV_HEADER = [
    # parcel context (from input)
    "gush", "chelka", "locality", "municipality",
    "status", "legal_area_sqm", "area_sqm",
    "centroid_lat", "centroid_lon",
    # parcel meta (from /deal-info)
    "meta_setl_name", "meta_neigh_name", "meta_base_level",
    # deal fields (from /deal-data)
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "trend_rate", "trend_years", "prev_deals",
    "address", "parcelNum", "neighborhoodName", "ownership",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId", "row_id",
    # provenance
    "worker_id", "scraped_at",
]


def _get_store():
    """Lazy import to avoid circular dependency with app.py at module load."""
    from app import store
    return store


def _deals_csv_path() -> str:
    from app import TEMP_DIR
    return os.path.join(TEMP_DIR, _DEALS_CSV_NAME)


def _ensure_deals_header():
    path = _deals_csv_path()
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = _csv.writer(f)
            w.writerow(_DEALS_CSV_HEADER)


def _append_deals(rows: list[dict]) -> int:
    """Append worker-supplied deal rows.

    Prefers Postgres (survives redeploys). Falls back to the on-disk CSV
    for local dev / when DATABASE_URL isn't set.
    """
    if not rows:
        return 0
    try:
        from pg_store import get_pg_store
        pg = get_pg_store()
    except Exception:
        pg = None
    if pg is not None:
        return pg.append_deals(rows)

    path = _deals_csv_path()
    n = 0
    with _DEALS_CSV_LOCK:
        _ensure_deals_header()
        with open(path, "a", encoding="utf-8-sig", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=_DEALS_CSV_HEADER,
                                extrasaction="ignore")
            for r in rows:
                w.writerow(r)
                n += 1
    return n


@nadlan_api_bp.route("/bulk-queue", methods=["POST"])
def bulk_queue():
    """Admin: enqueue parcels from an uploaded parcels.csv.

    Multipart form: file=<parcels.csv>. Each row must have at least
    gush, chelka. Idempotent (rows with existing parcel_id are skipped).
    """
    if not _admin_or_worker():
        return jsonify({"error": "admin or worker key required"}), 403
    upload = request.files.get("file")
    if not upload:
        return jsonify({"error": "no file"}), 400

    text = upload.read().decode("utf-8-sig", errors="replace")
    reader = _csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return jsonify({"error": "empty CSV"}), 400

    # Optional server-side filter so admins can re-use the full parcels.csv.
    filter_status = (request.form.get("filter_status") or "").strip()
    if filter_status:
        rows = [r for r in rows if (r.get("status") or "").strip() == filter_status]

    result = _get_store().nadlan_create_tasks(rows)
    logger.info("bulk-queue: %s", result)
    return jsonify({**result, "filter_status": filter_status or None,
                    "received_rows": len(rows)})


@nadlan_api_bp.route("/bulk-claim", methods=["POST"])
def bulk_claim():
    """Worker: atomically claim up to N tasks.

    JSON body: {worker_id: "host-1", count: 5}
    """
    body = request.get_json(silent=True) or {}
    worker_id = str(body.get("worker_id") or "").strip()
    count = int(body.get("count") or 1)
    if not worker_id:
        return jsonify({"error": "worker_id required"}), 400
    if count < 1 or count > 100:
        return jsonify({"error": "count must be 1..100"}), 400

    # Cheap housekeeping: reset stale claims so a crashed worker doesn't
    # block its parcels indefinitely.
    _get_store().nadlan_reset_stale(timeout_seconds=600)

    tasks = _get_store().nadlan_claim_tasks(worker_id, count)
    return jsonify({"tasks": tasks, "claimed": len(tasks)})


@nadlan_api_bp.route("/bulk-result/<parcel_id>", methods=["POST"])
def bulk_result(parcel_id):
    """Worker: upload deals for a completed parcel.

    Multipart form:
        file        = CSV with header matching _DEALS_CSV_HEADER deal columns
        worker_id   = caller
        deals_count = N (sanity check)

    Returns {"appended": N, "task_state": "done"} on success.
    """
    worker_id = (request.form.get("worker_id") or "").strip()
    if not worker_id:
        return jsonify({"error": "worker_id required"}), 400

    # 0 deals is a valid result (parcel exists in registry but has no nadlan
    # transactions). The CSV file is optional in that case.
    rows: list[dict] = []
    upload = request.files.get("file")
    if upload:
        text = upload.read().decode("utf-8-sig", errors="replace")
        rows = list(_csv.DictReader(io.StringIO(text)))

    appended = _append_deals(rows)
    _get_store().nadlan_complete_task(parcel_id, deals_count=appended)
    return jsonify({"appended": appended, "task_state": "done"})


@nadlan_api_bp.route("/bulk-fail/<parcel_id>", methods=["POST"])
def bulk_fail(parcel_id):
    """Worker: report a failure for a claimed parcel.

    Form fields:
        worker_id, error, transient (true|false; default true)

    Transient failures return the task to pending so it gets retried.
    """
    worker_id = (request.form.get("worker_id") or "").strip()
    error = (request.form.get("error") or "")[:500]
    transient_flag = (request.form.get("transient") or "true").lower() != "false"
    if not worker_id:
        return jsonify({"error": "worker_id required"}), 400
    _get_store().nadlan_fail_task(parcel_id, error, transient=transient_flag)
    return jsonify({"recorded": True, "transient": transient_flag})


@nadlan_api_bp.route("/bulk-status", methods=["GET"])
def bulk_status():
    """Public aggregate counts."""
    return jsonify(_get_store().nadlan_status())


@nadlan_api_bp.route("/bulk-reset-stale", methods=["POST"])
def bulk_reset_stale():
    """Admin: force-reset claimed-but-stuck tasks (>10min) to pending."""
    if not _admin_or_worker():
        return jsonify({"error": "admin or worker key required"}), 403
    timeout = int(request.form.get("timeout_seconds") or 600)
    n = _get_store().nadlan_reset_stale(timeout_seconds=timeout)
    return jsonify({"reset": n})


@nadlan_api_bp.route("/bulk-clear", methods=["POST"])
def bulk_clear():
    """Admin: drop all queued tasks. Use for testing / fresh restart."""
    if not _admin_or_worker():
        return jsonify({"error": "admin or worker key required"}), 403
    n = _get_store().nadlan_clear()
    # Optionally rotate the deals store so a fresh run starts empty.
    if (request.form.get("clear_deals") or "").lower() == "true":
        try:
            from pg_store import get_pg_store
            pg = get_pg_store()
        except Exception:
            pg = None
        if pg is not None:
            pg.clear_deals()
        else:
            path = _deals_csv_path()
            if os.path.exists(path):
                os.remove(path)
    return jsonify({"cleared": n})


@nadlan_api_bp.route("/bulk-deals.csv", methods=["GET"])
def bulk_deals_csv():
    """Download the central deals CSV (admin only — file may be large).

    Streams from Postgres when DATABASE_URL is set, otherwise from the
    on-disk CSV.
    """
    if not _admin_or_worker():
        return jsonify({"error": "admin or worker key required"}), 403

    try:
        from pg_store import get_pg_store
        pg = get_pg_store()
    except Exception:
        pg = None

    if pg is not None:
        from flask import Response
        return Response(
            pg.stream_deals_csv(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition":
                    'attachment; filename="nadlan_deals_master.csv"',
            },
        )

    path = _deals_csv_path()
    if not os.path.exists(path):
        return jsonify({"error": "no deals collected yet"}), 404
    return send_file(path, mimetype="text/csv",
                     as_attachment=True,
                     download_name="nadlan_deals_master.csv")
