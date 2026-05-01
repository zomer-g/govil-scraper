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
"""

import collections
import logging
import threading
import time

import requests
from flask import Blueprint, jsonify, request

from auth import is_admin

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
