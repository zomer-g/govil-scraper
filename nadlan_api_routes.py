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
        a trigger event in an in-memory ring buffer. Use this hook to wire
        future scheduling integrations (e.g. enqueue a task on OVER) without
        running Playwright on Render.
        Auth: X-Trigger-Key header == NADLAN_TRIGGER_KEY env var.

    GET  /api/nadlan/trigger-log
        Admin-only — last 50 trigger events (for debugging the cron pipeline).
"""

import logging
import os
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

    Auth: ``X-Trigger-Key: <NADLAN_TRIGGER_KEY>``. Separate from admin OAuth
    so a CI pipeline can call this without OAuth credentials.
    """
    expected = os.environ.get("NADLAN_TRIGGER_KEY", "").strip()
    if not expected:
        return jsonify({"error": "NADLAN_TRIGGER_KEY not configured on server"}), 503
    provided = request.headers.get("X-Trigger-Key", "").strip()
    if provided != expected:
        return jsonify({"error": "missing or invalid X-Trigger-Key"}), 403

    body = request.get_json(silent=True) or {}
    entry = {
        "ts": time.time(),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": str(body.get("source", "unknown"))[:80],
        "note": str(body.get("note", ""))[:500],
        "remote": (request.headers.get("X-Forwarded-For",
                                       request.remote_addr) or "").split(",")[0],
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
