"""
GovMap helper endpoints — layer catalog, BBOX previews, and one-shot
scrape trigger by layer name.

Endpoints:
    GET  /api/govmap/layers
        Public — returns the curated layer catalog from layers.json.
        Used by the Leaflet map UI to populate the layer dropdown.

    POST /api/govmap/preview-bbox
        Public (rate-limited) — body {layer, bbox, srs}. Hits GovMap's
        WFS once with resultType=hits and returns just the feature
        count. Use to warn the user before running a multi-million-
        feature scrape.

    POST /api/govmap/scrape
        Admin — body {layer, bbox?, srs?, mode?}. Triggers a scrape
        directly by layer NAME, no URL construction needed. Internally
        builds a synthetic govmap URL and dispatches it through the
        same /api/scrape pipeline; returns {job_id, url, mode}.

The catalog and preview endpoints are public; the scrape endpoint is
admin-only because it triggers an outbound WFS query and writes to the
collections store.
"""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

logger = logging.getLogger(__name__)

govmap_api_bp = Blueprint("govmap_api", __name__)

# Local rate limiter — independent storage so it doesn't fight with the app's
# global limiter. The blueprint can be registered standalone in tests too.
_limiter = Limiter(get_remote_address, storage_uri="memory://")


@govmap_api_bp.record_once
def _attach_limiter(state):
    """Wire our limiter to the host app once when the blueprint is registered."""
    _limiter.init_app(state.app)


@govmap_api_bp.route("/api/govmap/layers", methods=["GET"])
@_limiter.limit("60 per minute")
def list_layers():
    """Catalog of known GovMap layers. Drives the UI dropdown."""
    from govmap_engine import load_layer_catalog
    catalog = load_layer_catalog()
    return jsonify({"layers": [layer.to_dict() for layer in catalog.values()]})


@govmap_api_bp.route("/api/govmap/preview-bbox", methods=["POST"])
@_limiter.limit("30 per minute")
def preview_bbox():
    """Cheap count probe before a full scrape.

    Admin-only — probes GovMap WFS for a hit count. Issues outbound traffic
    and could be abused for reconnaissance, so we gate by admin/worker key.

    Body: {layer, bbox, srs}
      - layer: layer id ("FIRE_AREAS_ORDERS") or numeric (220826)
      - bbox: [xmin, ymin, xmax, ymax]
      - srs: "ITM" (EPSG:2039) or "WGS84" (EPSG:4326). Default ITM.

    Returns: {layer, bbox, srs, count}
    """
    from auth import is_admin, _check_worker_key
    if not (is_admin() or _check_worker_key()):
        return jsonify({"error": "admin or worker key required"}), 403

    from govmap_engine import resolve_layer
    from wfs_client import WFSClient, WFSError
    from scraper_engine import GovILSession
    import coords

    data = request.get_json(force=True, silent=True) or {}
    layer_key = (data.get("layer") or "").strip()
    bbox = data.get("bbox")
    srs = (data.get("srs") or "ITM").upper()

    if not layer_key:
        return jsonify({"error": "missing 'layer'"}), 400
    if srs not in ("ITM", "WGS84", "EPSG:2039", "EPSG:4326"):
        return jsonify({"error": f"unsupported srs: {srs}"}), 400

    try:
        layer = resolve_layer(layer_key)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # bbox is OPTIONAL — omit for a whole-country count probe.
    bbox_3857 = None
    bbox_t: tuple = ()
    if bbox:
        if len(bbox) != 4:
            return jsonify({"error": "bbox must be [xmin, ymin, xmax, ymax]"}), 400
        try:
            bbox_t = tuple(float(x) for x in bbox)
        except (TypeError, ValueError):
            return jsonify({"error": "BBOX values must be numeric"}), 400
        bbox_3857 = (coords.itm_to_wm_bbox(bbox_t)
                     if srs in ("ITM", "EPSG:2039")
                     else coords.wgs84_to_wm_bbox(bbox_t))

    session = None
    try:
        session = GovILSession(use_playwright_fallback=False)
        # Don't bother with full session warming — WFS GET works without it.
        try:
            session.warm()
        except Exception as e:
            logger.debug("warm skipped: %s", e)

        # Adapter so wfs_client can call get_raw on the session
        from govmap_engine import _WfsSessionAdapter
        client = WFSClient(_WfsSessionAdapter(session))
        count = client.hits(layer.type_name, bbox_3857=bbox_3857)
        return jsonify({
            "layer": layer.id,
            "type_name": layer.type_name,
            "bbox": list(bbox_t) if bbox_t else None,
            "srs": srs if bbox_t else None,
            "count": count,
        })
    except WFSError as e:
        return jsonify({"error": f"WFS error: {e}"}), 502
    except Exception as e:
        logger.exception("preview-bbox failed")
        return jsonify({"error": f"unexpected error: {e}"}), 500
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


@govmap_api_bp.route("/api/govmap/scrape", methods=["POST"])
@_limiter.limit("5 per minute")
def scrape_by_layer_name():
    """Trigger a GovMap scrape by layer name.

    Admin-only. Builds a synthetic govmap.gov.il URL and dispatches it
    through the same pipeline as /api/scrape (server mode) or /api/tasks
    (worker mode).

    Body:
        {
            "layer": "FIRE_AREAS_ORDERS"            // id from /api/govmap/layers,
                                                    // or numeric WFS id (e.g. "220826")
            "bbox": [xmin, ymin, xmax, ymax]?       // optional, ITM (EPSG:2039)
            "bbox_wgs84": [lon, lat, lon, lat]?     // optional, WGS84 (auto-converted)
            "mode": "auto" | "server" | "worker"    // optional, default "auto"
        }

    Response (202 Accepted):
        {
            "job_id": "abc123def456",   # use with /api/progress/{id}
            "url": "https://www.govmap.gov.il/?lay=220826&bbox_wgs84=...",
            "layer": "FIRE_AREAS_ORDERS",
            "mode": "server"
        }
    """
    # Late import to avoid circular imports at blueprint registration time.
    from auth import is_admin
    from govmap_engine import resolve_layer

    if not is_admin():
        return jsonify({"error": "נדרשת הרשאת מנהל"}), 403

    data = request.get_json(force=True, silent=True) or {}
    layer_key = (data.get("layer") or "").strip()
    if not layer_key:
        return jsonify({"error": "חסר שדה 'layer'"}), 400

    try:
        layer = resolve_layer(layer_key)
    except ValueError as e:
        return jsonify({"error": f"שכבה לא ידועה: {e}"}), 400

    # Build a synthetic URL — the existing parse_gov_url + GOVMAP_LAYER
    # dispatcher will resolve it back to the same Layer object.
    numeric_id = layer.type_name.replace("govmap:layer_", "")
    url = f"https://www.govmap.gov.il/?lay={numeric_id}"

    bbox = data.get("bbox")
    bbox_wgs84 = data.get("bbox_wgs84")
    if bbox and len(bbox) == 4:
        try:
            url += "&bbox=" + ",".join(str(float(x)) for x in bbox)
        except (TypeError, ValueError):
            return jsonify({"error": "ערכי bbox חייבים להיות מספריים"}), 400
    elif bbox_wgs84 and len(bbox_wgs84) == 4:
        try:
            url += "&bbox_wgs84=" + ",".join(str(float(x)) for x in bbox_wgs84)
        except (TypeError, ValueError):
            return jsonify({"error": "ערכי bbox_wgs84 חייבים להיות מספריים"}), 400

    mode = (data.get("mode") or "auto").lower()
    if mode not in ("auto", "server", "worker"):
        return jsonify({"error": f"mode לא חוקי: {mode}"}), 400

    # Forward to the existing app-level scrape pipeline. We import lazily
    # to avoid a circular import (app.py imports this blueprint).
    from app import (
        _run_scrape_job, _cleanup_old_jobs,
        jobs, jobs_lock, store, MAX_CONCURRENT_JOBS,
        _is_worker_online, Phase,
    )
    from queue import Queue
    import time, threading, uuid

    if mode == "auto":
        mode = "worker" if _is_worker_online() else "server"

    job_id = uuid.uuid4().hex[:12]

    if mode == "worker":
        # Persist as a worker-mode task so the next polling worker picks it up
        store.create_task(job_id, url, download_files=False, mode="worker")
        return jsonify({
            "job_id": job_id,
            "url": url,
            "layer": layer.id,
            "mode": "worker",
        }), 202

    # Server mode: enforce concurrent-job cap then spawn
    _cleanup_old_jobs()
    active = sum(
        1 for j in jobs.values()
        if j["status"].get("phase") not in (Phase.COMPLETE.value, Phase.ERROR.value, None)
    )
    if active >= MAX_CONCURRENT_JOBS:
        return jsonify({"error": "יותר מדי משימות פעילות. נסה שוב בעוד דקה."}), 429

    with jobs_lock:
        jobs[job_id] = {
            "queue": Queue(maxsize=200),
            "status": {"phase": Phase.INITIALIZING.value},
            "result_paths": {},
            "result_data": None,
            "error_log": None,
            "created": time.time(),
        }

    threading.Thread(
        target=_run_scrape_job,
        args=(job_id, url, False),       # download_files=False — no attachments for GIS
        daemon=True,
    ).start()

    return jsonify({
        "job_id": job_id,
        "url": url,
        "layer": layer.id,
        "mode": "server",
    }), 202
