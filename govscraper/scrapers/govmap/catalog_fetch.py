"""Fetch GovMap's online layers catalog and map numeric layer IDs to WFS type names.

The numeric IDs in govmap.gov.il URLs (`?lay=200541`) are catalog primary
keys, NOT WFS feature-type names. The actual WFS type name is stored in
the catalog entry under `serviceLayerId` (e.g. `govmap:layer_settl_territories_wb`).

GovMap's catalog endpoint requires two custom headers (empirically verified
2026-05-02): `x-user-id` and `x-trace-id`. Both accept any random UUID-hex
value — they're treated as anonymous-session identifiers.

Usage:
    >>> from govscraper.scrapers.govmap import catalog_fetch
    >>> entry = catalog_fetch.lookup_layer("200541")
    >>> entry["serviceLayerId"]
    'govmap:layer_settl_territories_wb'
"""
from __future__ import annotations

import logging
import threading
import uuid
from typing import Any

import requests

logger = logging.getLogger(__name__)

CATALOG_URL = "https://www.govmap.gov.il/api/layers-catalog/catalog?lang=he"

# Cache the parsed catalog in-process. The full payload is ~700 KB / ~50k
# layers; refetching per call would be wasteful. Refresh once per process.
_cache_lock = threading.Lock()
_cache: dict[str, dict[str, Any]] | None = None


def _make_headers() -> dict[str, str]:
    """Build the headers GovMap's catalog endpoint requires.

    `x-user-id` and `x-trace-id` are session/trace identifiers — any random
    UUID hex is accepted. The User-Agent is a stock Chrome string; the
    endpoint also accepts cloudscraper's UA, but a fresh random one keeps
    us decoupled from cloudscraper-specific behaviour.
    """
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/138.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
        "Referer": "https://www.govmap.gov.il/",
        "x-user-id": uuid.uuid4().hex,
        "x-trace-id": uuid.uuid4().hex,
    }


def _walk_layers(obj: Any, out: dict[str, dict[str, Any]]) -> None:
    """Recursively scan the catalog payload for layer entries — i.e. dicts
    with both `id` and `serviceLayerId` keys. The catalog nests categories
    inside categories so a flat scan is the simplest correct approach.
    """
    if isinstance(obj, dict):
        if "id" in obj and "serviceLayerId" in obj:
            out[str(obj["id"])] = obj
        for v in obj.values():
            _walk_layers(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _walk_layers(v, out)


def fetch_online_catalog(timeout_s: int = 30) -> dict[str, dict[str, Any]]:
    """Fetch + parse + cache GovMap's online catalog.

    Returns a dict keyed by string layer ID (`"200541"`) mapping to the
    full catalog entry (with `serviceLayerId`, `caption`, `name`, etc.).
    Cached after the first successful fetch.
    """
    global _cache
    with _cache_lock:
        if _cache is not None:
            return _cache

    logger.info("Fetching GovMap online catalog from %s", CATALOG_URL)
    r = requests.get(CATALOG_URL, headers=_make_headers(), timeout=timeout_s)
    if r.status_code != 200:
        logger.warning("GovMap catalog fetch failed: HTTP %d (%d bytes). Body[:200]: %s",
                       r.status_code, len(r.content), r.text[:200])
        with _cache_lock:
            _cache = {}
        return {}

    try:
        payload = r.json()
    except ValueError as e:
        logger.warning("GovMap catalog returned non-JSON: %s. Body[:200]: %s",
                       e, r.text[:200])
        with _cache_lock:
            _cache = {}
        return {}

    out: dict[str, dict[str, Any]] = {}
    _walk_layers(payload, out)
    logger.info("GovMap catalog: %d layers indexed", len(out))
    with _cache_lock:
        _cache = out
    return out


def lookup_layer(layer_id: str | int) -> dict[str, Any] | None:
    """Return the catalog entry for a numeric layer ID, or None if unknown.

    Triggers a one-time fetch of the catalog on first call.
    """
    catalog = fetch_online_catalog()
    return catalog.get(str(layer_id))


def reset_cache() -> None:
    """Clear the in-process cache (for testing or after long-running daemons)."""
    global _cache
    with _cache_lock:
        _cache = None
