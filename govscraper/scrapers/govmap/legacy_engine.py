"""
GovMap layer scraping via the public OGC WFS at /api/geoserver/wfs.

Plugs into scraper_engine.py via parse_govmap_url() and scrape_govmap()
which are dispatched from GovILScraper.scrape() when the host is
govmap.gov.il. The dispatcher uses lazy imports so projects that don't
need GIS support don't pull pyproj/wfs into their import graph.

Layer naming convention: every GovMap layer is exposed as the WFS feature
type `govmap:layer_<id>` (numeric or semantic name). Catalog of known
layers lives in `layers.json` at the repo root.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Callable, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from govscraper.geo import coords
from govscraper.geo.wfs_client import WFSClient, WFSError
from govscraper.scrapers.govil.legacy_engine import (
    PageType, ParsedURL, ScrapeResult, FileAttachment,
    InvalidURLError, GovILScraperError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_MAX_FEATURES = int(os.environ.get("GOVMAP_MAX_FEATURES", "100000"))


def _find_layers_json() -> str:
    """Locate layers.json. Tries (in order): GOVMAP_LAYERS_FILE env var,
    sibling of this file, project root (when this lives at govscraper/scrapers/govmap/),
    cwd. Returns the first existing path, else the in-package fallback (which
    load_layer_catalog will report as missing)."""
    env = os.environ.get("GOVMAP_LAYERS_FILE")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    # Walk up to project root (3 levels: govscraper/scrapers/govmap → root)
    repo_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
    candidates = [
        os.path.join(here, "layers.json"),
        os.path.join(repo_root, "layers.json"),
        os.path.join(os.getcwd(), "layers.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return candidates[0]  # original default; load_layer_catalog logs the miss


DEFAULT_LAYERS_FILE = _find_layers_json()


# ---------------------------------------------------------------------------
# Layer catalog
# ---------------------------------------------------------------------------

@dataclass
class Layer:
    id: str                                   # ASCII-stable internal id
    label_he: str                             # Hebrew display name
    label_en: str = ""
    type_name: str = ""                       # WFS type name "govmap:layer_<id>"
    endpoint_kind: str = "wfs"
    geometry_type: str = "Point"
    out_fields: List[str] = field(default_factory=lambda: ["*"])
    default_bbox: Optional[Tuple[float, float, float, float]] = None
    notes: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        if self.default_bbox is not None:
            d["default_bbox"] = list(self.default_bbox)
        return d


_catalog_cache: Optional[dict] = None


def load_layer_catalog(path: str = DEFAULT_LAYERS_FILE) -> dict:
    """Load layers.json into a dict keyed by Layer.id. Cached per-process."""
    global _catalog_cache
    if _catalog_cache is not None:
        return _catalog_cache
    if not os.path.exists(path):
        logger.warning("Layer catalog missing: %s — using empty catalog", path)
        _catalog_cache = {}
        return _catalog_cache

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    out: dict = {}
    for entry in data.get("layers", []):
        try:
            lid = entry["id"]
            out[lid] = Layer(
                id=lid,
                label_he=entry.get("label_he", lid),
                label_en=entry.get("label_en", ""),
                type_name=entry.get("type_name", f"govmap:layer_{lid.lower()}"),
                endpoint_kind=entry.get("endpoint_kind", "wfs"),
                geometry_type=entry.get("geometry_type", "Point"),
                out_fields=entry.get("out_fields", ["*"]),
                default_bbox=(tuple(entry["default_bbox"])
                              if entry.get("default_bbox") else None),
                notes=entry.get("notes", ""),
            )
        except KeyError as e:
            logger.warning("Skipping malformed layer entry (missing %s): %s", e, entry)
    logger.info("Loaded %d GovMap layers from %s", len(out), path)
    _catalog_cache = out
    return out


def resolve_layer(key: str) -> Layer:
    """Find a Layer by its internal id, by WFS type name, or by parsing a
    govmap.gov.il URL with `?lay=<id>`. Returns a synthetic Layer for
    unknown numeric IDs so users can scrape any GovMap layer they find via
    DevTools without first adding it to layers.json."""
    catalog = load_layer_catalog()
    if not key:
        raise ValueError("empty layer key")

    if key in catalog:
        return catalog[key]

    # WFS type name
    if key.startswith("govmap:layer_"):
        for layer in catalog.values():
            if layer.type_name == key:
                return layer
        # Synthesize
        return Layer(id=key.replace("govmap:", "").upper(),
                     label_he=key, type_name=key,
                     geometry_type="Unknown")

    # URL?
    if "://" in key:
        parsed = urlparse(key)
        qs = parse_qs(parsed.query)
        for param in ("lay", "layers", "layer"):
            if param in qs:
                first = qs[param][0].split(",")[0].strip()
                if first in catalog:
                    return catalog[first]
                target = f"govmap:layer_{first}"
                for layer in catalog.values():
                    if layer.type_name == target:
                        return layer
                # Synthesize for any numeric layer ID seen in URL
                return Layer(id=f"LAYER_{first}",
                             label_he=f"שכבה {first}",
                             type_name=target,
                             geometry_type="Unknown",
                             notes=f"Auto-generated from URL ?lay={first}")
        raise ValueError(f"No 'lay' param in URL: {key}")

    # Numeric id (synthesize)
    if key.isdigit():
        return Layer(id=f"LAYER_{key}",
                     label_he=f"שכבה {key}",
                     type_name=f"govmap:layer_{key}",
                     geometry_type="Unknown")

    raise ValueError(f"Unknown layer: {key!r}")


def parse_bbox_from_url(url: str) -> Optional[Tuple[float, float, float, float]]:
    """Extract a BBOX (in ITM, EPSG:2039) from a govmap URL.

    Supports:
      - bbox=xmin,ymin,xmax,ymax (ITM, explicit)
      - bbox_wgs84=lon_min,lat_min,lon_max,lat_max (auto-converted to ITM)
      - c=x,y&z=N (ITM center+zoom — expanded to a 5km half-width box)
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    # Explicit ITM bbox
    for param in ("bbox", "extent"):
        if param in qs:
            parts = re.split(r"[,;]", qs[param][0])
            if len(parts) == 4:
                try:
                    return tuple(float(p) for p in parts)
                except ValueError:
                    pass

    # WGS84 bbox — converted to ITM
    if "bbox_wgs84" in qs:
        parts = re.split(r"[,;]", qs["bbox_wgs84"][0])
        if len(parts) == 4:
            try:
                wgs = tuple(float(p) for p in parts)
                return coords.wgs84_to_itm_bbox(wgs)
            except (ValueError, Exception):
                pass

    # Center + zoom shorthand
    if "c" in qs:
        parts = re.split(r"[,;]", qs["c"][0])
        if len(parts) == 2:
            try:
                cx, cy = float(parts[0]), float(parts[1])
                half = 5000.0
                return (cx - half, cy - half, cx + half, cy + half)
            except ValueError:
                pass
    return None


# ---------------------------------------------------------------------------
# Result building
# ---------------------------------------------------------------------------

@dataclass
class Feature:
    """A single GovMap feature with both WGS84 and ITM geometry."""
    geometry: dict                          # GeoJSON, WGS84
    properties: dict
    geometry_itm: Optional[dict] = None     # for shapefile parity


def _detect_geometry_type(features: List[Feature]) -> str:
    types = {f.geometry.get("type") for f in features if f.geometry}
    if not types:
        return ""
    if len(types) == 1:
        return next(iter(types))
    return "Mixed"


def _derive_columns(features: List[Feature]) -> List[str]:
    """Union of property keys across all features, preserving first-seen order."""
    seen: dict = {}
    for f in features:
        for k in f.properties.keys():
            seen.setdefault(k, None)
    columns = list(seen.keys())
    columns.append("_geometry_wkt")
    return columns


# ---------------------------------------------------------------------------
# Public entry points (called from scraper_engine.py)
# ---------------------------------------------------------------------------

def parse_govmap_url(url: str, params: dict) -> ParsedURL:
    """Build a ParsedURL for a govmap.gov.il URL.

    Called from scraper_engine.parse_gov_url() when the host matches.
    """
    try:
        layer = resolve_layer(url)
    except ValueError as e:
        raise InvalidURLError(
            f"כתובת govmap.gov.il לא נתמכת: {url}\n{e}\n"
            "נתמך: ?lay=<מספר השכבה> (אופציונלי: &bbox=xmin,ymin,xmax,ymax או &c=x,y)"
        )
    bbox = parse_bbox_from_url(url)
    qp = {"layer_id": layer.id, "type_name": layer.type_name}
    if bbox:
        qp["bbox_itm"] = ",".join(str(x) for x in bbox)
    return ParsedURL(
        page_type=PageType.GOVMAP_LAYER,
        collector_name=layer.label_he,
        original_url=url,
        query_params=qp,
    )


def scrape_govmap(
    session,
    parsed: ParsedURL,
    progress_callback: Optional[Callable] = None,
    max_features: int = DEFAULT_MAX_FEATURES,
) -> ScrapeResult:
    """Scrape a GovMap layer. Returns a host-compatible ScrapeResult.

    `session` is the GovILSession from scraper_engine. `parsed` is the
    output of parse_govmap_url. `progress_callback` is the same callback
    convention used by other scrapers (kwargs: current, total, message).
    """
    progress = progress_callback or (lambda **_: None)

    layer = resolve_layer(parsed.original_url)
    type_name = layer.type_name

    bbox_str = parsed.query_params.get("bbox_itm")
    bbox_itm: Tuple[float, float, float, float] = ()
    bbox_wgs84: Tuple[float, float, float, float] = ()
    bbox_3857: Optional[Tuple[float, float, float, float]] = None
    if bbox_str:
        try:
            bbox_itm = tuple(float(x) for x in bbox_str.split(","))
            bbox_wgs84 = coords.itm_to_wgs84_bbox(bbox_itm)
            bbox_3857 = coords.itm_to_wm_bbox(bbox_itm)
        except (ValueError, TypeError):
            logger.warning("Bad bbox_itm in parsed URL: %s", bbox_str)

    client = WFSClient(_WfsSessionAdapter(session))

    # Probe count
    try:
        total = client.hits(type_name, bbox_3857=bbox_3857)
    except WFSError as e:
        raise GovILScraperError(f"GovMap WFS hits failed: {e}")

    progress(current=0, total=total,
             message=f"מצא {total} פיצ'רים בשכבה {layer.label_he}")

    if total == 0:
        return ScrapeResult(
            warning="לא נמצאו פיצ'רים בשכבה/באזור.",
            collector_name=f"{layer.label_he}__{int(time.time())}",
            page_type=PageType.GOVMAP_LAYER,
            column_headers=[],
            layer_id=layer.id,
            bbox_itm=bbox_itm,
            bbox_wgs84=bbox_wgs84,
            srs="ITM",
        )

    if total > max_features:
        return ScrapeResult(
            warning=(f"גדול מדי: {total} פיצ'רים (תקרה {max_features}). "
                     "צמצמו את ה-BBOX או העלו את GOVMAP_MAX_FEATURES."),
            collector_name=f"{layer.label_he}__{int(time.time())}",
            page_type=PageType.GOVMAP_LAYER,
            column_headers=[],
            layer_id=layer.id,
            bbox_itm=bbox_itm,
            bbox_wgs84=bbox_wgs84,
            geometry_type=layer.geometry_type,
            srs="ITM",
            total_count=total,
        )

    # Fetch features. WFS returns WGS84 directly when srsName=EPSG:4326.
    features: List[Feature] = []
    try:
        for raw in client.iter_features(
            type_name,
            page_size=500,
            bbox_3857=bbox_3857,
            max_features=max_features,
            progress_callback=lambda **kw: progress(**kw),
        ):
            geom = raw.get("geometry") or {}
            props = raw.get("properties") or {}
            geom_itm = coords.wgs84_geom_to_itm(geom) if geom else None
            features.append(Feature(
                geometry=geom,
                geometry_itm=geom_itm,
                properties=props,
            ))
    except WFSError as e:
        raise GovILScraperError(f"GovMap WFS GetFeature failed: {e}")

    # Build flat attribute table for CSV/Excel
    column_headers = _derive_columns(features)
    items: List[dict] = []
    for f in features:
        row = dict(f.properties)
        row["_geometry_wkt"] = coords.geom_to_wkt(f.geometry_itm or {})
        items.append(row)

    return ScrapeResult(
        items=items,
        total_count=len(features),
        file_attachments=[],
        collector_name=f"{layer.label_he}__{int(time.time())}",
        page_type=PageType.GOVMAP_LAYER,
        column_headers=column_headers,
        features=features,
        layer_id=layer.id,
        bbox_itm=bbox_itm,
        bbox_wgs84=bbox_wgs84,
        geometry_type=_detect_geometry_type(features),
        srs="ITM",
    )


# ---------------------------------------------------------------------------
# Adapter: GovILSession -> WFSClient's get_raw() interface
# ---------------------------------------------------------------------------

class _WfsSessionAdapter:
    """The host's GovILSession exposes get/post but not the simpler get_raw()
    that wfs_client.py expects. We adapt it here so wfs_client doesn't need
    to import scraper_engine."""

    def __init__(self, gov_session):
        self._s = gov_session

    def get_raw(self, url: str, params: Optional[dict] = None,
                retries: int = 3):
        """Return a raw requests.Response. Reuses cloudscraper's session under
        GovILSession but bypasses the GovIL host-only header handling since
        govmap.gov.il is a different host."""
        # Lazy attribute access — GovILSession has its session at _session
        scraper = getattr(self._s, "_session", None) or self._s
        last: Optional[Exception] = None
        for attempt in range(retries):
            try:
                # Strip Origin/Referer when calling govmap (its CORS expects
                # them removed for unauth'd public endpoints, OR set to govmap)
                headers = {"Origin": "https://www.govmap.gov.il",
                           "Referer": "https://www.govmap.gov.il/"}
                r = scraper.get(url, params=params, headers=headers, timeout=120)
                if r.status_code in (200, 400):
                    return r
                if r.status_code == 429:
                    time.sleep(2 ** (attempt + 1))
                    continue
                last = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            except Exception as e:
                last = e
                time.sleep(min(2 ** attempt, 10))
        raise RuntimeError(f"GET {url} failed after {retries}: {last}")
