"""GeoJSON writer + sidecar manifest.json — used by GovMap-layer scrapes."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Iterable

from .sanitize import sanitize_filename

logger = logging.getLogger(__name__)


def write_feature_collection(
    output_dir: str,
    name: str,
    features: Iterable,
    *,
    layer_id: str = "",
    bbox_itm: tuple | None = None,
    bbox_wgs84: tuple | None = None,
    geometry_type: str = "",
) -> str:
    """Write a WGS84 FeatureCollection to `<output_dir>/<sanitized name>.geojson`.

    `features` is an iterable of objects with `.geometry` and `.properties`
    attributes (or already-formed Feature dicts — duck-typed).
    """
    filename = sanitize_filename(name) + ".geojson"
    filepath = os.path.join(output_dir, filename)

    feature_list = list(features)
    fc = {
        "type": "FeatureCollection",
        "name": name,
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"},
        },
        "metadata": {
            "layer_id": layer_id,
            "bbox_itm": list(bbox_itm) if bbox_itm else None,
            "bbox_wgs84": list(bbox_wgs84) if bbox_wgs84 else None,
            "geometry_type": geometry_type,
            "feature_count": len(feature_list),
            "scraped_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
        "features": [
            f if isinstance(f, dict) else {
                "type": "Feature",
                "geometry": getattr(f, "geometry", None),
                "properties": getattr(f, "properties", {}),
            }
            for f in feature_list
        ],
    }
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, ensure_ascii=False)

    logger.info("GeoJSON exported: %s (%d features)", filepath, len(feature_list))
    return filepath


def write_manifest(
    output_dir: str,
    files: dict[str, str],
    *,
    layer_id: str = "",
    collector_name: str = "",
    page_type: str = "",
    bbox_itm: tuple | None = None,
    bbox_wgs84: tuple | None = None,
    geometry_type: str = "",
    feature_count: int = 0,
    srs: str = "",
    warning: str = "",
) -> str:
    """Write `<output_dir>/manifest.json` with the geo metadata the upload
    endpoint needs to populate the collections table's geo columns.
    `files` is a {category: absolute_path} dict; paths are stored relative
    to `output_dir`.
    """
    path = os.path.join(output_dir, "manifest.json")
    manifest: dict[str, Any] = {
        "layer_id": layer_id,
        "collector_name": collector_name,
        "page_type": page_type,
        "bbox_itm": list(bbox_itm) if bbox_itm else None,
        "bbox_wgs84": list(bbox_wgs84) if bbox_wgs84 else None,
        "geometry_type": geometry_type,
        "feature_count": feature_count,
        "srs": srs,
        "warning": warning,
        "files": {
            k: os.path.relpath(v, output_dir).replace("\\", "/")
            for k, v in files.items() if v
        },
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    return path
