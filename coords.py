"""
ITM (EPSG:2039) <-> WGS84 (EPSG:4326) coordinate transforms.

GovMap uses Israel Transverse Mercator (ITM) for all spatial data.
GeoJSON requires WGS84. We keep both: ITM for fidelity / shapefile
output, WGS84 for GeoJSON output and basemap rendering.
"""

from __future__ import annotations

from typing import Tuple, Any

from pyproj import Transformer

# always_xy=True forces (x, y) / (lon, lat) ordering regardless of CRS axis-order quirks.
_ITM_TO_WGS = Transformer.from_crs("EPSG:2039", "EPSG:4326", always_xy=True)
_WGS_TO_ITM = Transformer.from_crs("EPSG:4326", "EPSG:2039", always_xy=True)
_WM_TO_WGS = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
_WGS_TO_WM = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_ITM_TO_WM = Transformer.from_crs("EPSG:2039", "EPSG:3857", always_xy=True)
_WM_TO_ITM = Transformer.from_crs("EPSG:3857", "EPSG:2039", always_xy=True)

BBox = Tuple[float, float, float, float]


def itm_to_wgs84(x: float, y: float) -> Tuple[float, float]:
    """ITM (x, y) -> WGS84 (lon, lat)."""
    return _ITM_TO_WGS.transform(x, y)


def wgs84_to_itm(lon: float, lat: float) -> Tuple[float, float]:
    """WGS84 (lon, lat) -> ITM (x, y)."""
    return _WGS_TO_ITM.transform(lon, lat)


def itm_to_wgs84_bbox(bbox: BBox) -> BBox:
    """(xmin, ymin, xmax, ymax) ITM -> (lon_min, lat_min, lon_max, lat_max) WGS84."""
    xmin, ymin, xmax, ymax = bbox
    lon_min, lat_min = _ITM_TO_WGS.transform(xmin, ymin)
    lon_max, lat_max = _ITM_TO_WGS.transform(xmax, ymax)
    return (lon_min, lat_min, lon_max, lat_max)


def wgs84_to_itm_bbox(bbox: BBox) -> BBox:
    """(lon_min, lat_min, lon_max, lat_max) WGS84 -> (xmin, ymin, xmax, ymax) ITM."""
    lon_min, lat_min, lon_max, lat_max = bbox
    xmin, ymin = _WGS_TO_ITM.transform(lon_min, lat_min)
    xmax, ymax = _WGS_TO_ITM.transform(lon_max, lat_max)
    return (xmin, ymin, xmax, ymax)


def _transform_coords(coords: Any, fn) -> Any:
    """Recursively walk a GeoJSON 'coordinates' tree and apply fn to leaf [x,y] pairs."""
    if not coords:
        return coords
    # Leaf: [x, y] or [x, y, z]
    if isinstance(coords[0], (int, float)):
        x, y = coords[0], coords[1]
        nx, ny = fn(x, y)
        if len(coords) > 2:
            return [nx, ny, coords[2]]
        return [nx, ny]
    return [_transform_coords(c, fn) for c in coords]


def wm_to_wgs84(x: float, y: float) -> Tuple[float, float]:
    """Web Mercator (EPSG:3857) -> WGS84 (lon, lat)."""
    return _WM_TO_WGS.transform(x, y)


def wgs84_to_wm(lon: float, lat: float) -> Tuple[float, float]:
    """WGS84 (lon, lat) -> Web Mercator (EPSG:3857)."""
    return _WGS_TO_WM.transform(lon, lat)


def itm_to_wm(x: float, y: float) -> Tuple[float, float]:
    """ITM (EPSG:2039) -> Web Mercator (EPSG:3857)."""
    return _ITM_TO_WM.transform(x, y)


def wm_to_itm(x: float, y: float) -> Tuple[float, float]:
    """Web Mercator (EPSG:3857) -> ITM (EPSG:2039)."""
    return _WM_TO_ITM.transform(x, y)


def wgs84_to_wm_bbox(bbox: BBox) -> BBox:
    lon_min, lat_min, lon_max, lat_max = bbox
    a = _WGS_TO_WM.transform(lon_min, lat_min)
    b = _WGS_TO_WM.transform(lon_max, lat_max)
    return (a[0], a[1], b[0], b[1])


def itm_to_wm_bbox(bbox: BBox) -> BBox:
    xmin, ymin, xmax, ymax = bbox
    a = _ITM_TO_WM.transform(xmin, ymin)
    b = _ITM_TO_WM.transform(xmax, ymax)
    return (a[0], a[1], b[0], b[1])


def wm_geom_to_wgs84(geom: dict) -> dict:
    """Transform a GeoJSON geometry from EPSG:3857 to WGS84."""
    if not geom:
        return geom
    gtype = geom.get("type")
    if gtype == "GeometryCollection":
        return {
            "type": "GeometryCollection",
            "geometries": [wm_geom_to_wgs84(g) for g in geom.get("geometries", [])],
        }
    return {
        "type": gtype,
        "coordinates": _transform_coords(geom.get("coordinates"), wm_to_wgs84),
    }


def wm_geom_to_itm(geom: dict) -> dict:
    """Transform a GeoJSON geometry from EPSG:3857 to ITM."""
    if not geom:
        return geom
    gtype = geom.get("type")
    if gtype == "GeometryCollection":
        return {
            "type": "GeometryCollection",
            "geometries": [wm_geom_to_itm(g) for g in geom.get("geometries", [])],
        }
    return {
        "type": gtype,
        "coordinates": _transform_coords(geom.get("coordinates"), wm_to_itm),
    }


def itm_geom_to_wgs84(geom: dict) -> dict:
    """Transform a GeoJSON geometry from ITM to WGS84.

    Supports Point, MultiPoint, LineString, MultiLineString, Polygon,
    MultiPolygon, and GeometryCollection.
    """
    if not geom:
        return geom
    gtype = geom.get("type")
    if gtype == "GeometryCollection":
        return {
            "type": "GeometryCollection",
            "geometries": [itm_geom_to_wgs84(g) for g in geom.get("geometries", [])],
        }
    return {
        "type": gtype,
        "coordinates": _transform_coords(geom.get("coordinates"), itm_to_wgs84),
    }


def wgs84_geom_to_itm(geom: dict) -> dict:
    """Transform a GeoJSON geometry from WGS84 to ITM (recursively)."""
    if not geom:
        return geom
    gtype = geom.get("type")
    if gtype == "GeometryCollection":
        return {
            "type": "GeometryCollection",
            "geometries": [wgs84_geom_to_itm(g) for g in geom.get("geometries", [])],
        }
    return {
        "type": gtype,
        "coordinates": _transform_coords(geom.get("coordinates"), wgs84_to_itm),
    }


def itm_geom_to_wgs84_raw(geom: dict) -> dict:
    """Alias preserved for forward compatibility."""
    return itm_geom_to_wgs84(geom)


# ---------------------------------------------------------------------------
# Esri JSON geometry -> GeoJSON
# ---------------------------------------------------------------------------

def esri_to_geojson(esri_geom: dict, esri_geometry_type: str) -> dict | None:
    """Convert an Esri-format geometry (from ArcGIS REST f=json) to GeoJSON.

    Esri uses {x, y} for points, {paths: [[[x,y], ...]]} for polylines,
    and {rings: [[[x,y], ...]]} for polygons. Coordinates are in inSR/outSR
    space — we preserve them as-is here; conversion to WGS84 is the caller's job.
    """
    if not esri_geom:
        return None
    t = (esri_geometry_type or "").lower()

    if t in ("esrigeometrypoint", "point"):
        return {"type": "Point", "coordinates": [esri_geom["x"], esri_geom["y"]]}

    if t in ("esrigeometrymultipoint", "multipoint"):
        return {
            "type": "MultiPoint",
            "coordinates": esri_geom.get("points", []),
        }

    if t in ("esrigeometrypolyline", "polyline"):
        paths = esri_geom.get("paths", [])
        if len(paths) == 1:
            return {"type": "LineString", "coordinates": paths[0]}
        return {"type": "MultiLineString", "coordinates": paths}

    if t in ("esrigeometrypolygon", "polygon"):
        rings = esri_geom.get("rings", [])
        # Esri rings are not separated into outer/holes by structure; for our
        # use case we treat each ring as its own polygon (correct enough for
        # GovMap parcels which are usually simple polygons).
        if not rings:
            return None
        if len(rings) == 1:
            return {"type": "Polygon", "coordinates": [rings[0]]}
        return {"type": "MultiPolygon", "coordinates": [[r] for r in rings]}

    return None


# ---------------------------------------------------------------------------
# WKT serialisation (for CSV column "_geometry_wkt")
# ---------------------------------------------------------------------------

def _coord_str(c):
    return f"{c[0]} {c[1]}"


def geom_to_wkt(geom: dict) -> str:
    """Tiny GeoJSON -> WKT serialiser. Sufficient for our CSV side-channel."""
    if not geom:
        return ""
    t = geom.get("type", "")
    coords = geom.get("coordinates")

    if t == "Point":
        return f"POINT({_coord_str(coords)})"
    if t == "MultiPoint":
        return "MULTIPOINT(" + ", ".join(f"({_coord_str(c)})" for c in coords) + ")"
    if t == "LineString":
        return "LINESTRING(" + ", ".join(_coord_str(c) for c in coords) + ")"
    if t == "MultiLineString":
        return "MULTILINESTRING(" + ", ".join(
            "(" + ", ".join(_coord_str(c) for c in line) + ")" for line in coords
        ) + ")"
    if t == "Polygon":
        return "POLYGON(" + ", ".join(
            "(" + ", ".join(_coord_str(c) for c in ring) + ")" for ring in coords
        ) + ")"
    if t == "MultiPolygon":
        return "MULTIPOLYGON(" + ", ".join(
            "(" + ", ".join(
                "(" + ", ".join(_coord_str(c) for c in ring) + ")" for ring in poly
            ) + ")" for poly in coords
        ) + ")"
    return ""
