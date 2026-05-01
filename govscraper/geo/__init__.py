"""Geometry utilities — coordinate transforms (ITM ↔ WGS84 ↔ Web Mercator)
and a thin OGC WFS 2.0 client.
"""
from . import coords, wfs_client
from .coords import (
    BBox,
    esri_to_geojson,
    geom_to_wkt,
    itm_geom_to_wgs84,
    itm_to_wgs84,
    itm_to_wgs84_bbox,
    itm_to_wm,
    itm_to_wm_bbox,
    wgs84_geom_to_itm,
    wgs84_to_itm,
    wgs84_to_itm_bbox,
    wgs84_to_wm,
    wgs84_to_wm_bbox,
    wm_geom_to_itm,
    wm_geom_to_wgs84,
    wm_to_itm,
    wm_to_wgs84,
)
from .wfs_client import WFS_BASE, WFSClient, WFSError, WFSLayerMetadata

__all__ = [
    "coords",
    "wfs_client",
    "BBox",
    "WFS_BASE",
    "WFSClient",
    "WFSError",
    "WFSLayerMetadata",
    "itm_to_wgs84",
    "wgs84_to_itm",
    "itm_to_wgs84_bbox",
    "wgs84_to_itm_bbox",
    "wm_to_wgs84",
    "wgs84_to_wm",
    "itm_to_wm",
    "wm_to_itm",
    "wgs84_to_wm_bbox",
    "itm_to_wm_bbox",
    "wm_geom_to_wgs84",
    "wm_geom_to_itm",
    "itm_geom_to_wgs84",
    "wgs84_geom_to_itm",
    "esri_to_geojson",
    "geom_to_wkt",
]
