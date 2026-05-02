"""
GeoServer WFS 2.0 client for GovMap.

GovMap exposes a public OGC WFS endpoint at:
    https://www.govmap.gov.il/api/geoserver/wfs

Layer naming convention: `govmap:layer_<id>` (numeric) or
`govmap:layer_<semantic_name>` (e.g. `govmap:layer_fire_areas`).

Default CRS for responses is EPSG:3857 (Web Mercator). We always request
GeoJSON (`outputFormat=application/json`) so geometries come back as
proper GeoJSON dicts and not Esri JSON.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

WFS_BASE = "https://www.govmap.gov.il/api/geoserver/wfs"

# Namespace prefixes used in the WFS XML responses
WFS_NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "ows": "http://www.opengis.net/ows/1.1",
    "xsd": "http://www.w3.org/2001/XMLSchema",
}


class WFSError(Exception):
    pass


@dataclass
class WFSLayerMetadata:
    type_name: str            # e.g. "govmap:layer_220826"
    title: str = ""
    abstract: str = ""
    default_crs: str = "EPSG:3857"
    bbox_wgs84: Optional[Tuple[float, float, float, float]] = None
    field_names: List[str] = None  # populated by DescribeFeatureType


class WFSClient:
    """Thin OGC WFS 2.0 client around a single GovMap instance."""

    def __init__(self, session, base: str = WFS_BASE):
        self.session = session
        self.base = base.rstrip("?&")
        # Cache: type_name -> WFSLayerMetadata
        self._meta_cache: dict = {}

    # ---- type-name resolution -------------------------------------------

    @staticmethod
    def normalize_type_name(layer_id_or_name: str) -> str:
        """Accept '220826' / 'govmap:layer_220826' / 'fire_areas' / etc.,
        normalize to 'govmap:layer_<x>'."""
        s = str(layer_id_or_name).strip()
        if ":" in s:
            return s
        if s.startswith("layer_"):
            return f"govmap:{s}"
        return f"govmap:layer_{s}"

    # ---- metadata --------------------------------------------------------

    def describe_feature_type(self, type_name: str) -> WFSLayerMetadata:
        """Fetch field list for a layer via DescribeFeatureType."""
        type_name = self.normalize_type_name(type_name)
        if type_name in self._meta_cache:
            return self._meta_cache[type_name]

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "DescribeFeatureType",
            "typeName": type_name,
        }
        r = self.session.get_raw(self.base, params=params)
        if r.status_code != 200:
            raise WFSError(f"DescribeFeatureType {type_name}: HTTP {r.status_code}")

        # Parse XSD schema - extract <xsd:element name="..."/>
        root = ET.fromstring(r.content)
        fields: List[str] = []
        for el in root.iter("{http://www.w3.org/2001/XMLSchema}element"):
            name = el.attrib.get("name")
            if name and name not in ("FEATURE_COLLECTION_NAME_PLACEHOLDER",):
                fields.append(name)

        meta = WFSLayerMetadata(type_name=type_name, field_names=fields)
        self._meta_cache[type_name] = meta
        return meta

    def hits(self, type_name: str, cql_filter: Optional[str] = None,
             bbox_3857: Optional[Tuple[float, float, float, float]] = None) -> int:
        """Count matching features without fetching them (resultType=hits)."""
        type_name = self.normalize_type_name(type_name)
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": type_name,
            "resultType": "hits",
        }
        if cql_filter:
            params["CQL_FILTER"] = cql_filter
        if bbox_3857:
            params["bbox"] = ",".join(str(x) for x in bbox_3857) + ",EPSG:3857"
        logger.debug("WFS hits: GET %s params=%s", self.base, params)
        r = self.session.get_raw(self.base, params=params)
        if r.status_code != 200:
            raise WFSError(
                f"hits: HTTP {r.status_code} from {self.base} "
                f"(typeNames={type_name!r}, bbox_3857={bbox_3857}). "
                f"Body[:200]: {r.text[:200]}"
            )
        # XML response: <wfs:FeatureCollection numberMatched="N" ...>
        root = ET.fromstring(r.content)
        n = root.attrib.get("numberMatched") or root.attrib.get("numberOfFeatures")
        if n is None or n == "unknown":
            return 0
        try:
            return int(n)
        except ValueError:
            return 0

    # ---- feature retrieval -----------------------------------------------

    def get_features_geojson(
        self,
        type_name: str,
        count: int = 50000,
        cql_filter: Optional[str] = None,
        bbox_3857: Optional[Tuple[float, float, float, float]] = None,
        srs_name: str = "EPSG:4326",
    ) -> dict:
        """Fetch up to `count` features as GeoJSON.

        Note: GovMap's WFS rejects the `startIndex` parameter, so paging is
        done via CQL filtering on `objectid` (see iter_features) — we never
        send startIndex here. For most layers a single large `count` is
        sufficient given the GOVMAP_MAX_FEATURES safety cap.
        """
        type_name = self.normalize_type_name(type_name)
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": type_name,
            "outputFormat": "application/json",
            "count": count,
            "srsName": srs_name,
        }
        if cql_filter:
            params["CQL_FILTER"] = cql_filter
        if bbox_3857:
            params["bbox"] = ",".join(str(x) for x in bbox_3857) + ",EPSG:3857"
        logger.debug("WFS GetFeature: GET %s params=%s", self.base, params)
        r = self.session.get_raw(self.base, params=params)
        if r.status_code != 200:
            raise WFSError(
                f"GetFeature: HTTP {r.status_code} from {self.base} "
                f"(typeNames={type_name!r}, count={count}, bbox_3857={bbox_3857}). "
                f"Body[:200]: {r.text[:200]}"
            )
        try:
            return r.json()
        except ValueError as e:
            raise WFSError(
                f"Bad GeoJSON from {self.base} (typeNames={type_name!r}): {e}; "
                f"body[:200]={r.text[:200]}"
            )

    def iter_features(
        self,
        type_name: str,
        page_size: int = 5000,
        cql_filter: Optional[str] = None,
        bbox_3857: Optional[Tuple[float, float, float, float]] = None,
        max_features: int = 100000,
        progress_callback=None,
    ) -> Iterator[dict]:
        """Iterate all features for a layer.

        Implementation: GovMap's WFS rejects `startIndex`, so we either:
        - request the whole layer in one shot when total <= page_size, or
        - paginate via CQL on `objectid > last_id` (assumes the layer has
          an integer objectid field — true for all standard GovMap layers).
        Yields raw GeoJSON Feature dicts.
        """
        type_name = self.normalize_type_name(type_name)
        total = self.hits(type_name, cql_filter=cql_filter, bbox_3857=bbox_3857)
        if progress_callback:
            progress_callback(current=0, total=total,
                              message=f"מצא {total} פיצ'רים")

        if total == 0:
            return
        if total > max_features:
            raise WFSError(
                f"Layer {type_name} has {total} features (cap {max_features}). "
                "Use a CQL filter or BBOX, or raise GOVMAP_MAX_FEATURES."
            )

        # Fast path: fits in one request
        if total <= page_size:
            page = self.get_features_geojson(
                type_name=type_name,
                count=max(page_size, total),
                cql_filter=cql_filter,
                bbox_3857=bbox_3857,
            )
            feats = page.get("features", [])
            for i, f in enumerate(feats, 1):
                yield f
                if progress_callback and i % 200 == 0:
                    progress_callback(current=i, total=total,
                                      message=f"הורדו {i}/{total} פיצ'רים")
            if progress_callback:
                progress_callback(current=len(feats), total=total,
                                  message=f"הורדו {len(feats)}/{total} פיצ'רים")
            return

        # Slow path: paginate via objectid filter
        emitted = 0
        last_oid = -1
        while emitted < total:
            page_filter = f"objectid > {last_oid}"
            if cql_filter:
                page_filter = f"({cql_filter}) AND ({page_filter})"
            page = self.get_features_geojson(
                type_name=type_name,
                count=page_size,
                cql_filter=page_filter + " ORDER BY objectid",
                bbox_3857=bbox_3857,
            )
            feats = page.get("features", [])
            if not feats:
                break
            for f in feats:
                yield f
                emitted += 1
                oid = (f.get("properties") or {}).get("objectid")
                if isinstance(oid, int) and oid > last_oid:
                    last_oid = oid
            if progress_callback:
                progress_callback(current=emitted, total=total,
                                  message=f"הורדו {emitted}/{total} פיצ'רים")
            if len(feats) < page_size:
                break

    # ---- catalog ---------------------------------------------------------

    def list_feature_types(self) -> List[Tuple[str, str]]:
        """Parse GetCapabilities and return [(type_name, title), ...].

        WARNING: the full GetCapabilities response is ~25 MB on GovMap with
        ~50,000 layers. Cache the result.
        """
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetCapabilities",
        }
        r = self.session.get_raw(self.base, params=params)
        if r.status_code != 200:
            raise WFSError(f"GetCapabilities: HTTP {r.status_code}")
        root = ET.fromstring(r.content)
        out: List[Tuple[str, str]] = []
        for ft in root.iter("{http://www.opengis.net/wfs/2.0}FeatureType"):
            name_el = ft.find("{http://www.opengis.net/wfs/2.0}Name")
            title_el = ft.find("{http://www.opengis.net/wfs/2.0}Title")
            if name_el is not None and name_el.text:
                out.append((name_el.text, title_el.text if title_el is not None else ""))
        return out
