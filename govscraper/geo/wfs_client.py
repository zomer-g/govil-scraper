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
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

WFS_BASE = "https://www.govmap.gov.il/api/geoserver/wfs"

# GovMap's WFS occasionally returns transient 4xx/5xx (HTTP 400 with no
# changed input is the most common — empirically retrying after ~1s succeeds).
# Auth/not-found responses are NOT transient and short-circuit the loop.
_RETRY_BACKOFF_S = (1.0, 3.0, 7.0)   # delays before attempts 2, 3, 4
_NON_RETRY_STATUSES = frozenset({401, 403, 404})

# Empirical hard cap on a single GovMap WFS GetFeature response.
# Tested 2026-05-03: count=50000 → 200 (82MB, 8s); count=100000 → 500.
# Used as the default page_size so most reasonable BBOXes finish in one
# request — no pagination needed.
SERVER_HARD_CAP = 50_000

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

    # ---- transport with retry ------------------------------------------

    def _get_with_retry(self, url: str, params: dict, what: str = ""):
        """GET with bounded retry+backoff for transient WFS failures.

        Retries on network errors and HTTP statuses that aren't auth/not-found.
        Returns the response object on success (any retryable status that
        persists past the last attempt is returned as-is, so callers' existing
        error-handling for non-200 still fires)."""
        last_exc: Optional[Exception] = None
        last_resp = None
        attempts = len(_RETRY_BACKOFF_S) + 1
        for attempt in range(1, attempts + 1):
            try:
                r = self.session.get_raw(url, params=params)
            except Exception as e:
                last_exc = e
                if attempt < attempts:
                    delay = _RETRY_BACKOFF_S[attempt - 1]
                    logger.warning("WFS %s network error (attempt %d/%d): %s — "
                                   "retrying in %.1fs",
                                   what or "GET", attempt, attempts, e, delay)
                    time.sleep(delay)
                    continue
                raise
            if r.status_code == 200 or r.status_code in _NON_RETRY_STATUSES:
                return r
            last_resp = r
            if attempt < attempts:
                delay = _RETRY_BACKOFF_S[attempt - 1]
                logger.warning("WFS %s HTTP %d (attempt %d/%d) — retrying in %.1fs",
                               what or "GET", r.status_code, attempt, attempts, delay)
                time.sleep(delay)
        # Exhausted: return the last non-2xx response so the caller produces
        # the same error path it would have without retry.
        if last_resp is not None:
            return last_resp
        # Network errors all the way down — re-raise the last one.
        raise last_exc if last_exc else WFSError(f"WFS {what}: no response")

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
        r = self._get_with_retry(self.base, params=params,
                                 what=f"DescribeFeatureType {type_name}")
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
        r = self._get_with_retry(self.base, params=params,
                                 what=f"hits {type_name}")
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
        r = self._get_with_retry(self.base, params=params,
                                 what=f"GetFeature {type_name}")
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
        page_size: int = SERVER_HARD_CAP,
        cql_filter: Optional[str] = None,
        bbox_3857: Optional[Tuple[float, float, float, float]] = None,
        max_features: int = 100000,
        progress_callback=None,
    ) -> Iterator[dict]:
        """Iterate all features for a layer.

        Implementation strategy:
        - GovMap's WFS rejects ``startIndex`` (returns 400) and at least one
          important layer (``layer_nadlan``) rejects ``CQL_FILTER`` entirely
          (returns 500). The only reliable way to fetch is a single request.
        - Server caps a single response at ~50k features (``SERVER_HARD_CAP``);
          asking for more returns 500.
        - When ``total ≤ page_size``: one request — fast path. This covers
          most real BBOXes.
        - When ``total > page_size``: try CQL pagination as a fallback for
          layers that DO support it. If the very first paginated request
          fails, raise a clear actionable error instead of silently looping.

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

        # Fast path: fits in one request (covers the vast majority of cases
        # now that page_size defaults to the server hard cap of 50k).
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

        # Slow path: total > server hard cap. Try CQL pagination — works on
        # layers that support CQL_FILTER. If the very first request fails
        # (e.g. layer_nadlan rejects CQL with 500), surface a clear error
        # so the operator narrows the BBOX instead of seeing "Unknown error"
        # after retries exhaust.
        try:
            first_filter = "objectid > -1"
            if cql_filter:
                first_filter = f"({cql_filter}) AND ({first_filter})"
            first_page = self.get_features_geojson(
                type_name=type_name,
                count=page_size,
                cql_filter=first_filter + " ORDER BY objectid",
                bbox_3857=bbox_3857,
            )
        except WFSError as e:
            raise WFSError(
                f"BBOX מכיל {total:,} פיצ'רים בשכבה {type_name}, "
                f"מעל הגבול של {SERVER_HARD_CAP:,} לכל קריאה, "
                f"וסריקה במנות נכשלה (השכבה לא תומכת ב-CQL pagination). "
                f"צמצם את ה-BBOX. מקור השגיאה: {e}"
            ) from e

        emitted = 0
        last_oid = -1
        feats = first_page.get("features", [])
        while feats:
            for f in feats:
                yield f
                emitted += 1
                oid = (f.get("properties") or {}).get("objectid")
                if isinstance(oid, int) and oid > last_oid:
                    last_oid = oid
            if progress_callback:
                progress_callback(current=emitted, total=total,
                                  message=f"הורדו {emitted}/{total} פיצ'רים")
            if len(feats) < page_size or emitted >= total:
                break
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
        r = self._get_with_retry(self.base, params=params, what="GetCapabilities")
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
