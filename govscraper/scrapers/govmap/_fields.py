"""External API field names for www.govmap.gov.il (OGC WFS 2.0)."""
from __future__ import annotations

# Endpoints
WFS_BASE = "https://www.govmap.gov.il/api/geoserver/wfs"

# Layer naming convention
LAYER_TYPE_NAME_PATTERN = "govmap:layer_{layer_id}"

# WFS request params (URL query, OGC standard)
PARAM_SERVICE = "service"
PARAM_VERSION = "version"
PARAM_REQUEST = "request"
PARAM_TYPE_NAMES = "typeNames"
PARAM_OUTPUT_FORMAT = "outputFormat"
PARAM_BBOX = "bbox"
PARAM_SRS_NAME = "srsName"
PARAM_COUNT = "count"
PARAM_START_INDEX = "startIndex"
PARAM_VALUE_REFERENCE = "valueReference"

VAL_SERVICE = "WFS"
VAL_VERSION_2_0 = "2.0.0"
REQUEST_GET_FEATURE = "GetFeature"
REQUEST_GET_PROPERTY_VALUE = "GetPropertyValue"
REQUEST_DESCRIBE_FEATURE_TYPE = "DescribeFeatureType"

OUTPUT_FORMAT_JSON = "application/json"
DEFAULT_CRS = "EPSG:3857"  # Web Mercator — what GeoServer returns by default
ITM_CRS = "EPSG:2039"
WGS84_CRS = "EPSG:4326"

# URL-form params on govmap.gov.il page URLs
URL_PARAM_LAYER = "lay"
URL_PARAM_BBOX = "bbox"
URL_PARAM_BBOX_WGS84 = "bbox_wgs84"
URL_PARAM_CENTER = "c"
URL_PARAM_ZOOM = "z"
