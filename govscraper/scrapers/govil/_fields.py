"""External API field names for www.gov.il collectors. Wire-format strings,
preserved verbatim. Internal Python uses snake_case (`offset`, `office_id`).
"""
from __future__ import annotations

# Endpoints (from scraper_engine.py constants).
#
# DynamicCollector still lives at www.gov.il and is stable. The
# Traditional collector + Layout endpoints below are the PRE-MIGRATION
# paths kept here for historical reference — gov.il moved both to
# openapi-gc.digital.gov.il in mid-2026 and now gates them behind an
# x-client-id header. The live URLs are read at runtime from
# ``GovILRuntimeConfig`` in ``legacy_engine.py``; do not hard-code these
# strings in new code.
DYNAMIC_API_URL = "https://www.gov.il/he/api/DynamicCollector"
TRADITIONAL_API_URL = "https://www.gov.il/CollectorsWebApi/api/DataCollector/GetResults"
LAYOUT_API_URL = "https://www.gov.il/CollectorsWebApi/api/DataCollector/GetLayoutCollectorModel"

# DynamicCollector body keys
DYNAMIC_TEMPLATE_ID_KEY = "DynamicTemplateID"
QUERY_FILTERS_KEY = "QueryFilters"
SKIP_KEY = "skip"          # nested under QueryFilters.skip.Query
QUERY_NESTED_KEY = "Query"
FROM_KEY = "From"          # top-level offset for some custom DynamicCollector APIs

# Traditional collector params (URL query string)
COLLECTOR_TYPE_PARAM = "CollectorType"
OFFICE_ID_PARAM = "officeId"
CULTURE_PARAM = "culture"
SKIP_PARAM = "skip"
LIMIT_PARAM = "limit"

# Custom-API extras (extracted from page HTML)
RESULTS_API_URL_ATTR = "resultsApiURL"
X_CLIENT_ID_HEADER = "x-client-id"

# DynamicCollector response envelope
RESP_RESULTS = "Results"
RESP_TOTAL_RESULTS = "TotalResults"

# Traditional collector response envelope (lowercase keys!)
TRAD_RESP_RESULTS = "results"
TRAD_RESP_TOTAL = "total"
