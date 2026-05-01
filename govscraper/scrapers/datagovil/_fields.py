"""External CKAN API field names for data.gov.il. Preserved verbatim — these
are wire-format strings, not Python identifiers. Internal vars use
snake_case (`query`, `page_size`, `offset_rows`).
"""
from __future__ import annotations

# Action endpoints
ACTION_PACKAGE_SEARCH = "package_search"
ACTION_PACKAGE_SHOW = "package_show"
ACTION_DATASTORE_SEARCH = "datastore_search"

# package_search params
SEARCH_Q = "q"
SEARCH_ROWS = "rows"
SEARCH_START = "start"
SEARCH_FQ = "fq"

# package_show params
PACKAGE_ID = "id"

# datastore_search params
DS_RESOURCE_ID = "resource_id"
DS_OFFSET = "offset"
DS_LIMIT = "limit"
DS_FILTERS = "filters"
DS_Q = "q"
DS_FIELDS = "fields"

# Response envelope
RESP_SUCCESS = "success"
RESP_RESULT = "result"
RESP_ERROR = "error"
RESP_RECORDS = "records"
RESP_FIELDS = "fields"
RESP_TOTAL = "total"

# Pagination defaults — CKAN datastore caps `limit` at 32k; 10k keeps memory
# bounded while staying under the cap.
DATASTORE_PAGE_SIZE = 10_000
SEARCH_PAGE_SIZE = 100
