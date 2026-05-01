"""Thin CKAN API client for data.gov.il.

Targets v3 actions: package_search, package_show, datastore_search.
Uses cloudscraper if available so any future Cloudflare gating doesn't
break us (data.gov.il itself doesn't currently challenge, but the
upstream pattern is ubiquitous in Israeli government services).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterator
from urllib.parse import urljoin

from . import _fields as F

logger = logging.getLogger(__name__)

CKAN_BASE = "https://data.gov.il/"
ACTION_PATH = "api/3/action/{action}"


class CkanError(RuntimeError):
    pass


def _new_session() -> Any:
    """Prefer cloudscraper if available; fall back to plain requests."""
    try:
        import cloudscraper
        return cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
    except Exception:
        import requests
        return requests.Session()


class CkanClient:
    def __init__(self, base_url: str = CKAN_BASE, *, session: Any | None = None,
                 timeout: int = 60, max_retries: int = 3):
        self._base = base_url if base_url.endswith("/") else base_url + "/"
        self._session = session or _new_session()
        self._timeout = timeout
        self._max_retries = max_retries

    def _action_url(self, action: str) -> str:
        return urljoin(self._base, ACTION_PATH.format(action=action))

    def _call(self, action: str, params: dict[str, Any]) -> Any:
        """GET /api/3/action/<action>?... with retries on 5xx/network errors.

        Returns the unwrapped `result` field. Raises CkanError on auth/4xx
        failures or after all retries exhaust.
        """
        url = self._action_url(action)
        last_err: str = ""
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
            except Exception as e:
                last_err = f"network: {e}"
                if attempt < self._max_retries:
                    time.sleep(2 * attempt)
                    continue
                raise CkanError(last_err) from e
            if resp.status_code == 200:
                body = resp.json()
                if not body.get(F.RESP_SUCCESS, False):
                    raise CkanError(f"CKAN {action} returned success=false: {body.get(F.RESP_ERROR)}")
                return body.get(F.RESP_RESULT)
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
            if 500 <= resp.status_code < 600 and attempt < self._max_retries:
                time.sleep(5 * attempt)
                continue
            raise CkanError(last_err)
        raise CkanError(last_err or "unreachable")

    # ------------------------------------------------------------------
    # Public actions
    # ------------------------------------------------------------------

    def package_search(self, query: str = "*:*", *, page_size: int = F.SEARCH_PAGE_SIZE,
                       start: int = 0, fq: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {F.SEARCH_Q: query, F.SEARCH_ROWS: page_size, F.SEARCH_START: start}
        if fq:
            params[F.SEARCH_FQ] = fq
        return self._call(F.ACTION_PACKAGE_SEARCH, params)

    def package_show(self, dataset_name: str) -> dict[str, Any]:
        return self._call(F.ACTION_PACKAGE_SHOW, {F.PACKAGE_ID: dataset_name})

    def datastore_search(self, resource_id: str, *, offset_rows: int = 0,
                         page_size: int = F.DATASTORE_PAGE_SIZE,
                         filters: dict | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {
            F.DS_RESOURCE_ID: resource_id,
            F.DS_OFFSET: offset_rows,
            F.DS_LIMIT: page_size,
        }
        if filters:
            import json as _json
            params[F.DS_FILTERS] = _json.dumps(filters, ensure_ascii=False)
        return self._call(F.ACTION_DATASTORE_SEARCH, params)

    def iter_datastore(self, resource_id: str, *, page_size: int = F.DATASTORE_PAGE_SIZE,
                       filters: dict | None = None) -> Iterator[dict[str, Any]]:
        """Paginate datastore_search until exhausted. Yields one row at a time."""
        offset = 0
        while True:
            page = self.datastore_search(resource_id, offset_rows=offset, page_size=page_size, filters=filters)
            records = page.get(F.RESP_RECORDS) or []
            if not records:
                return
            for r in records:
                yield r
            if len(records) < page_size:
                return
            total = page.get(F.RESP_TOTAL)
            offset += len(records)
            if total is not None and offset >= int(total):
                return

    def fetch_datastore_all(self, resource_id: str, *, page_size: int = F.DATASTORE_PAGE_SIZE,
                            filters: dict | None = None,
                            progress_cb=None) -> tuple[list[dict], list[dict]]:
        """Convenience wrapper — returns (rows, fields). fields is the schema
        list from the first page (records + types as CKAN reports them)."""
        rows: list[dict] = []
        fields: list[dict] = []
        offset = 0
        first_page = True
        while True:
            page = self.datastore_search(resource_id, offset_rows=offset, page_size=page_size, filters=filters)
            records = page.get(F.RESP_RECORDS) or []
            if first_page:
                fields = page.get(F.RESP_FIELDS) or []
                first_page = False
            if not records:
                break
            rows.extend(records)
            offset += len(records)
            if progress_cb:
                progress_cb(current=offset, total=int(page.get(F.RESP_TOTAL) or 0))
            if len(records) < page_size:
                break
            total = page.get(F.RESP_TOTAL)
            if total is not None and offset >= int(total):
                break
        return rows, fields
