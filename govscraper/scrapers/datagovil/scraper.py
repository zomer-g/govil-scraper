"""data.gov.il scraper — CKAN package search, package show, datastore rows.

Three URL shapes are recognised:

  https://data.gov.il/dataset                                    -> search/listing
  https://data.gov.il/dataset?q=...                              -> filtered search
  https://data.gov.il/dataset/<name>                             -> dataset detail
  https://data.gov.il/dataset/<name>/resource/<uuid>             -> datastore rows

Output is a CkanCatalogResult. Single-resource fetches additionally
populate `rows_by_resource[uuid]` and `fields_by_resource[uuid]` so the
publisher layer can flatten to TabularResult when over.org.il push-version
is the destination.
"""
from __future__ import annotations

import re
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

from ...types import ParsedURL, Progress, ProgressFn
from ..base import BaseScraper, CkanCatalogResult
from ..registry import register
from .ckan_client import CkanClient

DATASET_LIST_RE = re.compile(r"^/dataset/?$")
DATASET_DETAIL_RE = re.compile(r"^/dataset/(?P<name>[^/]+)/?$")
RESOURCE_RE = re.compile(r"^/dataset/(?P<name>[^/]+)/resource/(?P<resource_id>[0-9a-fA-F\-]+)/?$")


@register
class DataGovIlScraper(BaseScraper):
    id: ClassVar[str] = "datagovil"

    def __init__(self, client: CkanClient | None = None) -> None:
        self._client = client

    @classmethod
    def parse_url(cls, url: str) -> ParsedURL | None:
        u = urlparse(url)
        host = (u.hostname or "").lower()
        if host != "data.gov.il" and not host.endswith(".data.gov.il"):
            return None
        path = u.path or "/"
        params = {k: (v[0] if v else "") for k, v in parse_qs(u.query).items()}

        m = RESOURCE_RE.match(path)
        if m:
            return ParsedURL(
                scraper_id=cls.id,
                canonical_url=url,
                params={
                    "form": "resource",
                    "dataset_name": m.group("name"),
                    "resource_id": m.group("resource_id").lower(),
                    "filters": _parse_filters(params.get("filters")),
                },
            )

        m = DATASET_DETAIL_RE.match(path)
        if m:
            return ParsedURL(
                scraper_id=cls.id,
                canonical_url=url,
                params={
                    "form": "dataset",
                    "dataset_name": m.group("name"),
                    "fetch_datastore": params.get("fetch_datastore", "").lower() in ("1", "true", "yes"),
                },
            )

        if DATASET_LIST_RE.match(path):
            return ParsedURL(
                scraper_id=cls.id,
                canonical_url=url,
                params={
                    "form": "search",
                    "query": params.get("q", "*:*"),
                    "page_size": int(params.get("rows") or 100),
                    "start": int(params.get("start") or 0),
                    "fq": params.get("fq") or None,
                },
            )
        return None

    # ------------------------------------------------------------------

    def fetch(self, parsed: ParsedURL, *, progress: ProgressFn) -> CkanCatalogResult:
        client = self._client or CkanClient()
        form = parsed.params.get("form")
        progress(Progress(phase="ckan", message=f"data.gov.il: {form}"))

        if form == "search":
            return self._fetch_search(client, parsed, progress)
        if form == "dataset":
            return self._fetch_dataset(client, parsed, progress)
        if form == "resource":
            return self._fetch_resource(client, parsed, progress)
        raise ValueError(f"Unknown CKAN URL form: {form!r}")

    def _fetch_search(self, client: CkanClient, parsed: ParsedURL, progress: ProgressFn) -> CkanCatalogResult:
        page = client.package_search(
            query=parsed.params["query"],
            page_size=parsed.params["page_size"],
            start=parsed.params["start"],
            fq=parsed.params.get("fq"),
        )
        datasets = page.get("results", []) or []
        progress(Progress(phase="ckan", current=len(datasets), total=int(page.get("count") or len(datasets)),
                          message=f"{len(datasets)} datasets"))
        return CkanCatalogResult(
            datasets=datasets,
            resources=[],
            rows_by_resource={},
            fields_by_resource={},
            source_url=parsed.canonical_url,
            metadata={"total": page.get("count"), "form": "search", "query": parsed.params["query"]},
        )

    def _fetch_dataset(self, client: CkanClient, parsed: ParsedURL, progress: ProgressFn) -> CkanCatalogResult:
        ds = client.package_show(parsed.params["dataset_name"])
        resources = ds.get("resources", []) or []
        progress(Progress(phase="ckan", current=len(resources), total=len(resources),
                          message=f"{len(resources)} resources"))

        rows_by_resource: dict[str, list] = {}
        fields_by_resource: dict[str, list] = {}
        if parsed.params.get("fetch_datastore"):
            for idx, r in enumerate(resources, 1):
                if not r.get("datastore_active"):
                    continue
                rid = r.get("id") or ""
                progress(Progress(phase="datastore", current=idx, total=len(resources),
                                  message=f"resource {rid}"))
                rows, fields = client.fetch_datastore_all(rid)
                rows_by_resource[rid] = rows
                fields_by_resource[rid] = fields

        return CkanCatalogResult(
            datasets=[ds],
            resources=resources,
            rows_by_resource=rows_by_resource,
            fields_by_resource=fields_by_resource,
            source_url=parsed.canonical_url,
            metadata={"form": "dataset", "dataset_name": parsed.params["dataset_name"],
                      "metadata_modified": ds.get("metadata_modified")},
        )

    def _fetch_resource(self, client: CkanClient, parsed: ParsedURL, progress: ProgressFn) -> CkanCatalogResult:
        rid = parsed.params["resource_id"]

        def _cb(*, current=0, total=0):
            progress(Progress(phase="datastore", current=current, total=total,
                              message=f"{current}/{total}" if total else f"{current}"))

        rows, fields = client.fetch_datastore_all(rid, filters=parsed.params.get("filters"), progress_cb=_cb)

        # Try to fetch dataset metadata so the result is self-describing.
        dataset_name = parsed.params["dataset_name"]
        try:
            ds = client.package_show(dataset_name)
            datasets = [ds]
            resources = ds.get("resources", []) or []
        except Exception:
            datasets = []
            resources = []

        return CkanCatalogResult(
            datasets=datasets,
            resources=resources,
            rows_by_resource={rid: rows},
            fields_by_resource={rid: fields},
            source_url=parsed.canonical_url,
            metadata={"form": "resource", "dataset_name": dataset_name, "resource_id": rid,
                      "collector_name": dataset_name},
        )


# ---------------------------------------------------------------------------

def _parse_filters(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        import json
        return json.loads(raw)
    except Exception:
        return None
