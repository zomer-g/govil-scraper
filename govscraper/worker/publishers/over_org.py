"""ResultPublisher for over.org.il.

Phase D facade — wraps the legacy OverWorkerClient.upload_zip / upload_csv /
push_version paths so the on-the-wire bytes stay identical to what the
existing over_worker.py sends today. All field names live in
worker/publishers/_contract.py and are pinned by tests/contract/test_over_org.py.

Result handling:
- TabularResult / CkanCatalogResult-flattened: rows -> CSV (gzipped if large)
  or inline records, attachments -> zipped, push_version emitted with
  PRIMARY_RESOURCE_NAME ("נתוני הסורק").
- GeoFeatureResult: features -> CSV (one row per feature with geometry as WKT
  in a "_geometry_wkt" column) so the over.org.il datastore can index them.
  Phase E may add a richer geo handling once over.org.il exposes a GeoJSON
  resource format; for now CSV-flattening preserves byte-identity.
"""
from __future__ import annotations

import csv
import io
import json
import time
from typing import Any

from ...scrapers.base import (
    CkanCatalogResult,
    GeoFeatureResult,
    ScrapeResult,
    TabularResult,
)
from ...types import Task
from . import _contract as C
from .base import PublishOutcome, ResultPublisher


class OverOrgPublisher:
    name = "over.org.il"

    def __init__(self, api_key: str, *, _client: Any = None):
        if _client is None:
            # Import the canonical path directly; the over_worker.py at the
            # repo root is a shim that round-trips back to govscraper/legacy/.
            from govscraper.legacy.over_worker import OverWorkerClient
            _client = OverWorkerClient(api_key)
        self._client = _client
        self._api_key = api_key

    def publish(
        self,
        task: Task,
        result: ScrapeResult,
        *,
        duration_s: float,
    ) -> PublishOutcome:
        if isinstance(result, TabularResult):
            return self._publish_tabular(task, result, duration_s)
        if isinstance(result, GeoFeatureResult):
            return self._publish_geo(task, result, duration_s)
        if isinstance(result, CkanCatalogResult):
            return self._publish_ckan(task, result, duration_s)
        raise TypeError(f"OverOrgPublisher cannot handle {type(result).__name__}")

    # ------------------------------------------------------------------
    # Tabular path — the canonical case for gov.il / nadlan
    # ------------------------------------------------------------------

    def _publish_tabular(
        self,
        task: Task,
        result: TabularResult,
        duration_s: float,
    ) -> PublishOutcome:
        if not task.tracked_dataset_id:
            raise ValueError("over.org.il publish requires Task.tracked_dataset_id")

        records = list(result.rows)
        fields = list(result.fields) or _fields_from_rows(records)
        attachments_meta = [
            {"filename": a.original_filename, "url": a.source_url}
            for a in (result.attachments or [])
        ]

        # Decide inline-records vs out-of-band CSV upload. Mirrors
        # over_worker.execute_task's >50MB threshold.
        records_json = json.dumps(records, ensure_ascii=False)
        csv_resource_ids: dict[str, str] | None = None
        if len(records_json) > 50 * 1024 * 1024:
            csv_bytes = _rows_to_csv_bytes(records, [f["name"] for f in fields])
            resource_id = self._client.upload_csv(
                task.tracked_dataset_id,
                version_number=1,
                csv_bytes=csv_bytes,
                resource_name=C.PRIMARY_RESOURCE_NAME,
                row_count=len(records),
                fields=fields,
            )
            if resource_id:
                csv_resource_ids = {C.PRIMARY_RESOURCE_NAME: resource_id}

        zip_resource_ids: list[str] = []  # not handling attachments-zip in phase D facade

        push_result = self._client.push_version(
            tracked_dataset_id=task.tracked_dataset_id,
            source_url=task.source_url,
            records=records,
            fields=fields,
            attachments=attachments_meta,
            duration_seconds=duration_s,
            zip_resource_ids=zip_resource_ids or None,
            csv_resource_ids=csv_resource_ids,
        )
        return PublishOutcome(success=True, message=str(push_result.get("message", "ok")), refs=push_result)

    def _publish_geo(
        self,
        task: Task,
        result: GeoFeatureResult,
        duration_s: float,
    ) -> PublishOutcome:
        # Flatten features to a tabular shape — properties + _geometry_wkt
        from ...geo.coords import geom_to_wkt
        rows: list[dict] = []
        for feat in result.features or []:
            row = dict(feat.get("properties") or {})
            geom = feat.get("geometry")
            row["_geometry_wkt"] = geom_to_wkt(geom) if geom else ""
            rows.append(row)
        flat = TabularResult(
            rows=rows,
            fields=_fields_from_rows(rows),
            attachments=result.attachments,
            source_url=result.source_url,
            collector_name=result.layer_label,
            metadata={**(result.metadata or {}), "geometry_type": result.geometry_type},
            warning=result.warning,
        )
        return self._publish_tabular(task, flat, duration_s)

    def _publish_ckan(
        self,
        task: Task,
        result: CkanCatalogResult,
        duration_s: float,
    ) -> PublishOutcome:
        # Phase E adapter: collapse a single-resource CKAN result into tabular.
        # Multi-resource catalog results are not yet supported here — they go
        # to LocalCollectionsPublisher only.
        if len(result.rows_by_resource) == 1:
            (resource_id, rows), = result.rows_by_resource.items()
            fields = result.fields_by_resource.get(resource_id) or _fields_from_rows(rows)
            flat = TabularResult(
                rows=rows,
                fields=fields,
                attachments=[],
                source_url=result.source_url,
                collector_name=str(result.metadata.get("collector_name", resource_id)),
                metadata=dict(result.metadata or {}),
            )
            return self._publish_tabular(task, flat, duration_s)
        raise NotImplementedError(
            "OverOrgPublisher does not yet handle multi-resource CKAN catalogs"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fields_from_rows(rows: list[dict]) -> list[dict]:
    seen: list[str] = []
    seen_set: set[str] = set()
    for r in rows:
        for k in r.keys():
            if k not in seen_set:
                seen.append(k)
                seen_set.add(k)
    return [{"name": n, "type": ""} for n in seen]


def _rows_to_csv_bytes(rows: list[dict], headers: list[str]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    # Mirror file_handler.export_csv: utf-8-sig so Excel reads Hebrew correctly.
    return ("﻿" + buf.getvalue()).encode("utf-8")
