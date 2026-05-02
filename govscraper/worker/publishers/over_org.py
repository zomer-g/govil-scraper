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
  in a "geometry_wkt" column) so the over.org.il datastore can index them.
  CKAN datastore reserves the leading-underscore namespace for internal
  columns (_id, _full_text, …) — using `_geometry_wkt` triggers HTTP 409
  on datastore_create, hence the un-prefixed name.
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
        *,
        extra_zip_resource_ids: list[str] | None = None,
        extra_geojson_resource_ids: list[str] | None = None,
        dataset_title_he: str | None = None,
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

        # Phase D facade still doesn't handle attachment-zip rotation; the
        # `extra_zip_resource_ids` arg is reserved for future use. govmap
        # GeoJSON now travels via `extra_geojson_resource_ids` → the
        # dedicated /upload-geojson endpoint added 2026-05-02.
        zip_resource_ids: list[str] = list(extra_zip_resource_ids or [])
        geojson_resource_ids: list[str] = list(extra_geojson_resource_ids or [])

        push_result = self._client.push_version(
            tracked_dataset_id=task.tracked_dataset_id,
            source_url=task.source_url,
            records=records,
            fields=fields,
            attachments=attachments_meta,
            duration_seconds=duration_s,
            zip_resource_ids=zip_resource_ids or None,
            csv_resource_ids=csv_resource_ids,
            geojson_resource_ids=geojson_resource_ids or None,
            dataset_title_he=dataset_title_he,
        )
        return PublishOutcome(success=True, message=str(push_result.get("message", "ok")), refs=push_result)

    def _publish_geo(
        self,
        task: Task,
        result: GeoFeatureResult,
        duration_s: float,
    ) -> PublishOutcome:
        """Publish a GovMap layer scrape to over.org.il.

        Sends TWO CKAN resources — the tabular form for datastore indexing
        AND the lossless geometry as a directly-renderable GeoJSON file:

        1. CSV (primary)
           - properties + `geometry_wkt` column (un-prefixed name; CKAN
             reserves `_`-prefix for internal columns).
           - records sent inline via push_version's `records` field.

        2. GeoJSON (secondary)
           - WGS84 FeatureCollection.
           - Uploaded via the dedicated POST /api/worker/upload-geojson/<id>
             endpoint (added 2026-05-02 in OVER) — returns a resource_id
             that we pass through `geojson_resource_ids` in push_version.
             over.org.il publishes it as a CKAN resource with format=GeoJSON.
           - Earlier the GeoJSON went via upload-zip → zip_resource_ids;
             that path is replaced because the ZIP wrapper made the file
             non-discoverable as geo-data on the consumer side.

        Also surfaces `dataset_title_he` (the layer caption from GovMap's
        catalog) inside scrape_metadata so OVER can update the dataset
        title from "GovMap layer 200541" to "גבולות ישובים בשטחי איו\\"ש".
        """
        # ---- 1. Build the flattened tabular form for the CSV resource ---
        from ...geo.coords import geom_to_wkt
        rows: list[dict] = []
        for feat in result.features or []:
            row = dict(feat.get("properties") or {})
            geom = feat.get("geometry")
            row["geometry_wkt"] = geom_to_wkt(geom) if geom else ""
            rows.append(row)

        # ---- 2. Upload the GeoJSON directly (skip if no features) ------
        geojson_resource_id: str | None = None
        if result.features:
            try:
                geojson_resource_id = self._upload_geojson(task, result)
            except Exception as e:
                # Never fail the publish over the secondary GeoJSON — the CSV
                # is the primary obligation. Log and continue.
                import logging
                logging.getLogger(__name__).warning(
                    "GeoJSON upload failed for task %s: %s. "
                    "Continuing with CSV-only publish.", task.task_id, e
                )
        else:
            import logging
            logging.getLogger(__name__).info(
                "Empty FeatureCollection for task %s — skipping GeoJSON upload",
                task.task_id,
            )

        # ---- 3. Resolve the layer caption for dataset_title_he --------
        dataset_title_he = self._resolve_dataset_title(result)

        # ---- 4. Push the version with the CSV + GeoJSON resource_id ---
        flat = TabularResult(
            rows=rows,
            fields=_fields_from_rows(rows),
            attachments=result.attachments,
            source_url=result.source_url,
            collector_name=result.layer_label,
            metadata={**(result.metadata or {}), "geometry_type": result.geometry_type},
            warning=result.warning,
        )
        return self._publish_tabular(
            task, flat, duration_s,
            extra_geojson_resource_ids=([geojson_resource_id]
                                        if geojson_resource_id else None),
            dataset_title_he=dataset_title_he,
        )

    def _upload_geojson(
        self,
        task: Task,
        result: GeoFeatureResult,
    ) -> str | None:
        """Build a WGS84 FeatureCollection and upload via /upload-geojson.

        The over.org.il backend (added 2026-05-02) exposes the upload as a
        CKAN resource with format=GeoJSON, name=<resource_name>. We pass
        the layer caption as resource_name so the resource appears
        self-describing (not numeric-id-based).
        """
        import json as _json
        import os
        import tempfile

        from ...io import geojson_writer
        from ...io.sanitize import sanitize_filename

        layer_label = result.layer_label or result.layer_id or "layer"
        with tempfile.TemporaryDirectory(prefix="ovr_geo_") as tmp:
            geojson_path = geojson_writer.write_feature_collection(
                output_dir=tmp,
                name=layer_label,
                features=result.features or [],
                layer_id=result.layer_id,
                bbox_itm=result.bbox_itm,
                bbox_wgs84=result.bbox_wgs84,
                geometry_type=result.geometry_type,
            )
            with open(geojson_path, "rb") as f:
                geojson_bytes = f.read()

            base = sanitize_filename(layer_label)
            return self._client.upload_geojson(
                tracked_dataset_id=task.tracked_dataset_id,
                version_number=1,
                geojson_bytes=geojson_bytes,
                resource_name=layer_label,
                filename=f"{base}.geojson",
            )

    def _resolve_dataset_title(
        self,
        result: GeoFeatureResult,
    ) -> str | None:
        """Return the GovMap catalog caption for the layer, or None.

        Tries in order: result.layer_label (already populated by
        resolve_layer when the URL matches a known layer), then a fresh
        catalog lookup by layer_id. Returns None when neither is usable —
        OVER then leaves the existing tracked_dataset.title untouched.
        """
        # Already resolved by resolve_layer (layers.json hit OR online catalog)
        candidate = (result.layer_label or "").strip()
        if candidate and candidate.lower() != (result.layer_id or "").lower():
            # Skip placeholder labels like "LAYER_200541" that == layer_id
            return candidate

        # Fallback: try the online catalog
        try:
            from ...scrapers.govmap import catalog_fetch
            entry = catalog_fetch.lookup_layer(result.layer_id)
            if entry:
                cap = (entry.get("caption") or "").strip()
                return cap or None
        except Exception:
            pass
        return None

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
