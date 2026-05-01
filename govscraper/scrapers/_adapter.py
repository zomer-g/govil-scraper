"""Convert legacy scraper_engine.ScrapeResult into the new BaseScraper result union.

Phase C wraps the existing engines (scraper_engine.GovILScraper,
nadlan_incremental_engine, govmap_engine) behind BaseScraper without
rewriting them. This adapter is the seam where the legacy dataclass
(scraper_engine.ScrapeResult — one shape with all fields optional) becomes
TabularResult or GeoFeatureResult depending on page_type.
"""
from __future__ import annotations

from typing import Any

from ..types import FileAttachment as NewFileAttachment, ParsedURL as NewParsedURL
from .base import GeoFeatureResult, ScrapeResult, TabularResult


def legacy_attachments_to_new(attachments: list[Any]) -> list[NewFileAttachment]:
    out: list[NewFileAttachment] = []
    for a in attachments or []:
        url = getattr(a, "url", "") or ""
        filename = getattr(a, "filename", "") or url.rsplit("/", 1)[-1]
        out.append(NewFileAttachment(
            local_path=None,  # type: ignore[arg-type]   filled at download time
            original_filename=filename,
            source_url=url,
        ))
    return out


def legacy_result_to_new(legacy_result: Any, source_url: str) -> ScrapeResult:
    """Convert a scraper_engine.ScrapeResult into the new union variant.

    Inspects `page_type` to decide between Tabular and Geo. Content pages,
    dynamic/traditional collectors, and nadlan parcels all become Tabular.
    GovMap layers become Geo.
    """
    page_type = getattr(legacy_result, "page_type", None)
    page_type_name = getattr(page_type, "name", str(page_type or "")).upper()

    if "GOVMAP" in page_type_name:
        return GeoFeatureResult(
            features=[
                {
                    "type": "Feature",
                    "geometry": getattr(f, "geometry", None),
                    "properties": getattr(f, "properties", {}),
                }
                for f in (legacy_result.features or [])
            ],
            itm_features=[
                {
                    "type": "Feature",
                    "geometry": getattr(f, "geometry_itm", None),
                    "properties": getattr(f, "properties", {}),
                }
                for f in (legacy_result.features or [])
                if getattr(f, "geometry_itm", None)
            ] or None,
            crs_wgs84=True,
            layer_id=getattr(legacy_result, "layer_id", "") or "",
            layer_label=legacy_result.collector_name or "",
            bbox_wgs84=tuple(legacy_result.bbox_wgs84) if legacy_result.bbox_wgs84 else None,  # type: ignore[arg-type]
            bbox_itm=tuple(legacy_result.bbox_itm) if legacy_result.bbox_itm else None,  # type: ignore[arg-type]
            geometry_type=getattr(legacy_result, "geometry_type", "") or "",
            feature_count=legacy_result.total_count or 0,
            attachments=legacy_attachments_to_new(legacy_result.file_attachments),
            source_url=source_url,
            metadata={
                "srs": getattr(legacy_result, "srs", "") or "",
                "page_type": page_type_name,
            },
            warning=legacy_result.warning,
        )

    return TabularResult(
        rows=list(legacy_result.items or []),
        fields=[{"name": h, "type": ""} for h in (legacy_result.column_headers or [])],
        attachments=legacy_attachments_to_new(legacy_result.file_attachments),
        source_url=source_url,
        collector_name=legacy_result.collector_name or "",
        metadata={"page_type": page_type_name},
        warning=legacy_result.warning,
    )


def legacy_parsed_to_new(legacy_parsed: Any, scraper_id: str) -> NewParsedURL:
    """Wrap a scraper_engine.ParsedURL (or govmap_engine.ParsedURL) for the registry."""
    return NewParsedURL(
        scraper_id=scraper_id,
        canonical_url=getattr(legacy_parsed, "original_url", "") or "",
        params={
            "office_id": getattr(legacy_parsed, "office_id", None),
            "collector_name": getattr(legacy_parsed, "collector_name", "") or "",
            "page_type": getattr(getattr(legacy_parsed, "page_type", None), "value", ""),
            "query_params": dict(getattr(legacy_parsed, "query_params", {}) or {}),
            "_legacy": legacy_parsed,
        },
    )
