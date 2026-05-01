"""FileHandler — thin facade over the io.* writer modules.

After phase D-cleanup the export logic was extracted into one module per
output kind:
    govscraper.io.csv_writer       — utf-8-sig CSV
    govscraper.io.excel_writer     — RTL Hebrew Excel
    govscraper.io.geojson_writer   — WGS84 GeoJSON + manifest.json
    govscraper.io.zip_packager     — bundled ZIP
    govscraper.io.attachments      — multi-file downloader

This class keeps the original `FileHandler(session, output_dir)` API for
back-compat with worker.py, over_worker.py, app.py, and local_scrape.py
— everything that already says `FileHandler(...).export_csv(result)` keeps
working unchanged. New code is encouraged to call the modules directly.
"""
from __future__ import annotations

import logging
import os
import tempfile
from typing import Callable, List, Optional

from govscraper.scrapers.govil.legacy_engine import GovILSession, ScrapeResult, FileAttachment

from . import attachments as _attachments
from . import csv_writer as _csv_writer
from . import excel_writer as _excel_writer
from . import geojson_writer as _geojson_writer
from . import zip_packager as _zip_packager
from .sanitize import sanitize_filename  # re-export

logger = logging.getLogger(__name__)


class FileHandler:
    """Thin facade. Each method delegates to the matching io.* module."""

    def __init__(self, session: GovILSession, output_dir: Optional[str] = None):
        self.session = session
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="govil_")
        os.makedirs(self.output_dir, exist_ok=True)

    def export_csv(self, result: ScrapeResult) -> str:
        return _csv_writer.write(
            self.output_dir,
            result.collector_name,
            result.items,
            result.column_headers,
        )

    def export_excel(self, result: ScrapeResult) -> str:
        return _excel_writer.write(
            self.output_dir,
            result.collector_name,
            result.items,
            result.column_headers,
        )

    def download_attachments(
        self,
        attachments: List[FileAttachment],
        progress_callback: Optional[Callable] = None,
        skip_existing: bool = False,
    ) -> List[str]:
        return _attachments.download_all(
            self.session,
            self.output_dir,
            attachments,
            progress_callback=progress_callback,
            skip_existing=skip_existing,
        )

    def create_zip(
        self,
        csv_path: str,
        excel_path: str,
        attachment_paths: List[str],
    ) -> str:
        return _zip_packager.package(
            self.output_dir, csv_path, excel_path, attachment_paths,
        )

    def export_geojson(self, result: ScrapeResult) -> str:
        return _geojson_writer.write_feature_collection(
            self.output_dir,
            result.collector_name,
            result.features or [],
            layer_id=getattr(result, "layer_id", ""),
            bbox_itm=result.bbox_itm if result.bbox_itm else None,
            bbox_wgs84=result.bbox_wgs84 if result.bbox_wgs84 else None,
            geometry_type=getattr(result, "geometry_type", ""),
        )

    def write_manifest(self, result: ScrapeResult, files: dict) -> str:
        page_type = result.page_type
        page_type_str = page_type.value if hasattr(page_type, "value") else str(page_type or "")
        return _geojson_writer.write_manifest(
            self.output_dir,
            files,
            layer_id=getattr(result, "layer_id", ""),
            collector_name=result.collector_name,
            page_type=page_type_str,
            bbox_itm=result.bbox_itm if result.bbox_itm else None,
            bbox_wgs84=result.bbox_wgs84 if result.bbox_wgs84 else None,
            geometry_type=getattr(result, "geometry_type", ""),
            feature_count=result.total_count,
            srs=getattr(result, "srs", ""),
            warning=result.warning or "",
        )

    def get_all_file_records(self, base_dir: str) -> List[dict]:
        """Scan output_dir, return file metadata for the database files table."""
        records = []
        for root, _dirs, files in os.walk(self.output_dir):
            for fname in files:
                full = os.path.join(root, fname)
                rel = os.path.relpath(full, base_dir).replace("\\", "/")
                ext = os.path.splitext(fname)[1].lower().lstrip(".")
                if ext == "csv":
                    category = "csv"
                elif ext == "xlsx":
                    category = "excel"
                elif ext == "geojson":
                    category = "geojson"
                elif ext == "json" and fname == "manifest.json":
                    category = "manifest"
                else:
                    category = "attachment"
                records.append({
                    "filename": fname,
                    "file_type": ext,
                    "category": category,
                    "size_bytes": os.path.getsize(full),
                    "rel_path": rel,
                })
        return records
