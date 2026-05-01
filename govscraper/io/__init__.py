"""IO writers (CSV / Excel / GeoJSON / ZIP / attachments) and filename helpers.

Phase B exposes only `sanitize_filename`. The full split of file_handler.py
into csv_writer / excel_writer / geojson_writer / zip_packager / attachments
lands in phase C, after scraper_engine.py is decoupled.
"""
from .sanitize import sanitize_filename

__all__ = ["sanitize_filename"]
