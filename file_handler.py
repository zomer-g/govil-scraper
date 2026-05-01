"""Re-export shim — canonical implementation lives in
govscraper.io.file_handler.

Kept for back-compat with `from file_handler import FileHandler, sanitize_filename`.
The split into csv_writer / excel_writer / geojson_writer / zip_packager /
attachments still lives at the canonical location and remains a future
cleanup; for now we ship a single file_handler.py module with the original
FileHandler class.
"""
from govscraper.io.file_handler import *  # noqa: F401,F403
from govscraper.io.file_handler import FileHandler  # noqa: F401
from govscraper.io.sanitize import sanitize_filename  # noqa: F401
