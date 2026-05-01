"""ZIP packager — bundles CSV + Excel + attachment files into one archive."""
from __future__ import annotations

import logging
import os
import zipfile
from typing import Iterable

from .sanitize import sanitize_filename

logger = logging.getLogger(__name__)


def package(
    output_dir: str,
    csv_path: str,
    excel_path: str,
    attachment_paths: Iterable[str] = (),
) -> str:
    """Create a deflated ZIP at `<output_dir>/<base>.zip` containing the CSV,
    Excel, and any attachments under an `attachments/` subfolder. Returns
    the absolute ZIP path.

    The folder name inside the ZIP mirrors the CSV filename (without `.csv`).
    """
    base = os.path.splitext(os.path.basename(csv_path))[0]
    zip_name = sanitize_filename(base) + ".zip"
    zip_path = os.path.join(output_dir, zip_name)

    folder = base
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, f"{folder}/{os.path.basename(csv_path)}")
        zf.write(excel_path, f"{folder}/{os.path.basename(excel_path)}")
        for path in attachment_paths:
            zf.write(path, f"{folder}/attachments/{os.path.basename(path)}")

    logger.info("ZIP created: %s", zip_path)
    return zip_path
