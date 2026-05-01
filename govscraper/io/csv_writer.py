"""CSV writer — utf-8-sig encoding for Hebrew-friendly Excel imports."""
from __future__ import annotations

import csv
import logging
import os
from typing import Iterable, Sequence

from .sanitize import sanitize_filename

logger = logging.getLogger(__name__)


def write(
    output_dir: str,
    name: str,
    rows: Iterable[dict],
    columns: Sequence[str],
) -> str:
    """Write `rows` to `<output_dir>/<sanitized name>.csv`. Returns absolute path.

    Uses utf-8-sig (BOM + UTF-8) so Excel on Windows opens Hebrew correctly.
    `extrasaction="ignore"` drops any keys not in `columns` rather than raising.
    """
    filename = sanitize_filename(name) + ".csv"
    filepath = os.path.join(output_dir, filename)

    rows_list = list(rows)
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row)

    logger.info("CSV exported: %s (%d rows)", filepath, len(rows_list))
    return filepath
