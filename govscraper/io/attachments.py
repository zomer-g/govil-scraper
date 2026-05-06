"""Attachment downloader — strips API-only headers, dedups filenames, optionally skips existing."""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from typing import Any, Callable, Iterable, List, Optional, Protocol, Sequence

from .sanitize import sanitize_filename

logger = logging.getLogger(__name__)


class _DownloadingSession(Protocol):
    def download_file(self, url: str): ...   # returns object with .content (bytes)


def _resolve_filename(att, fallback_url: str) -> str:
    """Pick a filesystem-safe filename. Falls back to URL basename if the
    attachment's reported filename is missing or sanitises to "unnamed".
    """
    raw = getattr(att, "filename", "") or ""
    name = sanitize_filename(raw)
    if not name or name == "unnamed":
        guess = fallback_url.split("/")[-1].split("?")[0] or "file"
        name = sanitize_filename(guess)
    return name


def _dedup(name: str, used: set[str]) -> str:
    """Append _1, _2, ... before the extension if the lower-cased name has
    already been used in this download batch.
    """
    if name.lower() not in used:
        used.add(name.lower())
        return name
    base, ext = os.path.splitext(name)
    counter = 1
    while True:
        candidate = f"{base}_{counter}{ext}"
        if candidate.lower() not in used:
            used.add(candidate.lower())
            return candidate
        counter += 1


def download_all(
    session: _DownloadingSession,
    output_dir: str,
    attachments: Iterable,
    *,
    progress_callback: Callable | None = None,
    skip_existing: bool = False,
) -> list[Optional[str]]:
    """Download every attachment into `<output_dir>/attachments/`. Returns
    list of local absolute paths in the same order as `attachments`, with
    `None` in the position of any attachment whose download failed.

    Preserving 1:1 alignment with the input is what lets callers join the
    on-disk filename back to its source row (via `FileAttachment.item_index`)
    — needed so the CSV can carry an `attachment_filename` column whose value
    is the actual post-dedup basename inside the ZIP.

    `skip_existing=True` reuses on-disk files that already have non-zero
    size — useful when re-running a long scrape locally without redoing
    the network work. Failures on individual downloads are logged but
    don't abort the batch.
    """
    attachments = list(attachments)
    if not attachments:
        return []

    att_dir = os.path.join(output_dir, "attachments")
    os.makedirs(att_dir, exist_ok=True)

    paths: list[Optional[str]] = []
    used_names: set[str] = set()
    skipped = 0
    failed = 0

    for idx, att in enumerate(attachments):
        result_path: Optional[str] = None
        try:
            url = getattr(att, "url", "") or ""
            filename = _resolve_filename(att, fallback_url=url)
            dest_path = os.path.join(att_dir, filename)

            if (
                skip_existing
                and os.path.exists(dest_path)
                and os.path.getsize(dest_path) > 0
            ):
                result_path = dest_path
                used_names.add(filename.lower())
                skipped += 1
            else:
                resp = session.download_file(url)
                if not resp.content:
                    raise RuntimeError(f"Empty response for {url}")
                final_name = _dedup(filename, used_names)
                final_path = os.path.join(att_dir, final_name)
                with open(final_path, "wb") as f:
                    f.write(resp.content)
                result_path = final_path
                logger.debug("Downloaded: %s (%d bytes)", final_path, len(resp.content))
        except Exception as e:
            logger.warning("Failed to download %s: %s", getattr(att, "url", "?"), e)
            failed += 1

        paths.append(result_path)

        if progress_callback:
            progress_callback(
                current=idx + 1,
                total=len(attachments),
                message=f"הורדו {idx + 1} מתוך {len(attachments)} קבצים",
            )

    if skipped:
        logger.info("Skipped %d existing files", skipped)
    successful = len(attachments) - failed
    logger.info(
        "Downloaded %d / %d attachments (%d skipped, %d failed)",
        successful - skipped, len(attachments), skipped, failed,
    )
    return paths


def inject_attachment_columns(
    items: List[dict],
    file_attachments: Sequence[Any],
    paths: Sequence[Optional[str]],
    column_headers: List[str],
) -> None:
    """Write the post-dedup PDF basename + source URL back into each CSV row.

    `file_attachments[i]` carries `item_index` (the row it belongs to) and
    `paths[i]` is the local file path the downloader wrote (or None on
    failure). The basename of that path is exactly what ends up inside
    the ZIP, so it is the deterministic key for joining a CSV row to its
    PDF — no positional fallback needed on the consumer side.

    Multiple attachments per row are joined with "; ". Rows with no
    attachment get an empty string. The two new columns are appended to
    `column_headers` if not already present, preserving original order.

    Mutates `items` and `column_headers` in place.
    """
    row_atts: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for att, path in zip(file_attachments, paths):
        if path is None:
            continue
        row_atts[getattr(att, "item_index", 0)].append(
            (os.path.basename(path), getattr(att, "url", "") or "")
        )

    for idx, item in enumerate(items):
        atts = row_atts.get(idx, [])
        item["attachment_filename"] = "; ".join(b for b, _ in atts)
        item["attachment_url"] = "; ".join(u for _, u in atts)

    for col in ("attachment_filename", "attachment_url"):
        if col not in column_headers:
            column_headers.append(col)
