"""Attachment downloader — strips API-only headers, dedups filenames, optionally skips existing."""
from __future__ import annotations

import logging
import os
from typing import Callable, Iterable, Protocol

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
) -> list[str]:
    """Download every attachment into `<output_dir>/attachments/`. Returns
    list of local absolute paths in the same order as `attachments`.

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

    paths: list[str] = []
    used_names: set[str] = set()
    skipped = 0

    for idx, att in enumerate(attachments):
        try:
            url = getattr(att, "url", "") or ""
            filename = _resolve_filename(att, fallback_url=url)
            dest_path = os.path.join(att_dir, filename)

            if (
                skip_existing
                and os.path.exists(dest_path)
                and os.path.getsize(dest_path) > 0
            ):
                paths.append(dest_path)
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
                paths.append(final_path)
                logger.debug("Downloaded: %s (%d bytes)", final_path, len(resp.content))
        except Exception as e:
            logger.warning("Failed to download %s: %s", getattr(att, "url", "?"), e)

        if progress_callback:
            progress_callback(
                current=idx + 1,
                total=len(attachments),
                message=f"הורדו {idx + 1} מתוך {len(attachments)} קבצים",
            )

    if skipped:
        logger.info("Skipped %d existing files", skipped)
    logger.info(
        "Downloaded %d / %d attachments (%d skipped)",
        len(paths) - skipped, len(attachments), skipped,
    )
    return paths
