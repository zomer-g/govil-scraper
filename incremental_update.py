#!/usr/bin/env python3
"""
CLI wrapper for incremental archiving of a gov.il collector page.
Core logic lives in archive_engine.py.

Usage
-----
Bootstrap (first time):
  python incremental_update.py --url "https://www.gov.il/he/collectors/policies?Type=..." \\
      --archive-dir ./policies_archive --bootstrap --log-file

Daily incremental (scheduled):
  python incremental_update.py --url "https://www.gov.il/he/collectors/policies?Type=..." \\
      --archive-dir ./policies_archive --log-file

Dry-run (check without writing):
  python incremental_update.py --url "..." --archive-dir ./policies_archive --dry-run

Windows Task Scheduler (run once in admin PowerShell to register):
  schtasks /Create /TN "GovIL Policy Archive" \\
    /TR "python \"C:\\...\\incremental_update.py\" --url \"...\" --archive-dir \"...\" --log-file" \\
    /SC DAILY /ST 06:00 /RU %USERNAME% /F
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

from archive_engine import run_bootstrap, run_incremental
from scraper_engine import CloudflareBlockError, GovILScraperError, GovILSession, InvalidURLError

CHECKPOINT_FILENAME = "checkpoint.json"
LOG_FILENAME = "incremental_update.log"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File-based checkpoint helpers (CLI only)
# ---------------------------------------------------------------------------

def load_checkpoint(archive_dir: str):
    path = os.path.join(archive_dir, CHECKPOINT_FILENAME)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["known_urls"] = set(data.get("known_urls") or [])
    return data


def save_checkpoint(archive_dir: str, checkpoint: dict) -> None:
    path = os.path.join(archive_dir, CHECKPOINT_FILENAME)
    tmp = path + ".tmp"
    serialisable = dict(checkpoint)
    serialisable["known_urls"] = sorted(checkpoint.get("known_urls") or [])
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(serialisable, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Incremental daily archiver for a gov.il collector page.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--url", required=True, help="Gov.il collector URL")
    parser.add_argument("--archive-dir", required=True,
                        help="Directory for master CSV, Excel, and checkpoint")
    parser.add_argument("--bootstrap", action="store_true",
                        help="Force full re-scrape and reset checkpoint")
    parser.add_argument("--dry-run", action="store_true",
                        help="Check for new items without writing any files")
    parser.add_argument("--collector-name",
                        help="Override output filename base")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Disable Playwright fallback")
    parser.add_argument("--log-file", action="store_true",
                        help=f"Also write logs to <archive-dir>/{LOG_FILENAME}")
    args = parser.parse_args()

    archive_dir = os.path.abspath(args.archive_dir)
    os.makedirs(archive_dir, exist_ok=True)

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(
            os.path.join(archive_dir, LOG_FILENAME), encoding="utf-8",
        ))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    lock_path = os.path.join(archive_dir, ".incremental.lock")
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(lock_fd)
    except FileExistsError:
        logger.error("Another instance is running (lock: %s). If stale, delete it.", lock_path)
        sys.exit(4)

    session = None
    exit_code = 0
    try:
        session = GovILSession(use_playwright_fallback=not args.no_playwright)
        logger.info("Warming session...")
        session.warm()

        checkpoint = None if args.bootstrap else load_checkpoint(archive_dir)

        if checkpoint is None:
            if not args.bootstrap:
                logger.info("No checkpoint found — running bootstrap.")
            _, checkpoint = run_bootstrap(
                url=args.url,
                archive_dir=archive_dir,
                session=session,
                name_override=args.collector_name or "",
            )
            save_checkpoint(archive_dir, checkpoint)
        else:
            if args.dry_run:
                # Peek at total without writing
                from scraper_engine import GovILScraper
                scraper = GovILScraper(session)
                current_total, _ = scraper.fetch_traditional_page(args.url, skip=0, limit=1)
                delta = max(0, current_total - checkpoint["last_known_total"])
                logger.info("[DRY RUN] %d new item(s) available (total %d → %d).",
                            delta, checkpoint["last_known_total"], current_total)
            else:
                new_count, checkpoint = run_incremental(
                    url=args.url,
                    archive_dir=archive_dir,
                    checkpoint=checkpoint,
                    session=session,
                )
                save_checkpoint(archive_dir, checkpoint)
                if new_count == 0:
                    logger.info("Archive is up to date.")

    except CloudflareBlockError as e:
        logger.error("Cloudflare block: %s", e)
        exit_code = 2
    except InvalidURLError as e:
        logger.error("Invalid URL: %s", e)
        exit_code = 1
    except GovILScraperError as e:
        logger.error("Scraper error: %s", e)
        exit_code = 3
    except KeyboardInterrupt:
        logger.info("Interrupted.")
        exit_code = 130
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        exit_code = 1
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass
        try:
            os.unlink(lock_path)
        except OSError:
            pass

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
