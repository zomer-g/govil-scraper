#!/usr/bin/env python3
"""
Local Gov.il Scraper CLI — scrape data and files to a local folder.

Usage:
    python local_scrape.py --url <GOV_IL_URL> [--dest ./scraped_data] [--no-files]

Features:
    - Saves to dest/{collector_name}/ subfolder
    - Skips files that already exist (won't re-download)
    - Exports CSV + Excel
    - No server, no database — standalone operation
"""

import argparse
import logging
import os
import sys

from scraper_engine import (
    GovILSession, GovILScraper, GovILScraperError,
    InvalidURLError, CloudflareBlockError,
)
from file_handler import FileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("local_scrape")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape gov.il collector pages to local files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python local_scrape.py --url https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict
  python local_scrape.py --url https://www.gov.il/he/collectors/publications --dest ./data --no-files
        """,
    )
    parser.add_argument(
        "--url", required=True,
        help="Gov.il collector page URL",
    )
    parser.add_argument(
        "--dest", default="./scraped_data",
        help="Base destination folder (default: ./scraped_data)",
    )
    parser.add_argument(
        "--no-files", action="store_true",
        help="Skip downloading attached files (only export CSV/Excel)",
    )
    parser.add_argument(
        "--no-skip", action="store_true",
        help="Re-download files even if they already exist",
    )
    args = parser.parse_args()

    url = args.url.strip()
    if "gov.il" not in url.lower():
        logger.error("URL must be from gov.il")
        sys.exit(1)

    session = None
    try:
        # Warm session
        logger.info("Connecting to gov.il...")
        session = GovILSession(use_playwright_fallback=False)
        session.warm()
        logger.info("Connection established")

        # Scrape
        def progress_cb(**kwargs):
            current = kwargs.get("current", 0)
            total = kwargs.get("total", 0)
            msg = kwargs.get("message", "")
            if total > 0:
                logger.info("  [%d/%d] %s", current, total, msg)

        scraper = GovILScraper(session, progress_callback=progress_cb)
        logger.info("Scraping: %s", url)
        result = scraper.scrape(url)
        logger.info("Found %d records, %d attachments",
                     result.total_count, len(result.file_attachments))

        if result.warning:
            logger.warning("Warning: %s", result.warning)

        # Create output directory: dest/{collector_name}/
        collector_dir = os.path.join(
            args.dest,
            result.collector_name or "unknown",
        )
        os.makedirs(collector_dir, exist_ok=True)
        logger.info("Output directory: %s", os.path.abspath(collector_dir))

        handler = FileHandler(session, output_dir=collector_dir)

        # Export CSV + Excel
        csv_path = handler.export_csv(result)
        logger.info("CSV: %s", csv_path)

        excel_path = handler.export_excel(result)
        logger.info("Excel: %s", excel_path)

        # Download attachments
        if not args.no_files and result.file_attachments:
            skip_existing = not args.no_skip
            logger.info("Downloading %d attachments (skip_existing=%s)...",
                         len(result.file_attachments), skip_existing)

            paths = handler.download_attachments(
                result.file_attachments,
                progress_callback=progress_cb,
                skip_existing=skip_existing,
            )
            logger.info("Downloaded %d files to %s/attachments/",
                         len(paths), collector_dir)
        elif args.no_files:
            logger.info("Skipping file downloads (--no-files)")
        else:
            logger.info("No attachments found")

        # Summary
        logger.info("=" * 50)
        logger.info("Done! %d records scraped", result.total_count)
        logger.info("Output: %s", os.path.abspath(collector_dir))
        logger.info("=" * 50)

    except InvalidURLError as e:
        logger.error("Invalid URL: %s", e)
        sys.exit(1)
    except CloudflareBlockError as e:
        logger.error("Blocked by Cloudflare: %s", e)
        sys.exit(1)
    except GovILScraperError as e:
        logger.error("Scrape error: %s", e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(1)
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
