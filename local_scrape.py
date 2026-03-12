#!/usr/bin/env python3
"""
Local Gov.il Scraper CLI — scrape data and files to a local folder,
optionally upload to a remote server.

Usage:
    python local_scrape.py --url <GOV_IL_URL> [--dest ./scraped_data] [--no-files]
    python local_scrape.py --url <URL> --upload --server https://govil-scraper.onrender.com --token <ADMIN_TOKEN>

Features:
    - Saves to dest/{collector_name}/ subfolder
    - Skips files that already exist (won't re-download)
    - Exports CSV + Excel
    - Optional --upload to push scraped data to remote server
"""

import argparse
import logging
import os
import sys
import zipfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("local_scrape")


def create_upload_zip(collector_dir: str) -> str:
    """Create a ZIP from a scraped collector directory for upload."""
    zip_path = collector_dir.rstrip("/\\") + "_upload.zip"
    folder_name = os.path.basename(collector_dir)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(collector_dir):
            for fname in files:
                full = os.path.join(root, fname)
                arc_name = os.path.relpath(full, os.path.dirname(collector_dir))
                zf.write(full, arc_name)

    size_mb = os.path.getsize(zip_path) / 1e6
    logger.info("Upload ZIP created: %s (%.1f MB)", zip_path, size_mb)
    return zip_path


def upload_to_server(zip_path: str, source_url: str, server: str,
                     collector_name: str = "", cookie: str = ""):
    """Upload a ZIP file to the remote server's upload endpoint."""
    import requests

    upload_url = server.rstrip("/") + "/api/collections/upload"
    logger.info("Uploading to %s ...", upload_url)

    with open(zip_path, "rb") as f:
        files = {"file": (os.path.basename(zip_path), f, "application/zip")}
        data = {"source_url": source_url}
        if collector_name:
            data["collector_name"] = collector_name

        headers = {}
        if cookie:
            headers["Cookie"] = cookie

        resp = requests.post(upload_url, files=files, data=data,
                             headers=headers, timeout=120)

    if resp.status_code == 201:
        result = resp.json()
        logger.info("Upload successful!")
        logger.info("  Collection ID: %s", result.get("id"))
        logger.info("  Records: %s", result.get("record_count"))
        logger.info("  Files: %s", result.get("file_count"))
        return result
    elif resp.status_code == 403:
        logger.error("Upload denied — admin authentication required.")
        logger.error("Use --cookie with your session cookie, or log in via browser first.")
        sys.exit(1)
    else:
        logger.error("Upload failed: %s %s", resp.status_code, resp.text[:500])
        sys.exit(1)


def main():
    from scraper_engine import (
        GovILSession, GovILScraper, GovILScraperError,
        InvalidURLError, CloudflareBlockError,
    )
    from file_handler import FileHandler

    parser = argparse.ArgumentParser(
        description="Scrape gov.il collector pages to local files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape locally only:
  python local_scrape.py --url https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict

  # Scrape and upload to server:
  python local_scrape.py --url https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict \\
      --upload --server https://govil-scraper.onrender.com --cookie "session=abc123"

  # Upload an existing folder (no scraping):
  python local_scrape.py --upload-only ./scraped_data/ministers_conflict \\
      --source-url https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict \\
      --server https://govil-scraper.onrender.com --cookie "session=abc123"
        """,
    )
    parser.add_argument(
        "--url",
        help="Gov.il collector page URL (required unless --upload-only)",
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

    # Upload flags
    parser.add_argument(
        "--upload", action="store_true",
        help="Upload scraped data to the remote server after scraping",
    )
    parser.add_argument(
        "--upload-only",
        help="Upload an existing local folder (skip scraping). Provide folder path.",
    )
    parser.add_argument(
        "--source-url",
        help="Source URL for --upload-only (required with --upload-only)",
    )
    parser.add_argument(
        "--server", default="https://govil-scraper.onrender.com",
        help="Server URL for upload (default: https://govil-scraper.onrender.com)",
    )
    parser.add_argument(
        "--cookie",
        help="Session cookie for admin auth (e.g. 'session=abc123')",
    )

    args = parser.parse_args()

    # --- Upload-only mode (no scraping) ---
    if args.upload_only:
        folder = args.upload_only
        if not os.path.isdir(folder):
            logger.error("Folder not found: %s", folder)
            sys.exit(1)
        source_url = args.source_url
        if not source_url:
            logger.error("--source-url is required with --upload-only")
            sys.exit(1)

        zip_path = create_upload_zip(folder)
        upload_to_server(
            zip_path, source_url, args.server,
            collector_name=os.path.basename(folder),
            cookie=args.cookie or "",
        )
        os.remove(zip_path)
        logger.info("Temp ZIP removed")
        return

    # --- Normal scrape mode ---
    if not args.url:
        parser.error("--url is required (unless using --upload-only)")

    url = args.url.strip()
    if "gov.il" not in url.lower():
        logger.error("URL must be from gov.il")
        sys.exit(1)

    session = None
    collector_dir = None
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

        # Upload if requested
        if args.upload:
            zip_path = create_upload_zip(collector_dir)
            upload_to_server(
                zip_path, url, args.server,
                collector_name=result.collector_name,
                cookie=args.cookie or "",
            )
            os.remove(zip_path)
            logger.info("Temp upload ZIP removed")

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
