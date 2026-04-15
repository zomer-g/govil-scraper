#!/usr/bin/env python3
"""
גרסאות לעם — Worker Client for over.org.il

Polls over.org.il for scrape tasks, executes them locally using the
govil-scraper engine, and pushes results back via the push-version API.

Usage:
    python over_worker.py --key <API_KEY>
    python over_worker.py  # uses OVER_API_KEY env var
"""

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("over_worker")

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("cloudscraper").setLevel(logging.WARNING)

SERVER = "https://www.over.org.il"


class OverWorkerClient:
    """Worker that polls over.org.il and pushes scraped versions back."""

    def __init__(self, api_key: str, poll_interval: int = 30):
        self.api_key = api_key
        self.poll_interval = poll_interval
        self._running = True

        import requests
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })

    def _url(self, path: str) -> str:
        return f"{SERVER}{path}"

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def poll(self) -> dict | None:
        """Poll for the next pending scrape task. Returns task dict or None."""
        try:
            resp = self._session.get(self._url("/api/worker/poll"), timeout=15)
            if resp.status_code == 204:
                return None
            if resp.status_code == 200:
                return resp.json()
            logger.warning("Poll returned %d: %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.warning("Poll failed: %s", e)
        return None

    def report_progress(self, task_id: str, phase: str, current: int = 0,
                        total: int = 0, message: str = ""):
        """Send progress update to the server."""
        pct = int(current / total * 100) if total > 0 else 0
        try:
            self._session.post(
                self._url(f"/api/worker/progress/{task_id}"),
                json={
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "percentage": pct,
                    "message": message,
                },
                timeout=10,
            )
        except Exception as e:
            logger.debug("Progress report failed: %s", e)

    def report_failure(self, task_id: str, error: str, phase: str = "scraping"):
        """Report task failure to the server."""
        try:
            self._session.post(
                self._url(f"/api/worker/fail/{task_id}"),
                json={"error": error, "phase": phase},
                timeout=10,
            )
        except Exception as e:
            logger.warning("Failure report failed: %s", e)

    def push_version(self, tracked_dataset_id: str, source_url: str,
                     records: list, fields: list, attachments: list,
                     duration_seconds: float,
                     zip_file: dict | None = None) -> dict:
        """Push scraped data as a new version to over.org.il."""
        payload = {
            "tracked_dataset_id": tracked_dataset_id,
            "metadata_modified": datetime.now().isoformat(),
            "resources": [
                {
                    "name": "נתוני הסורק",
                    "format": "CSV",
                    "records": records,
                    "fields": fields,
                    "row_count": len(records),
                }
            ],
            "attachments": attachments,
            "scrape_metadata": {
                "source_url": source_url,
                "scrape_duration_seconds": round(duration_seconds, 1),
                "total_items": len(records),
                "total_files": len(attachments),
                "scraper_version": "1.0.0",
            },
        }
        if zip_file:
            payload["zip_file"] = zip_file

        import json as _json
        payload_size = len(_json.dumps(payload, ensure_ascii=False))
        logger.info("Pushing version: %d records, %d fields, %d attachments, %s, payload ~%d KB",
                     len(records), len(fields), len(attachments),
                     f"ZIP {zip_file['size'] // 1024}KB" if zip_file else "no ZIP",
                     payload_size // 1024)
        resp = self._session.post(
            self._url("/api/worker/push-version"),
            json=payload,
            timeout=300,
        )
        if resp.status_code == 200:
            result = resp.json()
            logger.info("Push-version response: %s", result.get("message", "ok"))
            return result
        raise RuntimeError(f"push-version failed: {resp.status_code} {resp.text[:300]}")

    # ------------------------------------------------------------------
    # Task execution
    # ------------------------------------------------------------------

    def execute_task(self, task: dict):
        """Execute a single scrape task and push results to over.org.il."""
        from scraper_engine import (
            GovILSession, GovILScraper, GovILScraperError,
            InvalidURLError, CloudflareBlockError,
        )

        task_id = task["task_id"]
        tracked_dataset_id = task["tracked_dataset_id"]
        source_url = task["source_url"]
        config = task.get("scraper_config", {})
        download_files = config.get("download_files", False)

        logger.info("=" * 50)
        logger.info("Task %s: %s", task_id, source_url)
        logger.info("=" * 50)

        session = None
        start_time = time.time()
        last_report = [0.0]

        def _progress(**kwargs):
            now = time.time()
            if now - last_report[0] >= 5:
                phase = kwargs.pop("phase", "scraping")
                self.report_progress(task_id, phase, **kwargs)
                last_report[0] = now

        try:
            self.report_progress(task_id, "initializing", 0, 1, "מתחבר לאתר gov.il...")
            session = GovILSession(use_playwright_fallback=False)
            session.warm()

            self.report_progress(task_id, "scraping", 0, 1, "מזהה סוג דף...")

            def progress_cb(**kwargs):
                _progress(**kwargs)

            scraper = GovILScraper(session, progress_callback=progress_cb)
            result = scraper.scrape(source_url)

            logger.info("Scraped %d records, %d attachments",
                        result.total_count, len(result.file_attachments))

            # Convert ScrapeResult → push-version format
            records = result.items  # already flat dicts
            fields = [
                {"id": col, "type": "text"}
                for col in (result.column_headers or [])
            ]
            # If no column_headers, derive from first record
            if not fields and records:
                fields = [{"id": k, "type": "text"} for k in records[0].keys()]

            attachments = [
                {
                    "name": f.filename,
                    "url": f.url,
                }
                for f in result.file_attachments
            ]

            # Download files and create ZIP if there are attachments
            zip_data = None
            if result.file_attachments:
                import tempfile
                import base64
                from file_handler import FileHandler

                self.report_progress(task_id, "downloading", 0,
                                     len(result.file_attachments),
                                     "מוריד קבצים מצורפים...")
                try:
                    tmp_dir = tempfile.mkdtemp(prefix="govil_worker_")
                    handler = FileHandler(session, output_dir=tmp_dir)

                    # Export CSV for the ZIP
                    csv_path = handler.export_csv(result)

                    # Download attachments
                    def dl_progress(**kw):
                        self.report_progress(task_id, "downloading",
                                             kw.get("current", 0),
                                             kw.get("total", 0),
                                             kw.get("message", ""))

                    att_paths = handler.download_attachments(
                        result.file_attachments,
                        progress_callback=dl_progress,
                    )

                    if att_paths:
                        # Create ZIP with CSV + attachments (skip excel to save size)
                        zip_path = handler.create_zip(csv_path, csv_path, att_paths)
                        zip_size = os.path.getsize(zip_path)
                        logger.info("ZIP created: %s (%d KB)", zip_path, zip_size // 1024)

                        with open(zip_path, "rb") as f:
                            zip_bytes = f.read()
                        zip_data = {
                            "filename": os.path.basename(zip_path),
                            "content_base64": base64.b64encode(zip_bytes).decode(),
                            "size": zip_size,
                        }

                    # Cleanup temp dir
                    import shutil
                    shutil.rmtree(tmp_dir, ignore_errors=True)

                except Exception as e:
                    logger.warning("Failed to create ZIP (continuing without): %s", e)

            self.report_progress(task_id, "exporting", 1, 1, "שולח נתונים לשרת...")

            duration = time.time() - start_time
            push_result = self.push_version(
                tracked_dataset_id=tracked_dataset_id,
                source_url=source_url,
                records=records,
                fields=fields,
                attachments=attachments,
                duration_seconds=duration,
                zip_file=zip_data,
            )
            logger.info("Task %s completed: %s", task_id, push_result.get("message"))

        except (InvalidURLError, CloudflareBlockError, GovILScraperError) as e:
            logger.error("Scrape error in task %s: %s", task_id, e)
            self.report_failure(task_id, str(e), phase="scraping")
        except RuntimeError as e:
            logger.error("Push error in task %s: %s", task_id, e)
            self.report_failure(task_id, str(e), phase="exporting")
        except Exception as e:
            logger.exception("Unexpected error in task %s", task_id)
            self.report_failure(task_id, f"{type(e).__name__}: {e}")
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def stop(self):
        self._running = False
        logger.info("Shutdown requested...")

    def run(self):
        logger.info("Over.org.il worker starting")
        logger.info("Server: %s", SERVER)
        logger.info("Poll interval: %ds", self.poll_interval)

        while self._running:
            task = self.poll()
            if task:
                self.execute_task(task)
            else:
                time.sleep(self.poll_interval)

        logger.info("Worker stopped")


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    # Load .env if present
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(
        description="גרסאות לעם — govil-scraper worker for over.org.il",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python over_worker.py --key my-api-key
  python over_worker.py  # uses OVER_API_KEY env var
        """,
    )
    parser.add_argument(
        "--key",
        default=os.environ.get("OVER_API_KEY", ""),
        help="API key for over.org.il (default: OVER_API_KEY env var)",
    )
    parser.add_argument(
        "--poll-interval", type=int, default=30,
        help="Seconds between polls (default: 30)",
    )

    args = parser.parse_args()

    if not args.key:
        logger.error("API key required. Use --key or set OVER_API_KEY env var.")
        sys.exit(1)

    worker = OverWorkerClient(api_key=args.key, poll_interval=args.poll_interval)

    def handle_signal(sig, frame):
        worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()


if __name__ == "__main__":
    main()
