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

# Max ZIP size before splitting — stays comfortably under Cloudflare's 100MB
# edge limit that fronts Render (exceeding it returns 502 Bad Gateway).
MAX_ZIP_SIZE = 80 * 1024 * 1024


def split_attachments_into_zips(
    attachment_paths: list,
    csv_path: str,
    output_dir: str,
    base_name: str,
) -> list[str]:
    """Pack the CSV + all attachment files into one or more ZIPs, each under
    MAX_ZIP_SIZE. First ZIP contains the CSV. Returns ZIP paths in order.

    Size is tracked using uncompressed bytes of inputs as a proxy (deflate
    usually shrinks further, so the actual ZIP is almost always smaller —
    this is intentionally conservative so we never exceed the limit).
    """
    import zipfile

    def _safe_name(base: str, idx: int) -> str:
        return os.path.join(output_dir, f"{base}-part-{idx}.zip")

    zips: list[str] = []
    part_idx = 1
    zip_path = _safe_name(base_name, part_idx)
    current = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)
    zips.append(zip_path)
    current_bytes = 0

    # CSV goes into the first part
    current.write(csv_path, f"{base_name}/{os.path.basename(csv_path)}")
    current_bytes += os.path.getsize(csv_path)

    for path in attachment_paths:
        size = os.path.getsize(path)
        if current_bytes + size > MAX_ZIP_SIZE and current_bytes > 0:
            current.close()
            part_idx += 1
            zip_path = _safe_name(base_name, part_idx)
            current = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)
            zips.append(zip_path)
            current_bytes = 0
        current.write(path, f"{base_name}/attachments/{os.path.basename(path)}")
        current_bytes += size

    current.close()
    return zips


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
        """Report task failure to the server. Retry up to 3 times since it's
        critical that the task gets marked failed (otherwise stays stuck forever)."""
        for attempt in range(1, 4):
            try:
                resp = self._session.post(
                    self._url(f"/api/worker/fail/{task_id}"),
                    json={"error": error, "phase": phase},
                    timeout=15,
                )
                if resp.status_code == 200:
                    logger.info("Reported failure for task %s (attempt %d)", task_id, attempt)
                    return
                logger.warning("fail endpoint returned %d (attempt %d): %s",
                               resp.status_code, attempt, resp.text[:200])
            except Exception as e:
                logger.warning("Failure report failed (attempt %d): %s", attempt, e)
            if attempt < 3:
                time.sleep(5)
        logger.error("All 3 attempts to report failure failed for task %s", task_id)

    def upload_zip(self, tracked_dataset_id: str, version_number: int,
                   zip_path: str, attachment_count: int,
                   part: int | None = None, total_parts: int | None = None) -> str | None:
        """Upload ZIP via multipart to /api/worker/upload-zip. Returns resource_id.
        If part/total_parts are given, the resource is named as a multi-part ZIP."""
        import os
        try:
            with open(zip_path, "rb") as f:
                files = {"file": (os.path.basename(zip_path), f, "application/zip")}
                data = {
                    "version_number": str(version_number),
                    "attachment_count": str(attachment_count),
                }
                if part is not None and total_parts is not None:
                    data["part"] = str(part)
                    data["total_parts"] = str(total_parts)
                # Use a fresh session without Content-Type: application/json
                import requests
                resp = requests.post(
                    self._url(f"/api/worker/upload-zip/{tracked_dataset_id}"),
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    files=files,
                    data=data,
                    timeout=600,  # 10 min for large uploads
                )
            if resp.status_code == 200:
                result = resp.json()
                logger.info("ZIP uploaded → resource_id=%s, size=%d KB",
                            result.get("resource_id"), result.get("size", 0) // 1024)
                return result.get("resource_id")
            logger.error("ZIP upload failed: %d %s", resp.status_code, resp.text[:300])
        except Exception as e:
            logger.exception("ZIP upload error: %s", e)
        return None

    def push_version(self, tracked_dataset_id: str, source_url: str,
                     records: list, fields: list, attachments: list,
                     duration_seconds: float,
                     zip_resource_id: str | None = None,
                     zip_resource_ids: list[str] | None = None) -> dict:
        """Push scraped data as a new version to over.org.il.
        zip_resource_id: single ZIP (legacy). zip_resource_ids: list of parts (preferred)."""
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
        if zip_resource_ids:
            payload["zip_resource_ids"] = zip_resource_ids
        elif zip_resource_id:
            payload["zip_resource_id"] = zip_resource_id

        import json as _json
        payload_size = len(_json.dumps(payload, ensure_ascii=False))
        zip_info = (f"{len(zip_resource_ids)} parts" if zip_resource_ids
                    else (zip_resource_id or "none"))
        logger.info("Pushing version: %d records, %d fields, %d attachments, ZIP=%s, payload ~%d KB",
                     len(records), len(fields), len(attachments),
                     zip_info,
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

            # Download files, create ZIPs (split into ≤80MB parts to fit under
            # Cloudflare's 100MB edge limit), and upload each part via multipart.
            zip_resource_ids: list[str] = []
            tmp_dir = None
            if result.file_attachments:
                import tempfile
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
                        # Pack into ≤80MB ZIPs (first contains the CSV too)
                        zip_paths = split_attachments_into_zips(
                            att_paths, csv_path, tmp_dir,
                            base_name=result.collector_name or "attachments",
                        )
                        total_parts = len(zip_paths)
                        logger.info("Created %d ZIP part(s) for %d attachments",
                                    total_parts, len(att_paths))

                        # Upload each part via multipart
                        for i, zp in enumerate(zip_paths, 1):
                            zp_mb = os.path.getsize(zp) // 1024 // 1024
                            self.report_progress(
                                task_id, "uploading", i - 1, total_parts,
                                f"מעלה ZIP {i}/{total_parts} ({zp_mb}MB)",
                            )
                            rid = self.upload_zip(
                                tracked_dataset_id=tracked_dataset_id,
                                version_number=1,
                                zip_path=zp,
                                attachment_count=len(att_paths),
                                part=i if total_parts > 1 else None,
                                total_parts=total_parts if total_parts > 1 else None,
                            )
                            if rid:
                                zip_resource_ids.append(rid)
                            else:
                                logger.warning("Part %d/%d failed to upload — continuing",
                                               i, total_parts)

                except Exception as e:
                    logger.warning("Failed to create/upload ZIPs (continuing without): %s", e)
                finally:
                    if tmp_dir:
                        import shutil
                        shutil.rmtree(tmp_dir, ignore_errors=True)

            self.report_progress(task_id, "exporting", 1, 1, "שולח נתונים לשרת...")

            duration = time.time() - start_time
            push_result = self.push_version(
                tracked_dataset_id=tracked_dataset_id,
                source_url=source_url,
                records=records,
                fields=fields,
                attachments=attachments,
                duration_seconds=duration,
                zip_resource_ids=zip_resource_ids or None,
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
