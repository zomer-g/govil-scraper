#!/usr/bin/env python3
"""
Gov.il Scraper — Remote Worker Client

Polls a Render server for scrape tasks, executes them locally,
and uploads results back.

Usage:
    python worker.py --server https://govil-scraper.onrender.com --key <API_KEY>
    python worker.py  # uses RENDER_SERVER_URL and WORKER_API_KEY env vars
"""

import argparse
import logging
import os
import platform
import signal
import sys
import time
import zipfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

# Reduce noise from dependencies
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("cloudscraper").setLevel(logging.WARNING)


class WorkerClient:
    """Remote worker that polls the server for tasks and executes them."""

    def __init__(self, server_url: str, api_key: str, worker_id: str,
                 dest_dir: str = "./worker_data", poll_interval: int = 10):
        self.server = server_url.rstrip("/")
        self.api_key = api_key
        self.worker_id = worker_id
        self.dest_dir = dest_dir
        self.poll_interval = poll_interval
        self._running = True
        self._current_task = None

        import requests
        self._session = requests.Session()
        self._session.headers.update({
            "X-Worker-Key": self.api_key,
            "X-Worker-ID": self.worker_id,
        })

    def _url(self, path: str) -> str:
        return f"{self.server}{path}"

    def heartbeat(self):
        """Send heartbeat to the server."""
        try:
            self._session.post(
                self._url("/api/worker/heartbeat"),
                json={"info": f"{platform.node()} / {platform.system()}"},
                timeout=10,
            )
        except Exception as e:
            logger.warning("Heartbeat failed: %s", e)

    def poll(self) -> dict | None:
        """Poll for a pending task. Returns task dict or None."""
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
        pct = (current / total * 100) if total > 0 else 0
        try:
            self._session.post(
                self._url(f"/api/worker/progress/{task_id}"),
                json={
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "percentage": round(pct, 1),
                    "message": message,
                },
                timeout=10,
            )
        except Exception as e:
            logger.debug("Progress report failed: %s", e)

    def report_failure(self, task_id: str, error: str):
        """Report task failure to the server."""
        try:
            self._session.post(
                self._url(f"/api/worker/fail/{task_id}"),
                json={"error": error},
                timeout=10,
            )
        except Exception as e:
            logger.warning("Failure report failed: %s", e)

    def upload_result(self, task_id: str, zip_path: str, source_url: str,
                      collector_name: str = ""):
        """Upload result ZIP to the server."""
        logger.info("Uploading results to server...")
        with open(zip_path, "rb") as f:
            files = {"file": (os.path.basename(zip_path), f, "application/zip")}
            data = {
                "source_url": source_url,
                "task_id": task_id,
            }
            if collector_name:
                data["collector_name"] = collector_name

            resp = self._session.post(
                self._url("/api/collections/upload"),
                files=files,
                data=data,
                timeout=120,
            )

        if resp.status_code == 201:
            result = resp.json()
            logger.info("Upload successful: %s (%d records, %d files)",
                         result.get("collector_name"), result.get("record_count", 0),
                         result.get("file_count", 0))
            return result
        else:
            raise RuntimeError(f"Upload failed: {resp.status_code} {resp.text[:300]}")

    def execute_task(self, task: dict):
        """Execute a single scrape task."""
        from scraper_engine import (
            GovILSession, GovILScraper, GovILScraperError,
            InvalidURLError, CloudflareBlockError,
        )
        from file_handler import FileHandler

        task_id = task["id"]
        url = task["url"]
        download_files = task.get("download_files", True)
        self._current_task = task_id

        logger.info("=" * 50)
        logger.info("Executing task %s: %s", task_id, url)
        logger.info("=" * 50)

        session = None
        try:
            # Warm session
            self.report_progress(task_id, "warming_session", 0, 1, "מתחבר לאתר gov.il...")
            session = GovILSession(use_playwright_fallback=False)
            session.warm()
            self.report_progress(task_id, "warming_session", 1, 1, "החיבור הצליח!")

            # Scrape
            self.report_progress(task_id, "detecting_type", 0, 1, "מזהה סוג דף...")

            last_report = [0.0]  # mutable for closure

            def progress_cb(**kwargs):
                now = time.time()
                if now - last_report[0] >= 3:  # Report at most every 3 seconds
                    self.report_progress(task_id, "scraping", **kwargs)
                    last_report[0] = now

            scraper = GovILScraper(session, progress_callback=progress_cb)
            result = scraper.scrape(url)
            logger.info("Scraped %d records, %d attachments",
                         result.total_count, len(result.file_attachments))

            # Create output directory
            collector_dir = os.path.join(self.dest_dir, task_id)
            os.makedirs(collector_dir, exist_ok=True)
            handler = FileHandler(session, output_dir=collector_dir)

            # Export CSV + Excel
            self.report_progress(task_id, "exporting", 0, 2, "מייצא CSV...")
            csv_path = handler.export_csv(result)
            self.report_progress(task_id, "exporting", 1, 2, "מייצא Excel...")
            excel_path = handler.export_excel(result)
            self.report_progress(task_id, "exporting", 2, 2, "הייצוא הושלם!")

            # Download attachments
            if download_files and result.file_attachments:
                def dl_progress(**kwargs):
                    now = time.time()
                    if now - last_report[0] >= 3:
                        self.report_progress(task_id, "downloading_files", **kwargs)
                        last_report[0] = now

                handler.download_attachments(
                    result.file_attachments,
                    progress_callback=dl_progress,
                    skip_existing=True,
                )

            # Create ZIP for upload
            self.report_progress(task_id, "registering_files", 0, 1, "מכין להעלאה...")
            zip_path = self._create_zip(collector_dir)

            # Upload to server
            self.upload_result(
                task_id, zip_path, url,
                collector_name=result.collector_name,
            )

            # Cleanup local files
            import shutil
            os.remove(zip_path)
            shutil.rmtree(collector_dir, ignore_errors=True)

            logger.info("Task %s completed successfully", task_id)

        except (InvalidURLError, CloudflareBlockError, GovILScraperError) as e:
            logger.error("Scrape error in task %s: %s", task_id, e)
            self.report_failure(task_id, str(e))
        except Exception as e:
            logger.exception("Unexpected error in task %s", task_id)
            self.report_failure(task_id, f"{type(e).__name__}: {e}")
        finally:
            self._current_task = None
            if session:
                try:
                    session.close()
                except Exception:
                    pass

    def _create_zip(self, directory: str) -> str:
        """Create a ZIP from a directory for upload."""
        zip_path = directory.rstrip("/\\") + ".zip"
        folder_name = os.path.basename(directory)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(directory):
                for fname in files:
                    full = os.path.join(root, fname)
                    arc_name = os.path.relpath(full, os.path.dirname(directory))
                    zf.write(full, arc_name)

        size_mb = os.path.getsize(zip_path) / 1e6
        logger.info("ZIP created: %s (%.1f MB)", zip_path, size_mb)
        return zip_path

    def stop(self):
        """Signal the worker to stop after current task."""
        self._running = False
        logger.info("Shutdown requested...")

    def run(self):
        """Main loop: heartbeat, poll, execute, repeat."""
        logger.info("Worker starting: %s", self.worker_id)
        logger.info("Server: %s", self.server)
        logger.info("Poll interval: %ds", self.poll_interval)
        logger.info("Working directory: %s", os.path.abspath(self.dest_dir))
        os.makedirs(self.dest_dir, exist_ok=True)

        # Initial heartbeat
        self.heartbeat()

        heartbeat_interval = 30
        last_heartbeat = time.time()

        while self._running:
            # Periodic heartbeat
            if time.time() - last_heartbeat >= heartbeat_interval:
                self.heartbeat()
                last_heartbeat = time.time()

            # Poll for tasks
            task = self.poll()
            if task:
                self.execute_task(task)
                self.heartbeat()
                last_heartbeat = time.time()
            else:
                # No task available, sleep
                time.sleep(self.poll_interval)

        logger.info("Worker stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Gov.il Scraper remote worker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python worker.py --server https://govil-scraper.onrender.com --key my-secret-key
  python worker.py  # uses RENDER_SERVER_URL and WORKER_API_KEY env vars
        """,
    )
    parser.add_argument(
        "--server",
        default=os.environ.get("RENDER_SERVER_URL", "http://localhost:5000"),
        help="Server URL (default: RENDER_SERVER_URL env or http://localhost:5000)",
    )
    parser.add_argument(
        "--key",
        default=os.environ.get("WORKER_API_KEY", ""),
        help="Worker API key (default: WORKER_API_KEY env var)",
    )
    parser.add_argument(
        "--worker-id",
        default=platform.node(),
        help="Worker identifier (default: hostname)",
    )
    parser.add_argument(
        "--poll-interval", type=int, default=10,
        help="Seconds between polls (default: 10)",
    )
    parser.add_argument(
        "--dest", default="./worker_data",
        help="Local working directory (default: ./worker_data)",
    )

    args = parser.parse_args()

    if not args.key:
        logger.error("Worker API key required. Use --key or set WORKER_API_KEY env var.")
        sys.exit(1)

    worker = WorkerClient(
        server_url=args.server,
        api_key=args.key,
        worker_id=args.worker_id,
        dest_dir=args.dest,
        poll_interval=args.poll_interval,
    )

    # Handle shutdown signals
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
