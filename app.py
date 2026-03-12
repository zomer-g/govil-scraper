"""
Gov.il Scraper — Flask Application
Routes, background job management, SSE progress streaming.
"""

import os
import json
import uuid
import time
import logging
import threading
from queue import Queue, Empty
from datetime import datetime
from enum import Enum

from flask import Flask, request, jsonify, Response, send_file, render_template

from scraper_engine import (
    GovILSession, GovILScraper, GovILScraperError,
    InvalidURLError, CloudflareBlockError,
)
from file_handler import FileHandler

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

import tempfile
TEMP_DIR = os.environ.get("TEMP_DIR", os.path.join(tempfile.gettempdir(), "govil_scraper"))
os.makedirs(TEMP_DIR, exist_ok=True)

MAX_CONCURRENT_JOBS = 2

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

class Phase(str, Enum):
    INITIALIZING = "initializing"
    WARMING_SESSION = "warming_session"
    DETECTING_TYPE = "detecting_type"
    SCRAPING = "scraping"
    DOWNLOADING_FILES = "downloading_files"
    EXPORTING = "exporting"
    ZIPPING = "zipping"
    COMPLETE = "complete"
    ERROR = "error"


# job_id -> { "queue": Queue, "status": dict, "result_paths": dict, "created": float }
jobs: dict = {}
jobs_lock = threading.Lock()


def _update_progress(job_id: str, phase: Phase, current: int = 0,
                     total: int = 0, message: str = ""):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
    pct = (current / total * 100) if total > 0 else 0
    status = {
        "phase": phase.value,
        "current": current,
        "total": total,
        "percentage": round(pct, 1),
        "message": message,
    }
    job["status"] = status
    try:
        job["queue"].put_nowait(status)
    except Exception:
        pass


def _set_error(job_id: str, message: str, error_type: str = ""):
    """Store error details on the job and send error progress."""
    import traceback
    tb = traceback.format_exc()
    with jobs_lock:
        job = jobs.get(job_id)
        if job:
            job["error_log"] = (
                f"Error Type: {error_type}\n"
                f"Message: {message}\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Job ID: {job_id}\n\n"
                f"Traceback:\n{tb}"
            )
    _update_progress(job_id, Phase.ERROR, 0, 0, message)


def _cleanup_old_jobs():
    """Remove jobs older than 1 hour."""
    cutoff = time.time() - 3600
    with jobs_lock:
        expired = [jid for jid, j in jobs.items() if j["created"] < cutoff]
        for jid in expired:
            jobs.pop(jid, None)
            logger.info("Cleaned up expired job: %s", jid)


# ---------------------------------------------------------------------------
# Background scrape execution
# ---------------------------------------------------------------------------

def _run_scrape_job(job_id: str, url: str, download_files: bool):
    """Execute the full scrape pipeline in a background thread."""
    session = None
    try:
        _update_progress(job_id, Phase.INITIALIZING, 0, 0, "מאתחל...")

        # Create session and warm it
        _update_progress(job_id, Phase.WARMING_SESSION, 0, 1, "מתחבר לאתר gov.il...")
        session = GovILSession(use_playwright_fallback=True)
        session.warm()
        _update_progress(job_id, Phase.WARMING_SESSION, 1, 1, "החיבור הצליח!")

        # Detect page type
        _update_progress(job_id, Phase.DETECTING_TYPE, 0, 1, "מזהה סוג דף...")

        # Create scraper with progress callback
        def scrape_progress(**kwargs):
            _update_progress(job_id, Phase.SCRAPING, **kwargs)

        scraper = GovILScraper(session, progress_callback=scrape_progress)
        result = scraper.scrape(url)

        # Set up output directory for this job
        job_dir = os.path.join(TEMP_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)
        handler = FileHandler(session, output_dir=job_dir)

        # Export CSV + Excel
        _update_progress(job_id, Phase.EXPORTING, 0, 2, "מייצא CSV...")
        csv_path = handler.export_csv(result)
        _update_progress(job_id, Phase.EXPORTING, 1, 2, "מייצא Excel...")
        excel_path = handler.export_excel(result)
        _update_progress(job_id, Phase.EXPORTING, 2, 2, "הייצוא הושלם!")

        # Download attachments
        attachment_paths = []
        if download_files and result.file_attachments:
            def dl_progress(**kwargs):
                _update_progress(job_id, Phase.DOWNLOADING_FILES, **kwargs)

            attachment_paths = handler.download_attachments(
                result.file_attachments, progress_callback=dl_progress
            )

        # Create ZIP
        _update_progress(job_id, Phase.ZIPPING, 0, 1, "אורז קבצים...")
        zip_path = handler.create_zip(csv_path, excel_path, attachment_paths)

        # Store result for download/preview
        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["result_paths"] = {
                    "zip": zip_path,
                    "csv": csv_path,
                    "excel": excel_path,
                }
                job["result_data"] = {
                    "columns": result.column_headers,
                    "rows": result.items[:50],
                    "total": result.total_count,
                    "attachments_count": len(result.file_attachments),
                    "downloaded_count": len(attachment_paths),
                    "collector_name": result.collector_name,
                    "warning": result.warning,
                }

        _update_progress(
            job_id, Phase.COMPLETE, 1, 1,
            f"הושלם! נאספו {result.total_count} רשומות"
        )

    except InvalidURLError as e:
        _set_error(job_id, str(e), "InvalidURLError")
    except CloudflareBlockError as e:
        _set_error(job_id, str(e), "CloudflareBlockError")
    except GovILScraperError as e:
        _set_error(job_id, f"שגיאת סריקה: {e}", type(e).__name__)
    except Exception as e:
        logger.exception("Unexpected error in job %s", job_id)
        _set_error(job_id, f"שגיאה לא צפויה: {e}", type(e).__name__)
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    """Start a scraping job. Body: {"url": "...", "download_files": true}"""
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()

    if not url:
        return jsonify({"error": "חסרה כתובת URL"}), 400
    if "gov.il" not in url.lower():
        return jsonify({"error": "הכתובת חייבת להיות מאתר gov.il"}), 400

    # Check concurrent job limit
    _cleanup_old_jobs()
    active = sum(
        1 for j in jobs.values()
        if j["status"].get("phase") not in (Phase.COMPLETE.value, Phase.ERROR.value, None)
    )
    if active >= MAX_CONCURRENT_JOBS:
        return jsonify({"error": "יותר מדי משימות פעילות. נסה שוב בעוד דקה."}), 429

    download_files = data.get("download_files", True)
    job_id = uuid.uuid4().hex[:12]

    with jobs_lock:
        jobs[job_id] = {
            "queue": Queue(maxsize=200),
            "status": {"phase": Phase.INITIALIZING.value},
            "result_paths": {},
            "result_data": None,
            "error_log": None,
            "created": time.time(),
        }

    thread = threading.Thread(
        target=_run_scrape_job,
        args=(job_id, url, download_files),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id}), 202


@app.route("/api/progress/<job_id>")
def stream_progress(job_id):
    """SSE endpoint for real-time progress updates."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "משימה לא נמצאה"}), 404

    def generate():
        q = job["queue"]
        while True:
            try:
                status = q.get(timeout=30)
                yield f"data: {json.dumps(status, ensure_ascii=False)}\n\n"
                if status.get("phase") in (Phase.COMPLETE.value, Phase.ERROR.value):
                    break
            except Empty:
                # Send keepalive
                yield ": keepalive\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/status/<job_id>")
def get_status(job_id):
    """Polling fallback for progress."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "משימה לא נמצאה"}), 404
    resp = {**job["status"]}
    if job.get("error_log"):
        resp["error_log"] = job["error_log"]
    return jsonify(resp)


@app.route("/api/preview/<job_id>")
def preview_data(job_id):
    """Return first rows of scraped data for table preview."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "משימה לא נמצאה"}), 404
    if not job.get("result_data"):
        return jsonify({"error": "הנתונים עדיין לא מוכנים"}), 425
    return jsonify(job["result_data"])


@app.route("/api/download/<job_id>")
def download_result(job_id):
    """Download the result ZIP file."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "משימה לא נמצאה"}), 404

    zip_path = job.get("result_paths", {}).get("zip")
    if not zip_path or not os.path.exists(zip_path):
        return jsonify({"error": "קובץ ההורדה לא נמצא"}), 404

    collector = job.get("result_data", {}).get("collector_name", "data")
    date_str = datetime.now().strftime("%Y%m%d")
    download_name = f"{collector}_{date_str}.zip"

    return send_file(
        zip_path,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/zip",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
