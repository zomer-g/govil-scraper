"""
Gov.il Scraper — Flask Application
Routes, background job management, SSE progress streaming.
"""

import io
import os
import json
import uuid
import time
import logging
import threading
import zipfile
from queue import Queue, Empty
from datetime import datetime
from enum import Enum

from flask import Flask, request, jsonify, Response, send_file, render_template
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix

from scraper_engine import (
    GovILSession, GovILScraper, GovILScraperError,
    InvalidURLError, CloudflareBlockError,
)
from file_handler import FileHandler
from storage import CollectionStore
from auth import auth_bp, init_oauth, admin_required, get_current_user, is_admin

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
DISABLE_PLAYWRIGHT = os.environ.get("DISABLE_PLAYWRIGHT", "0") == "1"

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32).hex()
store = CollectionStore(TEMP_DIR)

# --- Auth (Google OAuth2 SSO) ---
init_oauth(app)
app.register_blueprint(auth_bp)

# --- Rate limiting ---
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
    storage_uri="memory://",
)


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
    REGISTERING_FILES = "registering_files"
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
        session = GovILSession(use_playwright_fallback=not DISABLE_PLAYWRIGHT)
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

        # Register individual files in the database (no ZIP creation)
        _update_progress(job_id, Phase.REGISTERING_FILES, 0, 1, "רושם קבצים...")
        file_records = handler.get_all_file_records(TEMP_DIR)
        if file_records:
            store.save_files_bulk(job_id, file_records)
        _update_progress(job_id, Phase.REGISTERING_FILES, 1, 1, "הקבצים נרשמו!")

        # Store result for download/preview
        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["result_paths"] = {
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

        # Persist to SQLite for the collections API
        try:
            store.save_collection(
                collection_id=job_id,
                source_url=url,
                collector_name=result.collector_name,
                page_type=result.page_type.value if result.page_type else "",
                record_count=result.total_count,
                attachment_count=len(result.file_attachments),
                downloaded_count=len(attachment_paths),
                column_headers=result.column_headers,
                zip_path="",
                csv_path=os.path.relpath(csv_path, TEMP_DIR),
                excel_path=os.path.relpath(excel_path, TEMP_DIR),
                warning=result.warning or "",
            )
            store.cleanup_oldest(max_bytes=800_000_000)
        except Exception as e:
            logger.warning("Failed to persist collection: %s", e)

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
    user = get_current_user()
    return render_template("index.html", user=user, is_admin=is_admin())


@app.route("/api/scrape", methods=["POST"])
@limiter.limit("5 per minute")
@admin_required
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
    """Download the result as an on-the-fly ZIP file."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "משימה לא נמצאה"}), 404

    # Build ZIP on the fly from job result paths
    result_paths = job.get("result_paths", {})
    csv_path = result_paths.get("csv")
    excel_path = result_paths.get("excel")

    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"error": "קובץ ההורדה לא נמצא"}), 404

    collector = job.get("result_data", {}).get("collector_name", "data")
    date_str = datetime.now().strftime("%Y%m%d")

    buf = io.BytesIO()
    folder_name = collector
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, f"{folder_name}/{os.path.basename(csv_path)}")
        if excel_path and os.path.exists(excel_path):
            zf.write(excel_path, f"{folder_name}/{os.path.basename(excel_path)}")
        # Include attachments if they exist
        att_dir = os.path.dirname(csv_path)
        att_subdir = os.path.join(att_dir, "attachments")
        if os.path.isdir(att_subdir):
            for fname in os.listdir(att_subdir):
                fpath = os.path.join(att_subdir, fname)
                if os.path.isfile(fpath):
                    zf.write(fpath, f"{folder_name}/attachments/{fname}")
    buf.seek(0)

    return send_file(
        buf,
        as_attachment=True,
        download_name=f"{collector}_{date_str}.zip",
        mimetype="application/zip",
    )


# ---------------------------------------------------------------------------
# Collections API — persistent access to completed scrapes
# ---------------------------------------------------------------------------

@app.route("/api/collections")
@limiter.limit("60 per minute")
def list_collections():
    """List all persisted collections with metadata."""
    collections = store.list_collections()
    # Strip internal paths from response
    for c in collections:
        c.pop("zip_path", None)
        c.pop("csv_path", None)
        c.pop("excel_path", None)
    return jsonify({"collections": collections})


@app.route("/api/collections/<cid>")
def get_collection(cid):
    """Get details of a specific collection, including preview rows."""
    coll = store.get_collection(cid)
    if not coll:
        return jsonify({"error": "אוסף לא נמצא"}), 404

    # Try to load preview rows from the CSV
    preview_rows = []
    csv_rel = coll.get("csv_path", "")
    if csv_rel:
        csv_abs = os.path.join(TEMP_DIR, csv_rel)
        if os.path.exists(csv_abs):
            import csv
            try:
                with open(csv_abs, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i >= 50:
                            break
                        preview_rows.append(dict(row))
            except Exception:
                pass

    # Include file count from files table
    files = store.list_files(cid)

    resp = {
        "id": coll["id"],
        "source_url": coll["source_url"],
        "collector_name": coll["collector_name"],
        "page_type": coll["page_type"],
        "scrape_date": coll["scrape_date"],
        "record_count": coll["record_count"],
        "attachment_count": coll["attachment_count"],
        "downloaded_count": coll["downloaded_count"],
        "column_headers": coll["column_headers"],
        "warning": coll["warning"],
        "size_bytes": coll["size_bytes"],
        "file_count": len(files),
        "rows": preview_rows,
    }
    return jsonify(resp)


@app.route("/api/collections/<cid>/download")
def download_collection_zip(cid):
    """Download a ZIP file for a collection — generated on the fly."""
    coll = store.get_collection(cid)
    if not coll:
        return jsonify({"error": "אוסף לא נמצא"}), 404

    # Try new per-file approach first
    files = store.list_files(cid)
    if files:
        buf = io.BytesIO()
        folder = coll["collector_name"]
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in files:
                abs_path = os.path.join(TEMP_DIR, f["rel_path"])
                if os.path.exists(abs_path):
                    if f["category"] == "attachment":
                        arc_name = f"{folder}/attachments/{f['filename']}"
                    else:
                        arc_name = f"{folder}/{f['filename']}"
                    zf.write(abs_path, arc_name)
        buf.seek(0)
        date_str = coll["scrape_date"][:10].replace("-", "")
        name = f"{coll['collector_name']}_{date_str}.zip"
        return send_file(buf, as_attachment=True, download_name=name,
                         mimetype="application/zip")

    # Legacy fallback: use old zip_path if no files in new table
    zip_rel = coll.get("zip_path", "")
    zip_abs = os.path.join(TEMP_DIR, zip_rel) if zip_rel else ""
    if not zip_abs or not os.path.exists(zip_abs):
        return jsonify({"error": "קובץ ZIP לא נמצא"}), 404

    date_str = coll["scrape_date"][:10].replace("-", "")
    name = f"{coll['collector_name']}_{date_str}.zip"
    return send_file(zip_abs, as_attachment=True, download_name=name,
                     mimetype="application/zip")


@app.route("/api/collections/<cid>/csv")
def download_collection_csv(cid):
    """Download just the CSV file for a collection."""
    coll = store.get_collection(cid)
    if not coll:
        return jsonify({"error": "אוסף לא נמצא"}), 404

    csv_rel = coll.get("csv_path", "")
    csv_abs = os.path.join(TEMP_DIR, csv_rel) if csv_rel else ""
    if not csv_abs or not os.path.exists(csv_abs):
        return jsonify({"error": "קובץ CSV לא נמצא"}), 404

    name = f"{coll['collector_name']}.csv"
    return send_file(csv_abs, as_attachment=True, download_name=name,
                     mimetype="text/csv; charset=utf-8")


@app.route("/api/collections/<cid>/excel")
def download_collection_excel(cid):
    """Download just the Excel file for a collection."""
    coll = store.get_collection(cid)
    if not coll:
        return jsonify({"error": "אוסף לא נמצא"}), 404

    excel_rel = coll.get("excel_path", "")
    excel_abs = os.path.join(TEMP_DIR, excel_rel) if excel_rel else ""
    if not excel_abs or not os.path.exists(excel_abs):
        return jsonify({"error": "קובץ Excel לא נמצא"}), 404

    name = f"{coll['collector_name']}.xlsx"
    return send_file(excel_abs, as_attachment=True, download_name=name,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ---------------------------------------------------------------------------
# Individual Files API
# ---------------------------------------------------------------------------

@app.route("/api/collections/<cid>/files")
def list_collection_files(cid):
    """List all individual files in a collection."""
    coll = store.get_collection(cid)
    if not coll:
        return jsonify({"error": "אוסף לא נמצא"}), 404

    files = store.list_files(cid)
    # Add download URL for each file
    for f in files:
        f["download_url"] = f"/api/collections/{cid}/files/{f['filename']}"
    return jsonify({"collection_id": cid, "files": files})


@app.route("/api/collections/<cid>/files/<filename>")
def download_collection_file(cid, filename):
    """Download a single file from a collection."""
    file_rec = store.get_file(cid, filename)
    if not file_rec:
        return jsonify({"error": "קובץ לא נמצא"}), 404

    abs_path = os.path.join(TEMP_DIR, file_rec["rel_path"])
    if not os.path.exists(abs_path):
        return jsonify({"error": "קובץ לא נמצא על הדיסק"}), 404

    return send_file(abs_path, as_attachment=True, download_name=filename)


@app.route("/api/files")
@limiter.limit("60 per minute")
def search_files():
    """Search files by name across all collections."""
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify({"error": "נדרשת מילת חיפוש (לפחות 2 תווים)"}), 400

    results = store.search_files(q)
    # Add download URL for each file
    for f in results:
        f["download_url"] = f"/api/collections/{f['collection_id']}/files/{f['filename']}"
    return jsonify({"query": q, "count": len(results), "files": results})


@app.route("/api/collections/<cid>", methods=["DELETE"])
@admin_required
def delete_collection(cid):
    """Delete a collection and its files."""
    if store.delete_collection(cid):
        return "", 204
    return jsonify({"error": "אוסף לא נמצא"}), 404


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
