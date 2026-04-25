"""
Gov.il Scraper — Flask Application
Routes, background job management, SSE progress streaming.
"""

# Load .env file for local development
from dotenv import load_dotenv
load_dotenv()

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
from auth import (
    auth_bp, init_oauth, admin_required, admin_or_worker,
    worker_auth_required, get_current_user, is_admin,
)
from archive_engine import run_bootstrap as _archive_bootstrap, run_incremental as _archive_incremental

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
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB max upload
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

# Worker heartbeat tracking (in-memory, ephemeral)
workers: dict = {}  # worker_id -> {"last_seen": float, "info": str}
workers_lock = threading.Lock()
WORKER_ONLINE_TIMEOUT = 60  # seconds


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
    """SSE endpoint for real-time progress updates.

    Works for both server-mode jobs (in-memory Queue) and worker-mode
    tasks (polls the tasks table).
    """
    with jobs_lock:
        job = jobs.get(job_id)

    if job:
        # Server-mode: stream from in-memory Queue
        def generate_server():
            q = job["queue"]
            while True:
                try:
                    status = q.get(timeout=30)
                    yield f"data: {json.dumps(status, ensure_ascii=False)}\n\n"
                    if status.get("phase") in (Phase.COMPLETE.value, Phase.ERROR.value):
                        break
                except Empty:
                    yield ": keepalive\n\n"

        return Response(
            generate_server(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Worker-mode: poll the tasks table
    task = store.get_task(job_id)
    if not task:
        return jsonify({"error": "משימה לא נמצאה"}), 404

    def generate_worker():
        last_progress = ""
        while True:
            t = store.get_task(job_id)
            if not t:
                break
            progress = t.get("progress", {})
            status = t.get("status", "pending")

            # Map task status to phase
            if status == "completed":
                progress["phase"] = "complete"
            elif status == "failed":
                progress["phase"] = "error"
                progress["message"] = t.get("error", "שגיאה")
            elif status == "pending":
                progress["phase"] = "pending"
                progress["message"] = "ממתין לעובד מרוחק..."

            progress_json = json.dumps(progress, ensure_ascii=False)
            if progress_json != last_progress:
                yield f"data: {progress_json}\n\n"
                last_progress = progress_json
                if status in ("completed", "failed"):
                    break
            else:
                yield ": keepalive\n\n"

            time.sleep(3)

    return Response(
        generate_worker(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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


# ---------------------------------------------------------------------------
# Upload API — upload locally-scraped data to the server
# ---------------------------------------------------------------------------

@app.route("/api/collections/upload", methods=["POST"])
@limiter.limit("10 per minute")
@admin_or_worker
def upload_collection():
    """Upload a locally-scraped ZIP. Files are stored individually.

    Accepts multipart form with:
      - file: ZIP file (required)
      - source_url: original gov.il URL (required)
      - collector_name: display name (optional, derived from CSV filename)
      - task_id: optional task ID to link upload to a worker task
    """
    import csv as csv_mod
    import shutil

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "חסר קובץ ZIP"}), 400
    if not f.filename.lower().endswith(".zip"):
        return jsonify({"error": "הקובץ חייב להיות ZIP"}), 400

    source_url = (request.form.get("source_url") or "").strip()
    if not source_url:
        return jsonify({"error": "חסרה כתובת מקור (source_url)"}), 400

    collector_name = (request.form.get("collector_name") or "").strip()

    # Save ZIP to temp location for validation
    collection_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(TEMP_DIR, collection_id)
    os.makedirs(job_dir, exist_ok=True)

    zip_tmp = os.path.join(job_dir, "_upload.zip")
    f.save(zip_tmp)

    # Validate it's a real ZIP
    if not zipfile.is_zipfile(zip_tmp):
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"error": "הקובץ אינו ZIP תקין"}), 400

    try:
        # Extract contents
        with zipfile.ZipFile(zip_tmp, "r") as zf:
            zf.extractall(job_dir)
        # Remove the upload ZIP itself — we store files individually
        os.remove(zip_tmp)

        # Scan extracted files and register them
        csv_path = ""
        excel_path = ""
        record_count = 0
        column_headers = []
        file_records = []

        for root, _dirs, filenames in os.walk(job_dir):
            for fname in filenames:
                full = os.path.join(root, fname)
                rel = os.path.relpath(full, TEMP_DIR).replace("\\", "/")
                ext = os.path.splitext(fname)[1].lower().lstrip(".")
                size = os.path.getsize(full)

                if ext == "csv":
                    category = "csv"
                    if not csv_path:
                        csv_path = full
                elif ext == "xlsx":
                    category = "excel"
                    if not excel_path:
                        excel_path = full
                else:
                    category = "attachment"

                file_records.append({
                    "filename": fname,
                    "file_type": ext,
                    "category": category,
                    "size_bytes": size,
                    "rel_path": rel,
                })

        # Read CSV to get row count and column headers
        if csv_path and os.path.exists(csv_path):
            try:
                with open(csv_path, encoding="utf-8-sig") as cf:
                    reader = csv_mod.DictReader(cf)
                    column_headers = list(reader.fieldnames or [])
                    rows = list(reader)
                    record_count = len(rows)
            except Exception:
                pass

        # Derive collector_name
        if not collector_name:
            if csv_path:
                collector_name = os.path.splitext(os.path.basename(csv_path))[0]
            else:
                collector_name = os.path.splitext(f.filename)[0]

        attachment_count = sum(1 for r in file_records if r["category"] == "attachment")

        # Register files in DB
        if file_records:
            store.save_files_bulk(collection_id, file_records)

        # Save collection metadata
        store.save_collection(
            collection_id=collection_id,
            source_url=source_url,
            collector_name=collector_name,
            page_type="upload",
            record_count=record_count,
            attachment_count=attachment_count,
            downloaded_count=attachment_count,
            column_headers=column_headers,
            zip_path="",
            csv_path=os.path.relpath(csv_path, TEMP_DIR).replace("\\", "/") if csv_path else "",
            excel_path=os.path.relpath(excel_path, TEMP_DIR).replace("\\", "/") if excel_path else "",
            warning="",
        )
        store.cleanup_oldest(max_bytes=800_000_000)

        logger.info("Uploaded collection %s (%s, %d records, %d files)",
                     collection_id, collector_name, record_count, len(file_records))

        # Link to worker task if task_id provided
        task_id = (request.form.get("task_id") or "").strip()
        if task_id:
            store.update_task_status(task_id, "completed", result={
                "collection_id": collection_id,
                "collector_name": collector_name,
                "record_count": record_count,
                "file_count": len(file_records),
            })

        return jsonify({
            "id": collection_id,
            "collector_name": collector_name,
            "record_count": record_count,
            "file_count": len(file_records),
            "attachment_count": attachment_count,
        }), 201

    except Exception as e:
        shutil.rmtree(job_dir, ignore_errors=True)
        logger.exception("Upload failed for %s", f.filename)
        return jsonify({"error": f"שגיאה בעיבוד הקובץ: {e}"}), 500


@app.route("/api/collections/<cid>", methods=["DELETE"])
@admin_required
def delete_collection(cid):
    """Delete a collection and its files."""
    if store.delete_collection(cid):
        return "", 204
    return jsonify({"error": "אוסף לא נמצא"}), 404


# ---------------------------------------------------------------------------
# Task Management API
# ---------------------------------------------------------------------------

def _is_worker_online() -> bool:
    """Check if any worker has sent a heartbeat recently."""
    cutoff = time.time() - WORKER_ONLINE_TIMEOUT
    with workers_lock:
        return any(w["last_seen"] > cutoff for w in workers.values())


@app.route("/api/tasks", methods=["POST"])
@limiter.limit("5 per minute")
@admin_required
def create_task():
    """Create a new scrape task.

    Body: {"url": "...", "download_files": true, "mode": "server|worker|auto"}
    """
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()

    if not url:
        return jsonify({"error": "חסרה כתובת URL"}), 400
    if "gov.il" not in url.lower():
        return jsonify({"error": "הכתובת חייבת להיות מאתר gov.il"}), 400

    download_files = data.get("download_files", True)
    mode = data.get("mode", "auto")

    # Auto mode: prefer worker if online, else server
    if mode == "auto":
        mode = "worker" if _is_worker_online() else "server"

    task_id = uuid.uuid4().hex[:12]
    store.create_task(task_id, url, download_files, mode)

    if mode == "server":
        # Run immediately on server (same as /api/scrape)
        _cleanup_old_jobs()
        active = sum(
            1 for j in jobs.values()
            if j["status"].get("phase") not in (Phase.COMPLETE.value, Phase.ERROR.value, None)
        )
        if active >= MAX_CONCURRENT_JOBS:
            store.update_task_status(task_id, "failed", error="יותר מדי משימות פעילות")
            return jsonify({"error": "יותר מדי משימות פעילות. נסה שוב בעוד דקה."}), 429

        with jobs_lock:
            jobs[task_id] = {
                "queue": Queue(maxsize=200),
                "status": {"phase": Phase.INITIALIZING.value},
                "result_paths": {},
                "result_data": None,
                "error_log": None,
                "created": time.time(),
            }

        store.update_task_status(task_id, "running")

        def server_job_wrapper():
            _run_scrape_job(task_id, url, download_files)
            # Update task status when done
            with jobs_lock:
                job = jobs.get(task_id)
            if job:
                phase = job["status"].get("phase", "")
                if phase == Phase.COMPLETE.value:
                    store.update_task_status(task_id, "completed", result={
                        "collection_id": task_id,
                    })
                elif phase == Phase.ERROR.value:
                    store.update_task_status(
                        task_id, "failed",
                        error=job["status"].get("message", "שגיאה"),
                    )

        thread = threading.Thread(target=server_job_wrapper, daemon=True)
        thread.start()

    return jsonify({"task_id": task_id, "mode": mode}), 202


@app.route("/api/tasks")
@admin_required
def list_tasks():
    """List all tasks."""
    status_filter = request.args.get("status")
    tasks = store.list_tasks(status=status_filter)
    return jsonify({"tasks": tasks})


@app.route("/api/tasks/<task_id>")
@admin_required
def get_task(task_id):
    """Get task details."""
    task = store.get_task(task_id)
    if not task:
        return jsonify({"error": "משימה לא נמצאה"}), 404
    return jsonify(task)


@app.route("/api/tasks/<task_id>", methods=["DELETE"])
@admin_required
def delete_task(task_id):
    """Delete/cancel a task."""
    if store.delete_task(task_id):
        return "", 204
    return jsonify({"error": "משימה לא נמצאה"}), 404


# ---------------------------------------------------------------------------
# Worker API
# ---------------------------------------------------------------------------

@app.route("/api/worker/poll")
@worker_auth_required
def worker_poll():
    """Claim next pending worker-mode task.

    Returns task details or 204 if no tasks available.
    """
    worker_id = request.headers.get("X-Worker-ID", "unknown")

    # Reset stale tasks before polling
    store.reset_stale_tasks(timeout_minutes=10)

    task = store.claim_task(worker_id)
    if not task:
        return "", 204

    logger.info("Worker %s claimed task %s (%s)", worker_id, task["id"], task["url"])
    return jsonify(task)


@app.route("/api/worker/progress/<task_id>", methods=["POST"])
@worker_auth_required
def worker_progress(task_id):
    """Worker sends progress update for a task."""
    data = request.get_json(force=True, silent=True) or {}
    store.update_task_progress(task_id, data)
    return "", 204


@app.route("/api/worker/complete/<task_id>", methods=["POST"])
@worker_auth_required
def worker_complete(task_id):
    """Worker marks a task as completed."""
    data = request.get_json(force=True, silent=True) or {}
    store.update_task_status(task_id, "completed", result=data)
    logger.info("Worker completed task %s", task_id)
    return "", 204


@app.route("/api/worker/fail/<task_id>", methods=["POST"])
@worker_auth_required
def worker_fail(task_id):
    """Worker marks a task as failed."""
    data = request.get_json(force=True, silent=True) or {}
    error = data.get("error", "שגיאה לא ידועה")
    store.update_task_status(task_id, "failed", error=error)
    logger.warning("Worker failed task %s: %s", task_id, error)
    return "", 204


@app.route("/api/worker/heartbeat", methods=["POST"])
@worker_auth_required
def worker_heartbeat():
    """Worker reports it's alive."""
    worker_id = request.headers.get("X-Worker-ID", "unknown")
    info = (request.get_json(force=True, silent=True) or {}).get("info", "")
    with workers_lock:
        workers[worker_id] = {"last_seen": time.time(), "info": info}
    return "", 204


@app.route("/api/worker/status")
def worker_status():
    """Public endpoint: check if any worker is online."""
    cutoff = time.time() - WORKER_ONLINE_TIMEOUT
    with workers_lock:
        online = [
            {"worker_id": wid, "info": w.get("info", ""),
             "last_seen": datetime.fromtimestamp(w["last_seen"]).isoformat()}
            for wid, w in workers.items()
            if w["last_seen"] > cutoff
        ]
    return jsonify({
        "online": len(online) > 0,
        "count": len(online),
        "workers": online,
    })


# ---------------------------------------------------------------------------
# Scheduled archive runner
# ---------------------------------------------------------------------------

def _register_archive_collection(
    collection_id: str,
    source_url: str,
    collector_name: str,
    file_info: dict,
) -> None:
    csv_rel = os.path.relpath(file_info["csv_path"], TEMP_DIR)
    excel_rel = os.path.relpath(file_info["excel_path"], TEMP_DIR)
    store.save_collection(
        collection_id=collection_id,
        source_url=source_url,
        collector_name=collector_name,
        page_type="archive",
        record_count=file_info["record_count"],
        column_headers=file_info["column_headers"],
        csv_path=csv_rel,
        excel_path=excel_rel,
        warning=file_info.get("warning") or "",
    )
    csv_size = os.path.getsize(file_info["csv_path"])
    excel_size = os.path.getsize(file_info["excel_path"])
    store.save_files_bulk(collection_id, [
        {"filename": file_info["csv_basename"], "file_type": "csv", "category": "csv",
         "size_bytes": csv_size, "rel_path": csv_rel},
        {"filename": file_info["excel_basename"], "file_type": "xlsx", "category": "excel",
         "size_bytes": excel_size, "rel_path": excel_rel},
    ])


def _run_archive_job(archive_id: str) -> None:
    archive = store.get_scheduled_archive(archive_id)
    if not archive:
        return

    session = None
    try:
        session = GovILSession(use_playwright_fallback=not DISABLE_PLAYWRIGHT)
        session.warm()

        checkpoint = archive["checkpoint"]
        collection_id = archive.get("collection_id") or archive_id
        archive_dir = os.path.join(TEMP_DIR, collection_id)

        # Need bootstrap if no checkpoint or master CSV is missing
        csv_name = (checkpoint or {}).get("archive_csv", "")
        needs_bootstrap = (
            not checkpoint
            or not csv_name
            or not os.path.exists(os.path.join(archive_dir, csv_name))
        )

        if needs_bootstrap:
            file_info, checkpoint = _archive_bootstrap(
                url=archive["url"],
                archive_dir=archive_dir,
                session=session,
                name_override=archive.get("name") or "",
            )
            new_count = file_info["record_count"]
            _register_archive_collection(
                collection_id=collection_id,
                source_url=archive["url"],
                collector_name=archive.get("name") or file_info["collector_name"],
                file_info=file_info,
            )
        else:
            # known_urls stored as list in DB → convert to set for engine
            if isinstance(checkpoint.get("known_urls"), list):
                checkpoint = dict(checkpoint)
                checkpoint["known_urls"] = set(checkpoint["known_urls"])

            new_count, checkpoint = _archive_incremental(
                url=archive["url"],
                archive_dir=archive_dir,
                checkpoint=checkpoint,
                session=session,
            )
            if new_count > 0:
                coll = store.get_collection(collection_id)
                if coll:
                    store.save_collection(
                        collection_id=collection_id,
                        source_url=archive["url"],
                        collector_name=coll["collector_name"],
                        page_type="archive",
                        record_count=checkpoint.get("total_archived", coll["record_count"]),
                        column_headers=checkpoint.get("column_headers", coll["column_headers"]),
                        csv_path=coll["csv_path"],
                        excel_path=coll["excel_path"],
                        warning=coll.get("warning") or "",
                    )

        store.finish_archive_run(
            archive_id=archive_id,
            status="idle",
            collection_id=collection_id,
            checkpoint=checkpoint,
            new_count=new_count,
            error="",
        )
        logger.info("Archive job %s finished. new_count=%d", archive_id, new_count)

    except Exception as e:
        logger.exception("Archive job %s failed: %s", archive_id, e)
        store.finish_archive_run(
            archive_id=archive_id,
            status="error",
            collection_id=archive.get("collection_id") or archive_id,
            checkpoint=archive.get("checkpoint") or {},
            new_count=0,
            error=str(e),
        )
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


def _archive_scheduler_loop() -> None:
    """Daemon thread: checks every minute whether any archive is due to run."""
    while True:
        time.sleep(60)
        try:
            now = datetime.now()
            for archive in store.list_scheduled_archives():
                if not archive.get("enabled"):
                    continue
                if archive.get("status") == "running":
                    continue
                sched_hour = int(archive.get("schedule_hour") or 6)
                if now.hour < sched_hour:
                    continue
                today_cutoff = now.replace(
                    hour=sched_hour, minute=0, second=0, microsecond=0,
                ).isoformat(timespec="seconds")
                last_run = archive.get("last_run") or ""
                if last_run >= today_cutoff:
                    continue
                if store.claim_archive_run(archive["id"], today_cutoff):
                    logger.info("Scheduler: launching archive job %s", archive["id"])
                    threading.Thread(
                        target=_run_archive_job,
                        args=(archive["id"],),
                        daemon=True,
                    ).start()
        except Exception as e:
            logger.exception("Archive scheduler error: %s", e)


# Start the scheduler once per worker process
_archive_scheduler = threading.Thread(target=_archive_scheduler_loop, daemon=True)
_archive_scheduler.start()


# ---------------------------------------------------------------------------
# Archive API (admin only)
# ---------------------------------------------------------------------------

@app.route("/api/archives", methods=["GET"])
@admin_required
def list_archives():
    archives = store.list_scheduled_archives()
    # Don't send known_urls over the wire — can be large
    for a in archives:
        a.get("checkpoint", {}).pop("known_urls", None)
    return jsonify(archives)


@app.route("/api/archives", methods=["POST"])
@admin_required
def create_archive():
    data = request.get_json() or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400
    name = (data.get("name") or "").strip()
    try:
        schedule_hour = int(data.get("schedule_hour") or 6)
        if not (0 <= schedule_hour <= 23):
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({"error": "schedule_hour must be 0–23"}), 400

    archive_id = str(uuid.uuid4())
    archive = store.upsert_scheduled_archive(
        archive_id=archive_id,
        url=url,
        name=name,
        schedule_hour=schedule_hour,
        enabled=True,
    )
    return jsonify(archive), 201


@app.route("/api/archives/<archive_id>", methods=["GET"])
@admin_required
def get_archive(archive_id):
    archive = store.get_scheduled_archive(archive_id)
    if not archive:
        return jsonify({"error": "Not found"}), 404
    archive.get("checkpoint", {}).pop("known_urls", None)
    return jsonify(archive)


@app.route("/api/archives/<archive_id>", methods=["DELETE"])
@admin_required
def delete_archive(archive_id):
    if not store.delete_scheduled_archive(archive_id):
        return jsonify({"error": "Not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/archives/<archive_id>/run", methods=["POST"])
@admin_required
def run_archive_now(archive_id):
    archive = store.get_scheduled_archive(archive_id)
    if not archive:
        return jsonify({"error": "Not found"}), 404
    if archive.get("status") == "running":
        return jsonify({"error": "Archive is already running"}), 409
    store.start_archive_run(archive_id)
    threading.Thread(target=_run_archive_job, args=(archive_id,), daemon=True).start()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
