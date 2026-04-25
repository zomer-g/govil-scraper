"""
Collection Store — SQLite-backed persistence for completed scrapes.
Stores metadata (source URL, scrape date, counts), individual file
records (attachments, CSV, Excel), and task queue for worker mode.
"""

import json
import os
import shutil
import sqlite3
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional, List

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS collections (
    id                TEXT PRIMARY KEY,
    source_url        TEXT NOT NULL,
    collector_name    TEXT NOT NULL,
    page_type         TEXT NOT NULL DEFAULT '',
    scrape_date       TEXT NOT NULL,
    record_count      INTEGER DEFAULT 0,
    attachment_count  INTEGER DEFAULT 0,
    downloaded_count  INTEGER DEFAULT 0,
    column_headers    TEXT DEFAULT '[]',
    zip_path          TEXT DEFAULT '',
    csv_path          TEXT DEFAULT '',
    excel_path        TEXT DEFAULT '',
    size_bytes        INTEGER DEFAULT 0,
    warning           TEXT DEFAULT ''
);
"""

FILES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id TEXT NOT NULL,
    filename      TEXT NOT NULL,
    file_type     TEXT NOT NULL DEFAULT '',
    category      TEXT NOT NULL DEFAULT 'attachment',
    size_bytes    INTEGER DEFAULT 0,
    rel_path      TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_collection ON files(collection_id);
CREATE INDEX IF NOT EXISTS idx_files_filename ON files(filename);
"""

TASKS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    id             TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    download_files INTEGER DEFAULT 1,
    mode           TEXT DEFAULT 'server',
    status         TEXT DEFAULT 'pending',
    worker_id      TEXT DEFAULT '',
    progress       TEXT DEFAULT '{}',
    result         TEXT DEFAULT '{}',
    error          TEXT DEFAULT '',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    claimed_at     TEXT DEFAULT '',
    completed_at   TEXT DEFAULT ''
);
"""

ARCHIVES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scheduled_archives (
    id            TEXT PRIMARY KEY,
    url           TEXT NOT NULL,
    name          TEXT NOT NULL DEFAULT '',
    schedule_hour INTEGER DEFAULT 6,
    enabled       INTEGER DEFAULT 1,
    collection_id TEXT DEFAULT '',
    checkpoint    TEXT DEFAULT '{}',
    status        TEXT DEFAULT 'idle',
    last_run      TEXT DEFAULT '',
    last_run_new  INTEGER DEFAULT 0,
    last_error    TEXT DEFAULT '',
    created_at    TEXT NOT NULL
);
"""


class CollectionStore:
    """Multi-process-safe SQLite store for collections, files, and tasks.

    Uses per-request connections (not persistent) so multiple gunicorn
    workers can access the DB without locking issues.
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
        self._db_path = os.path.join(base_dir, "collections.db")
        self._lock = threading.Lock()
        # Initialize schema (with retry for multi-worker startup)
        for attempt in range(5):
            try:
                with self._connect() as conn:
                    conn.executescript(SCHEMA_SQL)
                    conn.executescript(FILES_SCHEMA_SQL)
                    conn.executescript(TASKS_SCHEMA_SQL)
                    conn.executescript(ARCHIVES_SCHEMA_SQL)
                    conn.commit()
                break
            except sqlite3.OperationalError as e:
                if attempt < 4:
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    logger.warning("DB init retry %d: %s", attempt + 1, e)
                else:
                    raise

    @contextmanager
    def _connect(self):
        """Open a short-lived connection with WAL mode and a 30s busy timeout."""
        conn = sqlite3.connect(self._db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass  # WAL already set by another worker, safe to continue
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    # ---- Collection Write ----------------------------------------------------

    def save_collection(
        self,
        collection_id: str,
        source_url: str,
        collector_name: str,
        page_type: str = "",
        record_count: int = 0,
        attachment_count: int = 0,
        downloaded_count: int = 0,
        column_headers: Optional[List[str]] = None,
        zip_path: str = "",
        csv_path: str = "",
        excel_path: str = "",
        warning: str = "",
    ) -> dict:
        """Persist a completed scrape. Paths should be relative to base_dir."""
        now = datetime.now().isoformat(timespec="seconds")
        headers_json = json.dumps(column_headers or [], ensure_ascii=False)

        # Calculate total size from files table if available, else from disk
        size = self._calc_files_size(collection_id)
        if size == 0:
            # Fallback: scan disk directly
            for rel in (zip_path, csv_path, excel_path):
                if rel:
                    abs_path = os.path.join(self.base_dir, rel)
                    if os.path.exists(abs_path):
                        size += os.path.getsize(abs_path)
            att_dir = os.path.join(self.base_dir, collection_id, "attachments")
            if os.path.isdir(att_dir):
                for f in os.listdir(att_dir):
                    fp = os.path.join(att_dir, f)
                    if os.path.isfile(fp):
                        size += os.path.getsize(fp)

        with self._lock, self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO collections
                   (id, source_url, collector_name, page_type, scrape_date,
                    record_count, attachment_count, downloaded_count,
                    column_headers, zip_path, csv_path, excel_path,
                    size_bytes, warning)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (collection_id, source_url, collector_name, page_type, now,
                 record_count, attachment_count, downloaded_count,
                 headers_json, zip_path, csv_path, excel_path, size, warning),
            )
            conn.commit()
        logger.info("Saved collection %s (%s, %d records, %.1f MB)",
                     collection_id, collector_name, record_count, size / 1e6)
        return self.get_collection(collection_id)

    # ---- Collection Read -----------------------------------------------------

    def list_collections(self) -> List[dict]:
        """Return all collections, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM collections ORDER BY scrape_date DESC"
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_collection(self, collection_id: str) -> Optional[dict]:
        """Return a single collection by ID, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM collections WHERE id = ?", (collection_id,)
            ).fetchone()
        return self._row_to_dict(row) if row else None

    # ---- Collection Delete ---------------------------------------------------

    def delete_collection(self, collection_id: str) -> bool:
        """Delete a collection record, its files records, and disk files."""
        coll = self.get_collection(collection_id)
        if not coll:
            return False

        # Remove directory on disk
        job_dir = os.path.join(self.base_dir, collection_id)
        if os.path.isdir(job_dir):
            shutil.rmtree(job_dir, ignore_errors=True)
            logger.info("Deleted files for collection %s", collection_id)

        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM files WHERE collection_id = ?", (collection_id,))
            conn.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
            conn.commit()
        return True

    # ---- File Write ----------------------------------------------------------

    def save_file(
        self,
        collection_id: str,
        filename: str,
        file_type: str = "",
        category: str = "attachment",
        size_bytes: int = 0,
        rel_path: str = "",
    ) -> None:
        """Register an individual file belonging to a collection."""
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            conn.execute(
                """INSERT INTO files
                   (collection_id, filename, file_type, category,
                    size_bytes, rel_path, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (collection_id, filename, file_type, category,
                 size_bytes, rel_path, now),
            )
            conn.commit()

    def save_files_bulk(
        self,
        collection_id: str,
        file_records: List[dict],
    ) -> None:
        """Register multiple files at once for a collection."""
        now = datetime.now().isoformat(timespec="seconds")
        rows = [
            (collection_id, r["filename"], r.get("file_type", ""),
             r.get("category", "attachment"), r.get("size_bytes", 0),
             r["rel_path"], now)
            for r in file_records
        ]
        with self._lock, self._connect() as conn:
            conn.executemany(
                """INSERT INTO files
                   (collection_id, filename, file_type, category,
                    size_bytes, rel_path, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            conn.commit()
        logger.info("Saved %d file records for collection %s",
                     len(rows), collection_id)

    # ---- File Read -----------------------------------------------------------

    def list_files(self, collection_id: str) -> List[dict]:
        """Return all files for a collection."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM files WHERE collection_id = ? ORDER BY category, filename",
                (collection_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_file(self, collection_id: str, filename: str) -> Optional[dict]:
        """Return a single file record by collection + filename."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM files WHERE collection_id = ? AND filename = ?",
                (collection_id, filename),
            ).fetchone()
        return dict(row) if row else None

    def search_files(self, query: str, limit: int = 100) -> List[dict]:
        """Search files by filename across all collections."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT f.*, c.collector_name, c.source_url, c.scrape_date
                   FROM files f
                   JOIN collections c ON f.collection_id = c.id
                   WHERE f.filename LIKE ?
                   ORDER BY f.created_at DESC
                   LIMIT ?""",
                (f"%{query}%", limit),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- Task Write ----------------------------------------------------------

    def create_task(
        self,
        task_id: str,
        url: str,
        download_files: bool = True,
        mode: str = "server",
    ) -> dict:
        """Create a new scrape task."""
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            conn.execute(
                """INSERT INTO tasks
                   (id, url, download_files, mode, status, progress,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'pending', '{}', ?, ?)""",
                (task_id, url, 1 if download_files else 0, mode, now, now),
            )
            conn.commit()
        return self.get_task(task_id)

    def claim_task(self, worker_id: str) -> Optional[dict]:
        """Atomically claim the oldest pending worker-mode task.

        Returns the claimed task or None if no tasks available.
        """
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            # Atomic: select + update in one transaction
            row = conn.execute(
                """SELECT id FROM tasks
                   WHERE status = 'pending' AND mode = 'worker'
                   ORDER BY created_at ASC LIMIT 1"""
            ).fetchone()
            if not row:
                return None
            task_id = row["id"]
            conn.execute(
                """UPDATE tasks
                   SET status = 'claimed', worker_id = ?,
                       claimed_at = ?, updated_at = ?
                   WHERE id = ?""",
                (worker_id, now, now, task_id),
            )
            conn.commit()
        return self.get_task(task_id)

    def update_task_progress(self, task_id: str, progress: dict) -> None:
        """Update task progress (JSON dict with phase, current, total, etc.)."""
        now = datetime.now().isoformat(timespec="seconds")
        progress_json = json.dumps(progress, ensure_ascii=False)
        # Also set status to 'running' if it was 'claimed'
        with self._lock, self._connect() as conn:
            conn.execute(
                """UPDATE tasks
                   SET progress = ?, updated_at = ?,
                       status = CASE WHEN status = 'claimed' THEN 'running' ELSE status END
                   WHERE id = ?""",
                (progress_json, now, task_id),
            )
            conn.commit()

    def update_task_status(
        self,
        task_id: str,
        status: str,
        error: str = "",
        result: Optional[dict] = None,
    ) -> None:
        """Update task status. Set completed_at for terminal statuses."""
        now = datetime.now().isoformat(timespec="seconds")
        result_json = json.dumps(result or {}, ensure_ascii=False)
        completed = now if status in ("completed", "failed") else ""
        with self._lock, self._connect() as conn:
            conn.execute(
                """UPDATE tasks
                   SET status = ?, error = ?, result = ?,
                       updated_at = ?, completed_at = ?
                   WHERE id = ?""",
                (status, error, result_json, now, completed, task_id),
            )
            conn.commit()

    def delete_task(self, task_id: str) -> bool:
        """Delete a task."""
        with self._lock, self._connect() as conn:
            cur = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.commit()
        return cur.rowcount > 0

    # ---- Task Read -----------------------------------------------------------

    def list_tasks(self, status: Optional[str] = None, limit: int = 50) -> List[dict]:
        """List tasks, newest first. Optionally filter by status."""
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [self._task_to_dict(r) for r in rows]

    def get_task(self, task_id: str) -> Optional[dict]:
        """Return a single task by ID."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return self._task_to_dict(row) if row else None

    def reset_stale_tasks(self, timeout_minutes: int = 10) -> int:
        """Reset tasks stuck in 'claimed' for too long back to 'pending'."""
        cutoff = (datetime.now() - timedelta(minutes=timeout_minutes)).isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """UPDATE tasks
                   SET status = 'pending', worker_id = '', claimed_at = ''
                   WHERE status = 'claimed' AND claimed_at < ? AND claimed_at != ''""",
                (cutoff,),
            )
            conn.commit()
        if cur.rowcount > 0:
            logger.info("Reset %d stale tasks back to pending", cur.rowcount)
        return cur.rowcount

    # ---- Scheduled Archives --------------------------------------------------

    def list_scheduled_archives(self) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM scheduled_archives ORDER BY created_at DESC"
            ).fetchall()
        return [self._archive_to_dict(r) for r in rows]

    def get_scheduled_archive(self, archive_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM scheduled_archives WHERE id = ?", (archive_id,)
            ).fetchone()
        return self._archive_to_dict(row) if row else None

    def upsert_scheduled_archive(
        self,
        archive_id: str,
        url: str,
        name: str = "",
        schedule_hour: int = 6,
        enabled: bool = True,
    ) -> dict:
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            conn.execute(
                """INSERT INTO scheduled_archives
                   (id, url, name, schedule_hour, enabled, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     url=excluded.url, name=excluded.name,
                     schedule_hour=excluded.schedule_hour, enabled=excluded.enabled""",
                (archive_id, url, name, schedule_hour, 1 if enabled else 0, now),
            )
            conn.commit()
        return self.get_scheduled_archive(archive_id)

    def claim_archive_run(self, archive_id: str, today_cutoff: str) -> bool:
        """Atomically claim an archive for running.

        Sets status='running' only if not already running and not yet run today
        (last_run < today_cutoff). Returns True if this caller won the claim.
        """
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """UPDATE scheduled_archives
                   SET status = 'running', last_run = ?
                   WHERE id = ? AND status != 'running'
                     AND (last_run < ? OR last_run = '')""",
                (now, archive_id, today_cutoff),
            )
            conn.commit()
        return cur.rowcount > 0

    def start_archive_run(self, archive_id: str) -> None:
        """Unconditionally mark an archive as running (for manual triggers)."""
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE scheduled_archives SET status = 'running', last_run = ? WHERE id = ?",
                (now, archive_id),
            )
            conn.commit()

    def finish_archive_run(
        self,
        archive_id: str,
        status: str,
        collection_id: str,
        checkpoint: dict,
        new_count: int,
        error: str = "",
    ) -> None:
        # Serialize known_urls set → sorted list
        cp = dict(checkpoint or {})
        if isinstance(cp.get("known_urls"), set):
            cp["known_urls"] = sorted(cp["known_urls"])
        with self._lock, self._connect() as conn:
            conn.execute(
                """UPDATE scheduled_archives
                   SET status = ?, collection_id = ?, checkpoint = ?,
                       last_run_new = ?, last_error = ?
                   WHERE id = ?""",
                (status, collection_id, json.dumps(cp, ensure_ascii=False),
                 new_count, error, archive_id),
            )
            conn.commit()

    def delete_scheduled_archive(self, archive_id: str) -> bool:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM scheduled_archives WHERE id = ?", (archive_id,)
            )
            conn.commit()
        return cur.rowcount > 0

    # ---- Storage management --------------------------------------------------

    def get_total_size(self) -> int:
        """Total size in bytes of all stored collections."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) as total FROM collections"
            ).fetchone()
        return row["total"]

    def cleanup_oldest(self, max_bytes: int = 800_000_000):
        """Delete oldest collections until total size is under max_bytes."""
        while self.get_total_size() > max_bytes:
            with self._connect() as conn:
                oldest = conn.execute(
                    "SELECT id FROM collections ORDER BY scrape_date ASC LIMIT 1"
                ).fetchone()
            if not oldest:
                break
            logger.info("Disk cleanup: removing oldest collection %s", oldest["id"])
            self.delete_collection(oldest["id"])

    # ---- Helpers -------------------------------------------------------------

    def _calc_files_size(self, collection_id: str) -> int:
        """Sum size_bytes from the files table for a collection."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) as total FROM files WHERE collection_id = ?",
                (collection_id,),
            ).fetchone()
        return row["total"] if row else 0

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        try:
            d["column_headers"] = json.loads(d.get("column_headers") or "[]")
        except (json.JSONDecodeError, TypeError):
            d["column_headers"] = []
        return d

    @staticmethod
    def _task_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["download_files"] = bool(d.get("download_files", 1))
        for field in ("progress", "result"):
            try:
                d[field] = json.loads(d.get(field) or "{}")
            except (json.JSONDecodeError, TypeError):
                d[field] = {}
        return d

    @staticmethod
    def _archive_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["enabled"] = bool(d.get("enabled", 1))
        try:
            cp = json.loads(d.get("checkpoint") or "{}")
            # known_urls: keep as list in API responses (sets aren't JSON-serialisable)
            if isinstance(cp.get("known_urls"), list):
                cp["known_urls"] = cp["known_urls"]
            d["checkpoint"] = cp
        except (json.JSONDecodeError, TypeError):
            d["checkpoint"] = {}
        return d
