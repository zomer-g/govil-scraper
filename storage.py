"""
Collection Store — SQLite-backed persistence for completed scrapes.
Stores metadata (source URL, scrape date, counts) and references
to data files (CSV, Excel, ZIP) on disk.
"""

import json
import os
import shutil
import sqlite3
import logging
import threading
from contextlib import contextmanager
from datetime import datetime
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


class CollectionStore:
    """Multi-process-safe SQLite store for completed scrape collections.

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
                    conn.execute(SCHEMA_SQL)
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
        try:
            yield conn
        finally:
            conn.close()

    # ---- Write ---------------------------------------------------------------

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

        # Calculate total size of files on disk
        size = 0
        for rel in (zip_path, csv_path, excel_path):
            if rel:
                abs_path = os.path.join(self.base_dir, rel)
                if os.path.exists(abs_path):
                    size += os.path.getsize(abs_path)
        # Also count attachments dir
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

    # ---- Read ----------------------------------------------------------------

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

    # ---- Delete --------------------------------------------------------------

    def delete_collection(self, collection_id: str) -> bool:
        """Delete a collection record and its files from disk."""
        coll = self.get_collection(collection_id)
        if not coll:
            return False

        # Remove directory on disk
        job_dir = os.path.join(self.base_dir, collection_id)
        if os.path.isdir(job_dir):
            shutil.rmtree(job_dir, ignore_errors=True)
            logger.info("Deleted files for collection %s", collection_id)

        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
            conn.commit()
        return True

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

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        # Parse column_headers from JSON string
        try:
            d["column_headers"] = json.loads(d.get("column_headers") or "[]")
        except (json.JSONDecodeError, TypeError):
            d["column_headers"] = []
        return d
