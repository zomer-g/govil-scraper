#!/usr/bin/env python3
"""גרסאות לעם — Worker Client for over.org.il.

DEPRECATED entry point — prefer:
    python -m govscraper.cli worker --source over --key <OVER_API_KEY>

The new CLI uses OverOrgSource + OverOrgPublisher (govscraper.worker.*),
which delegate poll/progress/upload-zip/upload-csv/push-version to the
OverWorkerClient class defined below. This file remains the live
production entry until the CLI path has run for one release cycle.

The wire-format contract is captured byte-for-byte in
govscraper/worker/publishers/_contract.py; tests/contract/test_over_org.py
pins each constant against the literals here, so any drift on either
side trips the test.

Legacy usage:
    python over_worker.py --key <API_KEY>
    python over_worker.py  # uses OVER_API_KEY env var
"""

import argparse
import logging
import os
import signal
import sys
import threading
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

# Local file we keep updated with the worker's current state. `cat` it on the
# worker host to see what the worker is doing right now.
STATUS_FILE = os.environ.get("OVER_WORKER_STATUS_FILE", "worker_status.txt")


def _archive_root() -> str:
    """Resolve where to write `over_archives/<dataset_id>/...`.

    Original layout placed this next to over_worker.py at the project root.
    After the post-G move into govscraper/legacy/ the file's __file__ now
    points 3 levels deep, so naïvely using `dirname(__file__)` would write
    archives inside the package — non-obvious and pollutes site-packages
    on installed deployments.

    Resolution order:
      1. $OVER_ARCHIVES_DIR    (operator override)
      2. project root (3 levels up from this file)  — matches legacy behaviour
      3. cwd                                          — script-style fallback
    """
    env = os.environ.get("OVER_ARCHIVES_DIR")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    if os.path.isdir(repo_root) and os.path.exists(os.path.join(repo_root, "over_worker.py")):
        return repo_root
    return os.getcwd()


def _write_status(line: str) -> None:
    """Best-effort write of the worker's current state to a small file."""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            f.write(f"[{ts}] {line}\n")
    except Exception:
        pass

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

    def _resolve_govmap_layer_caption(self, result) -> str | None:
        """Return the Hebrew caption for a GovMap layer, or None.

        Order of attempts:
          1. result.collector_name — set by govmap_engine.scrape_govmap to
             the resolved Layer.label_he. Skipped if it looks like a
             placeholder ("LAYER_NNNNNN" or "שכבה NNNNNN").
          2. Online catalog lookup by result.layer_id via
             govscraper.scrapers.govmap.catalog_fetch.

        Returns None when neither yields a non-placeholder caption — OVER
        then leaves tracked_dataset.title untouched.
        """
        layer_id = (getattr(result, "layer_id", "") or "").strip()
        candidate = (getattr(result, "collector_name", "") or "").strip()

        # Strip a "__<timestamp>" suffix that scrape_govmap appends in some
        # paths, then normalise.
        if "__" in candidate:
            candidate = candidate.split("__", 1)[0].strip()

        is_placeholder = (
            not candidate
            or candidate.upper() == f"LAYER_{layer_id.upper()}"
            or candidate == f"שכבה {layer_id}"
        )
        if not is_placeholder:
            return candidate

        if not layer_id:
            return None

        # Fallback: live online catalog lookup
        try:
            from govscraper.scrapers.govmap import catalog_fetch
            entry = catalog_fetch.lookup_layer(layer_id)
            if entry:
                cap = (entry.get("caption") or "").strip()
                return cap or None
        except Exception as e:
            logger.warning("GovMap catalog lookup for %s failed: %s", layer_id, e)
        return None

    def _upload_govmap_geojson(self, tracked_dataset_id: str, result,
                               layer_caption: str) -> str | None:
        """Build a WGS84 FeatureCollection and POST it to /upload-geojson.

        Returns the resource_id from the OVER response, or None when the
        upload failed (caller logs and continues).
        """
        from govscraper.io import geojson_writer
        from govscraper.io.sanitize import sanitize_filename
        import os, tempfile

        with tempfile.TemporaryDirectory(prefix="ovr_geojson_") as tmp:
            geojson_path = geojson_writer.write_feature_collection(
                output_dir=tmp,
                name=layer_caption,
                features=getattr(result, "features", []) or [],
                layer_id=getattr(result, "layer_id", ""),
                bbox_itm=getattr(result, "bbox_itm", None) or None,
                bbox_wgs84=getattr(result, "bbox_wgs84", None) or None,
                geometry_type=getattr(result, "geometry_type", ""),
            )
            with open(geojson_path, "rb") as f:
                geojson_bytes = f.read()

            base = sanitize_filename(layer_caption)
            return self.upload_geojson(
                tracked_dataset_id=tracked_dataset_id,
                version_number=1,
                geojson_bytes=geojson_bytes,
                resource_name=layer_caption,
                filename=f"{base}.geojson",
            )

    def upload_geojson(self, tracked_dataset_id: str, version_number: int,
                       geojson_bytes: bytes, resource_name: str,
                       filename: str | None = None,
                       max_attempts: int = 3) -> str | None:
        """Upload a GeoJSON FeatureCollection via multipart to
        /api/worker/upload-geojson. Returns the odata resource_id (which
        the caller passes back via push_version's `geojson_resource_ids`).

        Per the over.org.il contract added 2026-05-02 the endpoint exposes
        the file as a CKAN resource with `format=GeoJSON` (not as a ZIP
        attachment), so downloaders see a directly-renderable geo file
        alongside the CSV. `resource_name` is optional — defaults to the
        filename without `.geojson`.

        Retries with backoff on 5xx/connection errors. Returns None on
        unrecoverable failure; caller can still publish the CSV resource
        without the GeoJSON.
        """
        import io, time as _time
        if not geojson_bytes:
            logger.warning("upload_geojson: empty payload — skipping")
            return None

        fname = filename or f"{resource_name}.geojson"

        last_err = ""
        for attempt in range(1, max_attempts + 1):
            try:
                files = {"file": (fname, io.BytesIO(geojson_bytes),
                                 "application/geo+json")}
                data = {"version_number": str(version_number)}
                if resource_name:
                    data["resource_name"] = resource_name
                import requests
                resp = requests.post(
                    self._url(f"/api/worker/upload-geojson/{tracked_dataset_id}"),
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    files=files,
                    data=data,
                    timeout=600,
                )
                if resp.status_code == 200:
                    result = resp.json()
                    logger.info("GeoJSON uploaded → resource_id=%s, size=%d KB",
                                result.get("resource_id"),
                                result.get("size", 0) // 1024)
                    return result.get("resource_id")
                last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                if resp.status_code >= 500 and attempt < max_attempts:
                    wait = 5 * attempt
                    logger.warning("GeoJSON upload attempt %d/%d got %d — retrying in %ds",
                                   attempt, max_attempts, resp.status_code, wait)
                    _time.sleep(wait)
                    continue
                logger.error("GeoJSON upload failed (attempt %d/%d): %s",
                             attempt, max_attempts, last_err)
                break
            except Exception as e:
                last_err = str(e)
                if attempt < max_attempts:
                    wait = 5 * attempt
                    logger.warning("GeoJSON upload attempt %d/%d raised: %s — retrying in %ds",
                                   attempt, max_attempts, last_err, wait)
                    _time.sleep(wait)
                    continue
                logger.exception("GeoJSON upload error after %d attempts: %s", attempt, e)
        return None

    def upload_csv(self, tracked_dataset_id: str, version_number: int,
                   csv_bytes: bytes, resource_name: str, row_count: int,
                   fields: list | None = None,
                   max_attempts: int = 3) -> str | None:
        """Upload a CSV file via multipart to /api/worker/upload-csv.
        Used when the records JSON would exceed Cloudflare's 100MB limit
        on push-version. Always sends gzip-compressed bytes — CSV compresses
        ~10:1 so even 100k+ row sets fit well under the 100MB edge limit.
        The server streams the upload through disk, uploads the plain CSV to
        odata, and pushes rows to the datastore in a background task.
        `fields` is forwarded so the datastore schema matches the inline path.
        Retries with backoff on 5xx/connection errors (the server may be mid
        restart or recovering).
        Returns the odata resource_id, or None after all attempts fail."""
        import gzip, io, json as _json, time as _time
        try:
            compressed = gzip.compress(csv_bytes, compresslevel=6)
            logger.info("CSV gzip: %d KB plain → %d KB compressed (%.1fx)",
                        len(csv_bytes) // 1024, len(compressed) // 1024,
                        len(csv_bytes) / max(len(compressed), 1))
        except Exception as e:
            logger.exception("CSV compression error: %s", e)
            return None

        last_err = ""
        for attempt in range(1, max_attempts + 1):
            try:
                files = {"file": (f"{resource_name}.csv.gz", io.BytesIO(compressed),
                                 "application/gzip")}
                data = {
                    "version_number": str(version_number),
                    "resource_name": resource_name,
                    "row_count": str(row_count),
                    "compression": "gzip",
                }
                if fields:
                    data["fields_json"] = _json.dumps(fields, ensure_ascii=False)
                import requests
                resp = requests.post(
                    self._url(f"/api/worker/upload-csv/{tracked_dataset_id}"),
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    files=files,
                    data=data,
                    timeout=1800,
                )
                if resp.status_code == 200:
                    result = resp.json()
                    logger.info("CSV uploaded → resource_id=%s, size=%d KB (%d rows), datastore=%s",
                                result.get("resource_id"),
                                result.get("size", 0) // 1024,
                                result.get("rows", row_count),
                                result.get("datastore", False))
                    return result.get("resource_id")
                last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                # 5xx and 502 in particular often mean server is restarting —
                # wait a bit and retry.
                if resp.status_code >= 500 and attempt < max_attempts:
                    wait = 15 * attempt
                    logger.warning("CSV upload attempt %d/%d got %d — retrying in %ds",
                                   attempt, max_attempts, resp.status_code, wait)
                    _time.sleep(wait)
                    continue
                logger.error("CSV upload failed (attempt %d/%d): %s",
                             attempt, max_attempts, last_err)
                break
            except Exception as e:
                last_err = str(e)
                if attempt < max_attempts:
                    wait = 10 * attempt
                    logger.warning("CSV upload attempt %d/%d raised: %s — retrying in %ds",
                                   attempt, max_attempts, last_err, wait)
                    _time.sleep(wait)
                    continue
                logger.exception("CSV upload error after %d attempts: %s", attempt, e)
        return None

    def push_version(self, tracked_dataset_id: str, source_url: str,
                     records: list, fields: list, attachments: list,
                     duration_seconds: float,
                     zip_resource_id: str | None = None,
                     zip_resource_ids: list[str] | None = None,
                     csv_resource_ids: dict[str, str] | None = None,
                     geojson_resource_ids: list[str] | None = None,
                     scraper_config_patch: dict | None = None,
                     dataset_title_he: str | None = None,
                     skip_version: bool = False) -> dict:
        """Push scraped data as a new version to over.org.il.
        zip_resource_id: single ZIP (legacy). zip_resource_ids: list of parts (preferred).
        csv_resource_ids: maps resource_name -> odata resource_id when records were
            uploaded out-of-band via /api/worker/upload-csv (used for very large record
            sets that would exceed the 100MB JSON push limit). When provided for a
            resource, send empty `records` for it.
        geojson_resource_ids: list of resource_ids returned by
            /api/worker/upload-geojson — over.org.il publishes them as CKAN
            resources with format=GeoJSON. Added 2026-05-02 for govmap.
        dataset_title_he: optional Hebrew title hint sent inside scrape_metadata.
            over.org.il uses it to update tracked_dataset.title only when the
            current title is still the auto-generated default (e.g.
            "GovMap layer 200541"). User-edited titles are preserved.
        """
        # If a resource has a pre-uploaded CSV, drop its records from the JSON
        # to keep the payload small (the server uses the uploaded file instead).
        records_for_resource = [] if (csv_resource_ids and "נתוני הסורק" in csv_resource_ids) else records

        scrape_metadata: dict = {
            "source_url": source_url,
            "scrape_duration_seconds": round(duration_seconds, 1),
            "total_items": len(records),
            "total_files": len(attachments),
            "scraper_version": "1.0.0",
        }
        if dataset_title_he:
            scrape_metadata["dataset_title_he"] = dataset_title_he

        payload = {
            "tracked_dataset_id": tracked_dataset_id,
            "metadata_modified": datetime.now().isoformat(),
            "resources": [
                {
                    "name": "נתוני הסורק",
                    "format": "CSV",
                    "records": records_for_resource,
                    "fields": fields,
                    "row_count": len(records),
                }
            ],
            "attachments": attachments,
            "scrape_metadata": scrape_metadata,
        }
        if zip_resource_ids:
            payload["zip_resource_ids"] = zip_resource_ids
        elif zip_resource_id:
            payload["zip_resource_id"] = zip_resource_id
        if csv_resource_ids:
            payload["csv_resource_ids"] = csv_resource_ids
        if geojson_resource_ids:
            payload["geojson_resource_ids"] = geojson_resource_ids
        if scraper_config_patch is not None:
            payload["scraper_config_patch"] = scraper_config_patch
        if skip_version:
            payload["skip_version"] = True

        import json as _json
        payload_size = len(_json.dumps(payload, ensure_ascii=False))
        zip_info = (f"{len(zip_resource_ids)} parts" if zip_resource_ids
                    else (zip_resource_id or "none"))
        csv_info = (f"{len(csv_resource_ids)} pre-uploaded" if csv_resource_ids else "inline")
        logger.info("Pushing version: %d records (%s), %d fields, %d attachments, ZIP=%s, payload ~%d KB",
                     len(records), csv_info, len(fields), len(attachments),
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

    def execute_archive_task(self, task: dict):
        """Execute an incremental archive task (scraper_config.archive == true).

        On each run:
          - Bootstrap (full scrape) if no checkpoint or local CSV is missing.
          - Otherwise fetch only the delta and append to the master CSV.
          - Upload the full master CSV to odata and push a new version.
          - If 0 new items, mark the task done without creating a new version.
          - Persist the updated checkpoint back to the server via scraper_config_patch.
        """
        from govscraper.io.archive_engine import run_bootstrap, run_incremental
        from govscraper.scrapers.govil.legacy_engine import (
            GovILSession, GovILScraperError,
            InvalidURLError, CloudflareBlockError,
        )

        task_id = task["task_id"]
        tracked_dataset_id = task["tracked_dataset_id"]
        source_url = task["source_url"]
        config = task.get("scraper_config", {})

        logger.info("=" * 70)
        logger.info("▶  ARCHIVE TASK START")
        logger.info("   task_id: %s", task_id)
        logger.info("   url:     %s", source_url)
        logger.info("=" * 70)
        _write_status(f"ARCHIVE on {source_url}")

        # Archive dir is kept next to the script and keyed by dataset ID so
        # it survives multiple runs without re-bootstrapping. After the
        # post-G package move, _archive_root() walks back up to the repo
        # root so archives stay in their pre-refactor location.
        archive_dir = os.path.join(_archive_root(), "over_archives", tracked_dataset_id)
        os.makedirs(archive_dir, exist_ok=True)

        # Load checkpoint from scraper_config; known_urls must be a set.
        raw_checkpoint = config.get("checkpoint")
        checkpoint = None
        if raw_checkpoint:
            checkpoint = dict(raw_checkpoint)
            checkpoint["known_urls"] = set(checkpoint.get("known_urls") or [])

        need_bootstrap = (
            checkpoint is None
            or not checkpoint.get("archive_csv")
            or not os.path.exists(
                os.path.join(archive_dir, checkpoint.get("archive_csv", ""))
            )
        )

        start_time = time.time()
        session = None
        last_state = {"phase": "initializing", "current": 0, "total": 1, "message": ""}
        _hb_stop = threading.Event()

        def _heartbeat():
            while not _hb_stop.wait(30):
                s = last_state.copy()
                self.report_progress(task_id, s["phase"], s["current"], s["total"], s["message"])

        _hb_thread = threading.Thread(target=_heartbeat, daemon=True, name="archive-heartbeat")
        _hb_thread.start()

        try:
            self.report_progress(task_id, "initializing", 0, 1, "מתחבר לאתר gov.il...")
            session = GovILSession(use_playwright_fallback=False)
            session.warm()

            if need_bootstrap:
                logger.info("Archive bootstrap required — full scrape")

                last_report = [0.0]

                def _progress_cb(**kw):
                    now = time.time()
                    current = kw.get("current", 0)
                    total = kw.get("total", 1) or 1
                    msg = kw.get("message", "")
                    last_state.update({"phase": "scraping", "current": current,
                                       "total": total, "message": msg})
                    if now - last_report[0] >= 5:
                        self.report_progress(task_id, "scraping", current, total, msg)
                        last_report[0] = now

                file_info, checkpoint = run_bootstrap(
                    url=source_url,
                    archive_dir=archive_dir,
                    session=session,
                    progress_cb=_progress_cb,
                )
                new_count = file_info["record_count"]
                logger.info("Bootstrap done: %d records", new_count)
            else:
                logger.info("Archive incremental run")
                self.report_progress(task_id, "scraping", 0, 1, "בודק עדכונים...")
                last_state.update({"phase": "scraping", "current": 0, "total": 1,
                                   "message": "בודק עדכונים..."})
                new_count, checkpoint = run_incremental(
                    url=source_url,
                    archive_dir=archive_dir,
                    checkpoint=checkpoint,
                    session=session,
                )
                logger.info("Incremental done: %d new items", new_count)

            # Serialize checkpoint: known_urls set → sorted list for JSON storage.
            checkpoint_serial = dict(checkpoint)
            checkpoint_serial["known_urls"] = sorted(checkpoint.get("known_urls") or [])
            scraper_config_patch = {"checkpoint": checkpoint_serial}

            if not need_bootstrap and new_count == 0:
                logger.info("No new items — completing task without new version")
                self.push_version(
                    tracked_dataset_id=tracked_dataset_id,
                    source_url=source_url,
                    records=[], fields=[], attachments=[],
                    duration_seconds=time.time() - start_time,
                    scraper_config_patch=scraper_config_patch,
                    skip_version=True,
                )
                return

            # Upload the full master CSV to odata.
            csv_path = os.path.join(archive_dir, checkpoint["archive_csv"])
            with open(csv_path, "rb") as fh:
                csv_bytes = fh.read()

            total_rows = checkpoint.get("total_archived", 0)
            fields = [{"id": col, "type": "text"} for col in (checkpoint.get("column_headers") or [])]
            self.report_progress(task_id, "uploading", 0, 1,
                                 f"מעלה CSV ({len(csv_bytes) // 1024 // 1024}MB, {total_rows} שורות)...")
            last_state.update({"phase": "uploading", "current": 0, "total": 1,
                                "message": "מעלה CSV..."})

            csv_rid = self.upload_csv(
                tracked_dataset_id=tracked_dataset_id,
                version_number=1,
                csv_bytes=csv_bytes,
                resource_name="נתוני הסורק",
                row_count=total_rows,
                fields=fields,
            )
            del csv_bytes  # free memory before push_version JSON encode

            if not csv_rid:
                self.report_failure(task_id, "CSV upload failed for archive task", phase="uploading")
                return

            self.push_version(
                tracked_dataset_id=tracked_dataset_id,
                source_url=source_url,
                records=[], fields=fields, attachments=[],
                duration_seconds=time.time() - start_time,
                csv_resource_ids={"נתוני הסורק": csv_rid},
                scraper_config_patch=scraper_config_patch,
            )
            logger.info("Archive task done: %d new items, total %d in archive",
                        new_count, total_rows)

        except (InvalidURLError, CloudflareBlockError, GovILScraperError) as e:
            logger.error("Archive scrape error in task %s: %s", task_id, e)
            self.report_failure(task_id, str(e), phase="scraping")
        except RuntimeError as e:
            logger.error("Archive push error in task %s: %s", task_id, e)
            self.report_failure(task_id, str(e), phase="exporting")
        except Exception as e:
            logger.exception("Unexpected error in archive task %s", task_id)
            self.report_failure(task_id, f"{type(e).__name__}: {e}")
        finally:
            _hb_stop.set()
            if session:
                try:
                    session.close()
                except Exception:
                    pass

    def execute_nadlan_archive_task(self, task: dict):
        """Execute a nadlan_settlements archive task.

        Mirrors execute_archive_task line-by-line; the differences are:
          - imports nadlan_incremental_engine (Playwright-driven, no GovILSession)
          - bootstrap/incremental signatures take no source_url
          - lookback_days + settlements_filter come from scraper_config
        """
        from govscraper.scrapers.nadlan.legacy_incremental import run_bootstrap, run_incremental

        task_id = task["task_id"]
        tracked_dataset_id = task["tracked_dataset_id"]
        source_url = task.get("source_url", "")
        config = task.get("scraper_config", {})

        logger.info("=" * 70)
        logger.info("▶  NADLAN ARCHIVE TASK START")
        logger.info("   task_id: %s", task_id)
        logger.info("   type:    nadlan_settlements")
        logger.info("=" * 70)
        _write_status("ARCHIVE nadlan_settlements")

        archive_dir = os.path.join(_archive_root(), "over_archives", tracked_dataset_id)
        os.makedirs(archive_dir, exist_ok=True)

        raw_checkpoint = config.get("checkpoint")
        checkpoint = None
        if raw_checkpoint:
            checkpoint = dict(raw_checkpoint)
            checkpoint["known_urls"] = set(checkpoint.get("known_urls") or [])

        need_bootstrap = (
            checkpoint is None
            or not checkpoint.get("archive_csv")
            or not os.path.exists(
                os.path.join(archive_dir, checkpoint.get("archive_csv", ""))
            )
        )

        lookback_days = int(config.get("nadlan_lookback_days", 90))
        settlements_filter = config.get("nadlan_settlements_filter")

        start_time = time.time()
        last_state = {"phase": "initializing", "current": 0, "total": 1, "message": ""}
        _hb_stop = threading.Event()

        def _heartbeat():
            while not _hb_stop.wait(30):
                s = last_state.copy()
                self.report_progress(task_id, s["phase"], s["current"], s["total"], s["message"])

        _hb_thread = threading.Thread(target=_heartbeat, daemon=True, name="nadlan-archive-heartbeat")
        _hb_thread.start()

        try:
            self.report_progress(task_id, "initializing", 0, 1, "טוען קטלוג יישובים...")

            last_report = [0.0]

            def _progress_cb(**kw):
                now = time.time()
                current = kw.get("current", 0)
                total = kw.get("total", 1) or 1
                msg = kw.get("message", "")
                last_state.update({"phase": "scraping", "current": current,
                                   "total": total, "message": msg})
                if now - last_report[0] >= 5:
                    self.report_progress(task_id, "scraping", current, total, msg)
                    last_report[0] = now

            if need_bootstrap:
                logger.info("Nadlan bootstrap required — settlement-by-settlement scan")
                file_info, checkpoint = run_bootstrap(
                    archive_dir=archive_dir,
                    progress_cb=_progress_cb,
                    settlements_filter=settlements_filter,
                )
                new_count = file_info["record_count"]
                logger.info("Bootstrap done: %d deals", new_count)
            else:
                logger.info("Nadlan incremental run (lookback=%dd)", lookback_days)
                self.report_progress(task_id, "scraping", 0, 1, "בודק עסקאות חדשות...")
                last_state.update({"phase": "scraping", "current": 0, "total": 1,
                                   "message": "בודק עסקאות חדשות..."})
                new_count, checkpoint = run_incremental(
                    archive_dir=archive_dir,
                    checkpoint=checkpoint,
                    progress_cb=_progress_cb,
                    lookback_days=lookback_days,
                    settlements_filter=settlements_filter,
                )
                logger.info("Incremental done: %d new deals", new_count)

            checkpoint_serial = dict(checkpoint)
            checkpoint_serial["known_urls"] = sorted(checkpoint.get("known_urls") or [])
            scraper_config_patch = {"checkpoint": checkpoint_serial}

            if not need_bootstrap and new_count == 0:
                logger.info("No new deals — completing task without new version")
                self.push_version(
                    tracked_dataset_id=tracked_dataset_id,
                    source_url=source_url,
                    records=[], fields=[], attachments=[],
                    duration_seconds=time.time() - start_time,
                    scraper_config_patch=scraper_config_patch,
                    skip_version=True,
                )
                return

            csv_path = os.path.join(archive_dir, checkpoint["archive_csv"])
            with open(csv_path, "rb") as fh:
                csv_bytes = fh.read()

            total_rows = checkpoint.get("total_archived", 0)
            fields = [{"id": col, "type": "text"}
                      for col in (checkpoint.get("column_headers") or [])]
            self.report_progress(task_id, "uploading", 0, 1,
                                 f"מעלה CSV ({len(csv_bytes) // 1024 // 1024}MB, {total_rows} שורות)...")
            last_state.update({"phase": "uploading", "current": 0, "total": 1,
                                "message": "מעלה CSV..."})

            csv_rid = self.upload_csv(
                tracked_dataset_id=tracked_dataset_id,
                version_number=1,
                csv_bytes=csv_bytes,
                resource_name="נתוני הסורק",
                row_count=total_rows,
                fields=fields,
            )
            del csv_bytes

            if not csv_rid:
                self.report_failure(task_id, "CSV upload failed for nadlan archive task",
                                    phase="uploading")
                return

            self.push_version(
                tracked_dataset_id=tracked_dataset_id,
                source_url=source_url,
                records=[], fields=fields, attachments=[],
                duration_seconds=time.time() - start_time,
                csv_resource_ids={"נתוני הסורק": csv_rid},
                scraper_config_patch=scraper_config_patch,
            )
            logger.info("Nadlan archive task done: +%d deals, total %d",
                        new_count, total_rows)

        except RuntimeError as e:
            logger.error("Nadlan push error in task %s: %s", task_id, e)
            self.report_failure(task_id, str(e), phase="exporting")
        except Exception as e:
            logger.exception("Unexpected error in nadlan archive task %s", task_id)
            self.report_failure(task_id, f"{type(e).__name__}: {e}")
        finally:
            _hb_stop.set()

    def execute_task(self, task: dict):
        """Execute a single scrape task and push results to over.org.il."""
        from govscraper.scrapers.govil.legacy_engine import (
            GovILSession, GovILScraper, GovILScraperError,
            InvalidURLError, CloudflareBlockError,
        )

        task_id = task["task_id"]
        tracked_dataset_id = task["tracked_dataset_id"]
        source_url = task["source_url"]
        config = task.get("scraper_config", {})

        # Archive mode: incremental append-only archiving via archive_engine
        # or its nadlan-specific sibling, selected by archive_type.
        if config.get("archive"):
            archive_type = config.get("archive_type")
            if archive_type == "nadlan_settlements":
                return self.execute_nadlan_archive_task(task)
            return self.execute_archive_task(task)

        download_files = config.get("download_files", False)

        logger.info("=" * 70)
        logger.info("▶  TASK START")
        logger.info("   task_id: %s", task_id)
        logger.info("   url:     %s", source_url)
        logger.info("=" * 70)
        _write_status(f"WORKING on {source_url}")

        session = None
        start_time = time.time()
        last_report = [0.0]
        last_local_log = [0.0]

        # Heartbeat thread — fires every 30 s even when the main thread is
        # stuck on a single page or file download, preventing server timeout.
        last_state = {"phase": "initializing", "current": 0, "total": 1, "message": ""}
        _hb_stop = threading.Event()

        def _heartbeat():
            while not _hb_stop.wait(30):
                s = last_state.copy()
                self.report_progress(task_id, s["phase"], s["current"], s["total"], s["message"])

        _hb_thread = threading.Thread(target=_heartbeat, daemon=True, name="heartbeat")
        _hb_thread.start()

        def _progress(**kwargs):
            now = time.time()
            phase = kwargs.get("phase", "scraping")
            current = kwargs.get("current", 0)
            total = kwargs.get("total", 0)
            message = kwargs.get("message", "")
            last_state.update({"phase": phase, "current": current, "total": total, "message": message})
            # Send to server (throttled to 5s)
            if now - last_report[0] >= 5:
                phase_kw = kwargs.pop("phase", "scraping")
                self.report_progress(task_id, phase_kw, **kwargs)
                last_report[0] = now
            # Also print to local log (throttled to 15s) so the operator sees
            # "still working" instead of complete silence during long phases.
            if now - last_local_log[0] >= 15:
                pct = f"{int(current/total*100)}%" if total else "?%"
                logger.info("  ⏳ phase=%s %s (%d/%d) %s",
                            phase, pct, current, total, message[:80] if message else "")
                _write_status(f"{phase} {pct} — {message[:120]}")
                last_local_log[0] = now

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

            # NOTE: `records`, `fields`, and `attachments` are built AFTER
            # the download+inject block below — otherwise `fields` would
            # be frozen against pre-injection column_headers and the new
            # `attachment_filename` / `attachment_url` columns would be
            # dropped by CKAN datastore on upload (the table schema
            # follows `fields`, not the record dicts' keys).

            # Download files, create ZIPs (split into ≤80MB parts to fit under
            # Cloudflare's 100MB edge limit), and upload each part via multipart.
            zip_resource_ids: list[str] = []
            tmp_dir = None
            if result.file_attachments:
                import tempfile
                from govscraper.io.file_handler import FileHandler

                self.report_progress(task_id, "downloading", 0,
                                     len(result.file_attachments),
                                     "מוריד קבצים מצורפים...")
                try:
                    tmp_dir = tempfile.mkdtemp(prefix="govil_worker_")
                    handler = FileHandler(session, output_dir=tmp_dir)

                    # Download attachments FIRST. Two reasons:
                    #  1. The on-disk basename after `_dedup` is the
                    #     authoritative ZIP-internal name; `att.filename`
                    #     before download isn't.
                    #  2. We need that authoritative name to land in the
                    #     CSV (column `attachment_filename`) so consumers
                    #     can join CSV rows to PDFs deterministically by
                    #     basename, instead of falling back to positional
                    #     ordering which silently drifts.
                    def dl_progress(**kw):
                        last_state.update({
                            "phase": "downloading",
                            "current": kw.get("current", 0),
                            "total": kw.get("total", 0),
                            "message": kw.get("message", ""),
                        })
                        self.report_progress(task_id, "downloading",
                                             kw.get("current", 0),
                                             kw.get("total", 0),
                                             kw.get("message", ""))

                    att_paths = handler.download_attachments(
                        result.file_attachments,
                        progress_callback=dl_progress,
                    )

                    from govscraper.io.attachments import inject_attachment_columns
                    inject_attachment_columns(
                        result.items, result.file_attachments,
                        att_paths, result.column_headers,
                    )

                    # Export CSV for the ZIP — now includes the new
                    # `attachment_filename` / `attachment_url` columns.
                    csv_path = handler.export_csv(result)

                    successful_paths = [p for p in att_paths if p is not None]
                    if successful_paths:
                        # Pack into ≤80MB ZIPs (first contains the CSV too)
                        zip_paths = split_attachments_into_zips(
                            successful_paths, csv_path, tmp_dir,
                            base_name=result.collector_name or "attachments",
                        )
                        total_parts = len(zip_paths)
                        logger.info("Created %d ZIP part(s) for %d attachments (%d failed)",
                                    total_parts, len(successful_paths),
                                    len(att_paths) - len(successful_paths))

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

            # Build push-version payload AFTER inject_attachment_columns has
            # had a chance to extend column_headers. `fields` defines the
            # CKAN datastore table schema; if we built it earlier, the new
            # `attachment_filename` / `attachment_url` columns would be
            # absent from the schema and the values would be silently
            # dropped on upsert.
            records = result.items  # already flat dicts (mutated in place by inject)
            fields = [
                {"id": col, "type": "text"}
                for col in (result.column_headers or [])
            ]
            if not fields and records:
                fields = [{"id": k, "type": "text"} for k in records[0].keys()]
            attachments = [
                {"name": f.filename, "url": f.url}
                for f in result.file_attachments
            ]

            # If the records JSON would exceed Cloudflare's 100MB body limit,
            # upload the CSV separately via multipart and skip inline records.
            # Threshold: 50MB JSON ≈ 80MB after key/quote overhead → safe margin.
            csv_resource_ids: dict[str, str] | None = None
            import json as _json
            est_json_bytes = len(_json.dumps(records, ensure_ascii=False)) if records else 0
            needs_multipart = est_json_bytes > 50 * 1024 * 1024
            if needs_multipart:
                self.report_progress(task_id, "uploading", 0, 1,
                                     f"מעלה CSV ({est_json_bytes // 1024 // 1024}MB) דרך multipart...")
                # Generate CSV bytes from records (utf-8-sig BOM for Excel)
                import csv as _csv, io as _io
                buf = _io.StringIO()
                fieldnames = [f["id"] for f in fields] if fields else (list(records[0].keys()) if records else [])
                writer = _csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                for row in records:
                    writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
                csv_bytes = buf.getvalue().encode("utf-8-sig")
                logger.info("Records JSON est=%dMB → CSV multipart upload (%d KB plain)",
                             est_json_bytes // 1024 // 1024, len(csv_bytes) // 1024)
                rid = self.upload_csv(
                    tracked_dataset_id=tracked_dataset_id,
                    version_number=1,
                    csv_bytes=csv_bytes,
                    resource_name="נתוני הסורק",
                    row_count=len(records),
                    fields=fields,
                )
                if rid:
                    csv_resource_ids = {"נתוני הסורק": rid}
                    # Free the large buffers now that the CSV is safely uploaded
                    # server-side; push-version only needs empty records + csv_resource_ids
                    del csv_bytes
                else:
                    # CSV multipart upload failed. We CANNOT fall back to an
                    # inline push-version with the full records — that payload
                    # would be >100MB and Cloudflare's edge would reject it
                    # with another 502. Fail the task cleanly instead, so the
                    # UI shows the real reason (upload-csv failed) rather
                    # than a misleading "push-version 502".
                    msg = (f"CSV multipart upload failed "
                           f"(JSON estimate {est_json_bytes // 1024 // 1024}MB "
                           f"exceeds inline push-version safe limit). "
                           f"Likely server OOM or deploy in progress — "
                           f"try again in a minute.")
                    logger.error(msg)
                    self.report_failure(task_id, msg, phase="uploading")
                    return

            # ---- GovMap layer extras ---------------------------------------
            # When the scrape produced a geo-layer ScrapeResult (features list
            # populated by govmap_engine.scrape_govmap), upload the lossless
            # GeoJSON as its own CKAN resource via /upload-geojson, and pass
            # the layer caption as `dataset_title_he` so OVER updates the
            # dataset title from "GovMap layer NNNNNN" to the real name.
            geojson_resource_ids: list[str] = []
            dataset_title_he: str | None = None
            features = getattr(result, "features", None) or []
            if features:
                # 1. Resolve the Hebrew layer caption — used both as the
                #    GeoJSON resource name and as the dataset_title_he hint.
                layer_caption = self._resolve_govmap_layer_caption(result)
                if layer_caption:
                    dataset_title_he = layer_caption

                # 2. Serialise the FeatureCollection and upload it.
                try:
                    self.report_progress(task_id, "uploading", 0, 1,
                                         "מעלה GeoJSON...")
                    rid = self._upload_govmap_geojson(
                        tracked_dataset_id=tracked_dataset_id,
                        result=result,
                        layer_caption=layer_caption or "layer",
                    )
                    if rid:
                        geojson_resource_ids.append(rid)
                except Exception as e:
                    # Never fail the task over the secondary GeoJSON path.
                    logger.warning(
                        "GeoJSON upload failed for task %s: %s. "
                        "Continuing with CSV-only publish.", task_id, e
                    )

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
                csv_resource_ids=csv_resource_ids,
                geojson_resource_ids=geojson_resource_ids or None,
                dataset_title_he=dataset_title_he,
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
            _hb_stop.set()
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
        logger.info("=" * 70)
        logger.info("Over.org.il worker starting")
        logger.info("  Server:        %s", SERVER)
        logger.info("  Poll interval: %ds", self.poll_interval)
        logger.info("  Status file:   %s", os.path.abspath(STATUS_FILE))
        logger.info("=" * 70)
        _write_status("idle (just started)")

        idle_polls = 0
        while self._running:
            task = self.poll()
            if task:
                idle_polls = 0
                self.execute_task(task)
                _write_status("idle (between tasks)")
            else:
                idle_polls += 1
                # Print "still alive, waiting" every minute when idle
                if idle_polls * self.poll_interval >= 60 and idle_polls % max(1, 60 // self.poll_interval) == 0:
                    minutes = (idle_polls * self.poll_interval) // 60
                    logger.info("⏸  Idle — no pending tasks (waited %d min so far, polling every %ds)",
                                minutes, self.poll_interval)
                    _write_status(f"idle for {minutes} min — polling every {self.poll_interval}s")
                time.sleep(self.poll_interval)

        logger.info("Worker stopped")
        _write_status("stopped")


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
