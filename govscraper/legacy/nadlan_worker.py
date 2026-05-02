"""Distributed bulk-nadlan worker.

DEPRECATED entry point — prefer:
    python -m govscraper.cli worker --source nadlan-queue --server <URL>

Each instance polls the central server for parcel tasks, scrapes them via
``nadlan_api.fetch_parcel_deals``, and uploads the deals back. The server
is the single source of truth (see nadlan_api_routes.py + storage.py).
The unified CLI uses NadlanQueueSource which wraps the NadlanWorkerClient
class defined here, so the on-the-wire behaviour is identical.

Quick start::

    # Server (admin) — enqueue 748k urban parcels once
    curl -F file=@parcels.csv -F filter_status=מוסדר \
         https://server/api/nadlan/bulk-queue

    # Each worker machine
    export NADLAN_SERVER_URL=https://govil-scraper.onrender.com
    export NADLAN_WORKER_ID=$(hostname)
    python nadlan_worker.py

The worker uses the same circuit-breaker logic as bulk_nadlan.py: after 10
consecutive transient (network) failures it sleeps and retries; the server
returns the task to pending automatically via the stale-task reset.
"""
import argparse
import csv
import io
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# These columns must match nadlan_api_routes._DEALS_CSV_HEADER deal section.
DEAL_FIELDS = [
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "trend_rate", "trend_years", "prev_deals",
    "address", "parcelNum", "neighborhoodName", "ownership",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId", "row_id",
]

PARCEL_FIELDS = [
    "gush", "chelka", "locality", "municipality",
    "status", "legal_area_sqm", "area_sqm",
    "centroid_lat", "centroid_lon",
]

META_FIELDS = ["meta_setl_name", "meta_neigh_name", "meta_base_level"]

# Network errors that should NOT mark a parcel as permanently failed.
_TRANSIENT_PATTERNS = (
    "ERR_INTERNET_DISCONNECTED", "ERR_TIMED_OUT", "ERR_NETWORK_CHANGED",
    "ERR_CONNECTION_REFUSED", "ERR_NAME_NOT_RESOLVED",
    "ERR_PROXY_CONNECTION_FAILED", "ERR_CONNECTION_RESET",
    "Timeout 60000ms exceeded", "Timeout 30000ms exceeded",
    "net::ERR_", "Browser closed", "Target closed",
)


def _is_transient(exc: BaseException) -> bool:
    msg = str(exc)
    return any(p in msg for p in _TRANSIENT_PATTERNS)


class NadlanWorkerClient:
    """Thin HTTP client for the /api/nadlan/bulk-* endpoints."""

    def __init__(self, server_url: str, worker_id: str, timeout: int = 30):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id
        self.timeout = timeout
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.server_url}{path}"

    def claim(self, count: int = 1) -> list[dict]:
        r = self.session.post(
            self._url("/api/nadlan/bulk-claim"),
            json={"worker_id": self.worker_id, "count": count},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json().get("tasks", [])

    def upload_result(self, parcel_id: str, rows: list[dict]) -> dict:
        """Upload deals for one parcel; server appends + marks task done."""
        # Even with 0 deals we must call this so the task is marked done.
        if rows:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()),
                               extrasaction="ignore")
            w.writeheader()
            for row in rows:
                w.writerow(row)
            files = {"file": (f"{parcel_id}.csv", buf.getvalue(),
                              "text/csv; charset=utf-8")}
        else:
            files = None

        r = self.session.post(
            self._url(f"/api/nadlan/bulk-result/{parcel_id}"),
            files=files,
            data={"worker_id": self.worker_id, "deals_count": len(rows)},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def report_failure(self, parcel_id: str, error: str,
                       transient: bool) -> None:
        try:
            self.session.post(
                self._url(f"/api/nadlan/bulk-fail/{parcel_id}"),
                data={
                    "worker_id": self.worker_id,
                    "error": str(error)[:500],
                    "transient": "true" if transient else "false",
                },
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            # Reporting the failure also failed; nothing we can do but log it.
            logger.warning("could not report failure for %s: %s", parcel_id, e)

    def status(self) -> dict:
        r = self.session.get(self._url("/api/nadlan/bulk-status"),
                             timeout=self.timeout)
        r.raise_for_status()
        return r.json()


def _build_deal_row(task: dict, deal: dict, meta: dict, worker_id: str) -> dict:
    """Merge parcel context + meta + deal into the row layout the server expects."""
    row = {f: task.get(f, "") for f in PARCEL_FIELDS}
    # status is named status_text in the DB but status in the CSV
    row["status"] = task.get("status_text") or task.get("status") or ""
    row["meta_setl_name"]  = meta.get("setl_name", "")
    row["meta_neigh_name"] = meta.get("neigh_name", "")
    row["meta_base_level"] = meta.get("base_level", "")
    for f in DEAL_FIELDS:
        row[f] = deal.get(f, "")
    row["worker_id"] = worker_id
    row["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return row


def run(server_url: str, worker_id: str,
        idle_sleep_s: int = 30,
        max_consecutive_transient: int = 10,
        backoff_on_outage_s: int = 120,
        per_parcel_pause_s: float = 10.0):
    """Main worker loop.

    - Polls server for tasks; on empty response, sleeps and retries.
    - Scrapes each task; on success, uploads deals.
    - Tracks consecutive transient failures for the circuit breaker.
    - On circuit-breaker trip (e.g. internet outage), backs off for
      ``backoff_on_outage_s`` seconds and resumes — the server already
      returned the in-flight tasks to pending via stale-task reset.
    """
    from playwright.sync_api import sync_playwright
    from govscraper.scrapers.nadlan.legacy_api import (
        fetch_parcel_deals, _launch_browser,
    )

    client = NadlanWorkerClient(server_url, worker_id)
    consecutive_transient = 0
    n_done = 0
    n_deals = 0
    headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"

    logger.info("nadlan worker '%s' starting against %s", worker_id, server_url)

    with sync_playwright() as pw:
        browser = _launch_browser(pw, headless=headless)
        try:
            while True:
                # Fetch one task. (We do one at a time so the stale-task reset
                # only loses one task's worth of work on a worker crash.)
                try:
                    tasks = client.claim(count=1)
                except requests.RequestException as e:
                    logger.warning("claim failed (server unreachable?): %s", e)
                    time.sleep(idle_sleep_s)
                    continue

                if not tasks:
                    logger.info("no pending tasks; sleeping %ds", idle_sleep_s)
                    time.sleep(idle_sleep_s)
                    continue

                task = tasks[0]
                parcel_id = task["parcel_id"]
                gush = task["gush"]
                chelka = task["chelka"]

                try:
                    # Fresh Chrome per parcel.  Sharing one browser across
                    # many parcels caused reCAPTCHA Enterprise to score
                    # subsequent contexts low — token-verify rejected and
                    # every parcel returned items=[]. Spawning a new Chrome
                    # each time costs ~2s but matches what run_single_parcel.py
                    # does (separate process per call) and reliably succeeds.
                    if browser is not None:
                        try:
                            browser.close()
                        except Exception:
                            pass
                    browser = _launch_browser(pw, headless=headless)
                    items, meta, warn = fetch_parcel_deals(
                        gush, chelka, browser=browser)
                except KeyboardInterrupt:
                    # Release the in-flight task so another worker (or a
                    # restart) picks it up promptly.
                    client.report_failure(parcel_id, "KeyboardInterrupt",
                                          transient=True)
                    raise
                except Exception as e:
                    transient = _is_transient(e)
                    client.report_failure(parcel_id, str(e), transient=transient)
                    if transient:
                        consecutive_transient += 1
                        logger.warning("[%s] transient failure (%d in a row): %s",
                                       parcel_id, consecutive_transient, e)
                        if consecutive_transient >= max_consecutive_transient:
                            logger.error(
                                "%d consecutive network failures — backing off "
                                "for %ds. Server will return claimed tasks to "
                                "pending automatically.",
                                consecutive_transient, backoff_on_outage_s)
                            time.sleep(backoff_on_outage_s)
                            consecutive_transient = 0
                    else:
                        logger.warning("[%s] permanent failure: %s", parcel_id, e)
                    continue

                # Success — clear circuit-breaker counter
                consecutive_transient = 0
                if warn:
                    logger.warning("[%s] %s", parcel_id, warn)

                rows = [_build_deal_row(task, d, meta, worker_id) for d in items]
                try:
                    client.upload_result(parcel_id, rows)
                    n_done += 1
                    n_deals += len(rows)
                    logger.info("[%s] %d deals uploaded (cumulative: %d parcels, %d deals)",
                                parcel_id, len(rows), n_done, n_deals)
                except requests.RequestException as e:
                    # Server didn't accept the upload — treat as transient and
                    # let the stale-task reset return the task to pending.
                    logger.warning("upload failed for %s: %s", parcel_id, e)
                    client.report_failure(parcel_id, f"upload error: {e}",
                                          transient=True)

                # Throttle: sleeping a few seconds between parcels keeps
                # nadlan.gov.il's reCAPTCHA Enterprise from flagging the IP.
                # Without this, 25-30 rapid scrapes started rejecting every
                # /token-verify with HTTP 400, returning items=[] from then on.
                if per_parcel_pause_s > 0:
                    time.sleep(per_parcel_pause_s)
        finally:
            try:
                browser.close()
            except Exception:
                pass


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--server", default=os.environ.get("NADLAN_SERVER_URL", ""),
                    help="server base URL (env NADLAN_SERVER_URL)")
    ap.add_argument("--worker-id",
                    default=os.environ.get("NADLAN_WORKER_ID", socket.gethostname()),
                    help="identifier for this worker (default: hostname)")
    ap.add_argument("--idle-sleep", type=int, default=30,
                    help="seconds to sleep when no tasks are available")
    ap.add_argument("--pause", type=float, default=10.0,
                    help="seconds to wait between parcels (default 10.0). "
                         "Lower values risk reCAPTCHA Enterprise flagging "
                         "the IP and rejecting all /token-verify requests "
                         "after ~80 successful scrapes. Empirically 10s is "
                         "the sustained-throughput sweet spot.")
    args = ap.parse_args()
    if not args.server:
        ap.error("--server or NADLAN_SERVER_URL is required")
    run(args.server.rstrip("/"), args.worker_id,
        idle_sleep_s=args.idle_sleep,
        per_parcel_pause_s=args.pause)


if __name__ == "__main__":
    main()
