"""
Distributed nadlan settlement-level worker.

Sister to ``nadlan_worker.py`` (per-parcel) but pulls FULL settlement scrapes
— each task = 1 settlement = up to thousands of deals via JWT pagination.

Throughput (server-side coverage):
  * 1,509 settlements total
  * ~1-2 hours of total deal-data calls (sum of total_fetch across all)
  * with rate-limit recovery: 1.5-3 days end-to-end on a single IP

Quick start::

    # Server (admin) — seed the catalog once
    curl -X POST -H "X-Worker-Key: $WORKER_API_KEY" \
         https://server/api/nadlan/settlement-seed

    # Each worker machine
    python nadlan_settlement_worker.py --server $SERVER --worker-id $(hostname)

The worker uses the same circuit-breaker logic as nadlan_worker.py — five
consecutive BLOCKED settlements stop it cleanly so the operator can rotate
the IP and re-launch.
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

import requests

logger = logging.getLogger(__name__)


# Same field layout as the parcel-level deals (matches PgStore.append_deals).
DEAL_COL_ORDER = [
    "gush", "chelka", "locality", "municipality",
    "status", "legal_area_sqm", "area_sqm",
    "centroid_lat", "centroid_lon",
    "meta_setl_name", "meta_neigh_name", "meta_base_level",
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "trend_rate", "trend_years", "prev_deals",
    "address", "parcelNum", "neighborhoodName", "ownership",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId", "row_id",
    "worker_id", "scraped_at",
]


_TRANSIENT_PATTERNS = (
    "ERR_INTERNET_DISCONNECTED", "ERR_TIMED_OUT", "ERR_NETWORK_CHANGED",
    "ERR_CONNECTION_REFUSED", "ERR_NAME_NOT_RESOLVED",
    "ERR_PROXY_CONNECTION_FAILED", "ERR_CONNECTION_RESET",
    "Timeout 60000ms exceeded", "Timeout 30000ms exceeded",
    "net::ERR_", "Browser closed", "Target closed",
)


def _is_transient(e: BaseException) -> bool:
    msg = str(e)
    return any(p in msg for p in _TRANSIENT_PATTERNS)


class NadlanSettlementClient:
    def __init__(self, server_url: str, worker_id: str, timeout: int = 60):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id
        self.timeout = timeout
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.server_url}{path}"

    def claim(self, count: int = 1) -> list[dict]:
        r = self.session.post(
            self._url("/api/nadlan/settlement-claim"),
            json={"worker_id": self.worker_id, "count": count},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json().get("tasks", [])

    def upload_result(self, setl_code: str, rows: list[dict],
                      total_fetch: int) -> dict:
        # Settlement scrapes can produce thousands of rows — chunk if huge.
        files = None
        if rows:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=DEAL_COL_ORDER,
                                extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
            files = {"file": (f"setl_{setl_code}.csv", buf.getvalue(),
                              "text/csv; charset=utf-8")}
        data = {
            "worker_id": self.worker_id,
            "deals_count": len(rows),
            "total_fetch": str(total_fetch),
        }
        r = self.session.post(
            self._url(f"/api/nadlan/settlement-result/{setl_code}"),
            files=files, data=data, timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def report_failure(self, setl_code: str, error: str, transient: bool):
        try:
            self.session.post(
                self._url(f"/api/nadlan/settlement-fail/{setl_code}"),
                data={
                    "worker_id": self.worker_id,
                    "error": str(error)[:500],
                    "transient": "true" if transient else "false",
                },
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            logger.warning("could not report failure for setl %s: %s",
                           setl_code, e)


def _flatten_deal_for_db(deal: dict, setl_code: str, setl_name: str,
                          worker_id: str) -> dict:
    """Match the column layout the server's append_deals expects.

    Settlement-level scrapes have less per-parcel context than per-parcel
    scrapes — we leave municipality/centroid/area_sqm blank and fill them
    in later from parcels.csv if needed.
    """
    parcel_num = deal.get("parcelNum") or ""
    parts = parcel_num.split("-") if parcel_num else []
    gush = parts[0] if len(parts) >= 1 else ""
    chelka = parts[1] if len(parts) >= 2 else ""
    return {
        "gush": gush, "chelka": chelka,
        "locality": setl_name,
        "municipality": "", "status": "",
        "legal_area_sqm": "", "area_sqm": "",
        "centroid_lat": "", "centroid_lon": "",
        "meta_setl_name": setl_name,
        "meta_neigh_name": deal.get("neighborhoodName") or "",
        "meta_base_level": "settlement",
        "dealDate": deal.get("dealDate") or "",
        "dealAmount": str(deal.get("dealAmount") or ""),
        "priceSM": str(deal.get("priceSM") or ""),
        "roomNum": str(deal.get("roomNum") or ""),
        "floor": str(deal.get("floor") or ""),
        "assetArea": str(deal.get("assetArea") or ""),
        "yearBuilt": str(deal.get("yearBuilt") or ""),
        "buildingFloors": str(deal.get("buildingFloors") or ""),
        "dealNature": deal.get("dealNature") or "",
        "hokHamecher": str(deal.get("hokHamecher") or ""),
        "trend_rate": "",  # not reported by settlement endpoint
        "trend_years": "",
        "prev_deals": "",
        "address": deal.get("address") or "",
        "parcelNum": parcel_num,
        "neighborhoodName": deal.get("neighborhoodName") or "",
        "ownership": "",
        "assetId": str(deal.get("assetId") or ""),
        "addressId": str(deal.get("addressId") or ""),
        "polygonId": str(deal.get("polygonId") or ""),
        "streetCode": str(deal.get("streetCode") or ""),
        "settlmentID": setl_code,
        "neighborhoodId": str(deal.get("neighborhoodId") or ""),
        "row_id": str(deal.get("row_id") or ""),
        "worker_id": worker_id,
        "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def run(server_url: str, worker_id: str,
        idle_sleep_s: int = 60,
        per_settlement_pause_s: float = 30.0,
        max_consecutive_blocked: int = 5):
    """Main loop: claim → scrape → upload → repeat."""
    from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser

    client = NadlanSettlementClient(server_url, worker_id)
    consecutive_blocked = 0
    n_done = 0
    n_deals = 0
    headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"

    logger.info("settlement worker '%s' starting against %s",
                worker_id, server_url)

    # One persistent NadlanBrowser session — its reCAPTCHA stays warm
    # across many settlements (huge throughput win vs per-call browsers).
    with NadlanBrowser(headless=headless) as nb:
        while True:
            try:
                tasks = client.claim(count=1)
            except requests.RequestException as e:
                logger.warning("claim failed: %s", e)
                time.sleep(idle_sleep_s)
                continue

            if not tasks:
                logger.info("no pending settlements; sleeping %ds", idle_sleep_s)
                time.sleep(idle_sleep_s)
                continue

            task = tasks[0]
            setl_code = task["setl_code"]
            setl_name = task.get("setl_name") or ""

            logger.info("[setl %s '%s'] starting scrape", setl_code, setl_name)
            t0 = time.time()
            try:
                deals = nb.fetch_settlement(setl_code)
            except KeyboardInterrupt:
                client.report_failure(setl_code, "KeyboardInterrupt",
                                      transient=True)
                raise
            except Exception as e:
                transient = _is_transient(e)
                client.report_failure(setl_code, str(e), transient=transient)
                logger.warning("[setl %s] %s failure: %s", setl_code,
                               "transient" if transient else "permanent", e)
                if transient:
                    time.sleep(per_settlement_pause_s)
                continue
            elapsed = time.time() - t0

            # Heuristic for "we got blocked": expected lots of deals (big
            # settlement) but got 0. The legacy_incremental retry already
            # tries to refresh recaptcha — if it still gives 0, we assume
            # the IP is on the bot list.
            if not deals:
                consecutive_blocked += 1
                logger.warning("[setl %s] 0 deals returned (%d consecutive). "
                               "Likely BLOCKED.", setl_code, consecutive_blocked)
                client.report_failure(setl_code,
                                       "0 deals — possible block",
                                       transient=True)
                if consecutive_blocked >= max_consecutive_blocked:
                    logger.error(
                        "%d settlements in a row returned 0 deals — IP appears "
                        "rate-limited. Stopping cleanly. Rotate IP and "
                        "re-launch the worker.", consecutive_blocked)
                    return
                time.sleep(per_settlement_pause_s)
                continue
            consecutive_blocked = 0

            # Convert deals → flat rows for upload.
            rows = [_flatten_deal_for_db(d, setl_code, setl_name, worker_id)
                    for d in deals]
            try:
                client.upload_result(setl_code, rows, total_fetch=0)
                n_done += 1
                n_deals += len(rows)
                logger.info("[setl %s '%s'] %d deals uploaded in %.1fs "
                            "(cumulative: %d settlements, %d deals)",
                            setl_code, setl_name, len(rows), elapsed,
                            n_done, n_deals)
            except requests.RequestException as e:
                logger.warning("upload failed for setl %s: %s", setl_code, e)
                client.report_failure(setl_code, f"upload error: {e}",
                                       transient=True)

            time.sleep(per_settlement_pause_s)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--server", default=os.environ.get("NADLAN_SERVER_URL", ""))
    ap.add_argument("--worker-id",
                    default=os.environ.get("NADLAN_WORKER_ID", socket.gethostname()))
    ap.add_argument("--idle-sleep", type=int, default=60)
    ap.add_argument("--pause", type=float, default=30.0,
                    help="seconds to wait between settlements (default 30 — "
                         "bigger than per-parcel because each settlement is "
                         "many deal-data calls already)")
    args = ap.parse_args()
    if not args.server:
        ap.error("--server or NADLAN_SERVER_URL is required")
    run(args.server.rstrip("/"), args.worker_id,
        idle_sleep_s=args.idle_sleep,
        per_settlement_pause_s=args.pause)


if __name__ == "__main__":
    main()
