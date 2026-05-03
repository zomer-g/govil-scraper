"""
Distributed nadlan slice-level worker.

Sister to ``nadlan_settlement_worker.py`` but pulls SMALLER slices —
each task = (settlement, room_filter, sort_order) → one /deal-data
call returning ≤500 deals via UI clicks.

Why slices: nadlan rate-limits the /deal-data endpoint when fetch_number
is incremented (pagination beyond page 2 = blocked). Filter-based clicks
on `room_num` and `type_order` don't trigger the same limit, so we
expand coverage from 1000 → 7000 deals/settlement (or more with date
filters) on a single IP.

Quick start::

    # Server (admin) — seed the slice queue once
    curl -X POST -H "X-Worker-Key: $WORKER_API_KEY" \\
         https://server/api/nadlan/slice-seed

    # Each worker machine
    python nadlan_slice_worker.py --server $SERVER --worker-id $(hostname)

The worker claims a batch of slices for one settlement at a time so it
can reuse a single browser session for all of that settlement's slices
(big efficiency win — one navigation, many UI clicks).
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
    "room_no_response", "sort_no_response",
)
# Permanent failures — never retry. The 'room=None' slices in the seeded
# queue will hit this because "כל החדרים" is the dropdown toggle, not a
# clickable option.
_PERMANENT_PATTERNS = (
    "room_click_failed",
    "sort_click_failed",
)


def _classify_failure(err: str) -> tuple[bool, bool]:
    """Returns (is_failure, is_transient)."""
    s = str(err)
    if any(p in s for p in _PERMANENT_PATTERNS):
        return True, False
    if any(p in s for p in _TRANSIENT_PATTERNS):
        return True, True
    return True, True  # Default to transient for unknown errors


def _is_transient(e: BaseException) -> bool:
    msg = str(e)
    return any(p in msg for p in _TRANSIENT_PATTERNS)


class NadlanSliceClient:
    def __init__(self, server_url: str, worker_id: str, timeout: int = 60):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id
        self.timeout = timeout
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.server_url}{path}"

    def claim(self, count: int = 14) -> list[dict]:
        r = self.session.post(
            self._url("/api/nadlan/slice-claim"),
            json={"worker_id": self.worker_id, "count": count},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json().get("tasks", [])

    def upload_result(self, slice_key: str, rows: list[dict],
                      total_rows: int) -> dict:
        files = None
        if rows:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=DEAL_COL_ORDER,
                                extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
            files = {"file": (f"slice_{slice_key.replace(':', '_')}.csv",
                              buf.getvalue(), "text/csv; charset=utf-8")}
        data = {
            "worker_id": self.worker_id,
            "deals_count": len(rows),
            "total_rows": str(total_rows),
        }
        r = self.session.post(
            self._url(f"/api/nadlan/slice-result/{slice_key}"),
            files=files, data=data, timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def report_failure(self, slice_key: str, error: str, transient: bool):
        try:
            self.session.post(
                self._url(f"/api/nadlan/slice-fail/{slice_key}"),
                data={
                    "worker_id": self.worker_id,
                    "error": str(error)[:500],
                    "transient": "true" if transient else "false",
                },
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            logger.warning("could not report failure for slice %s: %s",
                           slice_key, e)


def _flatten_deal_for_db(deal: dict, setl_code: str, setl_name: str,
                          worker_id: str) -> dict:
    """Match the column layout the server's append_deals expects."""
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
        "meta_base_level": "slice",
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
        "trend_rate": "", "trend_years": "", "prev_deals": "",
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


def _group_slices_by_settlement(tasks: list[dict]) -> dict[str, list[dict]]:
    """Group task dicts by setl_code so we can scrape one settlement
    in one browser session (multiple UI clicks)."""
    out: dict[str, list[dict]] = {}
    for t in tasks:
        out.setdefault(str(t["setl_code"]), []).append(t)
    return out


def run(server_url: str, worker_id: str,
        idle_sleep_s: int = 60,
        per_settlement_pause_s: float = 5.0,
        slices_per_claim: int = 14,
        max_consecutive_failed_settlements: int = 5):
    """Main loop: claim batch of slices → group by settlement → scrape
    each settlement in one browser session → upload → repeat."""
    from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser

    client = NadlanSliceClient(server_url, worker_id)
    consecutive_failed = 0
    n_done = 0
    n_deals = 0
    headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"

    logger.info("slice worker '%s' starting against %s",
                worker_id, server_url)

    with NadlanBrowser(headless=headless) as nb:
        while True:
            try:
                tasks = client.claim(count=slices_per_claim)
            except requests.RequestException as e:
                logger.warning("claim failed: %s", e)
                time.sleep(idle_sleep_s)
                continue

            if not tasks:
                logger.info("no pending slices; sleeping %ds", idle_sleep_s)
                time.sleep(idle_sleep_s)
                continue

            grouped = _group_slices_by_settlement(tasks)
            for setl_code, slice_tasks in grouped.items():
                setl_name = slice_tasks[0].get("setl_name") or ""
                t0 = time.time()
                slices = [{
                    "room_filter": t.get("room_filter"),
                    "sort_order": t.get("sort_order"),
                    "slice_key": t.get("slice_key"),
                } for t in slice_tasks]

                try:
                    results = nb.fetch_settlement_slices(setl_code, slices)
                except Exception as e:
                    transient = _is_transient(e)
                    logger.warning("setl %s entire fetch failed: %s "
                                   "(transient=%s)", setl_code, e, transient)
                    for t in slice_tasks:
                        client.report_failure(t["slice_key"], str(e), transient)
                    consecutive_failed += 1
                    if consecutive_failed >= max_consecutive_failed_settlements:
                        logger.error("too many consecutive failed settlements "
                                      "(%d) — IP likely blocked, stopping",
                                      consecutive_failed)
                        return
                    continue

                # Group: did this settlement produce ANY deals?
                any_success = any(r.get("error") is None for r in results)
                if any_success:
                    consecutive_failed = 0
                else:
                    consecutive_failed += 1

                # Per-slice upload
                slice_n_deals = 0
                for r in results:
                    sk = r.get("slice_key")
                    if not sk:
                        continue
                    err = r.get("error")
                    if err:
                        _, transient = _classify_failure(err)
                        client.report_failure(sk, err, transient)
                        continue
                    deals = r.get("deals") or []
                    flat = [_flatten_deal_for_db(d, setl_code, setl_name,
                                                   worker_id) for d in deals]
                    try:
                        client.upload_result(sk, flat,
                                              total_rows=r.get("total_rows", 0))
                        slice_n_deals += len(deals)
                    except requests.RequestException as e:
                        logger.warning("upload failed for slice %s: %s", sk, e)
                        client.report_failure(sk, str(e), True)

                n_done += 1
                n_deals += slice_n_deals
                elapsed = time.time() - t0
                logger.info("setl %s [%s]: %d slices, %d deals total (%.1fs). "
                             "Worker totals: %d settlements, %d deals.",
                             setl_code, setl_name, len(results),
                             slice_n_deals, elapsed, n_done, n_deals)

                if consecutive_failed >= max_consecutive_failed_settlements:
                    logger.error("too many consecutive failed settlements "
                                  "(%d) — IP likely blocked, stopping",
                                  consecutive_failed)
                    return

                time.sleep(per_settlement_pause_s)


def main():
    parser = argparse.ArgumentParser(description="nadlan slice worker")
    parser.add_argument("--server", required=True,
                         help="Server URL, e.g. https://your-app.onrender.com")
    parser.add_argument("--worker-id", default=socket.gethostname(),
                         help="Worker identifier (default: hostname)")
    parser.add_argument("--idle-sleep", type=int, default=60,
                         help="Seconds to sleep when no slices available")
    parser.add_argument("--per-settlement-pause", type=float, default=5.0,
                         help="Seconds between settlements")
    parser.add_argument("--slices-per-claim", type=int, default=14,
                         help="Slices to claim per round (=1 settlement default)")
    parser.add_argument("--max-failed", type=int, default=5,
                         help="Stop after N consecutive failed settlements")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(),
                         format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sys.stdout.reconfigure(encoding="utf-8")

    run(args.server, args.worker_id,
        idle_sleep_s=args.idle_sleep,
        per_settlement_pause_s=args.per_settlement_pause,
        slices_per_claim=args.slices_per_claim,
        max_consecutive_failed_settlements=args.max_failed)


if __name__ == "__main__":
    main()
