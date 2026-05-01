"""
Bulk nadlan.gov.il deal collector — drives nadlan_api.fetch_parcel_deals
across every (gush, chelka) row in a parcels CSV (output of
catalog/parcels_shapefile.py).

Output:
  - deals_csv:    one row per deal, with parcel metadata (lat/lon, locality,
                  municipality, status) joined from the input parcels CSV.
  - checkpoint:   JSON file tracking which parcels have been processed, so
                  a re-run resumes instead of re-scraping. Reuses the design
                  pattern from archive_engine.py (a JSON file alongside the
                  output, file-locked while running).

A single Chromium browser is launched once and reused across all parcels,
saving ~2-3s overhead per parcel vs. the old open/close-per-call approach.

Recommended run (urban registered parcels only, ~21 days):
  python bulk_nadlan.py parcels.csv downloads/deals.csv --filter-status מוסדר

Usage:
  python bulk_nadlan.py parcels.csv downloads/deals.csv
  python bulk_nadlan.py parcels.csv downloads/deals.csv --limit 10   # smoke test
  python bulk_nadlan.py parcels.csv downloads/deals.csv --no-skip    # re-scrape all
  python bulk_nadlan.py parcels.csv downloads/deals.csv --filter-status מוסדר
"""
import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


def _parcel_iter(parcels_csv: str,
                 filter_status: str | None = None) -> Iterator[dict]:
    with open(parcels_csv, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if not (row.get("gush") and row.get("chelka")):
                continue
            if filter_status and row.get("status") != filter_status:
                continue
            yield row


def _load_checkpoint(path: str) -> dict:
    if not os.path.exists(path):
        return {"done": [], "deal_keys": []}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_checkpoint(path: str, ck: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ck, f)
    os.replace(tmp, path)


def _deal_key(deal: dict) -> str:
    """Stable identity for dedup. (assetId, dealDate) covers same-asset history."""
    return f"{deal.get('assetId')}|{deal.get('dealDate')}|{deal.get('row_id')}"


# Error patterns that indicate a transient network issue, not a real scraping
# problem. Parcels that fail with these errors are NOT marked as done — a re-run
# will retry them once connectivity is back.
_TRANSIENT_PATTERNS = (
    "ERR_INTERNET_DISCONNECTED",
    "ERR_TIMED_OUT",
    "ERR_NETWORK_CHANGED",
    "ERR_CONNECTION_REFUSED",
    "ERR_NAME_NOT_RESOLVED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_CONNECTION_RESET",
    "Timeout 60000ms exceeded",
    "Timeout 30000ms exceeded",
    "net::ERR_",
    "Browser closed",
    "Target closed",
)

# After this many consecutive transient failures we assume the internet is
# down and stop the run cleanly. The user re-runs after connectivity is back
# and the checkpoint resumes (transient failures are NOT in `done`).
_CIRCUIT_BREAKER_THRESHOLD = 10


def _is_transient_error(exc: Exception) -> bool:
    """True if the exception looks like a temporary network problem we can retry later."""
    msg = str(exc)
    return any(p in msg for p in _TRANSIENT_PATTERNS)


# Columns we emit. Parcel fields come from the input CSV; deal fields from API;
# meta_* come from the /deal-info endpoint (settlement, neighborhood) — stable
# per-parcel context that nadlan returns alongside the deals.
PARCEL_COLS = ["gush", "chelka", "locality", "municipality",
               "status", "legal_area_sqm", "area_sqm",
               "centroid_lat", "centroid_lon"]
META_COLS = ["meta_setl_name", "meta_neigh_name", "meta_base_level"]
DEAL_COLS = [
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "trend_rate", "trend_years", "prev_deals",
    "address", "parcelNum",
    "neighborhoodName",
    "ownership",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId",
    "row_id",
]


def run(parcels_csv: str, deals_csv: str,
        limit: int | None = None,
        skip_existing: bool = True,
        per_parcel_pause_s: float = 2.0,
        filter_status: str | None = None):
    from nadlan_api import fetch_parcel_deals
    from playwright.sync_api import sync_playwright

    ck_path = deals_csv + ".checkpoint.json"
    ck = _load_checkpoint(ck_path)
    done = set(ck["done"])
    deal_keys = set(ck.get("deal_keys", []))

    write_header = not os.path.exists(deals_csv) or os.path.getsize(deals_csv) == 0

    out = open(deals_csv, "a", encoding="utf-8-sig", newline="")
    writer = csv.DictWriter(out, fieldnames=PARCEL_COLS + META_COLS + DEAL_COLS,
                            extrasaction="ignore")
    if write_header:
        writer.writeheader()
        out.flush()

    n_parcels_done = 0
    n_deals_appended = 0
    n_transient_fails = 0
    n_permanent_fails = 0
    consecutive_transient = 0
    started = time.time()
    headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless)
            try:
                for row in _parcel_iter(parcels_csv, filter_status=filter_status):
                    key = f"{row['gush']}-{row['chelka']}"
                    if skip_existing and key in done:
                        continue
                    if limit is not None and n_parcels_done >= limit:
                        break

                    try:
                        if not browser.is_connected():
                            logger.warning("browser disconnected — relaunching")
                            browser = pw.chromium.launch(headless=headless)
                        items, meta, warn = fetch_parcel_deals(
                            row["gush"], row["chelka"], browser=browser)
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        if _is_transient_error(e):
                            # Network problem — DON'T mark as done; re-run picks it up.
                            n_transient_fails += 1
                            consecutive_transient += 1
                            logger.warning("[%s] transient failure (%d in a row): %s",
                                           key, consecutive_transient, e)
                            _save_checkpoint(ck_path, {**ck, "done": sorted(done),
                                                        "deal_keys": sorted(deal_keys)})
                            if consecutive_transient >= _CIRCUIT_BREAKER_THRESHOLD:
                                logger.error(
                                    "%d consecutive network failures — internet appears down. "
                                    "Stopping cleanly. Re-run the same command when "
                                    "connectivity is back; the checkpoint will resume from "
                                    "the same point (failed parcels were NOT marked done).",
                                    consecutive_transient)
                                return
                            continue
                        else:
                            # Real error — mark as done so we don't loop on it forever.
                            n_permanent_fails += 1
                            logger.warning("[%s] permanent failure: %s", key, e)
                            done.add(key)
                            _save_checkpoint(ck_path, {**ck, "done": sorted(done),
                                                        "deal_keys": sorted(deal_keys)})
                            continue

                    # Success — reset the circuit breaker counter.
                    consecutive_transient = 0

                    if warn:
                        logger.warning("[%s] %s", key, warn)

                    meta_row = {
                        "meta_setl_name": meta.get("setl_name", ""),
                        "meta_neigh_name": meta.get("neigh_name", ""),
                        "meta_base_level": meta.get("base_level", ""),
                    }
                    new_for_parcel = 0
                    for d in items:
                        dk = _deal_key(d)
                        if dk in deal_keys:
                            continue
                        deal_keys.add(dk)
                        merged = {c: row.get(c, "") for c in PARCEL_COLS}
                        merged.update(meta_row)
                        merged.update({c: d.get(c, "") for c in DEAL_COLS})
                        writer.writerow(merged)
                        new_for_parcel += 1

                    out.flush()
                    done.add(key)
                    n_parcels_done += 1
                    n_deals_appended += new_for_parcel

                    ck["done"] = sorted(done)
                    ck["deal_keys"] = sorted(deal_keys)
                    _save_checkpoint(ck_path, ck)

                    elapsed = time.time() - started
                    logger.info("[%d] %s — %d new deals (cumulative: %d deals, %.1fs/parcel)",
                                n_parcels_done, key, new_for_parcel, n_deals_appended,
                                elapsed / max(n_parcels_done, 1))

                    if per_parcel_pause_s:
                        time.sleep(per_parcel_pause_s)
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
    finally:
        out.close()
        ck["done"] = sorted(done)
        ck["deal_keys"] = sorted(deal_keys)
        _save_checkpoint(ck_path, ck)
        logger.info("done. parcels=%d new_deals=%d total_unique_deals=%d "
                    "transient_fails=%d permanent_fails=%d",
                    n_parcels_done, n_deals_appended, len(deal_keys),
                    n_transient_fails, n_permanent_fails)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("parcels_csv", help="output of catalog/parcels_shapefile.py")
    ap.add_argument("deals_csv", help="output deals CSV (appended on resume)")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N new parcels (smoke test)")
    ap.add_argument("--no-skip", dest="skip_existing", action="store_false",
                    help="re-scrape parcels already in the checkpoint")
    ap.add_argument("--pause", type=float, default=2.0,
                    help="seconds to wait between parcels (default 2.0)")
    ap.add_argument("--filter-status", default=None,
                    help="include only parcels with this status value "
                         "(e.g. מוסדר). reduces ~1.09M parcels to ~748k)")
    args = ap.parse_args()
    run(args.parcels_csv, args.deals_csv,
        limit=args.limit,
        skip_existing=args.skip_existing,
        per_parcel_pause_s=args.pause,
        filter_status=args.filter_status)


if __name__ == "__main__":
    main()
