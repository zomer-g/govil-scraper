"""
Bulk nadlan.gov.il deal collector — drives nadlan_api.fetch_parcel_deals
across every (gush, chelka) row in a parcels CSV (output of
catalog/parcels_shapefile.py).

Output:
  - deals_csv:    one row per deal, with parcel metadata (lat/lon, locality)
                  joined from the input parcels CSV.
  - checkpoint:   JSON file tracking which parcels have been processed, so
                  a re-run resumes instead of re-scraping. Reuses the design
                  pattern from archive_engine.py (a JSON file alongside the
                  output, file-locked while running).

The actual API call rate is bounded by Playwright (each parcel = one navigation
roughly 8-15 s with a visible Chromium). A full run over Israel's ~250k parcels takes
days and is meant to be left running unattended; the checkpoint makes it safe
to interrupt and resume.

Usage:
  python bulk_nadlan.py parcels.csv deals.csv
  python bulk_nadlan.py parcels.csv deals.csv --limit 100      # dry-run smoke test
  python bulk_nadlan.py parcels.csv deals.csv --skip-existing  # default: skip
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


def _parcel_iter(parcels_csv: str) -> Iterator[dict]:
    with open(parcels_csv, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("gush") and row.get("chelka"):
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


# Columns we emit. Parcel fields come from the input CSV; deal fields from API;
# meta_* come from the /deal-info endpoint (settlement, neighborhood) — stable
# per-parcel context that nadlan returns alongside the deals.
PARCEL_COLS = ["gush", "chelka", "locality", "area_sqm",
               "centroid_lat", "centroid_lon"]
META_COLS = ["meta_setl_name", "meta_neigh_name", "meta_base_level"]
DEAL_COLS = [
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "address", "parcelNum",
    "neighborhoodName",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId",
    "row_id",
]


def run(parcels_csv: str, deals_csv: str,
        limit: int | None = None,
        skip_existing: bool = True,
        per_parcel_pause_s: float = 2.0):
    from nadlan_api import fetch_parcel_deals

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
    started = time.time()

    try:
        for row in _parcel_iter(parcels_csv):
            key = f"{row['gush']}-{row['chelka']}"
            if skip_existing and key in done:
                continue
            if limit is not None and n_parcels_done >= limit:
                break

            try:
                items, meta, warn = fetch_parcel_deals(row["gush"], row["chelka"])
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.warning("nadlan fetch failed for %s: %s", key, e)
                done.add(key)  # mark to avoid retry storms; user can clear checkpoint
                continue

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
        out.close()
        ck["done"] = sorted(done)
        ck["deal_keys"] = sorted(deal_keys)
        _save_checkpoint(ck_path, ck)
        logger.info("done. parcels=%d new_deals=%d total_unique_deals=%d",
                    n_parcels_done, n_deals_appended, len(deal_keys))


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
    args = ap.parse_args()
    run(args.parcels_csv, args.deals_csv,
        limit=args.limit,
        skip_existing=args.skip_existing,
        per_parcel_pause_s=args.pause)


if __name__ == "__main__":
    main()
