"""Scrape one (gush, chelka) parcel and write a CSV.

Usage:  python run_single_parcel.py <gush> <chelka> [output.csv]
"""
import csv
import sys

from bulk_nadlan import DEAL_COLS, META_COLS
from nadlan_api import fetch_parcel_deals


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    gush, chelka = sys.argv[1], sys.argv[2]
    out = sys.argv[3] if len(sys.argv) > 3 else f"{gush}-{chelka}.csv"

    items, meta, warn = fetch_parcel_deals(gush, chelka)
    if warn:
        print(f"  warning: {warn}")

    fieldnames = ["gush", "chelka"] + META_COLS + DEAL_COLS
    meta_row = {
        "meta_setl_name": meta.get("setl_name", ""),
        "meta_neigh_name": meta.get("neigh_name", ""),
        "meta_base_level": meta.get("base_level", ""),
    }

    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for d in items:
            w.writerow({"gush": gush, "chelka": chelka, **meta_row, **d})

    print(f"wrote {len(items)} deals to {out}")


if __name__ == "__main__":
    main()
