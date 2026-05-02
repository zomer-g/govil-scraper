"""End-to-end test of fetch_settlement_slices on Tel Aviv (setl=5000)."""
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    # Order slices so room changes between every slice (no same-value
    # re-clicks which the SPA treats as no-ops).
    # Rooms cycle 1,2,3,4,5,6plus then repeat. Sort alternates per cycle.
    rooms = ["1", "2", "3", "4", "5", "6plus"]
    slices = []
    for sort in ["dealDate_down", "dealDate_up"]:
        for room in rooms:
            slices.append({"room_filter": room, "sort_order": sort})

    print(f"=== fetch_settlement_slices on Tel Aviv ({len(slices)} slices) ===")
    t0 = time.time()
    with NadlanBrowser() as nb:
        results = nb.fetch_settlement_slices("5000", slices)
    elapsed = time.time() - t0

    print(f"\n done in {elapsed:.1f}s ({elapsed/len(slices):.1f}s/slice avg)")
    print(f"\n--- per-slice ---")
    all_deals_keys = set()
    for r in results:
        room = r.get("room_filter") or "all"
        sort = r.get("sort_order")
        deals = r.get("deals") or []
        total = r.get("total_rows", 0)
        err = r.get("error")
        print(f"  room={room:6} sort={sort:18} → {len(deals):4d} deals "
              f"(total={total:>6}) err={err}")
        for d in deals:
            all_deals_keys.add((d.get("assetId"), d.get("dealDate")))

    print(f"\n--- unique deals across all slices: {len(all_deals_keys)} ---")
    successes = sum(1 for r in results if not r.get("error"))
    print(f"\n--- {successes}/{len(results)} slices succeeded ---")


if __name__ == "__main__":
    main()
