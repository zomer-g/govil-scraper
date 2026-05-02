"""
Smoke test for the settlement-level scraper with full pagination.

Run this on an unblocked IP to verify:
  1. Chrome + warmup get past token-verify
  2. fetch_number=1 returns 500 deals + total_fetch
  3. JWT re-signing for fetch_number=2..N actually works (hardest part)
  4. Total deals returned ≈ total_rows reported by API

Default test target: גילון (setl_code=68) — small enough to scrape fully
in a minute, big enough that total_fetch > 1.

Override:
    python test_settlement_pagination.py 5000   # Tel Aviv (huge — ~76 pages)
"""
import logging
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser


def main():
    setl_code = sys.argv[1] if len(sys.argv) > 1 else "68"
    sys.stdout.reconfigure(encoding="utf-8")
    print(f"=== testing settlement {setl_code} ===")
    t0 = time.time()
    with NadlanBrowser() as nb:
        deals = nb.fetch_settlement(setl_code)
    elapsed = time.time() - t0
    print(f"\n✓ done in {elapsed:.1f}s")
    print(f"  deals returned: {len(deals)}")
    if deals:
        # Sanity-check unique deal keys
        keys = set()
        dups = 0
        for d in deals:
            k = (d.get("assetId"), d.get("dealDate"), d.get("row_id"))
            if k in keys:
                dups += 1
            keys.add(k)
        print(f"  unique by (assetId,dealDate,row_id): {len(keys)}")
        print(f"  duplicates: {dups}")
        # Show date range
        dates = sorted(d.get("dealDate", "") for d in deals if d.get("dealDate"))
        if dates:
            print(f"  earliest deal: {dates[0]}")
            print(f"  latest deal:   {dates[-1]}")
        # Sample first/last deal
        first = deals[0]
        last = deals[-1]
        print(f"  first: {first.get('dealDate')} {first.get('dealAmount')} @ {first.get('address')}")
        print(f"  last:  {last.get('dealDate')} {last.get('dealAmount')} @ {last.get('address')}")
    else:
        print("  ✗ NO DEALS — likely IP blocked or pagination failed")


if __name__ == "__main__":
    main()
