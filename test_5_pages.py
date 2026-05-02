"""Limit pagination to 5 pages on Tel Aviv to speed up iteration."""
import logging
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

import govscraper.scrapers.nadlan.legacy_incremental as M

# Patch _paginate_remaining to stop after 4 paginated pages (= 2500 deals total)
_orig = M.NadlanBrowser._paginate_remaining

def _patched(self, setl_code, captured_request, total_fetch, deals, seen):
    capped = min(total_fetch, 5)
    return _orig(self, setl_code, captured_request, capped, deals, seen)

M.NadlanBrowser._paginate_remaining = _patched

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    print("=== test 5-page Tel Aviv ===")
    t0 = time.time()
    with NadlanBrowser() as nb:
        deals = nb.fetch_settlement("5000")
    print(f"\n done in {time.time() - t0:.1f}s")
    print(f"deals returned: {len(deals)}")


if __name__ == "__main__":
    main()
