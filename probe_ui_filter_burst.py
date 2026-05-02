"""
Phase 0.6 — drive filter changes via UI clicks (SPA fires its own
/deal-data) and see how many succeed before rate limit.

Goal: confirm that 10+ UI-driven filter changes succeed → we can
collect 7000+ deals/settlement on a single IP, no proxy needed.
"""
import base64
import gzip
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ui-burst")

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt,
)

URL = "https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals"


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    captures = []  # all /deal-data responses

    with NadlanBrowser() as nb:
        page = nb._page

        def on_response(resp):
            if "/deal-data" not in resp.url:
                return
            try:
                body = resp.body()
                decoded = json.loads(gzip.decompress(base64.b64decode(body)))
                inner = decoded.get("statusCode")
                data = decoded.get("data") or {}
                rows = len(data.get("items") or [])
                total = data.get("total_rows", 0)
                captures.append({
                    "ts": time.time(),
                    "http": resp.status,
                    "inner": inner,
                    "rows": rows,
                    "total": total,
                })
            except Exception as e:
                captures.append({"ts": time.time(), "http": resp.status,
                                  "err": str(e)})

        page.on("response", on_response)

        log.info("navigating...")
        page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)
        log.info("baseline: %d /deal-data responses captured",
                 len(captures))

        # Cycle through rooms, then sorts, watching each call's status.
        for i, room_label in enumerate(["1 חדרים", "2 חדרים", "3 חדרים",
                                          "4 חדרים", "5 חדרים", "6+ חדרים"]):
            try:
                page.locator("button.roomsBtn").first.click(timeout=5000)
                page.wait_for_timeout(400)
                page.locator(f"button.whomBtn:has-text('{room_label}')").first.click(timeout=5000)
                page.wait_for_timeout(2500)
                last = captures[-1] if captures else {}
                log.info("rooms=%s  →  HTTP %s inner=%s rows=%d total=%s",
                          room_label, last.get("http"), last.get("inner"),
                          last.get("rows", 0), last.get("total"))
            except Exception as e:
                log.warning("rooms %s click failed: %s", room_label, e)

        # Try 4 sort orders (current room filter persists)
        for sort_label in ["מחיר העסקה - סדר יורד",
                            "מחיר העסקה - סדר עולה",
                            "תאריך עסקה - סדר יורד",
                            "תאריך עסקה - סדר עולה"]:
            try:
                page.locator("button.filterBtn:has-text('מיון')").first.click(timeout=5000)
                page.wait_for_timeout(400)
                page.locator(f"button.dropdownBtn:has-text('{sort_label}')").first.click(timeout=5000)
                page.wait_for_timeout(2500)
                last = captures[-1] if captures else {}
                log.info("sort=%s  →  HTTP %s inner=%s rows=%d total=%s",
                          sort_label, last.get("http"), last.get("inner"),
                          last.get("rows", 0), last.get("total"))
            except Exception as e:
                log.warning("sort %s click failed: %s", sort_label, e)

    log.info("\n=== SUMMARY ===")
    log.info("total /deal-data responses: %d", len(captures))
    successes = sum(1 for c in captures if c.get("http") == 200 and c.get("inner") == 200)
    log.info("successful (HTTP 200 + inner 200): %d", successes)
    failures = [c for c in captures if c.get("http") != 200 or c.get("inner") != 200]
    if failures:
        log.info("failures:")
        for f in failures[:5]:
            log.info("  %s", f)
    if successes >= 10:
        log.info("\n🟢 NO RATE LIMIT on UI-driven filters! No proxy needed!")
    elif successes >= 5:
        log.info("\n🟡 partial: rate-limit kicks in mid-burst")
    else:
        log.info("\n🔴 BLOCKED: filter requests count toward the same per-IP limit")


if __name__ == "__main__":
    main()
