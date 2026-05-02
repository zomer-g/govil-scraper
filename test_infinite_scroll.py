"""
Probe whether the deals page uses infinite scroll: scroll to bottom
repeatedly and watch for new /deal-data + /token-verify pairs.
"""
import logging
import sys
import time

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scroll")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    captured = []

    with NadlanBrowser() as nb:
        page = nb._page

        def on_request(req):
            if "/deal-data" in req.url or "/token-verify" in req.url:
                kind = "deal-data" if "/deal-data" in req.url else "token-verify"
                captured.append({"kind": kind, "ts": time.time(), "url": req.url})
                log.info("REQ #%d [%s]", len(captured), kind)

        page.on("request", on_request)
        log.info("Navigating to Tel Aviv deals…")
        page.goto(
            "https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
            wait_until="domcontentloaded", timeout=60_000,
        )
        page.wait_for_timeout(5000)
        log.info("After initial load: %d requests", len(captured))

        log.info("Beginning scroll loop (10 iterations)…")
        for i in range(10):
            before = len(captured)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2500)
            new = len(captured) - before
            log.info("Scroll #%d: +%d requests (total %d)", i + 1, new, len(captured))
            if new == 0:
                # Try alternate scroll on inner containers
                page.evaluate("""
                    () => {
                        for (const el of document.querySelectorAll('*')) {
                            if (el.scrollHeight > el.clientHeight + 50) {
                                el.scrollTop = el.scrollHeight;
                            }
                        }
                    }
                """)
                page.wait_for_timeout(2500)
                more = len(captured) - before
                log.info("  inner-scroll fallback: +%d", more)

    log.info("\n=== SUMMARY ===")
    log.info("Total requests: %d", len(captured))
    for i, c in enumerate(captured):
        log.info(" #%d: [%s] @ +%.1fs",
                  i + 1, c["kind"], c["ts"] - captured[0]["ts"] if i else 0)


if __name__ == "__main__":
    main()
