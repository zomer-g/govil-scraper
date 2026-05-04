"""
Reproduce the exact worker click sequence that's failing on setl 3652
(בית אריה): click rooms=5 then rooms=6plus.

Connects to user's existing Chrome via CDP. Captures all /deal-data
responses. Reports whether the API responded.
"""
import base64
import gzip
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("debug")

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser


SETL = "3652"  # בית אריה
URL = f"https://www.nadlan.gov.il/?view=settlement&id={SETL}&page=deals&_n={int(time.time()*1000)}"


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    captures = []
    requests_all = []

    with NadlanBrowser() as nb:
        page = nb._page

        def on_request(req):
            if "deal-data" in req.url or "token-verify" in req.url:
                requests_all.append({
                    "method": req.method, "url": req.url,
                    "ts": time.time(),
                })
                log.info("REQ %s %s", req.method, req.url)

        def on_response(resp):
            if "deal-data" not in resp.url and "token-verify" not in resp.url:
                return
            try:
                body = resp.body()
            except Exception as e:
                log.warning("resp.body() error: %s", e)
                return
            try:
                if "token-verify" in resp.url:
                    text = body.decode("utf-8", errors="replace")
                    log.info("RESP %s status=%s body=%s",
                             resp.url, resp.status, text[:200])
                    return
                # deal-data: base64+gzip
                decoded = json.loads(gzip.decompress(base64.b64decode(body)))
                inner = decoded.get("statusCode")
                data = decoded.get("data") or {}
                items = data.get("items") or []
                total = data.get("total_rows", 0)
                log.info("RESP /deal-data http=%s inner=%s rows=%d total=%d",
                         resp.status, inner, len(items), total)
                captures.append({"http": resp.status, "inner": inner,
                                  "rows": len(items), "total": total,
                                  "ts": time.time()})
            except Exception as e:
                log.warning("decode error: %s", e)

        page.on("request", on_request)
        page.on("response", on_response)

        log.info("=" * 60)
        log.info("Step 1: navigate fresh to %s", URL)
        log.info("=" * 60)
        page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3500)
        log.info("After 3.5s wait: %d /deal-data responses captured (expected 0)",
                 len(captures))

        # Click rooms=5 like the worker does
        log.info("\n%s\nStep 2: click rooms dropdown -> rooms=5\n%s", "=" * 60, "=" * 60)
        try:
            page.locator("button.roomsBtn").first.click(timeout=5000)
            log.info("clicked rooms dropdown")
            page.wait_for_timeout(900)
            opt = page.locator("button.whomBtn:has-text('5 חדרים')").first
            opt.click(timeout=5000)
            log.info("clicked '5 חדרים'")
        except Exception as e:
            log.error("click failed: %s", e)

        # Wait up to 12s for /deal-data response
        log.info("waiting up to 12s for /deal-data...")
        deadline = time.time() + 12
        before = len(captures)
        while len(captures) <= before and time.time() < deadline:
            time.sleep(0.3)
        if len(captures) > before:
            log.info("✅ rooms=5 SUCCESS: got /deal-data")
        else:
            log.error("❌ rooms=5 FAIL: no /deal-data fired in 12s")

        page.wait_for_timeout(2500)

        # Click rooms=6plus
        log.info("\n%s\nStep 3: click rooms dropdown -> 6+\n%s", "=" * 60, "=" * 60)
        try:
            page.locator("button.roomsBtn").first.click(timeout=5000)
            log.info("clicked rooms dropdown")
            page.wait_for_timeout(900)
            opt = page.locator("button.whomBtn:has-text('6+ חדרים')").first
            opt.click(timeout=5000)
            log.info("clicked '6+ חדרים'")
        except Exception as e:
            log.error("click failed: %s", e)

        log.info("waiting up to 12s for /deal-data...")
        deadline = time.time() + 12
        before = len(captures)
        while len(captures) <= before and time.time() < deadline:
            time.sleep(0.3)
        if len(captures) > before:
            log.info("✅ rooms=6plus SUCCESS: got /deal-data")
        else:
            log.error("❌ rooms=6plus FAIL: no /deal-data fired in 12s")

        log.info("\n%s\nSUMMARY\n%s", "=" * 60, "=" * 60)
        log.info("Total /deal-data captured: %d", len(captures))
        for i, c in enumerate(captures):
            log.info("  #%d: http=%s inner=%s rows=%d total=%d",
                     i + 1, c["http"], c["inner"], c["rows"], c["total"])


if __name__ == "__main__":
    main()
