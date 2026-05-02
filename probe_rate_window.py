"""
Probe: how long must we wait between /deal-data calls to avoid 403?

Strategy:
  - Use one warm browser context (CDP-attached Chrome)
  - Get fetch=2 first (works)
  - Then test fetch=3 with progressively longer waits
  - Find the minimum cooldown that lets fetch=3 succeed
"""
import base64
import gzip
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("probe")

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt, _sign_reversed_jwt,
)


def navigate_and_capture(page, fetch_num):
    """Re-navigate (with storage clear) and capture fresh JWT, then POST /deal-data."""
    captured = {"jwt": None, "headers": None, "url": None}

    def on_request(req):
        if req.method == "POST" and "/deal-data" in req.url and captured["jwt"] is None:
            try:
                wrap = json.loads(req.post_data or "")
                h, p = _decode_reversed_jwt(wrap.get("##"))
                captured["jwt"] = (h, p)
                captured["headers"] = dict(req.headers)
                captured["url"] = req.url
            except Exception:
                pass

    # Force fresh /token-verify by clearing storage
    page.evaluate("""() => {
        try { sessionStorage.clear(); } catch (e) {}
        try { localStorage.clear(); } catch (e) {}
    }""")
    page.context.clear_cookies()
    page.goto("https://www.nadlan.gov.il/", wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(800)

    page.on("request", on_request)
    page.goto("https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
              wait_until="domcontentloaded", timeout=60_000)
    deadline = time.time() + 12
    while captured["jwt"] is None and time.time() < deadline:
        page.wait_for_timeout(300)
    page.remove_listener("request", on_request)

    if captured["jwt"] is None:
        return None, "no-jwt"

    h, p = captured["jwt"]
    new_p = dict(p)
    new_p["fetch_number"] = fetch_num
    jwt = _sign_reversed_jwt(h, new_p)
    body = json.dumps({"##": jwt})
    resp = page.request.post(captured["url"], data=body.encode("utf-8"),
                              headers=captured["headers"], timeout=30000)
    rows = 0
    if resp.status == 200:
        try:
            decoded = json.loads(gzip.decompress(base64.b64decode(resp.text())))
            rows = len((decoded.get("data") or {}).get("items") or [])
        except Exception:
            pass
    return resp.status, rows


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    with NadlanBrowser() as nb:
        page = nb._page

        # Page 1 baseline
        page.goto("https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
                  wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(5000)
        log.info("baseline page 1 loaded")

        # First pagination — should work
        s, r = navigate_and_capture(page, 2)
        log.info("fetch=2 (no wait): HTTP %s rows=%s", s, r)
        if s != 200:
            log.error("baseline failed — abort")
            return

        # Now test progressively longer cooldowns
        for cooldown in [10, 30, 60, 90, 120]:
            log.info("=== cooldown %ds ===", cooldown)
            time.sleep(cooldown)
            fetch_num = 3 + cooldown  # use distinct fetch_number for each test
            s, r = navigate_and_capture(page, fetch_num)
            log.info("fetch=%d (after %ds wait): HTTP %s rows=%s",
                      fetch_num, cooldown, s, r)
            if s == 200 and r > 0:
                log.info("BREAKTHROUGH: %ds cooldown bypasses rate limit", cooldown)


if __name__ == "__main__":
    main()
