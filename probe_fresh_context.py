"""Test if fresh browser contexts bypass the per-IP rate limit."""
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


def fetch_via_fresh_context(browser, fetch_num):
    """Open fresh context, navigate, capture JWT, fire deal-data with our fetch_num."""
    captured = {"jwt": None, "headers": None, "url": None}
    ctx = browser.new_context(locale="he-IL", viewport={"width": 1280, "height": 900})
    page = ctx.new_page()

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

    page.on("request", on_request)
    try:
        page.goto("https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
                  wait_until="domcontentloaded", timeout=60_000)
        deadline = time.time() + 15
        while captured["jwt"] is None and time.time() < deadline:
            page.wait_for_timeout(300)
        if captured["jwt"] is None:
            return None, "no-jwt-captured"

        h, p = captured["jwt"]
        new_p = dict(p)
        new_p["fetch_number"] = fetch_num
        jwt = _sign_reversed_jwt(h, new_p)
        body = json.dumps({"##": jwt})
        resp = page.request.post(captured["url"], data=body.encode("utf-8"),
                                  headers=captured["headers"], timeout=30000)
        status = resp.status
        rows = 0
        if status == 200:
            try:
                decoded = json.loads(gzip.decompress(base64.b64decode(resp.text())))
                rows = len((decoded.get("data") or {}).get("items") or [])
            except Exception:
                pass
        return status, f"rows={rows}"
    finally:
        try: ctx.close()
        except Exception: pass


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    with NadlanBrowser() as nb:
        browser = nb._browser
        for fetch_num in [2, 3, 4, 5]:
            status, info = fetch_via_fresh_context(browser, fetch_num)
            log.info("fresh-context fetch=%d → HTTP %s %s", fetch_num, status, info)
            time.sleep(3)


if __name__ == "__main__":
    main()
