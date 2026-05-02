"""
After ONE navigation, try POST /deal-data with fetch_number=2,3,4,5,6
back-to-back. See how many succeed before the server rejects.
"""
import base64
import gzip
import json
import logging
import sys
import time

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt, _sign_reversed_jwt,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("probe")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    captured_jwt = {"hp": None, "headers": None, "url": None}

    with NadlanBrowser() as nb:
        page = nb._page

        def on_request(req):
            if req.method == "POST" and "/deal-data" in req.url and captured_jwt["hp"] is None:
                try:
                    wrap = json.loads(req.post_data or "")
                    h, p = _decode_reversed_jwt(wrap.get("##"))
                    captured_jwt["hp"] = (h, p)
                    captured_jwt["headers"] = dict(req.headers)
                    captured_jwt["url"] = req.url
                    log.info("Captured initial JWT: payload[token]=%s payload[fetch_number]=%s",
                              p.get("token"), p.get("fetch_number"))
                except Exception:
                    pass

        page.on("request", on_request)

        log.info("Navigating…")
        page.goto(
            "https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
            wait_until="domcontentloaded", timeout=60_000,
        )
        deadline = time.time() + 12
        while captured_jwt["hp"] is None and time.time() < deadline:
            page.wait_for_timeout(300)
        if captured_jwt["hp"] is None:
            log.error("did not capture JWT")
            return
        page.wait_for_timeout(2000)

        h, p = captured_jwt["hp"]
        url = captured_jwt["url"]
        headers = captured_jwt["headers"]

        for fetch_num in [2, 3, 4, 5, 6, 10, 20]:
            new_p = dict(p)
            new_p["fetch_number"] = fetch_num
            jwt = _sign_reversed_jwt(h, new_p)
            body = json.dumps({"##": jwt})
            t0 = time.time()
            resp = page.request.post(
                url, data=body.encode("utf-8"),
                headers=headers, timeout=30000,
            )
            elapsed = time.time() - t0
            sc = resp.status
            sample = ""
            if sc == 200:
                try:
                    decoded = json.loads(gzip.decompress(base64.b64decode(resp.text())))
                    inner = decoded.get("statusCode")
                    rows = len((decoded.get("data") or {}).get("items") or [])
                    sample = f"inner={inner} rows={rows}"
                except Exception as e:
                    sample = f"decode-err {e}"
            else:
                try:
                    sample = resp.text()[:120]
                except Exception:
                    sample = "<no body>"
            log.info("fetch_number=%-3d → HTTP %s (%.2fs) %s",
                      fetch_num, sc, elapsed, sample)
            page.wait_for_timeout(1500)


if __name__ == "__main__":
    main()
