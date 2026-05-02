"""
Diagnostic: open Tel Aviv settlement, capture EVERY /deal-data and
/token-verify request the SPA fires for 90 seconds while you click
manual pagination in the headed browser.

Goal: see what changes in the request body between fetch_number=1 and
fetch_number=2 — specifically whether the SPA calls /token-verify
before each /deal-data, and what the relationship is.

Run:
    python capture_pagination.py

Then in the headed browser window:
  1. Wait for the deals page to load (you'll see the Tel Aviv list)
  2. Scroll to the bottom and click "Next Page" / "הבא" / arrow
  3. Watch the script log every captured request

After 90s the browser closes and the script prints summaries.
"""
import base64
import gzip
import json
import logging
import time

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("capture")


def main():
    captured = []  # list of dicts {url, body, headers, kind}

    with NadlanBrowser() as nb:
        page = nb._page

        def on_request(req):
            url = req.url
            if ("/deal-data" in url) or ("/token-verify" in url):
                kind = "deal-data" if "/deal-data" in url else "token-verify"
                entry = {
                    "kind": kind,
                    "url": url,
                    "method": req.method,
                    "body": req.post_data,
                    "headers": dict(req.headers),
                    "ts": time.time(),
                }
                captured.append(entry)
                log.info("REQ #%d [%s] %s %s (body=%s)",
                         len(captured), kind, req.method, url,
                         (req.post_data or "")[:80])

        def on_response(resp):
            url = resp.url
            if ("/deal-data" not in url) and ("/token-verify" not in url):
                return
            try:
                body_bytes = resp.body()
            except Exception:
                return
            # Try base64+gzip first; fall back to raw text
            decoded_text = None
            try:
                decoded_text = gzip.decompress(
                    base64.b64decode(body_bytes)
                ).decode("utf-8", errors="replace")
            except Exception:
                try:
                    decoded_text = body_bytes.decode("utf-8", errors="replace")
                except Exception:
                    decoded_text = "<binary>"
            log.info("RESP %s -> %s (sample=%s)",
                     url[-30:], resp.status, (decoded_text or "")[:120])

        page.on("request", on_request)
        page.on("response", on_response)

        log.info("Navigating to Tel Aviv settlement deals page...")
        page.goto(
            "https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        log.info("=" * 60)
        log.info("ACTION REQUIRED: in the browser window, click NEXT PAGE")
        log.info("(scroll down to find pagination, then click >)")
        log.info("Capturing for 90 seconds...")
        log.info("=" * 60)

        page.wait_for_timeout(90_000)

    # ----- Post-mortem -----
    log.info("\n" + "=" * 60)
    log.info("CAPTURE SUMMARY: %d requests", len(captured))
    log.info("=" * 60)

    deal_reqs = [c for c in captured if c["kind"] == "deal-data"]
    log.info("\n%d /deal-data requests:", len(deal_reqs))
    for i, c in enumerate(deal_reqs):
        log.info("\n--- /deal-data #%d ---", i + 1)
        log.info("url: %s", c["url"])
        body_str = c["body"] or ""
        try:
            wrapper = json.loads(body_str)
            jwt = wrapper.get("##", "")
            header, payload = _decode_reversed_jwt(jwt)
            log.info("payload: %s", json.dumps(
                payload, ensure_ascii=False, indent=2)[:600])
        except Exception as e:
            log.info("body raw (decode failed: %s): %s", e, body_str[:300])

    tv_reqs = [c for c in captured if c["kind"] == "token-verify"]
    log.info("\n%d /token-verify requests:", len(tv_reqs))
    for i, c in enumerate(tv_reqs):
        log.info("\n--- /token-verify #%d ---", i + 1)
        log.info("url: %s", c["url"])
        log.info("body: %s", (c["body"] or "")[:400])


if __name__ == "__main__":
    main()
