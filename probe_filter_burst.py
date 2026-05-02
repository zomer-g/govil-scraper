"""
Phase 0.5 — critical discovery probe: are filter-based fetch_number=1
requests rate-limited the same way as paginated fetch_number>=2?

If we can do 10+ filter requests rapid-fire without 403/405, we DON'T
need proxies at all — 7 rooms × 6 sorts = 42 slices/settlement, all on
one IP = ~$0 cost, no Decodo needed.
"""
import base64
import gzip
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("burst")

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt, _sign_reversed_jwt,
)

URL = "https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals"

ROOMS = ["1", "2", "3", "4", "5", "6plus"]  # we'll test 6 rooms
SORTS = ["dealDate_down", "dealDate_up", "dealAmount_down", "dealAmount_up"]


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    captured = {"jwt_payload": None, "url": None, "headers": None}

    with NadlanBrowser() as nb:
        page = nb._page

        def on_request(req):
            if req.method == "POST" and "/deal-data" in req.url and captured["jwt_payload"] is None:
                try:
                    wrap = json.loads(req.post_data or "")
                    h, p = _decode_reversed_jwt(wrap.get("##"))
                    captured["jwt_payload"] = p
                    captured["jwt_header"] = h
                    captured["url"] = req.url
                    captured["headers"] = dict(req.headers)
                except Exception:
                    pass

        page.on("request", on_request)
        log.info("navigating to deals page to capture template JWT...")
        page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        deadline = time.time() + 15
        while captured["jwt_payload"] is None and time.time() < deadline:
            page.wait_for_timeout(300)
        page.wait_for_timeout(2000)

        if captured["jwt_payload"] is None:
            log.error("no JWT captured")
            return

        h = captured["jwt_header"]
        base_payload = captured["jwt_payload"]
        url = captured["url"]
        headers = captured["headers"]

        log.info("template captured. base_id=%s, token=%s",
                 base_payload.get("base_id"), base_payload.get("token")[:20])

        # Fire 10 rapid-fire filter requests, measure time + status + total_rows
        results = []
        combos = [(r, s) for r in ROOMS for s in SORTS][:14]  # 14 combos
        t0 = time.time()
        for i, (room, sort) in enumerate(combos):
            new_p = dict(base_payload)
            new_p["room_num"] = room
            new_p["type_order"] = sort
            # Use the same fetch_number=1 (not paginated)
            new_p["fetch_number"] = 1
            jwt = _sign_reversed_jwt(h, new_p)
            body = json.dumps({"##": jwt})

            req_t0 = time.time()
            resp = page.request.post(url, data=body.encode("utf-8"),
                                      headers=headers, timeout=30000)
            req_elapsed = time.time() - req_t0
            sc = resp.status
            rows = 0
            total = 0
            inner_status = None
            if sc == 200:
                try:
                    decoded = json.loads(gzip.decompress(base64.b64decode(resp.text())))
                    inner_status = decoded.get("statusCode")
                    data = decoded.get("data") or {}
                    rows = len(data.get("items") or [])
                    total = data.get("total_rows", 0)
                except Exception as e:
                    log.warning("decode error: %s", e)
            log.info("burst %2d (rooms=%s sort=%s): HTTP %s inner=%s "
                     "rows=%d total=%s (%.2fs)",
                     i + 1, room, sort, sc, inner_status, rows, total, req_elapsed)
            results.append({
                "i": i + 1, "rooms": room, "sort": sort,
                "http": sc, "inner": inner_status,
                "rows": rows, "total": total, "elapsed_s": req_elapsed,
            })
            time.sleep(2)  # short pacing

        # Summary
        successes = sum(1 for r in results if r["http"] == 200 and r["inner"] == 200)
        log.info("\n=== SUMMARY ===")
        log.info("total combos: %d", len(results))
        log.info("successes: %d", successes)
        log.info("total wall time: %.1fs", time.time() - t0)
        if successes >= 10:
            log.info("🟢 NO RATE LIMIT on filter requests! Proxies UNNECESSARY!")
        elif successes >= 5:
            log.info("🟡 PARTIAL: rate limit kicks in mid-burst, may need spacing")
        else:
            log.info("🔴 BLOCKED: rate limit applies to filter requests too")


if __name__ == "__main__":
    main()
