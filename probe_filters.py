"""
Phase 0 probe: discover whether nadlan.gov.il /deal-data supports
server-side filtering on roomNum / sort_order / etc.

Strategy:
  1. Open Tel Aviv (setl=5000) deals page → capture baseline /deal-data
     POST + decoded JWT payload.
  2. Click filter buttons one at a time; after each click capture the
     SPA's resulting /deal-data POST + JWT payload.
  3. Diff payloads vs baseline → identify which JWT fields the SPA mutates
     and confirm `total_rows` in the response actually changes
     (server-side filtering, not just client-side cropping).

Output:
  - filter_probe_results.csv: one row per captured request
  - prints decision tree: GREEN (server-side ok), YELLOW (client-only),
    RED (filters not visible in payload at all).

Run:
    python probe_filters.py

Notes:
  - Spaced 60s between clicks to stay under the per-IP rate limit.
  - Uses CDP-attached Chrome (NadlanBrowser already does that).
  - If the IP is currently blocked you'll see HTTP 403 / 405 in responses;
    wait an hour and retry.
"""
import base64
import csv
import gzip
import json
import logging
import sys
import time
from typing import Optional

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("probe")

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt,
)


SETL_CODE = "5000"  # Tel Aviv (137,094 deals — perfect for testing total_rows changes)
URL = f"https://www.nadlan.gov.il/?view=settlement&id={SETL_CODE}&page=deals"


def install_capture(page):
    """Listen for /deal-data POSTs + responses; return the capture dict
    that gets populated by the listeners."""
    cap = {"jwt_payload": None, "url": None, "headers": None,
           "response_status": None, "response_body": None,
           "total_rows": None, "fetched_at": None}

    def on_request(req):
        if req.method == "POST" and "/deal-data" in req.url and cap["jwt_payload"] is None:
            try:
                wrap = json.loads(req.post_data or "")
                _, payload = _decode_reversed_jwt(wrap.get("##"))
                cap["jwt_payload"] = payload
                cap["url"] = req.url
                cap["headers"] = dict(req.headers)
                cap["fetched_at"] = time.time()
            except Exception as e:
                log.warning("decode error: %s", e)

    def on_response(resp):
        if "/deal-data" not in resp.url:
            return
        if cap["response_body"] is not None:
            return
        cap["response_status"] = resp.status
        try:
            body = resp.body()
            decoded = json.loads(gzip.decompress(base64.b64decode(body)))
            cap["response_body"] = decoded
            data = decoded.get("data") or {}
            cap["total_rows"] = data.get("total_rows")
        except Exception as e:
            log.warning("response decode error: %s", e)

    page.on("request", on_request)
    page.on("response", on_response)
    return cap


def reset_capture(page, cap):
    for k in cap:
        cap[k] = None


def wait_for_capture(page, cap, timeout_s=15):
    deadline = time.time() + timeout_s
    while (cap["jwt_payload"] is None or cap["response_body"] is None) \
            and time.time() < deadline:
        page.wait_for_timeout(300)
    return cap["jwt_payload"] is not None


def safe_click(page, selector, label):
    """Click an element by selector; log success/failure. Returns bool."""
    try:
        loc = page.locator(selector).first
        loc.wait_for(state="visible", timeout=5000)
        loc.click(timeout=5000)
        log.info("clicked: %s [%s]", label, selector)
        return True
    except Exception as e:
        log.warning("click failed for %s [%s]: %s", label, selector, e)
        return False


def click_room_button(page, room_label):
    """The rooms dropdown is opened by clicking the 'כל החדרים' button
    (class .roomsBtn). After it opens, .whomBtn options become visible.
    """
    if not safe_click(page, "button.roomsBtn", "open rooms dropdown"):
        return False
    page.wait_for_timeout(500)
    selector = f"button.whomBtn:has-text('{room_label}')"
    return safe_click(page, selector, f"room={room_label}")


def click_sort_dropdown_option(page, option_text):
    """Sort dropdown is opened by a button labelled 'מיון'; options have
    class .dropdownBtn."""
    if not safe_click(page, "button.filterBtn:has-text('מיון')", "open sort"):
        return False
    page.wait_for_timeout(500)
    return safe_click(page, f"button.dropdownBtn:has-text('{option_text}')",
                      f"sort={option_text}")


def diff_payload_keys(baseline: dict, mutated: dict) -> dict:
    """Return {added: {...}, changed: {...}, missing: [...]} between two payloads."""
    added = {k: mutated[k] for k in mutated if k not in baseline}
    changed = {k: (baseline[k], mutated[k])
               for k in mutated
               if k in baseline and baseline[k] != mutated[k]}
    missing = [k for k in baseline if k not in mutated]
    return {"added": added, "changed": changed, "missing": missing}


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    log.info("=" * 60)
    log.info("PHASE 0 — Filter Probing for nadlan.gov.il /deal-data")
    log.info("=" * 60)

    rows = []  # for CSV output

    with NadlanBrowser() as nb:
        page = nb._page
        cap = install_capture(page)

        # ---- 1. Baseline (page-load fetch=1, no filter) ----
        log.info("\n[1/4] Baseline: load deals page, capture initial /deal-data")
        page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2000)
        if not wait_for_capture(page, cap, timeout_s=20):
            log.error("baseline capture failed — IP may be blocked")
            return
        baseline_payload = dict(cap["jwt_payload"])
        baseline_total = cap["total_rows"]
        log.info("baseline payload keys: %s", sorted(baseline_payload.keys()))
        log.info("baseline total_rows: %s", baseline_total)
        log.info("baseline payload (truncated): %s",
                 json.dumps(baseline_payload, ensure_ascii=False)[:300])
        rows.append({
            "step": "baseline",
            "filter_clicked": "(none)",
            "response_status": cap["response_status"],
            "total_rows": baseline_total,
            "payload_added_keys": "",
            "payload_changed_keys": "",
            "full_payload": json.dumps(baseline_payload, ensure_ascii=False),
        })

        # ---- 2. Click "2 חדרים" filter ----
        log.info("\n[2/4] Wait 60s for rate-limit cooldown, then click rooms=2")
        time.sleep(60)
        reset_capture(page, cap)
        if not click_room_button(page, "2 חדרים"):
            log.warning("room-2 click failed; SPA might not have button visible")
        else:
            if wait_for_capture(page, cap, timeout_s=15):
                d = diff_payload_keys(baseline_payload, cap["jwt_payload"])
                log.info("rooms=2 capture: status=%s total_rows=%s",
                         cap["response_status"], cap["total_rows"])
                log.info("  added keys: %s", list(d["added"].keys()))
                log.info("  changed keys: %s", list(d["changed"].keys()))
                rows.append({
                    "step": "rooms=2",
                    "filter_clicked": "2 חדרים",
                    "response_status": cap["response_status"],
                    "total_rows": cap["total_rows"],
                    "payload_added_keys": ",".join(d["added"].keys()),
                    "payload_changed_keys": ",".join(d["changed"].keys()),
                    "full_payload": json.dumps(cap["jwt_payload"],
                                                ensure_ascii=False),
                })
            else:
                log.warning("rooms=2: no /deal-data captured (likely client-side filter)")
                rows.append({
                    "step": "rooms=2",
                    "filter_clicked": "2 חדרים",
                    "response_status": "(no request fired)",
                    "total_rows": "",
                    "payload_added_keys": "(client-side only)",
                    "payload_changed_keys": "",
                    "full_payload": "",
                })

        # ---- 3. Click sort: 'מחיר העסקה - סדר יורד' ----
        log.info("\n[3/4] Wait 60s, then click sort = priceSM_down")
        time.sleep(60)
        reset_capture(page, cap)
        if not click_sort_dropdown_option(page, "מחיר העסקה - סדר יורד"):
            log.warning("priceSM_down click failed")
        else:
            if wait_for_capture(page, cap, timeout_s=15):
                d = diff_payload_keys(baseline_payload, cap["jwt_payload"])
                log.info("priceSM_down capture: status=%s total_rows=%s",
                         cap["response_status"], cap["total_rows"])
                log.info("  added keys: %s", list(d["added"].keys()))
                log.info("  changed keys: %s", list(d["changed"].keys()))
                rows.append({
                    "step": "sort=priceSM_down",
                    "filter_clicked": "מחיר העסקה - סדר יורד",
                    "response_status": cap["response_status"],
                    "total_rows": cap["total_rows"],
                    "payload_added_keys": ",".join(d["added"].keys()),
                    "payload_changed_keys": ",".join(d["changed"].keys()),
                    "full_payload": json.dumps(cap["jwt_payload"],
                                                ensure_ascii=False),
                })
            else:
                log.warning("priceSM_down: no /deal-data captured")

        # ---- 4. Click sort: 'תאריך עסקה - סדר עולה' (oldest first) ----
        log.info("\n[4/4] Wait 60s, then click sort = dateAsc")
        time.sleep(60)
        reset_capture(page, cap)
        if not click_sort_dropdown_option(page, "תאריך עסקה - סדר עולה"):
            log.warning("dateAsc click failed")
        else:
            if wait_for_capture(page, cap, timeout_s=15):
                d = diff_payload_keys(baseline_payload, cap["jwt_payload"])
                log.info("dateAsc capture: status=%s total_rows=%s",
                         cap["response_status"], cap["total_rows"])
                log.info("  added keys: %s", list(d["added"].keys()))
                log.info("  changed keys: %s", list(d["changed"].keys()))
                rows.append({
                    "step": "sort=dateAsc",
                    "filter_clicked": "תאריך עסקה - סדר עולה",
                    "response_status": cap["response_status"],
                    "total_rows": cap["total_rows"],
                    "payload_added_keys": ",".join(d["added"].keys()),
                    "payload_changed_keys": ",".join(d["changed"].keys()),
                    "full_payload": json.dumps(cap["jwt_payload"],
                                                ensure_ascii=False),
                })

    # ---- 5. Output CSV + decision ----
    out_path = "filter_probe_results.csv"
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "step", "filter_clicked", "response_status", "total_rows",
            "payload_added_keys", "payload_changed_keys", "full_payload",
        ])
        writer.writeheader()
        writer.writerows(rows)
    log.info("\nwrote %d rows to %s", len(rows), out_path)

    # ---- Decision tree ----
    log.info("\n" + "=" * 60)
    log.info("DECISION")
    log.info("=" * 60)

    fired_requests = [r for r in rows if r["step"] != "baseline"
                      and r["payload_added_keys"] != "(client-side only)"
                      and r["full_payload"]]
    if not fired_requests:
        log.info("🟡 YELLOW: No /deal-data fired on filter clicks → "
                 "filters appear to be CLIENT-SIDE only.")
        log.info("→ Plan B: per-parcel scrape (35GB, ~$105 with proxy)")
        return

    payload_changes = [r for r in fired_requests
                       if r["payload_added_keys"] or r["payload_changed_keys"]]
    if not payload_changes:
        log.info("🔴 RED: /deal-data fires but JWT payload is unchanged →"
                 "filtering happens elsewhere (URL? cookies?). Investigate.")
        return

    total_rows_changes = [r for r in fired_requests
                          if r["total_rows"] and baseline_total
                          and str(r["total_rows"]) != str(baseline_total)]
    if total_rows_changes:
        log.info("🟢 GREEN: server-side filtering CONFIRMED.")
        log.info("    JWT mutations seen: %s",
                 sorted({k for r in payload_changes
                         for k in (r["payload_added_keys"]
                                   + "," + r["payload_changed_keys"]).split(",")
                         if k}))
        log.info("    total_rows shifts seen: %s",
                 [(r["step"], r["total_rows"]) for r in total_rows_changes])
        log.info("→ Proceed with slice-based plan ($3-5).")
    else:
        log.info("🟡 YELLOW: payload changes but total_rows is same →"
                 "filter is mainly sort-only or client-side post-process.")
        log.info("    Sort-only filters give us 2x slice multiplier (asc+desc)"
                 "but not 7x. Re-evaluate cost/time.")


if __name__ == "__main__":
    main()
