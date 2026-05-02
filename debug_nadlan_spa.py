"""
Diagnostic for nadlan.gov.il SPA.

Goal: figure out why `POST /deal-data` no longer fires automatically as
of 2026-05-02 and find the new trigger or endpoint.

Captures:
1. All HTTP requests (method, URL, request type) — looking for /deal-data
   or any new endpoint that returns deals.
2. All HTTP responses with statusCode + content-type.
3. Console messages (warnings/errors from the SPA).
4. The page's DOM after load (to see if any UI element needs clicking).
5. Tries a series of triggers (scroll, wait, click on any "deals" tab) to
   see if any of them cause /deal-data to fire.

Run:  python debug_nadlan_spa.py 6909 1
"""
import json
import os
import sys
import time

from playwright.sync_api import sync_playwright


def main():
    if len(sys.argv) < 3:
        print("Usage: debug_nadlan_spa.py <gush> <chelka>")
        sys.exit(2)
    gush, chelka = sys.argv[1], sys.argv[2]
    parcel_id = f"{gush}-{chelka}"
    url = f"https://www.nadlan.gov.il/?view=kparcel_all&id={parcel_id}&page=deals"

    sys.stdout.reconfigure(encoding='utf-8')
    print(f"=== diagnosing {parcel_id} ===")
    print(f"URL: {url}")
    print()

    requests_log = []   # list of (method, url, resource_type)
    responses_log = []  # list of (url, status, content_type, body_preview)
    console_log = []    # list of (level, text)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(
            locale="he-IL",
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/145.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()

        def on_request(req):
            requests_log.append((req.method, req.url, req.resource_type))
            # Capture body for the interesting endpoints
            if "token-verify" in req.url or "deal-data" in req.url or "deal-info" in req.url:
                try:
                    body = req.post_data
                    headers = req.headers
                    print(f"\n>>> {req.method} {req.url}")
                    interesting = {k: v for k, v in headers.items()
                                   if k.lower() in ('content-type', 'authorization',
                                                     'x-recaptcha-token', 'cookie')}
                    print(f"    headers: {interesting}")
                    if body:
                        print(f"    body[:300]: {body[:300]}")
                except Exception as e:
                    print(f"    error reading req: {e}")

        def on_response(resp):
            try:
                ct = resp.headers.get("content-type", "")
                body_preview = ""
                if "/deal" in resp.url or "json" in ct or "/api/" in resp.url:
                    try:
                        text = resp.text()
                        body_preview = text[:300]
                    except Exception:
                        body_preview = "(could not read body)"
                responses_log.append((resp.url, resp.status, ct, body_preview))
            except Exception as e:
                responses_log.append((resp.url, "?", "?", f"err: {e}"))

        def on_console(msg):
            console_log.append((msg.type, msg.text[:300]))

        page.on("request", on_request)
        page.on("response", on_response)
        page.on("console", on_console)

        print("[1] navigating...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        print(f"    nav complete, {len(requests_log)} requests so far")

        print("[2] waiting 8s for SPA to settle...")
        page.wait_for_timeout(8000)
        print(f"    {len(requests_log)} total requests")

        # Try triggers if /deal-data hasn't fired yet
        deal_data_seen = any("/deal-data" in u for _, u, _ in requests_log)
        deal_info_seen = any("/deal-info" in u for _, u, _ in requests_log)
        print(f"[3] /deal-data fired: {deal_data_seen}, /deal-info fired: {deal_info_seen}")

        if not deal_data_seen:
            print("[4] /deal-data did not fire — trying triggers...")

            # Trigger 1: scroll
            print("    trying scroll...")
            try:
                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(3000)
                print(f"    after scroll: {len(requests_log)} requests")
            except Exception as e:
                print(f"    scroll err: {e}")

            # Trigger 2: click any "deals" / "עסקאות" tab
            print("    looking for deals tab to click...")
            try:
                # Common selectors that might be the deals tab
                selectors = [
                    'text=עסקאות',
                    'text=Deals',
                    '[data-tab="deals"]',
                    'a[href*="page=deals"]',
                    'button:has-text("עסקאות")',
                ]
                clicked = False
                for sel in selectors:
                    try:
                        loc = page.locator(sel).first
                        if loc.count() > 0 and loc.is_visible():
                            loc.click(timeout=3000)
                            print(f"    clicked: {sel}")
                            clicked = True
                            page.wait_for_timeout(5000)
                            break
                    except Exception:
                        pass
                if not clicked:
                    print("    no deals tab found")
            except Exception as e:
                print(f"    click err: {e}")

            print(f"    after triggers: {len(requests_log)} requests")

        # Final check
        deal_data_seen = any("/deal-data" in u for _, u, _ in requests_log)
        print(f"[5] FINAL: /deal-data fired: {deal_data_seen}")

        # Look for any other interesting endpoints
        api_calls = [
            (m, u, t) for m, u, t in requests_log
            if "api.nadlan" in u or "/api/" in u
        ]
        print(f"\n=== API calls observed ({len(api_calls)}) ===")
        for m, u, t in api_calls:
            print(f"  {m:6} {u}")

        # Print response statuses for nadlan API responses
        print(f"\n=== API responses ===")
        for u, status, ct, body in responses_log:
            if "api.nadlan" in u or "/api/" in u or "/deal-" in u:
                print(f"  [{status}] {u}")
                if body:
                    print(f"     body: {body[:200]}")

        # Print console errors
        errors = [(t, m) for t, m in console_log if t in ("error", "warning")]
        if errors:
            print(f"\n=== Console warnings/errors ({len(errors)}) ===")
            for t, m in errors[:20]:
                print(f"  [{t}] {m}")

        # DOM snapshot
        try:
            print("\n=== Page text content (first 600 chars) ===")
            text = page.inner_text("body", timeout=5000)
            print(text[:600])
        except Exception as e:
            print(f"  could not read body text: {e}")

        # Check if there's a captcha or cookie banner blocking
        try:
            captcha = page.locator("iframe[src*='recaptcha']").count()
            print(f"\n=== reCAPTCHA iframes present: {captcha}")
        except Exception:
            pass

        print("\n[6] Leaving browser open 60s for manual inspection...")
        print("    Open DevTools yourself (F12) and look at the Network tab.")
        page.wait_for_timeout(60000)

        browser.close()


if __name__ == "__main__":
    main()
