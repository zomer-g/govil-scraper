"""
Test with real Chrome (not Chromium) + simulated user behavior.

reCAPTCHA Enterprise scores headless/automated browsers very low. Real
Chrome + mouse movement + scroll might pass.
"""
import sys, time, random
from playwright.sync_api import sync_playwright


def main():
    if len(sys.argv) < 3:
        print("Usage: debug_nadlan_chrome.py <gush> <chelka>")
        sys.exit(2)
    gush, chelka = sys.argv[1], sys.argv[2]
    parcel_id = f"{gush}-{chelka}"
    url = f"https://www.nadlan.gov.il/?view=kparcel_all&id={parcel_id}&page=deals"

    sys.stdout.reconfigure(encoding='utf-8')
    print(f"=== REAL CHROME test on {parcel_id} ===")

    deals_count = 0
    deal_responses = []
    verify_responses = []

    with sync_playwright() as pw:
        # Use real Chrome installed by `playwright install chrome`
        browser = pw.chromium.launch(
            channel="chrome",  # <-- the key: real Chrome instead of Chromium
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            locale="he-IL",
            viewport={"width": 1280, "height": 900},
        )
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = ctx.new_page()

        def on_response(resp):
            nonlocal deals_count
            try:
                if "token-verify" in resp.url:
                    body = resp.text()[:120]
                    verify_responses.append((resp.status, body))
                elif "deal-data" in resp.url:
                    deal_responses.append(resp.status)
                    if resp.status == 200:
                        try:
                            import base64, gzip, json as _json
                            text = resp.text()
                            decoded = _json.loads(gzip.decompress(base64.b64decode(text)))
                            if decoded.get("statusCode") == 200:
                                items = decoded.get("data", {}).get("items", [])
                                if items:
                                    deals_count = max(deals_count, len(items))
                        except Exception:
                            pass
            except Exception:
                pass

        page.on("response", on_response)

        print("[1] navigating with real Chrome...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        print("[2] simulating user — random mouse moves + scroll...")
        for _ in range(8):
            x = random.randint(100, 1100)
            y = random.randint(100, 800)
            page.mouse.move(x, y, steps=random.randint(5, 20))
            page.wait_for_timeout(random.randint(200, 600))
        page.mouse.wheel(0, 400)
        page.wait_for_timeout(2000)

        print("[3] waiting 20s more for token-verify retries to settle...")
        page.wait_for_timeout(20000)

        print(f"\n[4] RESULT: deals_extracted={deals_count}")
        print(f"    deal-data statuses: {deal_responses[:10]}{'...' if len(deal_responses) > 10 else ''}  (total {len(deal_responses)})")
        print(f"    token-verify responses ({len(verify_responses)}):")
        for status, body in verify_responses[:5]:
            print(f"      [{status}] {body}")
        time.sleep(2)
        browser.close()


if __name__ == "__main__":
    main()
