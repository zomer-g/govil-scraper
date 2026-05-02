"""
Test if anti-detection scripts let nadlan token-verify succeed.

The standard "Playwright detected as bot" mitigations:
- navigator.webdriver = false
- mock chrome.runtime
- spoof plugins/languages
- patch permissions API
"""
import sys, time
from playwright.sync_api import sync_playwright

STEALTH_INIT_SCRIPT = r"""
// Mask Playwright/automation indicators
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', {
    get: () => [1,2,3,4,5].map(() => ({}))
});
Object.defineProperty(navigator, 'languages', { get: () => ['he-IL', 'he', 'en-US', 'en'] });
window.chrome = { runtime: {} };
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters)
);
"""


def main():
    if len(sys.argv) < 3:
        print("Usage: debug_nadlan_stealth.py <gush> <chelka>")
        sys.exit(2)
    gush, chelka = sys.argv[1], sys.argv[2]
    parcel_id = f"{gush}-{chelka}"
    url = f"https://www.nadlan.gov.il/?view=kparcel_all&id={parcel_id}&page=deals"

    sys.stdout.reconfigure(encoding='utf-8')
    print(f"=== STEALTH test on {parcel_id} ===")

    deals_count = 0
    verify_ok = False
    deal_responses = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        ctx = browser.new_context(
            locale="he-IL",
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/145.0.7632.6 Safari/537.36",
        )
        ctx.add_init_script(STEALTH_INIT_SCRIPT)
        page = ctx.new_page()

        def on_response(resp):
            nonlocal verify_ok, deals_count
            try:
                if "token-verify" in resp.url and resp.status == 200:
                    body = resp.text()
                    if '"ok":true' in body or '"ok": true' in body:
                        verify_ok = True
                    print(f"  [{resp.status}] token-verify: {body[:120]}")
                elif "deal-data" in resp.url:
                    print(f"  [{resp.status}] deal-data")
                    deal_responses.append(resp.status)
                    if resp.status == 200:
                        try:
                            import base64, gzip, json as _json
                            text = resp.text()
                            decoded = _json.loads(gzip.decompress(base64.b64decode(text)))
                            if decoded.get("statusCode") == 200:
                                items = decoded.get("data", {}).get("items", [])
                                deals_count = max(deals_count, len(items))
                                print(f"        DEALS={len(items)}")
                            else:
                                print(f"        (statusCode={decoded.get('statusCode')})")
                        except Exception as e:
                            print(f"        decode err: {e}")
            except Exception:
                pass

        page.on("response", on_response)

        print("[1] navigating...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        print("[2] waiting 25s for SPA + reCAPTCHA + retries...")
        page.wait_for_timeout(25000)

        print(f"\n[3] RESULT: deals_extracted={deals_count}, verify_ok={verify_ok}")
        print(f"    deal-data status codes seen: {deal_responses}")
        time.sleep(3)
        browser.close()


if __name__ == "__main__":
    main()
