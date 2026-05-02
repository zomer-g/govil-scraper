"""Test if our self-minted recaptcha token passes /token-verify."""
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("test")

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    with NadlanBrowser() as nb:
        page = nb._page
        page.goto("https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
                  wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(8000)

        site_key = page.evaluate("""
            () => {
                const html = document.documentElement.outerHTML;
                const m = html.match(/['"`](6L[A-Za-z0-9_-]{30,})['"`]/);
                return m ? m[1] : null;
            }
        """)
        log.info("site_key=%s", site_key)
        if not site_key:
            return

        # Render widget
        wid = page.evaluate("""async (siteKey) => {
            return await new Promise((resolve) => {
                grecaptcha.enterprise.ready(() => {
                    let div = document.getElementById('rc-helper');
                    if (!div) {
                        div = document.createElement('div');
                        div.id = 'rc-helper';
                        div.style.position = 'absolute';
                        div.style.left = '-9999px';
                        document.body.appendChild(div);
                    }
                    try {
                        const id = grecaptcha.enterprise.render('rc-helper', {
                            sitekey: siteKey, size: 'invisible',
                        });
                        resolve(id);
                    } catch (e) {
                        resolve({err: String(e)});
                    }
                });
            });
        }""", site_key)
        log.info("widget_id=%s", wid)

        # Try 5 token mints + token-verify roundtrips
        for i in range(5):
            t = page.evaluate("""async (wid) => {
                try { return await grecaptcha.enterprise.execute(wid, {action: 'submit'}); }
                catch (e) { return null; }
            }""", wid)
            log.info("mint %d: token len=%d", i, len(t or ""))
            if not t:
                continue

            # Test all 3 content-types
            for ct in ["text/plain", "application/json", "text/plain;charset=UTF-8"]:
                resp = page.request.post(
                    "https://api.nadlan.gov.il/token-verify",
                    data=json.dumps({"token": t}).encode("utf-8"),
                    headers={"content-type": ct},
                    timeout=20_000,
                )
                body = ""
                try:
                    body = resp.text()[:200]
                except Exception:
                    pass
                log.info("  ct=%s: HTTP %d %s", ct, resp.status, body)
                if resp.status == 200:
                    log.info("  → success! token UUID is %s", body)
                    return
            page.wait_for_timeout(2000)


if __name__ == "__main__":
    main()
