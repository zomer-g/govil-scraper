"""Find ALL 6L... recaptcha site keys in the page."""
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    with NadlanBrowser() as nb:
        page = nb._page
        page.goto("https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
                  wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(8000)

        # Find all unique 6L keys in HTML, scripts, and grecaptcha config
        result = page.evaluate("""
            () => {
                const keys = new Set();
                // 1. HTML
                const html = document.documentElement.outerHTML;
                for (const m of html.matchAll(/['"`](6L[A-Za-z0-9_-]{30,})['"`]/g)) {
                    keys.add(m[1]);
                }
                // 2. ___grecaptcha_cfg
                try {
                    const cfg = window.___grecaptcha_cfg;
                    if (cfg && cfg.clients) {
                        const stack = [cfg.clients];
                        while (stack.length) {
                            const o = stack.pop();
                            if (!o || typeof o !== 'object') continue;
                            for (const k in o) {
                                if (typeof o[k] === 'string' && /^6L[A-Za-z0-9_-]{30,}$/.test(o[k])) {
                                    keys.add(o[k]);
                                }
                                if (typeof o[k] === 'object') stack.push(o[k]);
                            }
                        }
                    }
                } catch (e) {}
                // 3. JS scripts
                for (const s of document.querySelectorAll('script')) {
                    if (!s.textContent) continue;
                    for (const m of s.textContent.matchAll(/['"`](6L[A-Za-z0-9_-]{30,})['"`]/g)) {
                        keys.add(m[1]);
                    }
                }
                return Array.from(keys);
            }
        """)
        print("Found keys:")
        for k in result:
            print(f"  {k}")


if __name__ == "__main__":
    main()
