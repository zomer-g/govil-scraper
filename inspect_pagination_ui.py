"""
Inspect the deals page DOM for pagination UI elements.
"""
import logging
from govscraper.scrapers.nadlan.legacy_incremental import NadlanBrowser

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


def main():
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    with NadlanBrowser() as nb:
        page = nb._page
        page.goto(
            "https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        page.wait_for_timeout(8000)  # let SPA render fully

        # Scroll to bottom — pagination is often below
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Capture all interactive elements
        result = page.evaluate("""
        () => {
            const out = {buttons: [], links: [], aria: []};
            for (const el of document.querySelectorAll('button')) {
                if (el.offsetParent === null) continue;  // hidden
                out.buttons.push({
                    text: (el.innerText || el.textContent || '').trim().substring(0, 80),
                    aria: el.getAttribute('aria-label') || '',
                    cls: el.className.substring(0, 80),
                });
            }
            for (const el of document.querySelectorAll('a')) {
                if (el.offsetParent === null) continue;
                const text = (el.innerText || '').trim();
                if (text && text.length < 80) {
                    out.links.push({
                        text: text,
                        href: el.href.substring(0, 100),
                    });
                }
            }
            for (const el of document.querySelectorAll('[role="button"], [role="navigation"]')) {
                if (el.offsetParent === null) continue;
                out.aria.push({
                    role: el.getAttribute('role'),
                    text: (el.innerText || '').trim().substring(0, 80),
                });
            }
            return out;
        }
        """)
        print("\n=== BUTTONS ===")
        for b in result["buttons"][:30]:
            print(f"  text={b['text']!r}  aria={b['aria']!r}  cls={b['cls']!r}")
        print(f"\n=== LINKS (first 30 of {len(result['links'])}) ===")
        for l in result["links"][:30]:
            print(f"  {l['text']!r}  -> {l['href']!r}")
        print("\n=== ROLE BUTTONS/NAV ===")
        for a in result["aria"][:20]:
            print(f"  [{a['role']}] {a['text']!r}")


if __name__ == "__main__":
    main()
