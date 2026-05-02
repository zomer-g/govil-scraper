"""Quick probe: click 'סינון' button, inspect what filters appear,
specifically date/year. If found, click + capture JWT changes."""
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("date")

from govscraper.scrapers.nadlan.legacy_incremental import (
    NadlanBrowser, _decode_reversed_jwt,
)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    captures = []

    with NadlanBrowser() as nb:
        page = nb._page

        def on_request(req):
            if req.method == "POST" and "/deal-data" in req.url:
                try:
                    wrap = json.loads(req.post_data or "")
                    _, p = _decode_reversed_jwt(wrap.get("##"))
                    captures.append({"ts": time.time(), "payload": p})
                except Exception:
                    pass

        page.on("request", on_request)

        page.goto("https://www.nadlan.gov.il/?view=settlement&id=5000&page=deals",
                  wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)
        log.info("baseline payload keys: %s",
                 sorted(captures[-1]["payload"].keys()) if captures else "no capture")

        # Click 'סינון' button
        log.info("clicking 'סינון' (filter) button...")
        try:
            page.locator("button.filterBtn:has-text('סינון')").first.click(timeout=5000)
            page.wait_for_timeout(1500)
        except Exception as e:
            log.warning("filter click failed: %s", e)
            return

        # Read the filter panel
        log.info("inspecting filter panel HTML...")
        result = page.evaluate("""
        () => {
            const out = {inputs: [], buttons: [], selects: [], labels: []};
            for (const el of document.querySelectorAll('input, select, textarea')) {
                if (el.offsetParent === null) continue;
                out.inputs.push({
                    tag: el.tagName,
                    type: el.type || '',
                    placeholder: el.placeholder || '',
                    name: el.name || '',
                    value: el.value || '',
                });
            }
            for (const el of document.querySelectorAll('button, label')) {
                if (el.offsetParent === null) continue;
                const text = (el.innerText || el.textContent || '').trim();
                if (text && text.length < 50) {
                    if (el.tagName === 'BUTTON') out.buttons.push(text);
                    else out.labels.push(text);
                }
            }
            return out;
        }
        """)

        log.info("=== filter panel inputs ===")
        for inp in result["inputs"][:30]:
            log.info("  %s", inp)
        log.info("=== filter panel buttons ===")
        for b in result["buttons"][:30]:
            log.info("  %s", b)
        log.info("=== labels ===")
        for l in result["labels"][:30]:
            log.info("  %s", l)

        # Now click 'כל העסקאות מכל הזמנים' to see if a date dropdown opens
        log.info("\nclicking 'כל העסקאות מכל הזמנים'...")
        try:
            page.locator("button:has-text('כל העסקאות מכל הזמנים')").first.click(timeout=5000)
            page.wait_for_timeout(1500)
        except Exception as e:
            log.warning("date dropdown click failed: %s", e)
            return

        # Re-inspect — what new options appeared?
        result2 = page.evaluate("""
        () => {
            const out = [];
            for (const el of document.querySelectorAll('button, li, [role="option"]')) {
                if (el.offsetParent === null) continue;
                const text = (el.innerText || '').trim();
                if (text && text.length < 80 && (text.includes('שנ') || text.includes('עסקאות') || text.match(/20[0-9][0-9]/))) {
                    out.push({tag: el.tagName, text: text.substring(0, 80)});
                }
            }
            return out;
        }
        """)
        log.info("=== date options ===")
        for o in result2[:30]:
            log.info("  [%s] %s", o["tag"], o["text"])

        # If we see specific year/range buttons, try clicking one and capture
        # what the JWT payload changes to.
        log.info("\nlooking for a year/range option to click...")
        for opt in result2:
            if "5" in opt["text"] and ("שנים" in opt["text"] or "אחרונ" in opt["text"]):
                log.info("trying to click: %s", opt["text"])
                try:
                    page.locator(f"button:has-text('{opt['text']}')").first.click(timeout=5000)
                    page.wait_for_timeout(2500)
                    if captures:
                        log.info("after click: payload keys = %s",
                                 sorted(captures[-1]["payload"].keys()))
                        log.info("payload preview: %s",
                                 json.dumps(captures[-1]["payload"], ensure_ascii=False)[:300])
                except Exception as e:
                    log.warning("click failed: %s", e)
                break


if __name__ == "__main__":
    main()
