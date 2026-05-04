"""
Nadlan.gov.il incremental archive — sibling to ``archive_engine.py`` for
real-estate transactions.

Strategy
--------
We don't iterate ~250 k parcels daily. Instead we hit the **settlement-level**
``/deal-data`` endpoint for each of the ~1,509 settlements in Israel. Each
settlement returns its 500 newest deals in fetch_number=1, sorted dateDesc.
A daily run of all settlements ≈ 2-3 hours and reliably captures everything
new (no city has >500 deals/day, even Tel Aviv).

Settlement catalog comes from
``https://data.nadlan.gov.il/api/index/setl_types.json`` (recaptcha-free).

Each deal carries ``parcelNum = "<gush>-<chelka>-<sub>"`` so we can split it.

Bootstrap caveat
----------------
``run_bootstrap`` here also walks settlements with fetch_number=1 only — it
captures the 500 newest per settlement, which gives ~5 years for big cities and
all-time for small ones. For deeper historical coverage, run ``bulk_nadlan.py``
with a parcels.csv first; the resulting deals.csv can be placed at
``archive_dir/nadlan_master.csv`` before the first incremental run.

Exported (matches archive_engine signatures so over_worker can mirror its
existing dispatch):

    run_bootstrap(archive_dir, session, name_override="", progress_cb=None,
                  settlements_filter=None) -> (file_info, checkpoint)

    run_incremental(archive_dir, checkpoint, session=None, progress_cb=None,
                    lookback_days=90, settlements_filter=None)
        -> (new_count, updated_checkpoint)
"""

import base64
import csv
import gzip
import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Tuple

import requests

# JWT signing secret — reverse-engineered from the SPA bundle. The same one
# legacy_api documents. Used to build pagination requests for fetch_number>1.
_NADLAN_JWT_SECRET = "90c3e620192348f1bd46fcd9138c3c68"


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    s = s.encode("ascii")
    s += b"=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s)


def _decode_reversed_jwt(reversed_token: str) -> Tuple[dict, dict]:
    """Reverse a nadlan-style ``##`` payload and decode header + payload."""
    token = reversed_token[::-1]
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError(f"invalid JWT format (parts={len(parts)})")
    header = json.loads(_b64url_decode(parts[0]))
    payload = json.loads(_b64url_decode(parts[1]))
    return header, payload


def _sign_reversed_jwt(header: dict, payload: dict,
                        secret: str = _NADLAN_JWT_SECRET) -> str:
    """Build a fresh nadlan-style reversed JWT for the given payload."""
    h = _b64url_encode(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    msg = f"{h}.{p}".encode("ascii")
    sig = hmac.new(secret.encode("ascii"), msg, hashlib.sha256).digest()
    s = _b64url_encode(sig)
    forward = f"{h}.{p}.{s}"
    return forward[::-1]

from govscraper.io.archive_engine import append_to_csv, regenerate_excel_from_csv
from govscraper.io.sanitize import sanitize_filename

logger = logging.getLogger(__name__)

SETTLEMENTS_URL = "https://data.nadlan.gov.il/api/index/setl_types.json"

# Stable column order for the master CSV. parcel breakdown comes first because
# that's what users sort/filter by; deal economics next; ids last.
NADLAN_COLS = [
    "gush", "chelka", "subchelka", "parcelNum",
    "settlementCode", "settlementName",
    "neighborhoodId", "neighborhoodName",
    "address", "streetCode",
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "assetId", "addressId", "polygonId",
]


def _deal_key(d: dict) -> str:
    """Stable identity for dedup. assetId+dealDate is invariant once published."""
    return f"{d.get('assetId') or ''}|{d.get('dealDate') or ''}"


def _flatten_deal(deal: dict, setl_code: str, setl_name: str) -> dict:
    pn = (deal.get("parcelNum") or "").split("-")
    return {
        "gush": pn[0] if len(pn) > 0 else "",
        "chelka": pn[1] if len(pn) > 1 else "",
        "subchelka": pn[2] if len(pn) > 2 else "",
        "parcelNum": deal.get("parcelNum", ""),
        "settlementCode": setl_code,
        "settlementName": setl_name,
        "neighborhoodId": deal.get("neighborhoodId", ""),
        "neighborhoodName": deal.get("neighborhoodName", ""),
        "address": deal.get("address", ""),
        "streetCode": deal.get("streetCode", ""),
        "dealDate": deal.get("dealDate", ""),
        "dealAmount": deal.get("dealAmount", ""),
        "priceSM": deal.get("priceSM", ""),
        "roomNum": deal.get("roomNum", ""),
        "floor": deal.get("floor", ""),
        "assetArea": deal.get("assetArea", ""),
        "yearBuilt": deal.get("yearBuilt", ""),
        "buildingFloors": deal.get("buildingFloors", ""),
        "dealNature": deal.get("dealNature", ""),
        "hokHamecher": deal.get("hokHamecher", ""),
        "assetId": deal.get("assetId", ""),
        "addressId": deal.get("addressId", ""),
        "polygonId": deal.get("polygonId", ""),
    }


def _load_settlements() -> dict:
    """Catalog: {setlCode: {SETL_NAME, TYPE, POPULATION, GLOBAL_TYPE}}."""
    r = requests.get(SETTLEMENTS_URL, timeout=30)
    r.raise_for_status()
    return r.json()


def _warmup_human_signal(page) -> None:
    """Mouse + scroll gestures so reCAPTCHA Enterprise gives a passing score."""
    import random as _rnd
    try:
        for _ in range(8):
            x = _rnd.randint(150, 1100)
            y = _rnd.randint(150, 800)
            page.mouse.move(x, y, steps=_rnd.randint(8, 20))
            page.wait_for_timeout(_rnd.randint(120, 350))
        page.mouse.wheel(0, 400)
        page.wait_for_timeout(700)
    except Exception as e:
        logger.debug("warmup gesture failed: %s", e)


def _human_jitter_pause(page, min_ms: int = 1500, max_ms: int = 4000) -> None:
    """Random pause + a tiny mouse movement between clicks. The clicks
    burn recaptcha score if they happen on a perfect schedule with no
    movement — this simulates "still reading the page". Cheap (~2-3s)."""
    import random as _rnd
    try:
        x = _rnd.randint(200, 1100)
        y = _rnd.randint(200, 700)
        page.mouse.move(x, y, steps=_rnd.randint(3, 8))
        page.wait_for_timeout(_rnd.randint(min_ms, max_ms))
    except Exception:
        pass


def _aggressive_warmup(page, duration_s: int = 30) -> None:
    """Long-form warmup: many mouse movements, scrolls, and pauses over
    `duration_s` seconds. Used to recover from reCAPTCHA score depletion
    after token-verify started returning 400. Real users spend ~30s on a
    page reading deals before clicking filters; we simulate that."""
    import random as _rnd
    end = time.time() + duration_s
    try:
        while time.time() < end:
            # Mouse: 3-5 movements
            for _ in range(_rnd.randint(3, 5)):
                x = _rnd.randint(150, 1100)
                y = _rnd.randint(150, 700)
                page.mouse.move(x, y, steps=_rnd.randint(8, 25))
                page.wait_for_timeout(_rnd.randint(200, 600))
            # Scroll
            page.mouse.wheel(0, _rnd.randint(100, 500))
            page.wait_for_timeout(_rnd.randint(800, 2000))
            # Occasional reverse scroll (reading back)
            if _rnd.random() < 0.3:
                page.mouse.wheel(0, -_rnd.randint(80, 200))
                page.wait_for_timeout(_rnd.randint(500, 1200))
    except Exception as e:
        logger.debug("aggressive warmup error: %s", e)


# ---------------------------------------------------------------------------
# Playwright session — single Chromium reused across settlements.
# ---------------------------------------------------------------------------

class NadlanBrowser:
    """Context-manager wrapper for a Playwright Chromium driving nadlan.gov.il.

    A single ``BrowserContext`` is reused across all settlements: the recaptcha
    state and cookies persist, saving ~10s of overhead per settlement. If the
    server returns ``statusCode=405`` (token expired) we navigate to home to
    refresh it, then retry the settlement.

    Headed only — recaptcha enterprise rejects headless chromium.
    """

    def __init__(self, headless: Optional[bool] = None,
                 nav_timeout_ms: int = 60_000,
                 data_timeout_s: int = 30):
        if headless is None:
            headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"
        self.headless = headless
        self.nav_timeout_ms = nav_timeout_ms
        self.data_timeout_s = data_timeout_s
        self._pw = None
        self._browser = None
        self._ctx = None
        self._page = None
        # reCAPTCHA score tracker: incremented by every /token-verify 400.
        # When >0, signals score depletion → caller should call
        # recover_recaptcha_score() before more clicks.
        self._token_verify_failures = 0
        self._token_verify_listener_attached = False

    def __enter__(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        # Prefer CDP attach to the user's already-warmed Chrome (high
        # recaptcha enterprise score). See run_chrome_for_nadlan.bat.
        # Fall back to a fresh Chrome launch if CDP isn't available.
        # Use 127.0.0.1 explicitly — `localhost` resolves to ::1 first on
        # Windows and Chrome's CDP only listens on IPv4 by default.
        cdp = os.environ.get("NADLAN_CHROME_CDP", "http://127.0.0.1:9222")
        cdp_attached = False
        if cdp and cdp.lower() not in ("0", "false", "off", ""):
            try:
                self._browser = self._pw.chromium.connect_over_cdp(
                    cdp, timeout=5000)
                cdp_attached = True
                logger.info("nadlan: connected to existing Chrome at %s", cdp)
            except Exception as e:
                logger.info("nadlan: no Chrome at %s (%s) — launching fresh",
                            cdp, e)
        if not cdp_attached:
            args = ["--disable-blink-features=AutomationControlled"]
            try:
                self._browser = self._pw.chromium.launch(
                    channel="chrome", headless=self.headless, args=args)
            except Exception as e:
                logger.warning("real Chrome unavailable (%s) — falling back to Chromium. "
                               "token-verify likely to fail.", e)
                self._browser = self._pw.chromium.launch(
                    headless=self.headless, args=args)
        if cdp_attached and self._browser.contexts:
            self._ctx = self._browser.contexts[0]
            # Always open a NEW tab — reusing an existing tab can inherit
            # stuck SPA state (error modals, cached deals view) from
            # previous runs that prevents /deal-data from re-firing.
            self._page = self._ctx.new_page()
        else:
            self._ctx = self._browser.new_context(
                locale="he-IL",
                viewport={"width": 1280, "height": 900},
            )
            self._ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            self._page = self._ctx.new_page()
        self._cdp_attached = cdp_attached
        self._attach_token_verify_listener()
        # Warm up the SPA so recaptcha boots once + a human signal so it
        # scores us above the bot threshold.
        self._page.goto("https://www.nadlan.gov.il/",
                        wait_until="domcontentloaded",
                        timeout=self.nav_timeout_ms)
        _warmup_human_signal(self._page)
        return self

    def _attach_token_verify_listener(self):
        """Listen for /token-verify responses globally on the page.
        A 400/error means our reCAPTCHA Enterprise score is too low and
        ALL subsequent /deal-data calls will fail (inner=405). Caller
        should call recover_recaptcha_score() to rest + re-warm."""
        if self._token_verify_listener_attached or not self._page:
            return

        def _on_resp(resp):
            if "token-verify" not in resp.url:
                return
            if resp.status >= 400:
                self._token_verify_failures += 1
                if self._token_verify_failures % 5 == 1:
                    logger.warning("token-verify HTTP %d (count=%d) — "
                                    "reCAPTCHA score depleted",
                                    resp.status, self._token_verify_failures)

        try:
            self._page.on("response", _on_resp)
            self._token_verify_listener_attached = True
        except Exception as e:
            logger.debug("could not attach token-verify listener: %s", e)

    def recover_recaptcha_score(self, sleep_s: int = 1800,
                                  warmup_s: int = 60) -> None:
        """When /token-verify keeps failing, the ONLY fix is to let the
        score recover naturally (Google's anti-bot rolling window) AND
        signal renewed human engagement.

        Strategy:
          1. Clear nadlan + recaptcha cookies (drop poisoned session)
          2. Clear localStorage/sessionStorage
          3. Close the tainted tab
          4. Sleep `sleep_s` (rolling-window decay)
          5. Open fresh tab on home page (triggers fresh /token-verify)
          6. Run aggressive warmup (mouse + scroll for `warmup_s` sec)
          7. Reset failure counter
        """
        logger.warning("=== reCAPTCHA recovery: sleeping %ds + warmup %ds ===",
                        sleep_s, warmup_s)
        # Clear poisoned cookies + storage BEFORE sleep — gives the recaptcha
        # rolling window a chance to forget the bad signal.
        try:
            if self._page:
                self._page.evaluate("""() => {
                    try { localStorage.clear(); } catch (e) {}
                    try { sessionStorage.clear(); } catch (e) {}
                }""")
        except Exception:
            pass
        try:
            if self._ctx:
                # Drop cookies for nadlan + google recaptcha so we look like
                # a fresh visitor on next navigation.
                self._ctx.clear_cookies(domain="nadlan.gov.il")
                self._ctx.clear_cookies(domain=".nadlan.gov.il")
                self._ctx.clear_cookies(domain="www.nadlan.gov.il")
                self._ctx.clear_cookies(domain="api.nadlan.gov.il")
                self._ctx.clear_cookies(domain="data.nadlan.gov.il")
                self._ctx.clear_cookies(domain="www.google.com")
                self._ctx.clear_cookies(domain=".google.com")
                logger.info("cleared cookies for nadlan + google domains")
        except Exception as e:
            logger.warning("cookie clear partial failure: %s", e)
        try:
            if self._page:
                self._page.close()
        except Exception:
            pass
        time.sleep(sleep_s)
        # Open fresh tab
        try:
            if self._cdp_attached and self._ctx:
                self._page = self._ctx.new_page()
            elif self._ctx:
                self._page = self._ctx.new_page()
            self._token_verify_listener_attached = False
            self._attach_token_verify_listener()
            self._page.goto("https://www.nadlan.gov.il/",
                             wait_until="domcontentloaded",
                             timeout=self.nav_timeout_ms)
        except Exception as e:
            logger.error("post-cooldown new tab failed: %s", e)
            return
        # Aggressive warmup
        _aggressive_warmup(self._page, duration_s=warmup_s)
        self._token_verify_failures = 0
        logger.info("reCAPTCHA recovery complete")

    def __exit__(self, *_):
        try:
            # Close our tab even when CDP-attached (we created it fresh).
            if getattr(self, "_cdp_attached", False) and self._page:
                try:
                    self._page.close()
                except Exception:
                    pass
            if self._browser and not getattr(self, "_cdp_attached", False):
                self._browser.close()
        finally:
            if self._pw:
                self._pw.stop()

    # ------------------------------------------------------------------
    # Slice-based fetch (filter-driven, no rate-limit since SPA fetches)
    # ------------------------------------------------------------------
    # Phase 0 probes confirmed:
    #   * The /deal-data API filters server-side on ``room_num`` and
    #     ``type_order`` JWT fields. ``date_range`` is exposed via UI but
    #     wasn't probed for JWT payload.
    #   * Filter changes via UI clicks DON'T burn the per-IP rate limit:
    #     6+ clicks in 30s all returned HTTP 200.
    #   * Synthetic JWT requests via page.request.post still return 405
    #     (only SPA's fetch flow works). So we MUST drive UI clicks.
    #
    # Strategy: open settlement once, walk through (room, sort) combos
    # by clicking buttons. Each click triggers SPA's /deal-data and we
    # capture the response.

    SORT_OPTIONS = {
        "dealDate_down":   "תאריך עסקה - סדר יורד",   # newest first (default)
        "dealDate_up":     "תאריך עסקה - סדר עולה",   # oldest first
        "dealAmount_down": "מחיר העסקה - סדר יורד",   # most expensive first
        "dealAmount_up":   "מחיר העסקה - סדר עולה",   # cheapest first
        "roomNum_down":    "מס' חדרים - סדר יורד",
        "roomNum_up":      "מס' חדרים - סדר עולה",
    }
    ROOM_OPTIONS = {
        None:     None,             # no filter (all rooms)
        "1":      "1 חדרים",
        "2":      "2 חדרים",
        "3":      "3 חדרים",
        "4":      "4 חדרים",
        "5":      "5 חדרים",
        "6plus":  "6+ חדרים",
    }

    def fetch_settlement_slices(self, setl_code: str,
                                 slices: list[dict]) -> list[dict]:
        """Open the settlement deals page ONCE, then click filters in
        sequence — capture each /deal-data response as a slice.

        In-tab clicks beat per-slice navigation: probe_ui_filter_burst
        confirmed 6+ successive filter clicks all return 200 + 500 deals
        within one session. Re-navigation per slice was breaking session
        state (UUID quota, recaptcha) and only the first slice succeeded.
        """
        if not slices:
            return []
        captures: list[dict] = []

        def on_response(resp):
            if "/deal-data" not in resp.url or resp.status != 200:
                return
            try:
                body = resp.body()
            except Exception:
                return
            try:
                decoded = json.loads(gzip.decompress(base64.b64decode(body)))
                if decoded.get("statusCode") == 200:
                    data = decoded.get("data") or {}
                    captures.append({
                        "items": data.get("items") or [],
                        "total_rows": data.get("total_rows", 0),
                    })
            except Exception:
                pass

        page = self._page
        page.on("response", on_response)
        # Reset score tracker at settlement start. We'll detect new failures.
        score_failures_at_start = self._token_verify_failures
        results = []
        try:
            url = (f"https://www.nadlan.gov.il/?view=settlement"
                   f"&id={setl_code}&page=deals&_n={int(time.time()*1000)}")
            logger.info("setl %s: opening for %d slices", setl_code, len(slices))
            page.goto(url, wait_until="domcontentloaded",
                       timeout=self.nav_timeout_ms)
            page.wait_for_timeout(3500)  # SPA boot
            self._dismiss_error_modal()
            # Brief mouse jiggle so the page sees an "engaged" user before
            # we start machine-clicking filters.
            _human_jitter_pause(page, 800, 1500)

            consecutive_no_response = 0
            for idx, sl in enumerate(slices):
                room = sl.get("room_filter")
                sort = sl.get("sort_order") or "dealDate_down"
                t0 = time.time()
                err = None
                marker = len(captures)

                # CRITICAL: bail immediately if reCAPTCHA score is depleted.
                # Each click here would just trigger more /token-verify 400s
                # which further drops the score (death spiral).
                if self._token_verify_failures >= 2:
                    err = "score_depleted"
                    results.append({**sl, "deals": [], "total_rows": 0,
                                    "error": err})
                    # Skip remaining slices — they'll all fail too.
                    for sl2 in slices[idx + 1:]:
                        results.append({**sl2, "deals": [], "total_rows": 0,
                                         "error": err})
                    return results

                # Bail early if SPA is unresponsive: 3 consecutive no_response
                # = the SPA's data is cached and not re-firing /deal-data.
                # Mark remaining slices as no_response without trying.
                if consecutive_no_response >= 3:
                    results.append({**sl, "deals": [], "total_rows": 0,
                                    "error": "skip_spa_unresponsive"})
                    continue

                if not self._apply_room_filter(room):
                    err = "room_click_failed"
                elif not self._wait_for_capture(captures, marker + 1,
                                                  timeout_s=12):
                    err = "room_no_response"
                else:
                    page.wait_for_timeout(2500)
                    if sort != "dealDate_down":
                        marker = len(captures)
                        if not self._apply_sort_filter(sort):
                            err = "sort_click_failed"
                        elif not self._wait_for_capture(captures, marker + 1,
                                                          timeout_s=12):
                            err = "sort_no_response"
                        else:
                            _human_jitter_pause(page, 1800, 3500)

                if err:
                    if err in ("room_no_response", "sort_no_response"):
                        consecutive_no_response += 1
                    logger.warning("setl %s slice %d/%d (room=%s, sort=%s): %s",
                                    setl_code, idx + 1, len(slices),
                                    room or "all", sort, err)
                    results.append({**sl, "deals": [], "total_rows": 0,
                                    "error": err})
                    self._dismiss_error_modal()
                    # Reset dropdown state by clicking outside (body)
                    try:
                        page.evaluate(
                            "() => { document.body.click(); "
                            "document.querySelectorAll('.modal-backdrop,.dropdown.show').forEach("
                            "  (el) => { el.dispatchEvent(new MouseEvent('click',{bubbles:true})); }"
                            "); }"
                        )
                        page.wait_for_timeout(500)
                    except Exception:
                        pass
                    continue

                consecutive_no_response = 0
                last = captures[-1] if captures else {}
                results.append({
                    **sl,
                    "deals": last.get("items", []),
                    "total_rows": last.get("total_rows", 0),
                    "error": None,
                })
                logger.info("setl %s slice %d/%d (room=%s, sort=%s): "
                            "+%d deals, total=%s (%.1fs)",
                            setl_code, idx + 1, len(slices),
                            room or "all", sort,
                            len(last.get("items", [])),
                            last.get("total_rows"),
                            time.time() - t0)
            return results
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass

    def _fetch_one_slice(self, setl_code: str,
                          room_filter: Optional[str],
                          sort_order: str) -> Tuple[list[dict], int]:
        """Single-slice fetch: navigate fresh, click room (always) and sort
        (if non-default), capture the resulting /deal-data response."""
        captures: list[dict] = []

        def on_response(resp):
            if "/deal-data" not in resp.url or resp.status != 200:
                return
            try:
                body = resp.body()
            except Exception:
                return
            try:
                decoded = json.loads(gzip.decompress(base64.b64decode(body)))
                if decoded.get("statusCode") == 200:
                    data = decoded.get("data") or {}
                    captures.append({
                        "items": data.get("items") or [],
                        "total_rows": data.get("total_rows", 0),
                    })
            except Exception:
                pass

        page = self._page
        page.on("response", on_response)
        try:
            url = (f"https://www.nadlan.gov.il/?view=settlement"
                   f"&id={setl_code}&page=deals&_n={int(time.time()*1000)}")
            page.goto(url, wait_until="domcontentloaded",
                       timeout=self.nav_timeout_ms)
            # SPA needs ~3s to boot Vue + render filter UI before clicks work.
            page.wait_for_timeout(3500)
            self._dismiss_error_modal()

            marker = len(captures)
            if not self._apply_room_filter(room_filter):
                raise RuntimeError("room_click_failed")
            if not self._wait_for_capture(captures, marker + 1, timeout_s=10):
                raise RuntimeError("room_no_response")
            page.wait_for_timeout(500)

            if sort_order != "dealDate_down":
                marker = len(captures)
                if not self._apply_sort_filter(sort_order):
                    raise RuntimeError("sort_click_failed")
                if not self._wait_for_capture(captures, marker + 1,
                                                timeout_s=10):
                    raise RuntimeError("sort_no_response")
                page.wait_for_timeout(500)

            if not captures:
                raise RuntimeError("no_capture")
            last = captures[-1]
            return last["items"], last["total_rows"]
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass

    def _dismiss_error_modal(self) -> bool:
        """Detect the 'שגיאה בטעינת נתונים' modal and dismiss it. Returns
        True if a modal was dismissed."""
        try:
            dismissed = self._page.evaluate("""
                () => {
                    // Find any visible modal with the error title.
                    const modals = document.querySelectorAll('div.modal.show');
                    for (const m of modals) {
                        const title = m.querySelector('h3.title, .modal-title');
                        const titleText = title ? (title.innerText || '').trim() : '';
                        if (titleText.includes('שגיאה') || titleText.includes('בטעינת')) {
                            // Try clicking close button (X) first, then any 'אישור'
                            const closeBtn = m.querySelector('button.close, [aria-label="Close"]');
                            if (closeBtn) { closeBtn.click(); return true; }
                            const okBtn = m.querySelector('button');
                            if (okBtn) { okBtn.click(); return true; }
                        }
                    }
                    return false;
                }
            """)
            if dismissed:
                self._page.wait_for_timeout(500)
            return bool(dismissed)
        except Exception:
            return False

    def _wait_for_capture(self, captures: list, target_count: int,
                            timeout_s: float = 10) -> bool:
        """Block until ``len(captures) >= target_count`` or timeout. Uses
        page.wait_for_timeout to pump Playwright's event loop so the
        on_response handler can actually fire."""
        deadline = time.time() + timeout_s
        while len(captures) < target_count and time.time() < deadline:
            self._page.wait_for_timeout(300)
        return len(captures) >= target_count

    def _apply_room_filter(self, room_filter: Optional[str]) -> bool:
        """Open rooms dropdown then click the room option. Retries up to
        3 times because the Vue popover state is flaky between iterations."""
        if room_filter is None:
            label = "כל החדרים"
        else:
            label = self.ROOM_OPTIONS.get(room_filter)
            if not label:
                logger.warning("unknown room_filter=%s", room_filter)
                return False
        page = self._page
        for attempt in range(3):
            try:
                # Reset dropdown state — click elsewhere first
                if attempt > 0:
                    try:
                        page.locator("body").click(position={"x": 10, "y": 10},
                                                     timeout=2000)
                        page.wait_for_timeout(500)
                    except Exception:
                        pass
                page.locator("button.roomsBtn").first.click(timeout=5000)
                page.wait_for_timeout(400)  # match working probe pattern
                opt = page.locator(f"button.whomBtn:has-text('{label}')").first
                opt.click(timeout=5000)
                return True
            except Exception as e:
                if attempt < 2:
                    logger.debug("room [%s] attempt %d failed, retrying",
                                 label, attempt + 1)
                    page.wait_for_timeout(1500)
                else:
                    logger.warning("room click [%s] failed after 3 attempts: %s",
                                    label, str(e).splitlines()[0])
        return False

    def _apply_sort_filter(self, sort_order: str) -> bool:
        """Open sort dropdown + click option (matches working probe pattern)."""
        label = self.SORT_OPTIONS.get(sort_order)
        if not label:
            logger.warning("unknown sort_order=%s", sort_order)
            return False
        page = self._page
        try:
            page.locator("button.filterBtn:has-text('מיון')").first.click(timeout=5000)
            page.wait_for_timeout(400)
            opt = page.locator(f"button.dropdownBtn:has-text('{label}')").first
            opt.click(timeout=5000)
            return True
        except Exception as e:
            logger.warning("sort click [%s] failed: %s", label,
                            str(e).splitlines()[0])
            return False

    def fetch_settlement(self, setl_code: str, retries: int = 2) -> list[dict]:
        """Navigate to the settlement page and return ALL deals via pagination.

        Strategy:
          1. ``page.goto`` triggers the SPA's first /deal-data with
             fetch_number=1 (max 500 deals).
          2. We capture the request body so we have a valid JWT template
             with the right claims (signed reCAPTCHA token, etc.).
          3. For fetch_number=2..total_fetch we re-sign the JWT with the
             new fetch_number and POST it via ``page.evaluate(fetch(...))``
             so the request runs in the same browser context (same cookies,
             same session — token-verify passes once for all pages).
          4. Items are deduped by (assetId, dealDate, row_id) across pages.

        On 405 (token-verify rejected) we re-warm the SPA and retry the
        whole settlement.
        """
        url = f"https://www.nadlan.gov.il/?view=settlement&id={setl_code}&page=deals"

        for attempt in range(retries + 1):
            deals = []
            seen = set()
            captured_responses: list[bytes] = []
            captured_request: dict = {"body": None, "url": None, "headers": None}
            state = {"total_fetch": None, "total_rows": None}

            def on_request(req):
                if req.method == "POST" and "deal-data" in req.url:
                    if captured_request["body"] is None:
                        captured_request["body"] = req.post_data
                        captured_request["url"] = req.url
                        captured_request["headers"] = dict(req.headers)

            def on_response(resp):
                if "deal-data" not in resp.url:
                    return
                try:
                    captured_responses.append(resp.body())
                except Exception:
                    pass

            self._page.on("request", on_request)
            self._page.on("response", on_response)
            try:
                self._page.goto(url, wait_until="domcontentloaded",
                                timeout=self.nav_timeout_ms)
                _warmup_human_signal(self._page)

                # Wait for the first /deal-data response.
                deadline = time.time() + self.data_timeout_s
                while not captured_responses and time.time() < deadline:
                    self._page.wait_for_timeout(500)
                self._page.wait_for_timeout(1500)
            finally:
                try:
                    self._page.remove_listener("request", on_request)
                    self._page.remove_listener("response", on_response)
                except Exception:
                    pass

            # Parse responses captured so far.
            saw_405 = self._parse_responses(captured_responses, deals, seen, state)

            # If we got data and there are more pages, paginate.
            if deals and state["total_fetch"] and state["total_fetch"] > 1:
                logger.info("setl %s: got %d/%d deals on page 1 (total_fetch=%d), "
                            "paginating", setl_code, len(deals),
                            state["total_rows"], state["total_fetch"])
                ok = self._paginate_remaining(
                    setl_code, captured_request, state["total_fetch"],
                    deals, seen)
                if not ok:
                    logger.warning("setl %s: pagination failed at page %d, "
                                   "returning %d deals partial",
                                   setl_code, len(deals)//500 + 1, len(deals))

            if deals:
                return deals
            if not saw_405:
                # No 405 and no items — settlement is empty.
                return []

            # 405 only — refresh recaptcha and retry the settlement.
            logger.info("recaptcha refresh on settlement %s (attempt %d)",
                        setl_code, attempt + 1)
            self._page.goto("https://www.nadlan.gov.il/",
                            wait_until="domcontentloaded",
                            timeout=self.nav_timeout_ms)
            _warmup_human_signal(self._page)

        return []

    @staticmethod
    def _parse_responses(bodies: list[bytes], deals: list[dict],
                          seen: set, state: dict) -> bool:
        """Decode captured /deal-data bodies, append unique items, return saw_405."""
        saw_405 = False
        for body in bodies:
            try:
                decoded = json.loads(gzip.decompress(base64.b64decode(body)))
            except Exception as e:
                logger.warning("deal-data decode failed: %s", e)
                continue
            sc = decoded.get("statusCode")
            if sc == 405:
                saw_405 = True
                continue
            if sc == 200:
                data = decoded.get("data") or {}
                if state["total_fetch"] is None:
                    state["total_fetch"] = data.get("total_fetch")
                    state["total_rows"] = data.get("total_rows")
                for it in data.get("items") or []:
                    key = (it.get("assetId"), it.get("dealDate"),
                           it.get("row_id"))
                    if key not in seen:
                        seen.add(key)
                        deals.append(it)
        return saw_405

    _rc_widget_id = None

    def _ensure_grecaptcha_widget(self, site_key: str) -> bool:
        """The SPA loaded enterprise.js with ?render=explicit, so
        ``execute(siteKey, ...)`` is rejected.  Render an invisible widget
        with our site key — the resulting widgetId works with
        ``execute(widgetId, {action: ...})``."""
        if self._rc_widget_id is not None:
            return True
        try:
            wid = self._page.evaluate(
                """async (siteKey) => {
                    return await new Promise((resolve) => {
                        try {
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
                                        sitekey: siteKey,
                                        size: 'invisible',
                                        badge: 'bottomright',
                                    });
                                    resolve(id);
                                } catch (e) {
                                    resolve({err: String(e && e.message || e)});
                                }
                            });
                        } catch (e) {
                            resolve({err: 'ready threw: ' + String(e)});
                        }
                    });
                }""",
                site_key,
            )
        except Exception as e:
            logger.warning("recaptcha widget render threw: %s", e)
            return False
        if isinstance(wid, dict) and "err" in wid:
            logger.warning("recaptcha widget render err: %s", wid["err"])
            return False
        self._rc_widget_id = wid
        logger.info("recaptcha widget rendered, id=%s", wid)
        return True

    def _mint_token_verify_uuid(self, site_key: str, log: bool = False,
                                  setl_code: str = "") -> Optional[str]:
        """Replicate the SPA's pre-deal-data token mint:

           grecaptcha.execute → POST /token-verify → fresh UUID

        Returns the UUID string, or None on failure.
        """
        if not self._ensure_grecaptcha_widget(site_key):
            if log:
                logger.warning("setl %s: could not render recaptcha widget",
                               setl_code)
            return None

        # Step 1: fresh recaptcha token via widgetId (NOT siteKey string)
        try:
            result = self._page.evaluate(
                """async (widgetId) => {
                    const out = {token: null, err: null};
                    try {
                        out.token = await grecaptcha.enterprise.execute(
                            widgetId, {action: 'submit'});
                    } catch (e) {
                        out.err = String(e && e.message || e);
                    }
                    return out;
                }""",
                self._rc_widget_id,
            )
        except Exception as e:
            if log:
                logger.warning("setl %s: grecaptcha.execute threw: %s", setl_code, e)
            return None
        if log:
            logger.info("setl %s: grecaptcha token_len=%d err=%s",
                        setl_code, len(result.get("token") or ""), result.get("err"))
        recap = result.get("token") if result else None
        if not recap:
            return None

        # Step 2: POST to /token-verify (matching SPA's content-type=text/plain)
        try:
            resp = self._page.request.post(
                "https://api.nadlan.gov.il/token-verify",
                data=json.dumps({"token": recap}).encode("utf-8"),
                headers={"content-type": "text/plain"},
                timeout=20_000,
            )
        except Exception as e:
            if log:
                logger.warning("setl %s: /token-verify request failed: %s", setl_code, e)
            return None
        if resp.status != 200:
            if log:
                try:
                    err_body = resp.text()[:200]
                except Exception:
                    err_body = "<no body>"
                logger.warning("setl %s: /token-verify HTTP %d body=%s",
                                setl_code, resp.status, err_body)
            return None
        try:
            tv_body = resp.json()
        except Exception as e:
            if log:
                logger.warning("setl %s: /token-verify json decode failed: %s", setl_code, e)
            return None
        if log:
            logger.info("setl %s: /token-verify response=%s", setl_code,
                        json.dumps(tv_body, ensure_ascii=False)[:200])
        if not tv_body.get("ok"):
            return None
        return tv_body.get("token")

    def _paginate_remaining(self, setl_code: str, captured_request: dict,
                             total_fetch: int,
                             deals: list[dict], seen: set) -> bool:
        """Issue requests for fetch_number=2..total_fetch using the captured
        JWT template and the page's existing reCAPTCHA session.

        Returns True if all pages were fetched, False on first failure.
        """
        if not captured_request["body"]:
            logger.warning("setl %s: no request template captured, can't paginate",
                           setl_code)
            return False

        # Decode the original ## payload to learn the schema.
        try:
            wrapper = json.loads(captured_request["body"])
            reversed_jwt = wrapper.get("##")
            header, payload = _decode_reversed_jwt(reversed_jwt)
        except Exception as e:
            logger.warning("setl %s: could not decode JWT template: %s", setl_code, e)
            return False

        # Diagnostic: log what fields are in the JWT so we know what to
        # change when paginating. Also log URL + a non-sensitive header dump
        # so we can spot path-based or method-mismatch issues.
        logger.info("setl %s: JWT header keys=%s, payload keys=%s",
                    setl_code, list(header.keys()), list(payload.keys()))
        logger.info("setl %s: captured URL=%s", setl_code, captured_request["url"])
        _safe_hdrs = {
            k: v for k, v in (captured_request["headers"] or {}).items()
            if k.lower() not in ("cookie", "authorization")
        }
        logger.info("setl %s: captured headers keys=%s, content-type=%s",
                    setl_code, list(_safe_hdrs.keys()),
                    _safe_hdrs.get("content-type", _safe_hdrs.get("Content-Type")))

        url = captured_request["url"]
        headers = captured_request["headers"] or {}

        # reCAPTCHA Enterprise tokens are single-use: the server marks the
        # token spent after fetch_number=1 and rejects every subsequent JWT
        # with the same token (HTTP 405). To paginate, we mint a fresh
        # token from the page's own grecaptcha runtime for each page.
        # Find recaptcha site key. The SPA loads recaptcha with
        # ?render=explicit (manual mode) and passes the real site key into
        # grecaptcha.execute() at call time. So the script-tag URL is NOT
        # the site key — we have to dig.
        site_key = self._page.evaluate("""
            () => {
                const isSiteKey = (s) => typeof s === 'string'
                    && /^6L[A-Za-z0-9_-]{30,}$/.test(s);

                // 1. ___grecaptcha_cfg global (deep walk)
                try {
                    const cfg = window.___grecaptcha_cfg;
                    if (cfg && cfg.clients) {
                        const seen = new Set();
                        const stack = [cfg.clients];
                        while (stack.length) {
                            const o = stack.pop();
                            if (!o || typeof o !== 'object' || seen.has(o)) continue;
                            seen.add(o);
                            for (const k in o) {
                                if (isSiteKey(o[k])) return o[k];
                                if (typeof o[k] === 'object') stack.push(o[k]);
                            }
                        }
                    }
                } catch (e) {}

                // 2. Scan inline scripts and HTML for the 6L... pattern
                const html = document.documentElement.outerHTML;
                const m = html.match(/['"`](6L[A-Za-z0-9_-]{30,})['"`]/);
                if (m) return m[1];

                // 3. Walk all <iframe src=...> looking for k= param
                for (const f of document.querySelectorAll('iframe[src]')) {
                    const km = f.src.match(/[?&]k=(6L[A-Za-z0-9_-]+)/);
                    if (km) return km[1];
                }

                return null;
            }
        """)
        logger.info("setl %s: recaptcha site_key=%s", setl_code,
                    site_key[:30] + "..." if site_key else "NOT FOUND")

        # PAGINATION LIMIT (discovered by probing 2026-05-02):
        # The /deal-data endpoint rate-limits per IP at ~3 calls/minute.
        # Each navigation fires the SPA's auto-/deal-data (fetch=1) plus
        # our paginated call → 2 calls per pagination. After fetch=2
        # succeeds, fetch=3+ returns 403 even with fresh UUIDs / fresh
        # browser contexts.
        #
        # Practical cap: 2 pages = 1000 deals/settlement, up from 500.
        # For settlements with >1000 total deals (Tel Aviv, Jerusalem,
        # Haifa, etc.) historical depth requires per-parcel scraping
        # (legacy_api.py).
        max_fetches = min(total_fetch + 1, 3)  # fetch=2 only
        for fetch_num in range(2, max_fetches):
            new_payload = dict(payload)
            new_payload["fetch_number"] = fetch_num

            # 1. Capture a fresh /token-verify + /deal-data via re-navigation.
            # ABORT the SPA's own /deal-data so we don't burn the UUID's
            # quota — each UUID is only valid for 2 server-side calls.
            uuid_capture = {"value": None, "sk": None, "exp": None,
                            "deal_response": None, "captured_jwt": None,
                            "fresh_headers": None}

            def on_response(resp):
                if "/token-verify" in resp.url and resp.status == 200:
                    try:
                        body = resp.json()
                        if body.get("ok"):
                            uuid_capture["value"] = body.get("token")
                    except Exception:
                        pass

            def on_request(req):
                if (req.method == "POST" and "/deal-data" in req.url
                        and uuid_capture["captured_jwt"] is None):
                    try:
                        wrap = json.loads(req.post_data or "")
                        h2, p2 = _decode_reversed_jwt(wrap.get("##"))
                        uuid_capture["captured_jwt"] = (h2, p2)
                        uuid_capture["fresh_headers"] = dict(req.headers)
                    except Exception:
                        pass

            logger.info("setl %s fetch=%d: re-navigating", setl_code, fetch_num)
            # The SPA caches its /token-verify UUID in sessionStorage and
            # reuses it across navigations, so each UUID's 2-call quota is
            # exhausted after one pagination. Force a fresh /token-verify
            # by wiping storage + cookies before re-nav.
            try:
                self._page.evaluate("""() => {
                    try { sessionStorage.clear(); } catch (e) {}
                    try { localStorage.clear(); } catch (e) {}
                }""")
                # Also clear cookies on the api domain so any session token
                # is dropped.
                self._ctx.clear_cookies()
                self._page.goto(
                    "https://www.nadlan.gov.il/",
                    wait_until="domcontentloaded",
                    timeout=self.nav_timeout_ms,
                )
                self._page.wait_for_timeout(800)
            except Exception:
                pass

            self._page.on("response", on_response)
            self._page.on("request", on_request)
            try:
                self._page.goto(
                    f"https://www.nadlan.gov.il/?view=settlement&id={setl_code}"
                    f"&page=deals&_n={fetch_num}",
                    wait_until="domcontentloaded",
                    timeout=self.nav_timeout_ms,
                )
                logger.info("setl %s fetch=%d: nav complete, waiting for JWT",
                             setl_code, fetch_num)
                t_deadline = time.time() + 12
                while uuid_capture["captured_jwt"] is None and time.time() < t_deadline:
                    self._page.wait_for_timeout(300)
                logger.info("setl %s fetch=%d: jwt_captured=%s uuid=%s",
                             setl_code, fetch_num,
                             uuid_capture["captured_jwt"] is not None,
                             uuid_capture["value"][:20] if uuid_capture["value"] else None)
            finally:
                try:
                    self._page.remove_listener("response", on_response)
                    self._page.remove_listener("request", on_request)
                except Exception:
                    pass

            if not uuid_capture["captured_jwt"]:
                logger.warning("setl %s fetch=%d: SPA did not fire /deal-data on re-nav",
                                setl_code, fetch_num)
                return False

            # Use the freshly-captured JWT payload as the template for THIS page.
            h_fresh, p_fresh = uuid_capture["captured_jwt"]
            new_payload = dict(p_fresh)
            new_payload["fetch_number"] = fetch_num
            header = h_fresh
            # Use the fresh request headers so cookies / sec-ch-* match the
            # session that minted this UUID.
            request_headers = uuid_capture["fresh_headers"] or headers

            try:
                new_jwt = _sign_reversed_jwt(header, new_payload)
            except Exception as e:
                logger.warning("setl %s: re-sign failed at fetch=%d: %s",
                                setl_code, fetch_num, e)
                return False

            body = json.dumps({"##": new_jwt})
            # Use Playwright's APIRequestContext (page.request) — it inherits
            # cookies and TLS fingerprint from the page session. We pass the
            # full set of captured headers so the request is byte-identical
            # to what the SPA sent on fetch_number=1 (which the server
            # accepted).
            try:
                resp = self._page.request.post(
                    url,
                    data=body.encode("utf-8"),
                    headers=request_headers,
                    timeout=30000,
                )
            except Exception as e:
                logger.warning("setl %s fetch=%d: request failed: %s",
                                setl_code, fetch_num, e)
                return False
            if resp.status != 200:
                # 403 = rate limit / token-verify failure on this attempt.
                # Brief backoff and one retry before giving up.
                if resp.status in (403, 429):
                    logger.info("setl %s fetch=%d: HTTP %d — backoff 8s and retry",
                                 setl_code, fetch_num, resp.status)
                    self._page.wait_for_timeout(8000)
                    try:
                        resp = self._page.request.post(
                            url, data=body.encode("utf-8"),
                            headers=headers, timeout=30000)
                    except Exception as e:
                        logger.warning("setl %s fetch=%d: retry failed: %s",
                                        setl_code, fetch_num, e)
                        return False
                if resp.status != 200:
                    logger.warning("setl %s fetch=%d: HTTP %d — stopping pagination",
                                    setl_code, fetch_num, resp.status)
                    return False
            response_text = resp.text()
            # response_text is base64+gzip wrapper
            try:
                decoded = json.loads(
                    gzip.decompress(base64.b64decode(response_text))
                )
            except Exception as e:
                logger.warning("setl %s fetch=%d: response decode failed: %s",
                                setl_code, fetch_num, e)
                return False
            sc = decoded.get("statusCode")
            if sc != 200:
                logger.warning("setl %s fetch=%d: statusCode=%s — stopping pagination",
                                setl_code, fetch_num, sc)
                return False
            data = decoded.get("data") or {}
            new_items = 0
            for it in data.get("items") or []:
                key = (it.get("assetId"), it.get("dealDate"), it.get("row_id"))
                if key not in seen:
                    seen.add(key)
                    deals.append(it)
                    new_items += 1
            logger.info("setl %s fetch=%d: +%d new (cumulative %d)",
                        setl_code, fetch_num, new_items, len(deals))

            # Pacing between pages — re-navigation hits rate limit fast.
            # 4s gives the SPA + recaptcha enough breathing room.
            self._page.wait_for_timeout(4000)

        return True


# ---------------------------------------------------------------------------
# Bootstrap (first run)
# ---------------------------------------------------------------------------

def run_bootstrap(
    archive_dir: str,
    session=None,                   # unused — kept for archive_engine signature parity
    name_override: str = "",
    progress_cb: Optional[Callable] = None,
    settlements_filter: Optional[list] = None,
) -> Tuple[dict, dict]:
    """Walk every settlement once, capture top-500-newest deals each.

    Returns (file_info, checkpoint) shaped to match archive_engine.run_bootstrap
    so over_worker.execute_nadlan_archive_task can use the same push pipeline.
    """
    settlements = _load_settlements()
    if settlements_filter:
        keep = set(map(str, settlements_filter))
        settlements = {k: v for k, v in settlements.items() if k in keep}

    rows: list[dict] = []
    deal_keys: set[str] = set()
    failed: list[str] = []

    progress = progress_cb or (lambda **kw: None)

    with NadlanBrowser() as nb:
        for i, (setl_code, info) in enumerate(settlements.items(), 1):
            setl_name = info.get("SETL_NAME", "")
            try:
                deals = nb.fetch_settlement(setl_code)
            except Exception as e:
                logger.warning("settlement %s (%s) failed: %s", setl_code, setl_name, e)
                failed.append(setl_code)
                continue

            added = 0
            for d in deals:
                flat = _flatten_deal(d, setl_code, setl_name)
                key = _deal_key(flat)
                if key in deal_keys:
                    continue
                deal_keys.add(key)
                rows.append(flat)
                added += 1

            progress(current=i, total=len(settlements),
                     message=f"{setl_name}: +{added} (cumulative {len(rows)})")

    os.makedirs(archive_dir, exist_ok=True)
    safe = sanitize_filename(name_override or "nadlan_master")
    csv_path = os.path.join(archive_dir, f"{safe}.csv")
    excel_path = os.path.join(archive_dir, f"{safe}.xlsx")

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=NADLAN_COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    logger.info("Bootstrap CSV: %s (%d rows)", csv_path, len(rows))

    regenerate_excel_from_csv(csv_path, excel_path, NADLAN_COLS, safe)

    max_dd = max((r["dealDate"] for r in rows if r["dealDate"]), default="")
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    file_info = {
        "csv_path": csv_path,
        "excel_path": excel_path,
        "csv_basename": os.path.basename(csv_path),
        "excel_basename": os.path.basename(excel_path),
        "collector_name": "nadlan_settlements",
        "record_count": len(rows),
        "total_count": len(rows),
        "column_headers": NADLAN_COLS,
        "warning": (f"{len(failed)} settlements failed" if failed else ""),
    }
    checkpoint = {
        "archive_type": "nadlan_settlements",
        "archive_csv": os.path.basename(csv_path),
        "archive_excel": os.path.basename(excel_path),
        "column_headers": NADLAN_COLS,
        "total_archived": len(rows),
        "last_known_total": len(rows),
        "max_deal_date": max_dd,
        "last_run_utc": now_iso,
        "last_run_new_items": len(rows),
        # `known_urls` name kept for compatibility with archive_engine helpers
        # and over_worker checkpoint serialization. Holds deal keys here.
        "known_urls": deal_keys,
        "settlements_done": list(settlements.keys()),
        "settlements_failed": failed,
    }
    return file_info, checkpoint


# ---------------------------------------------------------------------------
# Incremental (daily run)
# ---------------------------------------------------------------------------

def run_incremental(
    archive_dir: str,
    checkpoint: dict,
    session=None,
    progress_cb: Optional[Callable] = None,
    lookback_days: int = 90,
    settlements_filter: Optional[list] = None,
) -> Tuple[int, dict]:
    """Top-500-newest scan per settlement; append new rows; write daily delta CSV."""
    if not checkpoint or not checkpoint.get("archive_csv"):
        raise RuntimeError("run_incremental requires a checkpoint with archive_csv set "
                           "(was bootstrap completed?)")

    settlements = _load_settlements()
    if settlements_filter:
        keep = set(map(str, settlements_filter))
        settlements = {k: v for k, v in settlements.items() if k in keep}

    deal_keys: set[str] = set(checkpoint.get("known_urls") or set())
    headers = checkpoint.get("column_headers") or NADLAN_COLS
    csv_path = os.path.join(archive_dir, checkpoint["archive_csv"])
    excel_path = os.path.join(archive_dir, checkpoint.get("archive_excel", ""))

    cutoff_iso = ""
    md = checkpoint.get("max_deal_date") or ""
    if md:
        try:
            d = datetime.fromisoformat(md)
            cutoff_iso = (d - timedelta(days=lookback_days)).date().isoformat()
        except ValueError:
            logger.warning("Bad max_deal_date in checkpoint: %r", md)

    new_rows: list[dict] = []
    failed: list[str] = []
    progress = progress_cb or (lambda **kw: None)

    with NadlanBrowser() as nb:
        for i, (setl_code, info) in enumerate(settlements.items(), 1):
            setl_name = info.get("SETL_NAME", "")
            try:
                deals = nb.fetch_settlement(setl_code)
            except Exception as e:
                logger.warning("settlement %s (%s) failed: %s", setl_code, setl_name, e)
                failed.append(setl_code)
                continue

            added = 0
            for d in deals:
                date = d.get("dealDate") or ""
                if cutoff_iso and date < cutoff_iso:
                    continue
                flat = _flatten_deal(d, setl_code, setl_name)
                key = _deal_key(flat)
                if key in deal_keys:
                    continue
                deal_keys.add(key)
                new_rows.append(flat)
                added += 1

            progress(current=i, total=len(settlements),
                     message=f"{setl_name}: +{added} new (total new: {len(new_rows)})")

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    checkpoint = dict(checkpoint)
    checkpoint["last_run_utc"] = now_iso
    checkpoint["last_run_new_items"] = len(new_rows)
    checkpoint["known_urls"] = deal_keys
    checkpoint["settlements_failed"] = failed

    if not new_rows:
        logger.info("Incremental: 0 new deals.")
        return 0, checkpoint

    append_to_csv(csv_path, new_rows, headers)
    delta_name = f"nadlan_delta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    delta_path = os.path.join(archive_dir, delta_name)
    with open(delta_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in new_rows:
            w.writerow(r)
    logger.info("Delta CSV: %s (%d rows)", delta_path, len(new_rows))

    if excel_path and os.path.exists(os.path.dirname(excel_path)):
        sheet = os.path.splitext(checkpoint["archive_csv"])[0]
        try:
            regenerate_excel_from_csv(csv_path, excel_path, headers, sheet)
        except Exception as e:
            logger.warning("Excel regenerate failed (non-fatal): %s", e)

    new_max = max((r["dealDate"] for r in new_rows if r["dealDate"]),
                  default=checkpoint.get("max_deal_date", ""))
    if new_max > (checkpoint.get("max_deal_date") or ""):
        checkpoint["max_deal_date"] = new_max
    checkpoint["total_archived"] = (checkpoint.get("total_archived", 0)
                                    + len(new_rows))
    checkpoint["last_known_total"] = checkpoint["total_archived"]

    logger.info("Incremental: +%d new deals (total %d).",
                len(new_rows), checkpoint["total_archived"])
    return len(new_rows), checkpoint
