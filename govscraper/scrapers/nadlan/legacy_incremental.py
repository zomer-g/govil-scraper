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

    def __enter__(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        # Prefer CDP attach to the user's already-warmed Chrome (high
        # recaptcha enterprise score). See run_chrome_for_nadlan.bat.
        # Fall back to a fresh Chrome launch if CDP isn't available.
        cdp = os.environ.get("NADLAN_CHROME_CDP", "http://localhost:9222")
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
            self._page = (self._ctx.pages[0]
                          if self._ctx.pages else self._ctx.new_page())
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
        # Warm up the SPA so recaptcha boots once + a human signal so it
        # scores us above the bot threshold.
        self._page.goto("https://www.nadlan.gov.il/",
                        wait_until="domcontentloaded",
                        timeout=self.nav_timeout_ms)
        _warmup_human_signal(self._page)
        return self

    def __exit__(self, *_):
        try:
            if self._browser and not getattr(self, "_cdp_attached", False):
                self._browser.close()
        finally:
            if self._pw:
                self._pw.stop()

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

        for fetch_num in range(2, total_fetch + 1):
            new_payload = dict(payload)
            new_payload["fetch_number"] = fetch_num

            # Mint a fresh UUID via the SPA's own /token-verify dance:
            # 1. grecaptcha.enterprise.execute(site_key) → recaptcha token
            # 2. POST /token-verify {"token": recap} → {"ok": true, "token": UUID}
            # 3. JWT payload['token'] := UUID
            # The UUID is single-use server-side, hence the per-page refresh.
            if site_key:
                fresh_uuid = self._mint_token_verify_uuid(site_key, log=(fetch_num == 2),
                                                          setl_code=setl_code)
                if fresh_uuid:
                    new_payload["token"] = fresh_uuid
                    # Bump exp ~30s into the future (server checks freshness).
                    new_payload["exp"] = int(time.time()) + 30
                else:
                    logger.warning("setl %s fetch=%d: failed to mint fresh UUID",
                                    setl_code, fetch_num)
                    return False

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
                    headers=headers,
                    timeout=30000,
                )
            except Exception as e:
                logger.warning("setl %s fetch=%d: request failed: %s",
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

            # Light pacing between pages to keep reCAPTCHA happy.
            self._page.wait_for_timeout(800)

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
