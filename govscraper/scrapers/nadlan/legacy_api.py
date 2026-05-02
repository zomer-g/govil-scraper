"""
Nadlan.gov.il (Israeli Tax Authority real-estate transactions) — API client.

⚠️  KNOWN ISSUE (verified 2026-05-02):  As of this date the SPA at
nadlan.gov.il no longer issues the `POST /deal-data` request automatically
on page load — it appears to require an explicit user gesture or has been
reworked behind a different endpoint. tests/nadlan/test_validation.py B1/B3
fail with `items=0` for that reason. The fetch_parcel_deals implementation
below is unchanged from when it worked; the failure is real-world API drift,
not a regression in this codebase. Re-verification of the protocol is
needed before relying on this scraper for new collections.

Background — reverse-engineered from the SPA's JS bundle on 2026-04-28:

The SPA at https://www.nadlan.gov.il/?view=kparcel_all&id=<gush>-<chelka>&page=deals
loads transactions via POST https://api.nadlan.gov.il/deal-data, with a
base64-gzip-wrapped JSON response of shape:

    { "statusCode": 200,
      "data": { "total_rows": N, "total_fetch": K, "total_page": P,
                "items": [{assetId, dealDate, dealAmount, parcelNum, ...}, ...] } }

The request is signed with an HS256 JWT (secret hardcoded in the JS bundle:
"90c3e620192348f1bd46fcd9138c3c68"), reversed character-by-character, and sent
as {"##": <reversed-jwt>}. The payload also requires a Google reCAPTCHA
Enterprise server-token; without one the response is statusCode 405 (empty data).

A sibling endpoint POST /deal-info returns parcel-level metadata
({settlement_name, neighborhood_name, parcel_id, ...}) without reCAPTCHA — we
capture it for free during the same page navigation.

Implementation strategy: rather than mint reCAPTCHA tokens ourselves (headless
chromium is fingerprinted and rejected — empirically returns statusCode 405),
we drive a *visible* Chromium instance via Playwright. The browser handles JWT
signing + reCAPTCHA naturally; we intercept the responses and decode them.
Set NADLAN_HEADLESS=1 to override (useful only with stealth-patched chromium
or xvfb).
"""

import base64
import gzip
import json
import logging
import os
import time
from typing import Optional, Callable

logger = logging.getLogger(__name__)

# Fields we promote to the top of the CSV in a stable order. Any extras the
# server returns are appended after these.
PRIMARY_FIELDS = [
    "gush", "chelka",
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "address", "parcelNum",
    "neighborhoodName", "settlementName",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId",
    "row_id",
]


def _decode(body_text: str) -> dict:
    """Decode the base64+gzip wrapper used by /deal-data."""
    return json.loads(gzip.decompress(base64.b64decode(body_text)))


def fetch_parcel_deals(gush, chelka,
                       progress: Optional[Callable] = None,
                       headless: Optional[bool] = None,
                       nav_timeout_ms: int = 60000,
                       data_timeout_s: int = 30,
                       browser=None):
    """Fetch real-estate deals for one (gush, chelka) parcel.

    Returns (items, parcel_meta, warning) where ``items`` is a list of deal
    dicts (with ``gush`` and ``chelka`` injected on each row), ``parcel_meta``
    is the /deal-info dict (settlement, neighborhood, etc.), and ``warning``
    is an optional string surfaced to the UI.

    Pass an existing Playwright ``browser`` instance to reuse it across calls
    (the caller is responsible for its lifecycle). When ``browser`` is None a
    fresh Chromium is launched and closed internally.

    Pagination note: the SPA loads up to 500 deals per fetch_number; parcels
    with more are split into multiple /deal-data calls triggered by clicking
    the pagination button. For MVP we capture only the first batch and warn
    if total_fetch > 1.
    """
    from playwright.sync_api import sync_playwright

    if headless is None:
        headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"

    if browser is None:
        # Own the full playwright lifecycle via the context manager. Calling
        # __enter__/__exit__ directly leaks state on Python 3.14 → asyncio
        # loop conflict on the next call from the same process.
        with sync_playwright() as pw:
            # NOTE (2026-05-02 fix): nadlan rolled out a /token-verify
            # gate that scores reCAPTCHA Enterprise tokens; vanilla Chromium
            # gets scored too low and verification rejects with HTTP 400. Real
            # Chrome (`channel="chrome"`) passes. Falls back to Chromium if
            # Chrome isn't installed (rare on Windows but possible on CI).
            br = _launch_browser(pw, headless=headless)
            try:
                return _fetch_with_browser(
                    gush, chelka, br, progress,
                    nav_timeout_ms, data_timeout_s)
            finally:
                try:
                    br.close()
                except Exception:
                    pass

    return _fetch_with_browser(
        gush, chelka, browser, progress,
        nav_timeout_ms, data_timeout_s)


def _warmup_human_signal(page):
    """Move the mouse and scroll a bit so reCAPTCHA Enterprise scores us
    as human and the second /token-verify call passes (200 'ok:true')
    instead of being rejected with HTTP 400 on every retry.

    More aggressive than the original — when the same browser is reused
    across many parcels (worker pool mode), Google's anti-bot scores the
    fresh context lower than a fresh process. The cumulative warmup of
    ~6s + multiple scrolls compensates and matches standalone success.
    """
    import random as _rnd
    try:
        # Phase 1: cluster of mouse moves
        for _ in range(8):
            x = _rnd.randint(150, 1100)
            y = _rnd.randint(150, 800)
            page.mouse.move(x, y, steps=_rnd.randint(8, 20))
            page.wait_for_timeout(_rnd.randint(120, 350))
        # Phase 2: scroll down
        page.mouse.wheel(0, 400)
        page.wait_for_timeout(700)
        # Phase 3: more mouse + scroll up (mimics user reading)
        for _ in range(4):
            x = _rnd.randint(150, 1100)
            y = _rnd.randint(150, 800)
            page.mouse.move(x, y, steps=_rnd.randint(8, 20))
            page.wait_for_timeout(_rnd.randint(120, 350))
        page.mouse.wheel(0, -200)
        page.wait_for_timeout(700)
    except Exception as e:
        # Warmup is best-effort — never fail the scrape over it.
        logger.debug("warmup gesture failed: %s", e)


def _retry_warmup_if_no_data(page, state, retry_window_s: int = 8):
    """If after the initial warmup we still have no /deal-data response,
    do another round of warmup gestures to nudge the SPA into retrying
    /token-verify. Costs nothing on parcels that already returned data."""
    if state["total_rows"] is not None:
        return  # already got data, no retry needed
    logger.debug("no deal-data yet — triggering retry warmup")
    _warmup_human_signal(page)
    page.wait_for_timeout(retry_window_s * 1000)


def _launch_browser(pw, headless: bool):
    """Try real Chrome first, fall back to Chromium if not installed.

    Real Chrome bypasses nadlan's token-verify check that flags Chromium
    as bot. Add --disable-blink-features=AutomationControlled so the
    `navigator.webdriver` flag isn't set even on Chrome.
    """
    args = ["--disable-blink-features=AutomationControlled"]
    try:
        return pw.chromium.launch(channel="chrome", headless=headless, args=args)
    except Exception as e:
        logger.warning("nadlan: real Chrome unavailable (%s) — falling back to Chromium. "
                       "Token-verify likely to fail. Run "
                       "'python -m playwright install chrome' to fix.", e)
        return pw.chromium.launch(headless=headless, args=args)


def _fetch_with_browser(gush, chelka, browser, progress,
                        nav_timeout_ms, data_timeout_s):
    """Run one scrape against an already-open Playwright browser."""
    parcel_id = f"{gush}-{chelka}"
    url = f"https://www.nadlan.gov.il/?view=kparcel_all&id={parcel_id}&page=deals"
    progress = progress or (lambda **kw: None)

    items: list[dict] = []
    parcel_meta: dict = {}
    state = {"total_rows": None, "total_fetch": None}

    progress(current=0, total=0,
             message=f"פותח דפדפן עבור גוש {gush} חלקה {chelka}")

    ctx = browser.new_context(locale="he-IL",
                              viewport={"width": 1280, "height": 900})
    # Mask the most obvious automation flag — defence in depth on top of the
    # `--disable-blink-features=AutomationControlled` launch arg.
    ctx.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
    )
    try:
        page = ctx.new_page()

        def on_response(resp):
            u = resp.url
            if u.endswith("/deal-info") and resp.status == 200:
                try:
                    parcel_meta.update(json.loads(resp.text()))
                except Exception as e:
                    logger.warning("nadlan deal-info parse failed: %s", e)
            elif u.endswith("/deal-data") and resp.status == 200:
                try:
                    decoded = _decode(resp.text())
                    if decoded.get("statusCode") != 200:
                        logger.warning("nadlan deal-data statusCode=%s",
                                       decoded.get("statusCode"))
                        return
                    data = decoded.get("data") or {}
                    for it in data.get("items", []):
                        items.append(it)
                    state["total_rows"] = data.get("total_rows")
                    state["total_fetch"] = data.get("total_fetch")
                except Exception as e:
                    logger.warning("nadlan deal-data decode failed: %s", e)

        page.on("response", on_response)
        page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)

        # User-behaviour warmup so reCAPTCHA Enterprise gives a passing score.
        # The first /token-verify usually fails (400); the SPA retries after
        # a few seconds and the second one passes once the user looks "real".
        _warmup_human_signal(page)

        # First wait — give the SPA up to 12s to retry token-verify and
        # produce a /deal-data response after our initial warmup.
        first_window = min(12, data_timeout_s // 2)
        deadline = time.time() + first_window
        while state["total_rows"] is None and time.time() < deadline:
            page.wait_for_timeout(500)

        # If still no data, do a second round of warmup gestures and wait
        # the remaining timeout. Empirically this rescues parcels that fail
        # the first time when the same browser is reused across many calls.
        if state["total_rows"] is None:
            _retry_warmup_if_no_data(page, state, retry_window_s=8)
            deadline = time.time() + (data_timeout_s - first_window)
            while state["total_rows"] is None and time.time() < deadline:
                page.wait_for_timeout(500)

        if state["total_rows"] is None:
            progress(message=f"לא התקבלה תשובה מ-nadlan עבור {parcel_id}")
            return items, parcel_meta, None

        page.wait_for_timeout(1500)

        progress(current=len(items), total=state["total_rows"],
                 message=f"נאספו {len(items)} מתוך {state['total_rows']} עסקאות")

        warn = None
        if state["total_fetch"] and state["total_fetch"] > 1:
            warn = (f"לחלקה זו יש {state['total_rows']} עסקאות בחלוקה "
                    f"ל-{state['total_fetch']} עמודים — נטען רק העמוד הראשון")
            logger.warning("nadlan parcel %s has %s fetch pages; only first loaded",
                           parcel_id, state["total_fetch"])

        for it in items:
            it["gush"] = gush
            it["chelka"] = chelka
            trend = it.pop("trend", None) or {}
            it["trend_rate"] = trend.get("rate", "")
            it["trend_years"] = trend.get("years", "")
            prev = it.pop("prevDeals", None) or []
            it["prev_deals"] = json.dumps(prev, ensure_ascii=False) if prev else ""
            own = it.pop("ownership", None) or []
            it["ownership"] = json.dumps(own, ensure_ascii=False) if own else ""

        return items, parcel_meta, warn
    finally:
        ctx.close()


def order_columns(items: list[dict]) -> list[str]:
    """Stable column order: PRIMARY_FIELDS first, then any extras seen."""
    seen = set()
    headers: list[str] = []
    for k in PRIMARY_FIELDS:
        if any(k in it for it in items):
            headers.append(k)
            seen.add(k)
    for it in items:
        for k in it:
            if k not in seen:
                headers.append(k)
                seen.add(k)
    return headers
