"""
Nadlan.gov.il (Israeli Tax Authority real-estate transactions) — API client.

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
                       data_timeout_s: int = 30):
    """Fetch real-estate deals for one (gush, chelka) parcel.

    Returns (items, parcel_meta, warning) where ``items`` is a list of deal
    dicts (with ``gush`` and ``chelka`` injected on each row), ``parcel_meta``
    is the /deal-info dict (settlement, neighborhood, etc.), and ``warning``
    is an optional string surfaced to the UI.

    Pagination note: the SPA loads up to 500 deals per fetch_number; parcels
    with more are split into multiple /deal-data calls triggered by clicking
    the pagination button. For MVP we capture only the first batch and warn
    if total_fetch > 1.
    """
    from playwright.sync_api import sync_playwright

    if headless is None:
        headless = os.environ.get("NADLAN_HEADLESS", "0") == "1"

    parcel_id = f"{gush}-{chelka}"
    url = f"https://www.nadlan.gov.il/?view=kparcel_all&id={parcel_id}&page=deals"
    progress = progress or (lambda **kw: None)

    items: list[dict] = []
    parcel_meta: dict = {}
    state = {"total_rows": None, "total_fetch": None}

    progress(current=0, total=0,
             message=f"פותח דפדפן עבור גוש {gush} חלקה {chelka}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            ctx = browser.new_context(locale="he-IL",
                                      viewport={"width": 1280, "height": 900})
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

            deadline = time.time() + data_timeout_s
            while state["total_rows"] is None and time.time() < deadline:
                page.wait_for_timeout(500)

            if state["total_rows"] is None:
                progress(message=f"לא התקבלה תשובה מ-nadlan עבור {parcel_id}")
                return items, parcel_meta

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

            return items, parcel_meta, warn
        finally:
            browser.close()


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
