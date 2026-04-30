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
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Tuple

import requests

from archive_engine import append_to_csv, regenerate_excel_from_csv
from file_handler import sanitize_filename

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
        self._browser = self._pw.chromium.launch(headless=self.headless)
        self._ctx = self._browser.new_context(
            locale="he-IL",
            viewport={"width": 1280, "height": 900},
        )
        self._page = self._ctx.new_page()
        # Warm up the SPA so recaptcha boots once.
        self._page.goto("https://www.nadlan.gov.il/",
                        wait_until="domcontentloaded",
                        timeout=self.nav_timeout_ms)
        self._page.wait_for_timeout(3000)
        return self

    def __exit__(self, *_):
        try:
            if self._browser:
                self._browser.close()
        finally:
            if self._pw:
                self._pw.stop()

    def fetch_settlement(self, setl_code: str, retries: int = 2) -> list[dict]:
        """Navigate to the settlement page, capture /deal-data, return deals.

        Returns the 500 most recent deals (fetch_number=1). If recaptcha is
        expired (statusCode 405 in decoded body) we re-warm the SPA and retry.
        """
        url = f"https://www.nadlan.gov.il/?view=settlement&id={setl_code}&page=deals"

        for attempt in range(retries + 1):
            captured: list[bytes] = []

            def on_response(resp):
                if "deal-data" not in resp.url:
                    return
                try:
                    captured.append(resp.body())
                except Exception:
                    pass

            self._page.on("response", on_response)
            try:
                self._page.goto(url, wait_until="domcontentloaded",
                                timeout=self.nav_timeout_ms)
                deadline = time.time() + self.data_timeout_s
                while not captured and time.time() < deadline:
                    self._page.wait_for_timeout(500)
                # Allow late paginated responses to land.
                self._page.wait_for_timeout(1500)
            finally:
                try:
                    self._page.remove_listener("response", on_response)
                except Exception:
                    pass

            deals = []
            saw_405 = False
            for body in captured:
                try:
                    decoded = json.loads(gzip.decompress(base64.b64decode(body)))
                except Exception as e:
                    logger.warning("deal-data decode failed for setl %s: %s", setl_code, e)
                    continue
                sc = decoded.get("statusCode")
                if sc == 405:
                    saw_405 = True
                    continue
                if sc == 200:
                    items = (decoded.get("data") or {}).get("items") or []
                    deals.extend(items)

            if deals:
                return deals
            if not saw_405:
                # No 405 and no items — settlement is empty.
                return []

            # 405 only — refresh recaptcha and retry.
            logger.info("recaptcha refresh on settlement %s (attempt %d)",
                        setl_code, attempt + 1)
            self._page.goto("https://www.nadlan.gov.il/",
                            wait_until="domcontentloaded",
                            timeout=self.nav_timeout_ms)
            self._page.wait_for_timeout(4000)

        return []


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
