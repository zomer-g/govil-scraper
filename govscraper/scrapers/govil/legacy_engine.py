"""
Gov.il Scraper Engine
Handles session management (cloudscraper + Playwright fallback),
URL parsing, API discovery, pagination, and data extraction.
"""

import re
import time
import logging
import html as html_mod
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Callable, List
from urllib.parse import urlparse, parse_qs

import cloudscraper
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class PageType(Enum):
    DYNAMIC_COLLECTOR = "dynamic_collector"
    TRADITIONAL_COLLECTOR = "traditional_collector"
    CONTENT_PAGE = "content_page"  # /he/pages/{name} — React SPA backed by ContentPageWebApi
    NADLAN_PARCEL = "nadlan_parcel"  # nadlan.gov.il single (gush, chelka) deal history
    GOVMAP_LAYER = "govmap_layer"  # govmap.gov.il GIS layer via OGC WFS (see govmap_engine)
    UNKNOWN = "unknown"


@dataclass
class ParsedURL:
    page_type: PageType
    collector_name: str
    office_id: Optional[str] = None
    original_url: str = ""
    query_params: dict = field(default_factory=dict)
    api_endpoint: str = ""
    api_method: str = "GET"


@dataclass
class FileAttachment:
    url: str
    filename: str
    item_index: int = 0
    file_type: str = ""


@dataclass
class ScrapeResult:
    items: List[dict] = field(default_factory=list)
    total_count: int = 0
    file_attachments: List[FileAttachment] = field(default_factory=list)
    collector_name: str = ""
    page_type: PageType = PageType.UNKNOWN
    column_headers: List[str] = field(default_factory=list)
    warning: Optional[str] = None
    # GovMap-only fields. Populated by govmap_engine; default values are
    # ignored by all other page types and by export_csv/export_excel.
    features: List = field(default_factory=list)        # list[govmap_engine.Feature]
    layer_id: str = ""
    bbox_itm: tuple = field(default_factory=tuple)
    bbox_wgs84: tuple = field(default_factory=tuple)
    geometry_type: str = ""
    srs: str = ""


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class GovILScraperError(Exception):
    pass

class CloudflareBlockError(GovILScraperError):
    pass

class InvalidURLError(GovILScraperError):
    pass

class APIEndpointError(GovILScraperError):
    pass


def _looks_like_cf_block(html: str) -> bool:
    """gov.il occasionally returns a Cloudflare 'you have been blocked' page
    with status 200, which makes every downstream check (json.loads, HTML
    anchor count, title parsing) report a misleading diagnosis. The block
    page consistently carries either an "Attention Required!" title with
    "you have been blocked" body, or the cf-error-details block."""
    lower = html.lower()
    return (
        ("attention required" in lower
         and "cloudflare" in lower
         and "you have been blocked" in lower)
        or "cf-error-details" in lower
    )


def _safe_json(resp, *, what: str):
    """Parse a response body as JSON, raising APIEndpointError with context
    on failure instead of a bare `JSONDecodeError: Expecting value: line 1
    column 1 (char 0)`.

    gov.il occasionally returns 200 OK with an empty body or an HTML error
    page (Cloudflare warm-ups, momentary backend hiccups, redirects to
    preview.newgov.gov.il). The bare json.JSONDecodeError that propagated
    out of `.json()` reached the worker as an opaque error with no URL or
    status — useless for debugging. Wrapping every API decode here turns
    those failures into actionable APIEndpointError messages, which the
    worker's existing GovILScraperError handler reports verbatim to the
    server's failure log.

    HTML body deserves its own message: cloudscraper warm-up handles real
    Cloudflare 403s, so a 200 + text/html here usually means the URL slug
    isn't actually backed by this JSON endpoint (page is static or the
    slug is wrong / page was removed) — the operator's fix is to update
    or untrack the dataset, not to retry.
    """
    import json
    text = resp.text or ""
    content_type = (resp.headers.get("content-type") or "").lower()

    if not text.strip():
        raise APIEndpointError(
            f"{what} returned empty body (status={resp.status_code}, "
            f"content-type={resp.headers.get('content-type', '?')})"
        )

    if "text/html" in content_type or text.lstrip().lower().startswith(("<!doctype html", "<html")):
        sample = text[:160].replace("\n", " ").replace("\r", " ")
        if _looks_like_cf_block(text):
            raise APIEndpointError(
                f"{what} blocked by Cloudflare (200 OK with block page). "
                f"The page is fine for a real browser; the scraper's "
                f"fingerprint is being flagged. Try again later, or this "
                f"URL may need a stealthier Playwright config. "
                f"(status={resp.status_code}, len={len(text)})"
            )
        raise APIEndpointError(
            f"{what} returned HTML instead of JSON — this URL is likely not "
            f"backed by the Content Page / collector API (wrong slug, removed "
            f"page, or page is static). Reconfigure the dataset's source URL "
            f"or untrack it. (status={resp.status_code}, len={len(text)}, "
            f"sample={sample!r})"
        )

    try:
        return resp.json()
    except (ValueError, json.JSONDecodeError) as e:
        sample = text[:160].replace("\n", " ").replace("\r", " ")
        raise APIEndpointError(
            f"{what} returned non-JSON body "
            f"(status={resp.status_code}, content-type={content_type or '?'}, "
            f"len={len(text)}, sample={sample!r}): {e}"
        ) from e


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

BASE_URL = "https://www.gov.il"

COMMON_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.gov.il/",
    "Origin": "https://www.gov.il",
}


class GovILSession:
    """HTTP session that handles Cloudflare challenges via cloudscraper,
    with automatic Playwright fallback."""

    def __init__(self, use_playwright_fallback: bool = True):
        self._use_playwright_fallback = use_playwright_fallback
        self._playwright_active = False
        self._browser = None
        self._pw_context = None
        self._session = self._init_cloudscraper()
        self._warmed = False

    # Browser configs to rotate on retry — different fingerprints
    _BROWSER_CONFIGS = [
        {"browser": "chrome", "platform": "windows", "mobile": False},
        {"browser": "chrome", "platform": "linux", "mobile": False},
        {"browser": "firefox", "platform": "windows", "mobile": False},
        {"browser": "chrome", "platform": "darwin", "mobile": False},
    ]

    def _init_cloudscraper(self, config_index: int = 0) -> cloudscraper.CloudScraper:
        cfg = self._BROWSER_CONFIGS[config_index % len(self._BROWSER_CONFIGS)]
        scraper = cloudscraper.create_scraper(browser=cfg, delay=10)
        scraper.headers.update(COMMON_HEADERS)
        return scraper

    # ---- Playwright fallback ------------------------------------------------

    def _init_playwright(self):
        """Lazy-init Playwright and extract cookies into the requests session.

        Stealth measures (in roughly increasing impact):
          - launch flags that disable the AutomationControlled blink
            feature, which Cloudflare's bot detection probes via
            navigator.webdriver
          - explicit User-Agent so we're not the default
            HeadlessChrome/<ver> string CF blocks on sight
          - realistic viewport so the page's CSS doesn't trip layout
            heuristics
          - init script that overrides navigator.webdriver to false and
            patches chrome runtime + permissions in the same shapes a
            real Chrome exposes

        These won't beat a serious anti-bot deployment but they're the
        cheapest changes that turn most "Attention Required" 200s into
        real responses on gov.il.
        """
        if self._playwright_active:
            return
        try:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--no-sandbox",
                ],
            )
            real_ua = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
            self._pw_context = self._browser.new_context(
                locale="he-IL",
                user_agent=real_ua,
                viewport={"width": 1920, "height": 1080},
                extra_http_headers=COMMON_HEADERS,
            )
            # Patch obvious headless tells before any page script runs.
            self._pw_context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['he-IL', 'he', 'en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                window.chrome = { runtime: {} };
                const origQuery = window.navigator.permissions && window.navigator.permissions.query;
                if (origQuery) {
                    window.navigator.permissions.query = (p) =>
                        p.name === 'notifications'
                            ? Promise.resolve({ state: Notification.permission })
                            : origQuery(p);
                }
            """)
            page = self._pw_context.new_page()
            # `domcontentloaded` rather than `networkidle`: with a realistic
            # UA gov.il pulls many third-party trackers (analytics, fonts,
            # tag managers) that never quiet down enough to satisfy
            # networkidle within 30s. We only need cookies + a parsed
            # document for the warm-up.
            page.goto(f"{BASE_URL}/he", wait_until="domcontentloaded", timeout=30000)
            # Extract cookies and inject into cloudscraper session
            for cookie in self._pw_context.cookies():
                self._session.cookies.set(
                    cookie["name"], cookie["value"],
                    domain=cookie.get("domain", ".gov.il"),
                    path=cookie.get("path", "/"),
                )
            # Bring cloudscraper's UA in line with the Playwright UA so the
            # subsequent .get() calls don't immediately re-trigger CF on a
            # UA-vs-cookie mismatch.
            self._session.headers["User-Agent"] = real_ua
            page.close()
            self._playwright_active = True
            self._warmed = True
            logger.info("Playwright fallback session established (stealth-tuned)")
        except Exception as e:
            logger.error("Playwright fallback failed: %s", e)
            raise CloudflareBlockError(
                "לא ניתן להתחבר לאתר gov.il — גם cloudscraper וגם Playwright נכשלו."
            ) from e

    # ---- Session warm-up ----------------------------------------------------

    def warm(self, max_retries: int = 5) -> bool:
        """Visit the main gov.il page to obtain Cloudflare cookies.
        Recreates cloudscraper with a different browser fingerprint on each retry."""
        if self._warmed:
            return True

        for attempt in range(max_retries):
            # Recreate cloudscraper with a rotated browser config on each retry
            if attempt > 0:
                logger.info("Rotating browser fingerprint (attempt %d)...", attempt + 1)
                self._session = self._init_cloudscraper(config_index=attempt)

            try:
                resp = self._session.get(f"{BASE_URL}/he", timeout=25)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    self._warmed = True
                    logger.info("cloudscraper session warmed (attempt %d)", attempt + 1)
                    return True
                logger.warning("Warm-up attempt %d: status=%d len=%d",
                               attempt + 1, resp.status_code, len(resp.text))
            except Exception as e:
                logger.warning("Warm-up attempt %d failed: %s", attempt + 1, e)

            if attempt < max_retries - 1:
                wait = 2 + attempt * 2  # 2s, 4s, 6s, 8s
                logger.info("Retrying warm-up in %ds...", wait)
                time.sleep(wait)

        # All cloudscraper retries failed — try Playwright
        if self._use_playwright_fallback:
            logger.info("Switching to Playwright fallback")
            try:
                self._init_playwright()
                return True
            except CloudflareBlockError:
                raise
            except Exception as e:
                logger.error("Playwright fallback also failed: %s", e)

        raise CloudflareBlockError(
            f"לא ניתן להתחבר ל-{BASE_URL} (warm-up של GovIlSession) — "
            f"cloudscraper נכשל לאחר {max_retries} ניסיונות. "
            f"הערה: GovIlSession מחמם תמיד מול {BASE_URL}; אם הסקרייפ בפועל "
            f"מכוון ליעד אחר (govmap.gov.il / nadlan.gov.il), כשל ה-warm "
            f"חוסם את כל הניסיונות הבאים."
        )

    # ---- HTTP methods with retry --------------------------------------------

    def get(self, url: str, params: dict = None, retries: int = 3,
            stream: bool = False) -> requests.Response:
        return self._request("GET", url, params=params, retries=retries,
                             stream=stream)

    def download_file(self, url: str, retries: int = 3) -> requests.Response:
        """Download a file, stripping API-specific headers for external domains.

        The Origin/Referer/Accept headers set for gov.il API calls cause
        external servers (e.g. police.gov.il) to reject requests with 404.
        This method temporarily removes those headers for the download.
        """
        if not self._warmed:
            self.warm()

        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
        is_external = host != "www.gov.il"

        if is_external:
            # Save and temporarily remove API-specific headers
            saved = {}
            for h in ("Accept", "Origin", "Referer"):
                if h in self._session.headers:
                    saved[h] = self._session.headers.pop(h)
            try:
                return self._request("GET", url, retries=retries, stream=False)
            finally:
                # Restore headers
                self._session.headers.update(saved)
        else:
            return self._request("GET", url, retries=retries, stream=False)

    def post(self, url: str, json_data: dict = None,
             retries: int = 3, extra_headers: dict = None) -> requests.Response:
        return self._request("POST", url, json_data=json_data, retries=retries,
                             extra_headers=extra_headers)

    def _request(self, method: str, url: str, params: dict = None,
                 json_data: dict = None, retries: int = 3,
                 stream: bool = False, extra_headers: dict = None) -> requests.Response:
        if not self._warmed:
            self.warm()

        last_error = None
        for attempt in range(retries):
            try:
                headers = {**extra_headers} if extra_headers else None
                if method == "GET":
                    resp = self._session.get(url, params=params, timeout=30,
                                             stream=stream, headers=headers)
                else:
                    resp = self._session.post(url, json=json_data, timeout=30,
                                              headers=headers)

                if resp.status_code == 403:
                    logger.warning("403 on attempt %d for %s", attempt + 1, url)
                    self._warmed = False
                    if attempt < retries - 1:
                        self.warm()
                    continue

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning("Rate limited, waiting %ds", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp

            except requests.RequestException as e:
                last_error = e
                logger.warning("Request error attempt %d: %s", attempt + 1, e)
                if attempt < retries - 1:
                    time.sleep(1)

        # All retries exhausted — try Playwright if not already active
        if self._use_playwright_fallback and not self._playwright_active:
            logger.info("All cloudscraper retries failed, trying Playwright")
            self._warmed = False
            self._init_playwright()
            return self._request(method, url, params=params,
                                 json_data=json_data, retries=1, stream=stream,
                                 extra_headers=extra_headers)

        raise APIEndpointError(
            f"כל הניסיונות נכשלו עבור {url}: {last_error}"
        )

    def render_page_html(self, url: str, *, timeout_ms: int = 30000) -> Optional[str]:
        """Render `url` in Playwright and return post-JS HTML, or None.

        Cloudscraper hands back the pre-render SPA shell for /he/pages/X
        URLs that aren't backed by the Content Page JSON API; the actual
        document links land in the DOM only after the page's JS runs.
        This method gives the HTML-fallback path a way to read those
        links by literally rendering the page in headless chromium.

        Returns None on any failure — Playwright init issues, navigation
        timeouts, etc. — so the caller treats absence uniformly as "we
        couldn't render this".
        """
        if not self._use_playwright_fallback:
            return None
        try:
            if not self._playwright_active:
                self._init_playwright()
        except Exception as e:
            logger.warning("Playwright init failed for render of %s: %s", url, e)
            return None
        if not self._pw_context:
            return None
        try:
            page = self._pw_context.new_page()
            try:
                # First try networkidle for a fully-rendered SPA (most JS-
                # heavy pages have it within ~10s). If trackers keep the
                # network warm past the budget, fall back to domcontentloaded
                # — that gives us the post-JS DOM after the page's own load
                # event without waiting for tag-manager noise.
                try:
                    page.goto(url, wait_until="networkidle", timeout=timeout_ms)
                except Exception as e:
                    logger.info(
                        "Playwright networkidle timeout for %s (%s); "
                        "retrying with domcontentloaded", url, e,
                    )
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    # Give late-loading content a moment after DCL.
                    try:
                        page.wait_for_load_state("load", timeout=10000)
                    except Exception:
                        pass
                return page.content()
            finally:
                page.close()
        except Exception as e:
            logger.warning("Playwright render failed for %s: %s", url, e)
            return None

    def close(self):
        if self._browser:
            self._browser.close()
        if hasattr(self, "_pw") and self._pw:
            self._pw.stop()


# ---------------------------------------------------------------------------
# URL Parsing
# ---------------------------------------------------------------------------

# Regex for /he/departments/dynamiccollectors/{name} (case-insensitive)
RE_DYNAMIC = re.compile(
    r"/he/departments?/dynamiccollectors?/([^/?#]+)", re.IGNORECASE
)
# Regex for /he/collectors/{name}
RE_TRADITIONAL = re.compile(
    r"/he/collectors?/([^/?#]+)", re.IGNORECASE
)
# Regex for /he/pages/{name} — React SPA pages backed by ContentPageWebApi
RE_CONTENT_PAGE = re.compile(
    r"/he/pages/([^/?#]+)", re.IGNORECASE
)


def parse_gov_url(url: str) -> ParsedURL:
    """Parse a gov.il page URL and determine the page type and parameters."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    params = parse_qs(parsed.query)
    host = (parsed.hostname or "").lower()

    # GovMap.gov.il GIS layer: ?lay=<id>(&c=x,y or &bbox=...)
    if host.endswith("govmap.gov.il"):
        # Lazy import to avoid loading pyproj/wfs deps for non-govmap users
        from govscraper.scrapers.govmap.legacy_engine import parse_govmap_url
        return parse_govmap_url(url, params)

    # Nadlan.gov.il parcel page: ?view=kparcel_all&id=<gush>-<chelka>
    if host.endswith("nadlan.gov.il"):
        view = (params.get("view", [None])[0] or "").lower()
        pid = params.get("id", [None])[0] or ""
        if view == "kparcel_all" and "-" in pid:
            gush, _, chelka = pid.partition("-")
            return ParsedURL(
                page_type=PageType.NADLAN_PARCEL,
                collector_name=f"nadlan_{gush}_{chelka}",
                office_id=None,
                original_url=url,
                query_params={"gush": gush, "chelka": chelka},
            )
        raise InvalidURLError(
            f"כתובת nadlan.gov.il לא נתמכת: {url}\n"
            "נתמך כרגע: ?view=kparcel_all&id=<גוש>-<חלקה>&page=deals"
        )

    # DynamicCollector
    m = RE_DYNAMIC.search(path)
    if m:
        collector_name = m.group(1)
        office_id = params.get("officeId", [None])[0] or params.get("OfficeId", [None])[0]
        return ParsedURL(
            page_type=PageType.DYNAMIC_COLLECTOR,
            collector_name=collector_name,
            office_id=office_id,
            original_url=url,
            query_params={k: v[0] for k, v in params.items()},
        )

    # Traditional Collector
    m = RE_TRADITIONAL.search(path)
    if m:
        collector_name = m.group(1)
        office_id = params.get("officeId", [None])[0] or params.get("OfficeId", [None])[0]
        return ParsedURL(
            page_type=PageType.TRADITIONAL_COLLECTOR,
            collector_name=collector_name,
            office_id=office_id,
            original_url=url,
            query_params={k: v[0] for k, v in params.items()},
        )

    # Content Page (React SPA)
    m = RE_CONTENT_PAGE.search(path)
    if m:
        return ParsedURL(
            page_type=PageType.CONTENT_PAGE,
            collector_name=m.group(1),
            office_id=None,
            original_url=url,
            query_params={k: v[0] for k, v in params.items()},
        )

    raise InvalidURLError(
        f"לא ניתן לזהות את סוג הדף עבור הכתובת: {url}\n"
        "נתמכים: DynamicCollectors, Collectors (Publications), ו-/he/pages/"
    )


# ---------------------------------------------------------------------------
# GUID extraction from page HTML
# ---------------------------------------------------------------------------

# DynamicCollector API endpoint (always POST)
DYNAMIC_API_URL = f"{BASE_URL}/he/api/DynamicCollector"

# Traditional collector API (GET)
TRADITIONAL_ENDPOINT = f"{BASE_URL}/CollectorsWebApi/api/DataCollector/GetResults"
TRADITIONAL_LAYOUT_ENDPOINT = f"{BASE_URL}/CollectorsWebApi/api/DataCollector/GetLayoutCollectorModel"

# Regex to extract the GUID from the ng-init attribute
# In raw HTML the quotes are entity-encoded: &#39; for ' and &quot; for "
# Pattern matches GUID anywhere after initCtrl( in the ng-init value
RE_GUID_HTML = re.compile(
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)


@dataclass
class DynamicPageConfig:
    """Configuration extracted from a DynamicCollector HTML page."""
    template_id: str = ""
    results_api_url: str = ""      # Custom API URL (empty = use standard)
    x_client_id: str = ""          # x-client-id header for custom APIs
    items_per_page: int = 20
    page_title: str = ""           # Hebrew page title extracted from HTML


def _extract_page_title(page_html: str) -> str:
    """Extract a meaningful Hebrew page title from gov.il HTML.

    Tries <h1>, then <title> (stripping the ' | gov.il' suffix).
    Returns empty string if nothing found.
    """
    # Try <h1> first — usually the most accurate content title
    m = re.search(r'<h1[^>]*>\s*(.+?)\s*</h1>', page_html, re.DOTALL)
    if m:
        title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        if title:
            return title

    # Fallback to <title> tag, strip common suffixes
    m = re.search(r'<title[^>]*>\s*(.+?)\s*</title>', page_html, re.DOTALL | re.IGNORECASE)
    if m:
        title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        # Remove gov.il suffix patterns like " | gov.il" or " - אתר ממשלתי"
        title = re.sub(r'\s*[|\-–—]\s*gov\.il.*$', '', title, flags=re.IGNORECASE).strip()
        title = re.sub(r'\s*[|\-–—]\s*אתר ממשלתי.*$', '', title).strip()
        if title:
            return title

    return ""


def extract_dynamic_page_config(session: GovILSession, page_url: str) -> DynamicPageConfig:
    """Fetch the DynamicCollector HTML page and extract configuration from ng-init.

    Standard DynamicCollectors use POST /he/api/DynamicCollector with a GUID.
    Custom DynamicCollectors provide a resultsApiURL (e.g. menifa, legal-advisor-guidelines).
    """
    resp = session.get(page_url)
    page_html = resp.text
    config = DynamicPageConfig()
    config.page_title = _extract_page_title(page_html)

    m_init = re.search(r'ng-init="(dynamicCtrl\.Events\.initCtrl\([^"]*)"', page_html)
    if not m_init:
        # Fallback: search near 'initCtrl'
        idx = page_html.find("initCtrl")
        if idx >= 0:
            chunk = html_mod.unescape(page_html[idx:idx + 2000])
            guids = RE_GUID_HTML.findall(chunk)
            if guids:
                config.template_id = guids[0]
                return config
        raise APIEndpointError(
            "לא ניתן לחלץ הגדרות מדף ה-HTML.\n"
            f"כתובת: {page_url}"
        )

    init_text = html_mod.unescape(m_init.group(1))

    # Extract all GUIDs
    guids = RE_GUID_HTML.findall(init_text)

    # Extract custom API URL (https://...)
    url_match = re.search(r"'(https?://[^']+)'", init_text)
    if url_match:
        config.results_api_url = url_match.group(1)
        logger.info("Found custom resultsApiURL: %s", config.results_api_url)

    if guids:
        config.template_id = guids[0]
        # If there's a custom API URL, the second GUID is typically the x-client-id
        if config.results_api_url and len(guids) >= 2:
            config.x_client_id = guids[1]
        logger.info("Extracted DynamicTemplateID: %s", config.template_id)

    # Extract items per page (number after the URL in ng-init args)
    ipp_match = re.search(r"',\s*(\d+)\s*,", init_text)
    if ipp_match:
        config.items_per_page = int(ipp_match.group(1))

    if not config.template_id:
        raise APIEndpointError(
            "לא ניתן לחלץ את מזהה התבנית (DynamicTemplateID) מדף ה-HTML.\n"
            f"כתובת: {page_url}"
        )

    return config


def _extract_total(data: dict) -> Optional[int]:
    """Try to extract total result count from various JSON structures."""
    for key in ("TotalResults", "totalResults", "total", "Total", "count"):
        if key in data:
            try:
                return int(data[key])
            except (ValueError, TypeError):
                continue
    # Check nested
    if isinstance(data.get("result"), dict):
        return _extract_total(data["result"])
    if isinstance(data.get("Result"), dict):
        return _extract_total(data["Result"])
    return None


def _extract_items(data: dict) -> List[dict]:
    """Extract the results array from various JSON structures."""
    for key in ("Results", "results", "Items", "items", "data", "Data"):
        if key in data and isinstance(data[key], list):
            return data[key]
    # Check nested
    if isinstance(data.get("result"), dict):
        return _extract_items(data["result"])
    if isinstance(data.get("Result"), dict):
        return _extract_items(data["Result"])
    # Maybe the response itself is a list
    if isinstance(data, list):
        return data
    return []


# ---------------------------------------------------------------------------
# Plain-text extraction for HTML-valued fields
# ---------------------------------------------------------------------------

# Matches any HTML tag — used as a quick "looks like HTML" signal
_HTML_TAG_RE = re.compile(r"<[a-zA-Z/!][^>]*>")


def _looks_like_html(key: str, value) -> bool:
    """Heuristic: is this string value meant to be HTML?

    - Explicit key name hints win (`...Html`, `...HtmlString`, `Html...`)
    - Otherwise, presence of at least one real tag + one structural tag
      (<p>, <div>, <li>, <br>) is treated as HTML.
    """
    if not isinstance(value, str) or not value:
        return False
    lkey = key.lower()
    if "html" in lkey:
        return True
    # Fallback: structural HTML tags mean it's meant to be rendered
    if _HTML_TAG_RE.search(value) and any(
        tag in value.lower() for tag in ("<p", "<div", "<li", "<br", "<table")
    ):
        return True
    return False


def _html_to_plain_text(key: str, value) -> Optional[str]:
    """Convert an HTML-looking string value to readable plain text.

    Returns None if the value isn't HTML (caller should then skip adding a
    plain-text column). Uses BeautifulSoup for parsing and collapses
    whitespace to keep CSV cells tidy.
    """
    if not _looks_like_html(key, value):
        return None
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None
    try:
        soup = BeautifulSoup(value, "html.parser")
        # Drop style/script/head blocks (they leak CSS/JS into the text)
        for tag in soup(["style", "script", "head"]):
            tag.decompose()
        # Insert line breaks for block-level boundaries so paragraphs
        # don't collapse onto one line
        for tag in soup.find_all(["p", "li", "br", "tr", "div", "h1", "h2",
                                   "h3", "h4", "h5", "h6"]):
            tag.append("\n")
        text = soup.get_text(separator=" ")
        # Collapse runs of whitespace, preserve single newlines
        lines = [re.sub(r"[ \t\xa0]+", " ", ln).strip()
                 for ln in text.splitlines()]
        text = "\n".join(ln for ln in lines if ln)
        return text
    except Exception:
        return None


def _plain_key_for(full_key: str) -> str:
    """Produce a sibling column name for the plain-text version of an HTML
    field. Strips trailing `HtmlString` / `Html` where present, else appends
    `_text`.
    """
    for suffix in ("HtmlString", "HtmlText", "Html", "_html"):
        if full_key.endswith(suffix):
            stripped = full_key[: -len(suffix)]
            # Avoid empty or trailing-dot names
            return (stripped.rstrip("._") or full_key) + "_text"
    return full_key + "_text"


# ---------------------------------------------------------------------------
# Main Scraper
# ---------------------------------------------------------------------------

class GovILScraper:
    """Scrapes gov.il collector pages via their internal APIs."""

    def __init__(self, session: GovILSession,
                 progress_callback: Optional[Callable] = None,
                 page_size: int = 20):
        self.session = session
        self.progress = progress_callback or (lambda **kw: None)
        self.page_size = page_size

    def scrape(self, url: str) -> ScrapeResult:
        parsed = parse_gov_url(url)

        if parsed.page_type == PageType.DYNAMIC_COLLECTOR:
            return self._scrape_dynamic(parsed)
        elif parsed.page_type == PageType.TRADITIONAL_COLLECTOR:
            return self._scrape_traditional(parsed)
        elif parsed.page_type == PageType.CONTENT_PAGE:
            return self._scrape_content_page(parsed)
        elif parsed.page_type == PageType.NADLAN_PARCEL:
            return self._scrape_nadlan(parsed)
        elif parsed.page_type == PageType.GOVMAP_LAYER:
            from govscraper.scrapers.govmap.legacy_engine import scrape_govmap
            return scrape_govmap(self.session, parsed,
                                 progress_callback=self.progress)
        else:
            raise InvalidURLError(f"סוג דף לא נתמך: {parsed.page_type}")

    # ---- DynamicCollector ---------------------------------------------------

    def _scrape_dynamic(self, parsed: ParsedURL) -> ScrapeResult:
        # Step 1: Fetch the HTML page to extract config (GUID, custom API URL, etc.)
        config = extract_dynamic_page_config(self.session, parsed.original_url)
        parsed.api_method = "POST"

        # Use page title instead of URL slug when available
        if config.page_title:
            parsed.collector_name = config.page_title
            logger.info("Using page title as collector name: %s", config.page_title)

        use_custom_api = bool(config.results_api_url)
        api_url = config.results_api_url or DYNAMIC_API_URL
        parsed.api_endpoint = api_url
        page_size = config.items_per_page or self.page_size

        if use_custom_api:
            logger.info("Using custom API: %s (x-client-id: %s)",
                        api_url, config.x_client_id or "none")

        all_items = []
        total_count = 0
        skip = 0
        warning = None

        # Some custom APIs (e.g. gov.il police endpoints) accept a `limit`
        # parameter to return many items in one call. Using it turns a 32k
        # record scrape from ~3200 calls (default 10/page) into ~7 calls.
        # The DOJ open-api endpoint returns 500 if `limit` is included, so
        # we only opt in per-API.
        supports_limit = use_custom_api and "/api/police/" in api_url.lower()
        batch_limit = 5000 if supports_limit else None

        # Extra filters from the URL (e.g. publications_subject=1, Type=2).
        # Pagination params are controlled internally, so we exclude them.
        _reserved = {"skip", "limit", "from"}
        extra_filters: dict[str, object] = {
            k: v for k, v in (parsed.query_params or {}).items()
            if k.lower() not in _reserved and v not in (None, "")
        }

        while True:
            try:
                if use_custom_api:
                    # Custom API: POST {"skip": N} with x-client-id header
                    extra_headers = {}
                    if config.x_client_id:
                        extra_headers["x-client-id"] = config.x_client_id
                    body = {"skip": skip}
                    if batch_limit:
                        body["limit"] = batch_limit
                    # Forward URL filters as-is (custom APIs typically accept
                    # them at the top level, e.g. {"skip":0,"Type":2})
                    for k, v in extra_filters.items():
                        body.setdefault(k, v)
                    resp = self.session.post(api_url, json_data=body,
                                             extra_headers=extra_headers)
                    _what = f"DynamicCollector custom API {api_url} (skip={skip})"
                else:
                    # Standard DynamicCollector API — each filter goes in
                    # QueryFilters as {"<name>": {"Query": <value>}}. Without
                    # this, filters like publications_subject=1 on the URL
                    # are silently ignored and we scrape the entire dataset
                    # instead of the filtered subset.
                    query_filters: dict[str, dict] = {"skip": {"Query": skip}}
                    for k, v in extra_filters.items():
                        # DynamicCollector expects string/number in Query
                        try:
                            qv = int(v)
                        except (TypeError, ValueError):
                            qv = v
                        query_filters[k] = {"Query": qv}
                    resp = self.session.post(DYNAMIC_API_URL, json_data={
                        "DynamicTemplateID": config.template_id,
                        "QueryFilters": query_filters,
                        "From": skip,
                    })
                    _what = f"DynamicCollector API (template={config.template_id}, skip={skip})"

                try:
                    data = _safe_json(resp, what=_what)
                except APIEndpointError as _api_err:
                    # CF block on a DynamicCollector page mid-scrape: warm
                    # Playwright once (sets stealth UA + CF cookies on
                    # cloudscraper) and retry the same skip without losing
                    # progress. If we've already warmed and still hit it,
                    # re-raise — a stealthier strategy would have to be added.
                    if (
                        "blocked by Cloudflare" in str(_api_err)
                        and not self.session._playwright_active
                    ):
                        logger.info(
                            "DynamicCollector CF-blocked at skip=%d; "
                            "warming Playwright and retrying",
                            skip,
                        )
                        try:
                            self.session._init_playwright()
                        except Exception:
                            raise _api_err
                        continue
                    raise
                items = _extract_items(data)

                if skip == 0:
                    total_count = _extract_total(data) or len(items)

                all_items.extend(items)
                self.progress(
                    current=len(all_items),
                    total=total_count,
                    message=f"נאספו {len(all_items)} מתוך {total_count} רשומות",
                )

                if not items or len(all_items) >= total_count:
                    break

                # Advance by the actual number received — safer than relying
                # on page_size, since with `limit` the server may return
                # batch_limit items while page_size is still the page's
                # default (e.g. 10).
                skip += len(items)
                delay = 1.0 if total_count > 500 else 0.5
                time.sleep(delay)

            except GovILScraperError:
                raise
            except Exception as e:
                logger.error("Pagination error at skip=%d: %s", skip, e)
                warning = f"חלק מהנתונים עלולים להיות חסרים (שגיאה בעמוד {skip})"
                break

        return self._build_result(all_items, total_count, parsed, warning)

    # ---- Traditional Collector ----------------------------------------------

    def _scrape_traditional(self, parsed: ParsedURL) -> ScrapeResult:
        parsed.api_endpoint = TRADITIONAL_ENDPOINT

        # Dynamically discover CollectorType values from the layout model API.
        # Must use the original URL slug for the API call.
        collector_types = self._discover_collector_types(parsed.collector_name)

        # Extract page title from HTML for a meaningful collector name
        try:
            resp = self.session.get(parsed.original_url)
            page_title = _extract_page_title(resp.text)
            if page_title:
                parsed.collector_name = page_title
                logger.info("Using page title as collector name: %s", page_title)
            else:
                # SPA page — clean up the URL slug as fallback
                parsed.collector_name = parsed.collector_name.replace("-", " ").replace("_", " ")
        except Exception as e:
            logger.warning("Failed to extract page title: %s", e)
        logger.info("Collector types for '%s': %s", parsed.collector_name, collector_types)

        all_items = []
        total_count = 0
        skip = 0
        warning = None

        # Forward any extra filter params from the original URL (e.g. `Type=`,
        # which narrows /he/collectors/policies?Type=... to a specific category).
        # Reserved keys we set ourselves are excluded.
        from urllib.parse import quote
        reserved = {"officeid", "culture", "skip", "limit", "collectortype"}
        extra_filters = {
            k: v for k, v in (parsed.query_params or {}).items()
            if k.lower() not in reserved and v not in (None, "")
        }
        if extra_filters:
            logger.info("Forwarding extra filters: %s", extra_filters)

        while True:
            try:
                # Build URL with repeated CollectorType params
                # e.g., CollectorType=reports&CollectorType=rfp&...
                query_parts = [f"CollectorType={ct}" for ct in collector_types]
                query_parts.append("culture=he")
                query_parts.append(f"skip={skip}")
                query_parts.append(f"limit={self.page_size}")
                if parsed.office_id:
                    query_parts.append(f"officeId={parsed.office_id}")
                for k, v in extra_filters.items():
                    query_parts.append(f"{k}={quote(str(v), safe='')}")

                url = f"{TRADITIONAL_ENDPOINT}?{'&'.join(query_parts)}"
                resp = self.session.get(url)
                try:
                    data = _safe_json(resp, what=f"Traditional collector {url}")
                except APIEndpointError as _api_err:
                    # Same CF-recovery as DynamicCollector: warm Playwright
                    # once, retry the same skip. Some traditional collectors
                    # (gov decisions, FOI publications) get CF-blocked
                    # specifically on this endpoint while their HTML page
                    # loads fine in a real browser.
                    if (
                        "blocked by Cloudflare" in str(_api_err)
                        and not self.session._playwright_active
                    ):
                        logger.info(
                            "Traditional collector CF-blocked at skip=%d; "
                            "warming Playwright and retrying",
                            skip,
                        )
                        try:
                            self.session._init_playwright()
                        except Exception:
                            raise _api_err
                        continue
                    raise
                # Response: { total: N, results: [...] } (lowercase)
                items = _extract_items(data)

                if skip == 0:
                    total_count = _extract_total(data) or len(items)

                all_items.extend(items)
                self.progress(
                    current=len(all_items),
                    total=total_count,
                    message=f"נאספו {len(all_items)} מתוך {total_count} רשומות",
                )

                if not items or len(all_items) >= total_count:
                    break

                skip += self.page_size
                delay = 1.0 if total_count > 500 else 0.5
                time.sleep(delay)

            except GovILScraperError:
                raise
            except Exception as e:
                logger.error("Pagination error at skip=%d: %s", skip, e)
                warning = f"חלק מהנתונים עלולים להיות חסרים (שגיאה בעמוד {skip})"
                break

        return self._build_result(all_items, total_count, parsed, warning)

    def fetch_traditional_page(self, url: str, skip: int = 0, limit: int = 1):
        """Fetch one page from a traditional collector and return (total_count, raw_items).

        Does not paginate, flatten items, or download attachments — designed for
        incremental delta checks where only a small slice of the top (newest) items
        is needed without triggering a full scrape.
        """
        from urllib.parse import quote
        parsed = parse_gov_url(url)
        if parsed.page_type != PageType.TRADITIONAL_COLLECTOR:
            raise InvalidURLError(
                f"fetch_traditional_page requires a /he/collectors/ URL, got: {url}"
            )

        collector_types = self._discover_collector_types(parsed.collector_name)

        reserved = {"officeid", "culture", "skip", "limit", "collectortype"}
        extra_filters = {
            k: v for k, v in (parsed.query_params or {}).items()
            if k.lower() not in reserved and v not in (None, "")
        }

        query_parts = [f"CollectorType={ct}" for ct in collector_types]
        query_parts.append("culture=he")
        query_parts.append(f"skip={skip}")
        query_parts.append(f"limit={limit}")
        if parsed.office_id:
            query_parts.append(f"officeId={parsed.office_id}")
        for k, v in extra_filters.items():
            query_parts.append(f"{k}={quote(str(v), safe='')}")

        api_url = f"{TRADITIONAL_ENDPOINT}?{'&'.join(query_parts)}"
        resp = self.session.get(api_url)
        data = _safe_json(resp, what=f"Traditional page {api_url}")
        total = _extract_total(data) or 0
        items = _extract_items(data)
        return total, items

    # ---- Traditional collector type discovery --------------------------------

    def _discover_collector_types(self, collector_name: str) -> List[str]:
        """Discover CollectorType values by querying GetLayoutCollectorModel.

        The layout model's filter URLs contain collectionTypes params
        (e.g. collectionTypes=policy&collectionTypes=pmopolicy) which map
        to the CollectorType values needed for GetResults.
        """
        try:
            url = f"{TRADITIONAL_LAYOUT_ENDPOINT}?collectorId={collector_name}&culture=he"
            resp = self.session.get(url)
            text = resp.text
            # Extract collectionTypes values from the JSON text
            matches = re.findall(r"collectionTypes?=([^&\"]+)", text)
            if matches:
                # Deduplicate while preserving order
                seen = set()
                types = []
                for t in matches:
                    if t not in seen:
                        seen.add(t)
                        types.append(t)
                return types
        except Exception as e:
            logger.warning("Failed to discover collector types for '%s': %s",
                           collector_name, e)

        # Fallback: use the collector name itself
        return [collector_name]

    # ---- Result building ----------------------------------------------------

    def _build_result(self, items: List[dict], total_count: int,
                      parsed: ParsedURL, warning: Optional[str]) -> ScrapeResult:
        # Flatten items and extract file attachments
        flat_items = []
        attachments = []
        is_traditional = (parsed.page_type == PageType.TRADITIONAL_COLLECTOR)

        for idx, item in enumerate(items):
            flat = self._flatten_item(item)
            flat_items.append(flat)

            # Extract file attachments from item data
            item_atts = self._extract_attachments(item, idx)

            # For traditional collectors: also fetch PDFs from item content pages
            if is_traditional and not item_atts:
                page_atts = self._fetch_content_page_files(item, idx)
                item_atts.extend(page_atts)
                if idx % 10 == 0 and idx > 0:
                    self.progress(
                        current=idx,
                        total=len(items),
                        message=f"מחפש קבצים מצורפים בדפי תוכן ({idx}/{len(items)})",
                    )
                    time.sleep(0.3)  # Be gentle on the API

            attachments.extend(item_atts)

        # Build column headers from all items
        headers = []
        seen = set()
        for item in flat_items:
            for key in item.keys():
                if key not in seen:
                    headers.append(key)
                    seen.add(key)

        return ScrapeResult(
            items=flat_items,
            total_count=total_count,
            file_attachments=attachments,
            collector_name=parsed.collector_name,
            page_type=parsed.page_type,
            column_headers=headers,
            warning=warning,
        )

    # ---- Nadlan parcel deals --------------------------------------------------

    def _scrape_nadlan(self, parsed: ParsedURL) -> ScrapeResult:
        """Scrape real-estate deal history for one (gush, chelka) parcel.

        Drives a Playwright Chromium (visible — headless is rejected by the
        site's reCAPTCHA) and intercepts the /deal-data + /deal-info responses.
        See nadlan_api.py for the protocol notes.
        """
        from govscraper.scrapers.nadlan.legacy_api import fetch_parcel_deals, order_columns

        gush = parsed.query_params.get("gush")
        chelka = parsed.query_params.get("chelka")

        items, parcel_meta, warning = fetch_parcel_deals(
            gush, chelka, progress=self.progress
        )

        # Merge parcel-level metadata into each row so the CSV is self-contained.
        for it in items:
            for k, v in parcel_meta.items():
                # Don't overwrite per-deal fields; only fill in parcel context.
                it.setdefault(f"parcel_{k}", v)

        headers = order_columns(items) if items else []

        return ScrapeResult(
            items=items,
            total_count=len(items),
            file_attachments=[],
            collector_name=parsed.collector_name,
            page_type=parsed.page_type,
            column_headers=headers,
            warning=warning,
        )

    # ---- ContentPage (React SPA /he/pages/...) ------------------------------

    def _scrape_content_page(self, parsed: ParsedURL) -> ScrapeResult:
        """Scrape a /he/pages/{name} page.

        These are React SPAs whose HTML is empty; all content is fetched via
        ``GET /ContentPageWebApi/api/content-pages/{name}?culture=he[&chapterIndex=N]``.
        The response has a ``contentMain.sideNav.tagItems[]`` list of tabs
        (each with a ``url`` containing chapterIndex) and one or more HTML
        blobs in ``contentMain.htmlContents[*].sectionData`` which contain
        ``<a href>`` links to PDFs (and sometimes other resources).

        Each link becomes one row: ``{chapter, title, url}``. Links pointing
        to gov.il BlobFolder with document extensions are tracked as
        FileAttachments so the worker can download+ZIP them.
        """
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin
        import re as _re

        api_base = f"{BASE_URL}/ContentPageWebApi/api/content-pages"
        name = parsed.collector_name

        # Fetch the default view first to learn the tab structure
        first_url = f"{api_base}/{name}?culture=he"

        first = None
        api_err: Optional[APIEndpointError] = None
        try:
            first_resp = self.session.get(first_url)
            first = _safe_json(first_resp, what=f"ContentPage API {first_url}")
        except APIEndpointError as e:
            api_err = e

        # CF block recovery: when the JSON API is being Cloudflare-blocked
        # (200 OK + "Attention Required" body), warming Playwright once is
        # usually enough to unstick cloudscraper for the rest of the run —
        # _init_playwright() injects cookies + a realistic Chrome UA into
        # the cloudscraper session. Retry the same JSON URL once; if it
        # comes back as proper JSON, the entire normal scrape path works
        # from here, no HTML extraction needed.
        if api_err is not None and "blocked by Cloudflare" in str(api_err):
            logger.info(
                "ContentPage %s: CF block on JSON API — warming Playwright "
                "and retrying", name,
            )
            try:
                self.session._init_playwright()
            except Exception as e:
                logger.warning("Playwright warm for CF retry failed: %s", e)
            else:
                try:
                    first_resp = self.session.get(first_url)
                    first = _safe_json(
                        first_resp,
                        what=f"ContentPage API {first_url} (post-stealth warm)",
                    )
                    logger.info(
                        "ContentPage %s: post-warm JSON API succeeded", name,
                    )
                    api_err = None
                except APIEndpointError as e2:
                    api_err = e2
                    logger.info(
                        "ContentPage %s: post-warm retry still failing", name,
                    )

        # Last resort: render the actual page URL and pull doc links from
        # the HTML. Helps for /he/pages/X that genuinely isn't a Content
        # Page (rare) and as a safety net for anything stealth warm-up
        # couldn't unblock.
        if first is None:
            assert api_err is not None
            html_result, fallback_diag = self._scrape_content_page_html_fallback(
                parsed, name,
            )
            if html_result is not None:
                logger.info(
                    "ContentPage %s: API failed; HTML fallback found "
                    "%d items / %d attachments",
                    name, len(html_result.items),
                    len(html_result.file_attachments),
                )
                return html_result
            raise APIEndpointError(
                f"{api_err} | HTML fallback ({parsed.original_url}): "
                f"{fallback_diag}"
            ) from api_err

        tabs = ((first.get("contentMain") or {}).get("sideNav") or {}).get("tagItems") or []

        # Figure out which chapterIndex values to loop through
        chapter_indices: List[int] = []
        chapter_titles: List[str] = []
        if tabs:
            for tab in tabs:
                m = _re.search(r"chapterIndex=(\d+)", tab.get("url", "") or "")
                chapter_indices.append(int(m.group(1)) if m else 1)
                chapter_titles.append((tab.get("title") or "").strip())
        else:
            # Single-tab page
            chapter_indices = [1]
            page_title = ((first.get("contentHead") or {}).get("title") or name).strip()
            chapter_titles = [page_title]

        total_chapters = len(chapter_indices)
        logger.info("ContentPage %s: %d chapter(s) detected", name, total_chapters)

        all_items: List[dict] = []
        all_attachments: List[FileAttachment] = []
        doc_exts = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")

        def _extract_links_from_html(html: str, chapter_title: str, category: str):
            """Parse one HTML blob and append rows + attachments for each <a>."""
            if not html:
                return
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = (a["href"] or "").strip()
                if not href or href.startswith(("mailto:", "javascript:", "#")):
                    continue
                link_title = a.get_text(strip=True) or href
                absolute = urljoin(BASE_URL + "/", href)
                row_idx = len(all_items)
                all_items.append({
                    "chapter": chapter_title,
                    "category": category,
                    "title": link_title,
                    "url": absolute,
                })
                low = absolute.lower()
                if "gov.il" in low and low.endswith(doc_exts):
                    filename = href.split("/")[-1].split("?")[0] or f"file_{row_idx}"
                    try:
                        from urllib.parse import unquote
                        filename = unquote(filename)
                    except Exception:
                        pass
                    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
                    all_attachments.append(FileAttachment(
                        url=absolute,
                        filename=filename,
                        item_index=row_idx,
                        file_type=ext,
                    ))

        for i, ch in enumerate(chapter_indices):
            self.progress(
                phase="scraping",
                current=i,
                total=total_chapters,
                message=f"גורד לשונית {i + 1}/{total_chapters}: {chapter_titles[i] or f'ch{ch}'}",
            )

            # First chapter's data is already in hand for the common case (ch==1)
            if i == 0 and ch == 1:
                data = first
            else:
                chapter_url = f"{api_base}/{name}?culture=he&chapterIndex={ch}"
                resp = self.session.get(chapter_url)
                data = _safe_json(resp, what=f"ContentPage chapter API {chapter_url}")

            main = data.get("contentMain") or {}
            chapter_title = chapter_titles[i] or f"Chapter {ch}"

            # Path 1: plain htmlContents (e.g. guidelines_state chapter 2,
            # deputies_guidelines all chapters)
            for sec in main.get("htmlContents") or []:
                _extract_links_from_html(
                    sec.get("sectionData") or "",
                    chapter_title=chapter_title,
                    category="",
                )

            # Path 2: FAQ-style accordion (e.g. guidelines_state chapters
            # 1, 3, 4 — "guide" contentType). Each FAQ item has a headTitle
            # (used as category) and regionContent (HTML with link table).
            for faq in main.get("faqs") or []:
                for item in ((faq.get("sectionData") or {}).get("dataItems") or []):
                    _extract_links_from_html(
                        item.get("regionContent") or "",
                        chapter_title=chapter_title,
                        category=(item.get("headTitle") or "").strip(),
                    )

        # Path 3: contentSub.filesToDownload (e.g. legal_counsel,
        # contentType="legalinfo"). Page-level groups of file metadata.
        # Used as a fallback when contentMain has no htmlContents/faqs.
        if not all_items:
            files_to_download = ((first.get("contentSub") or {})
                                 .get("filesToDownload") or {})
            page_chapter = chapter_titles[0] if chapter_titles else name
            for group in files_to_download.get("filesGroupItems") or []:
                if not isinstance(group, dict):
                    continue
                category = (group.get("title") or "").strip()
                for item in group.get("items") or []:
                    if not isinstance(item, dict):
                        continue
                    url = (item.get("url") or "").strip()
                    if not url:
                        continue
                    absolute = url if url.startswith("http") else urljoin(BASE_URL + "/", url)
                    display = (item.get("displayName")
                               or item.get("fileName")
                               or absolute).strip()
                    row_idx = len(all_items)
                    all_items.append({
                        "chapter": page_chapter,
                        "category": category,
                        "title": display,
                        "url": absolute,
                    })
                    filename = (item.get("fileName")
                                or absolute.rsplit("/", 1)[-1]).strip()
                    ext = (item.get("extension") or "").lower()
                    if ext and not filename.lower().endswith(f".{ext}"):
                        filename = f"{filename}.{ext}"
                    if not ext and "." in filename:
                        ext = filename.rsplit(".", 1)[-1].lower()
                    all_attachments.append(FileAttachment(
                        url=absolute,
                        filename=filename,
                        item_index=row_idx,
                        file_type=ext,
                    ))

        logger.info(
            "ContentPage %s: %d items, %d attachments across %d chapters",
            name, len(all_items), len(all_attachments), total_chapters,
        )

        return ScrapeResult(
            items=all_items,
            total_count=len(all_items),
            file_attachments=all_attachments,
            collector_name=name,
            page_type=PageType.CONTENT_PAGE,
            column_headers=["chapter", "category", "title", "url"],
        )

    def _scrape_content_page_html_fallback(
        self, parsed: ParsedURL, name: str
    ) -> tuple[Optional[ScrapeResult], str]:
        """Fallback for /he/pages/{name} URLs whose JSON API serves the SPA
        shell instead of structured data. Fetches the page URL itself and
        extracts gov.il document links (PDF/DOC/XLS/PPT) from the HTML.

        Two-stage:
          1. cloudscraper GET the page URL → parse static HTML.
          2. If 0 docs found, render the page via Playwright (post-JS) and
             re-parse the rendered DOM. Some /he/pages/X URLs are pure SPA
             where every document link is appended by client-side JS.

        Returns (ScrapeResult, diag) on success and (None, diag) when no
        documents were found. The diag string is propagated into the
        operator-facing error message so a failed fallback is immediately
        debuggable from the dashboard.

        Conservative on what it picks up: only gov.il-hosted document
        attachments. Random nav/footer/social links would otherwise
        pollute every "scrape" with hundreds of irrelevant rows.
        """
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin, unquote

        page_url = parsed.original_url
        doc_exts = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")

        def _extract(html: str) -> tuple[List[dict], List[FileAttachment], int, str]:
            """Pull doc links from HTML; return (items, attachments,
            anchor_count, page_title)."""
            soup = BeautifulSoup(html, "html.parser")
            anchors = soup.find_all("a", href=True)
            ttl_tag = soup.find("title")
            page_title = (
                ttl_tag.get_text(strip=True) if ttl_tag and ttl_tag.get_text(strip=True)
                else name
            )
            local_items: List[dict] = []
            local_atts: List[FileAttachment] = []
            seen: set[str] = set()
            for a in anchors:
                href = (a["href"] or "").strip()
                if not href or href.startswith(("mailto:", "javascript:", "#")):
                    continue
                absolute = urljoin(BASE_URL + "/", href)
                low = absolute.lower()
                if "gov.il" not in low or not low.endswith(doc_exts):
                    continue
                if absolute in seen:
                    continue
                seen.add(absolute)

                link_title = a.get_text(strip=True) or absolute
                row_idx = len(local_items)
                local_items.append({
                    "chapter": page_title,
                    "category": "",
                    "title": link_title,
                    "url": absolute,
                })
                filename = href.split("/")[-1].split("?")[0] or f"file_{row_idx}"
                try:
                    filename = unquote(filename)
                except Exception:
                    pass
                ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
                local_atts.append(FileAttachment(
                    url=absolute,
                    filename=filename,
                    item_index=row_idx,
                    file_type=ext,
                ))
            return local_items, local_atts, len(anchors), page_title

        # Stage 1: cloudscraper static HTML
        try:
            resp = self.session.get(page_url)
        except Exception as e:
            logger.warning("HTML fallback fetch failed for %s: %s", page_url, e)
            return None, f"fetch failed: {type(e).__name__}: {e}"

        text = resp.text or ""
        if not text.strip():
            return None, f"empty body (status={resp.status_code})"

        static_cf_blocked = _looks_like_cf_block(text)
        items, attachments, anchor_count, page_title = _extract(text)
        static_len = len(text)

        if not items:
            # Stage 2: render in Playwright to capture JS-appended links
            logger.info(
                "HTML fallback for %s: 0 docs in %d bytes / %d anchors%s — "
                "trying Playwright render",
                page_url, static_len, anchor_count,
                " (Cloudflare block)" if static_cf_blocked else "",
            )
            rendered = self.session.render_page_html(page_url)
            if rendered:
                rendered_cf_blocked = _looks_like_cf_block(rendered)
                items, attachments, rendered_anchors, page_title = _extract(rendered)
                if items:
                    diag = (
                        f"static HTML had 0 docs in {static_len} bytes / "
                        f"{anchor_count} anchors; Playwright render found "
                        f"{len(items)} doc(s) in {len(rendered)} bytes / "
                        f"{rendered_anchors} anchors"
                    )
                    logger.info("HTML fallback for %s: %s", page_url, diag)
                    return ScrapeResult(
                        items=items,
                        total_count=len(items),
                        file_attachments=attachments,
                        collector_name=name,
                        page_type=PageType.CONTENT_PAGE,
                        column_headers=["chapter", "category", "title", "url"],
                    ), diag
                if rendered_cf_blocked:
                    diag = (
                        f"BLOCKED BY CLOUDFLARE on both stages — "
                        f"static HTML ({static_len} bytes) and Playwright "
                        f"render ({len(rendered)} bytes) both served the "
                        f"'Attention Required' block page. The URL works "
                        f"in a real browser; the scraper fingerprint is "
                        f"being flagged."
                    )
                else:
                    diag = (
                        f"static HTML: 0 docs in {static_len} bytes / "
                        f"{anchor_count} anchors. Playwright render: 0 docs in "
                        f"{len(rendered)} bytes / {rendered_anchors} anchors. "
                        f"Page truly has no gov.il document links."
                    )
            else:
                diag = (
                    f"static HTML: 0 docs in {static_len} bytes / "
                    f"{anchor_count} anchors{' (Cloudflare block)' if static_cf_blocked else ''}. "
                    f"Playwright render unavailable or failed."
                )
            logger.info("HTML fallback for %s: %s", page_url, diag)
            return None, diag

        diag = (
            f"static HTML found {len(items)} gov.il document(s) in "
            f"{static_len} bytes / {anchor_count} anchors"
        )
        return ScrapeResult(
            items=items,
            total_count=len(items),
            file_attachments=attachments,
            collector_name=name,
            page_type=PageType.CONTENT_PAGE,
            column_headers=["chapter", "category", "title", "url"],
        ), diag

    # ---- Traditional: content page file extraction -------------------------

    CONTENT_PAGE_API = f"{BASE_URL}/ContentPageWebApi/api/content-pages"

    def _fetch_content_page_files(
        self, item: dict, item_index: int
    ) -> List[FileAttachment]:
        """For traditional collector items, fetch the item's content page to
        find downloadable files (PDFs etc.).

        Each item has a ``url`` like ``/he/pages/news11326``.  The content
        page API at ``/ContentPageWebApi/api/content-pages/<pageName>?culture=he``
        returns JSON with ``contentSub.filesToDownload.filesGroupItems[].items[]``
        containing the actual file URLs.
        """
        item_url = item.get("url") or ""
        if not item_url:
            return []

        # Extract page name (last segment)
        page_name = item_url.rstrip("/").rsplit("/", 1)[-1]
        if not page_name:
            return []

        try:
            resp = self.session.get(
                f"{self.CONTENT_PAGE_API}/{page_name}",
                params={"culture": "he"},
                retries=2,
            )
            data = resp.json()
        except Exception as e:
            logger.debug("Content page fetch failed for %s: %s", page_name, e)
            return []

        attachments = []
        content_sub = data.get("contentSub") or {}
        files_to_download = content_sub.get("filesToDownload") or {}
        groups = files_to_download.get("filesGroupItems") or []

        for group in groups:
            if not isinstance(group, dict):
                continue
            for f in (group.get("items") or []):
                if not isinstance(f, dict):
                    continue
                file_url = f.get("url") or ""
                filename = f.get("fileName") or f.get("displayName") or ""
                ext = f.get("extension") or ""

                if not file_url:
                    continue
                if file_url.startswith("/"):
                    file_url = BASE_URL + file_url

                if not filename:
                    filename = file_url.rsplit("/", 1)[-1]
                if ext and not filename.lower().endswith(f".{ext.lower()}"):
                    filename = f"{filename}.{ext}"

                attachments.append(FileAttachment(
                    url=file_url,
                    filename=filename,
                    item_index=item_index,
                    file_type=ext.lower(),
                ))

        return attachments

    def _flatten_item(self, item: dict, prefix: str = "") -> dict:
        """Flatten nested dicts/items based on the page type structure.

        DynamicCollector items: { Data: { field: val, file: [...] }, Description, UrlName }
        Traditional items: { title, description, url, tags: { metaData: {...} } }
        """
        flat = {}
        for key, value in item.items():
            full_key = f"{prefix}.{key}" if prefix else key

            # Skip Angular hash keys
            if key == "$$hashKey":
                continue

            # Skip file arrays — handled by _extract_attachments. The
            # canonical attachment info (basename, URL) is injected into
            # the CSV later as `attachment_filename` / `attachment_url`
            # via _inject_attachment_columns. Don't emit a placeholder
            # like "[1 קבצים]" here: it pretends to be data the consumer
            # can act on, but it's a fixed string with no link to the
            # actual file in the ZIP.
            if key.lower() in ("file", "files", "attachments", "fileattachments",
                                "filedata", "document"):
                continue

            # Handle tags.metaData (Traditional Collector) — flatten to readable values
            if key == "tags" and isinstance(value, dict):
                for section_name, section in value.items():
                    if isinstance(section, dict):
                        for field_name, field_vals in section.items():
                            if isinstance(field_vals, list):
                                titles = [
                                    v.get("title", "") for v in field_vals
                                    if isinstance(v, dict)
                                ]
                                flat[field_name] = ", ".join(t for t in titles if t)
                continue

            if isinstance(value, dict):
                flat.update(self._flatten_item(value, full_key))
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    # List of objects (not files) — show count
                    flat[full_key] = f"[{len(value)} פריטים]"
                else:
                    flat[full_key] = ", ".join(str(v) for v in value)
            else:
                flat[full_key] = value
                # If this looks like an HTML blob (common in police/hesder data
                # under `DescriptionHtmlString`), also emit a plain-text sibling
                # so Excel users get a readable version without losing the raw
                # HTML for anyone who needs it.
                plain = _html_to_plain_text(full_key, value)
                if plain is not None:
                    plain_key = _plain_key_for(full_key)
                    flat[plain_key] = plain

        return flat

    def _extract_attachments(self, item: dict, item_index: int) -> List[FileAttachment]:
        """Extract file attachment info from an item.

        DynamicCollector: file entries look like
            { FileName, FileMime, FileSize, Extension, DisplayName }
        but they can sit under ANY key in Data (e.g. `file`, `Document`,
        `file_name`, `attachments`, template-specific names etc). Rather
        than hardcoding key names, we walk every list-of-dicts under Data
        and treat any list whose items carry a FileName as attachments.

        Traditional: typically no direct file attachments (links in url field).
        """
        attachments = []
        seen_urls: set[str] = set()

        url_name = item.get("UrlName") or ""
        data = item.get("Data", {})
        if not isinstance(data, dict):
            return attachments

        def _looks_like_file_list(v) -> bool:
            if not isinstance(v, list) or not v:
                return False
            # Consider it a file list if ANY entry is a dict with FileName
            for entry in v:
                if isinstance(entry, dict) and (
                    entry.get("FileName") or entry.get("fileName")
                ):
                    return True
            return False

        # Walk all Data fields and pick out any list that looks like files
        for key, value in data.items():
            if not _looks_like_file_list(value):
                continue
            for f in value:
                if not isinstance(f, dict):
                    continue
                filename = f.get("FileName") or f.get("fileName") or ""
                display = f.get("DisplayName") or f.get("displayName") or ""
                ext = f.get("Extension") or f.get("extension") or ""

                if not filename:
                    continue

                # Determine download URL
                if filename.startswith("http"):
                    # Custom API: FileName is already a full URL
                    file_url = filename
                elif url_name:
                    # Standard DynamicCollector: build BlobFolder URL
                    file_url = f"{BASE_URL}/BlobFolder/dynamiccollectorresultitem/{url_name}/he/{filename}"
                else:
                    continue

                if file_url in seen_urls:
                    continue
                seen_urls.add(file_url)

                nice_name = display or filename.split("/")[-1]
                if ext and not nice_name.lower().endswith(f".{ext.lower()}"):
                    nice_name = f"{nice_name}.{ext}"

                attachments.append(FileAttachment(
                    url=file_url,
                    filename=nice_name,
                    item_index=item_index,
                    file_type=ext.lower(),
                ))

        # Also look for top-level file keys
        for key in ("Files", "files", "Attachments", "attachments"):
            files = item.get(key)
            if not isinstance(files, list):
                continue
            for f in files:
                url = ""
                filename = ""
                if isinstance(f, dict):
                    url = f.get("Url") or f.get("url") or f.get("FilePath") or ""
                    filename = f.get("FileName") or f.get("fileName") or f.get("Title") or ""
                elif isinstance(f, str):
                    url = f
                    filename = url.split("/")[-1] if "/" in url else url

                if url:
                    if url.startswith("/"):
                        url = BASE_URL + url
                    elif not url.startswith("http"):
                        url = BASE_URL + "/" + url
                    ext = url.rsplit(".", 1)[-1].lower() if "." in url else ""
                    if not filename:
                        filename = url.split("/")[-1]
                    attachments.append(FileAttachment(
                        url=url, filename=filename,
                        item_index=item_index, file_type=ext,
                    ))

        return attachments


# ---------------------------------------------------------------------------
# snake_case aliases (phase F)
# Old class names use the all-caps "IL" suffix (`GovILSession`, `GovILScraper`,
# `GovILScraperError`). PEP-8 treats two-letter abbreviations as acronyms so
# the correct casing is `Il`. Both spellings remain importable until phase G;
# new code should prefer the snake_case spellings.
# ---------------------------------------------------------------------------
GovIlSession = GovILSession
GovIlScraper = GovILScraper
GovIlScraperError = GovILScraperError
