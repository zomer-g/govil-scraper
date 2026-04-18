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
        """Lazy-init Playwright and extract cookies into the requests session."""
        if self._playwright_active:
            return
        try:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(headless=True)
            self._pw_context = self._browser.new_context(
                locale="he-IL",
                extra_http_headers=COMMON_HEADERS,
            )
            page = self._pw_context.new_page()
            page.goto(f"{BASE_URL}/he", wait_until="networkidle", timeout=30000)
            # Extract cookies and inject into cloudscraper session
            for cookie in self._pw_context.cookies():
                self._session.cookies.set(
                    cookie["name"], cookie["value"],
                    domain=cookie.get("domain", ".gov.il"),
                    path=cookie.get("path", "/"),
                )
            page.close()
            self._playwright_active = True
            self._warmed = True
            logger.info("Playwright fallback session established")
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
            "לא ניתן להתחבר לאתר gov.il — cloudscraper נכשל לאחר מספר ניסיונות"
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


def extract_dynamic_page_config(session: GovILSession, page_url: str) -> DynamicPageConfig:
    """Fetch the DynamicCollector HTML page and extract configuration from ng-init.

    Standard DynamicCollectors use POST /he/api/DynamicCollector with a GUID.
    Custom DynamicCollectors provide a resultsApiURL (e.g. menifa, legal-advisor-guidelines).
    """
    resp = session.get(page_url)
    page_html = resp.text
    config = DynamicPageConfig()

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
        else:
            raise InvalidURLError(f"סוג דף לא נתמך: {parsed.page_type}")

    # ---- DynamicCollector ---------------------------------------------------

    def _scrape_dynamic(self, parsed: ParsedURL) -> ScrapeResult:
        # Step 1: Fetch the HTML page to extract config (GUID, custom API URL, etc.)
        config = extract_dynamic_page_config(self.session, parsed.original_url)
        parsed.api_method = "POST"

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

                data = resp.json()
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
        # The layout model's filter URLs contain the actual collectionTypes params.
        collector_types = self._discover_collector_types(parsed.collector_name)
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
                data = resp.json()
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
        first_resp = self.session.get(f"{api_base}/{name}?culture=he")
        first = first_resp.json()

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
                resp = self.session.get(
                    f"{api_base}/{name}?culture=he&chapterIndex={ch}",
                )
                data = resp.json()

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

            # Skip file arrays — handled by _extract_attachments
            if key.lower() in ("file", "files", "attachments", "fileattachments",
                                "filedata", "document"):
                if isinstance(value, list):
                    flat[full_key] = f"[{len(value)} קבצים]"
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
