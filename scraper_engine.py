"""Re-export shim — canonical implementation lives in
govscraper.scrapers.govil.legacy_engine.

Kept for back-compat with `from scraper_engine import GovILScraper, ...`
and similar legacy imports across worker.py, over_worker.py, file_handler.py,
nadlan_incremental_engine.py, etc. New code should import from
`govscraper.scrapers.govil` directly. This file is removed in a future
cleanup pass after all internal callers have migrated.
"""
from govscraper.scrapers.govil.legacy_engine import *  # noqa: F401,F403

# Re-export module-level constants and private helpers that callers
# reference by name (some are accessed via `scraper_engine.X`).
from govscraper.scrapers.govil.legacy_engine import (  # noqa: F401
    BASE_URL,
    COMMON_HEADERS,
    PageType,
    ParsedURL,
    FileAttachment,
    ScrapeResult,
    GovILSession,
    GovILScraper,
    GovIlSession,
    GovIlScraper,
    GovILScraperError,
    GovIlScraperError,
    CloudflareBlockError,
    InvalidURLError,
    APIEndpointError,
    parse_gov_url,
    extract_dynamic_page_config,
    DYNAMIC_API_URL,
    RE_DYNAMIC,
    RE_TRADITIONAL,
    RE_CONTENT_PAGE,
)
