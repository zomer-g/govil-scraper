"""Per-target scrapers + registry. Importing this package self-registers
GovIlScraper, NadlanScraper, GovMapScraper (and DataGovIlScraper once
phase E lands).
"""
from .base import (
    BaseScraper,
    CkanCatalogResult,
    GeoFeatureResult,
    ScrapeResult,
    TabularResult,
)
from .registry import all_scrapers, dispatch, get_by_id, register

# Import each scraper module so the @register decorator runs.
# Order matters when two scrapers might match the same URL — keep the
# strictest matcher first. govmap and nadlan are subdomains of gov.il so
# they must be registered before the general govil matcher.
from .govmap import scraper as _govmap        # noqa: F401
from .nadlan import scraper as _nadlan        # noqa: F401
from .datagovil import scraper as _datagovil  # noqa: F401
from .govil import scraper as _govil          # noqa: F401

__all__ = [
    "BaseScraper",
    "TabularResult",
    "CkanCatalogResult",
    "GeoFeatureResult",
    "ScrapeResult",
    "register",
    "dispatch",
    "get_by_id",
    "all_scrapers",
]
