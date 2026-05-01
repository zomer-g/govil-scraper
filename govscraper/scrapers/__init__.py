"""Per-target scrapers + registry."""
from .base import (
    BaseScraper,
    CkanCatalogResult,
    GeoFeatureResult,
    ScrapeResult,
    TabularResult,
)
from .registry import all_scrapers, dispatch, get_by_id, register

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
