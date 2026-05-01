"""Re-export shim — canonical implementation lives in
govscraper.scrapers.govmap.legacy_engine.

Kept for back-compat with `from govmap_engine import scrape_govmap, …`.
New code should import from `govscraper.scrapers.govmap` directly.
"""
from govscraper.scrapers.govmap.legacy_engine import *  # noqa: F401,F403
from govscraper.scrapers.govmap.legacy_engine import (  # noqa: F401
    DEFAULT_MAX_FEATURES,
    DEFAULT_LAYERS_FILE,
    Layer,
    Feature,
    load_layer_catalog,
    resolve_layer,
    parse_bbox_from_url,
    parse_govmap_url,
    scrape_govmap,
)
