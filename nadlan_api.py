"""Re-export shim — canonical implementation lives in
govscraper.scrapers.nadlan.legacy_api.

Kept for back-compat with `from nadlan_api import fetch_parcel_deals, …`.
New code should import from `govscraper.scrapers.nadlan` directly.
"""
from govscraper.scrapers.nadlan.legacy_api import *  # noqa: F401,F403
from govscraper.scrapers.nadlan.legacy_api import (  # noqa: F401
    PRIMARY_FIELDS,
    fetch_parcel_deals,
    order_columns,
)
