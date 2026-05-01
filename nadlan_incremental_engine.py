"""Re-export shim — canonical implementation lives in
govscraper.scrapers.nadlan.legacy_incremental.

Kept for back-compat with `from nadlan_incremental_engine import run_bootstrap, run_incremental, …`.
"""
from govscraper.scrapers.nadlan.legacy_incremental import *  # noqa: F401,F403
from govscraper.scrapers.nadlan.legacy_incremental import (  # noqa: F401
    SETTLEMENTS_URL,
    NADLAN_COLS,
    NadlanBrowser,
    run_bootstrap,
    run_incremental,
)
