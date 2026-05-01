"""Re-export shim — canonical implementation lives in govscraper.geo.coords.

Kept for back-compat with `import coords` / `from coords import ...`. New
code should `from govscraper.geo.coords import ...` directly. This file is
deleted in phase G.
"""
from govscraper.geo.coords import *  # noqa: F401,F403
from govscraper.geo.coords import (  # re-export private names used by tests
    _ITM_TO_WGS,
    _WGS_TO_ITM,
    _WM_TO_WGS,
    _WGS_TO_WM,
    _ITM_TO_WM,
    _WM_TO_ITM,
    _transform_coords,
    _coord_str,
)
