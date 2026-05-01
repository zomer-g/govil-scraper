"""Re-export shim — canonical implementation lives in govscraper.geo.wfs_client.

Kept for back-compat with `import wfs_client` / `from wfs_client import ...`.
New code should import from `govscraper.geo.wfs_client`. Deleted in phase G.
"""
from govscraper.geo.wfs_client import *  # noqa: F401,F403
from govscraper.geo.wfs_client import (  # noqa: F401  module-level constants
    WFS_BASE,
    WFS_NS,
    WFSClient,
    WFSError,
    WFSLayerMetadata,
)
