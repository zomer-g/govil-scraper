"""govscraper — unified package for Israeli public-data scraping.

Replaces the flat-layout collection of top-level modules (scraper_engine,
worker, over_worker, nadlan_worker, app, ...) with a structured package.
During phases A-F the old top-level modules remain as re-export shims; they
will be removed in phase G.
"""

__version__ = "0.1.0-refactor"
