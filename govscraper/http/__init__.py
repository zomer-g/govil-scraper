"""Shared HTTP plumbing — cloudscraper session factory + retry helpers.

Modules:
    session — `make_cloudscraper(config_index, delay, extra_headers)`
    retry   — `with_retry(fn, max_attempts, base_delay_s, backoff)`
"""
from .session import BROWSER_CONFIGS, make_cloudscraper
from .retry import RETRYABLE_STATUS, with_retry

__all__ = [
    "BROWSER_CONFIGS",
    "make_cloudscraper",
    "RETRYABLE_STATUS",
    "with_retry",
]
