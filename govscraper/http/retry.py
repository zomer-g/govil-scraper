"""Retry helpers — exponential backoff on transient HTTP failures.

The legacy code (scraper_engine.GovIlSession._request, govmap_engine
_WfsSessionAdapter.get_raw, nadlan_incremental_engine.NadlanBrowser
.fetch_settlement) implemented retry inline three times with slightly
different parameters. This module collects the policy in one place so
new code can reuse it without re-deriving the schedule.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

T = TypeVar("T")
logger = logging.getLogger(__name__)


# Status codes that almost always indicate transient server-side issues
# worth retrying. 401/403/404 are NOT here — those are client-correctness
# errors and retrying just wastes the rate-limit budget.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def with_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int = 3,
    base_delay_s: float = 5.0,
    backoff: float = 2.0,
    on_retry: Callable[[int, Exception | int], None] | None = None,
) -> T:
    """Call `fn()` up to `max_attempts` times. On each failure, sleep
    `base_delay_s * (backoff ** (attempt - 1))` seconds before the next try.

    `fn` should raise on transient failure; if it returns an int, that's
    treated as an HTTP status code and retried iff in `RETRYABLE_STATUS`.
    `on_retry(attempt, exc_or_status)` is called once per retry — useful
    for telemetry.
    """
    last: Exception | int | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = fn()
        except Exception as e:
            last = e
            if attempt >= max_attempts:
                raise
            wait = base_delay_s * (backoff ** (attempt - 1))
            logger.warning("attempt %d/%d failed: %s — retry in %.0fs",
                           attempt, max_attempts, e, wait)
            if on_retry:
                on_retry(attempt, e)
            time.sleep(wait)
            continue

        if isinstance(result, int) and result in RETRYABLE_STATUS:
            last = result
            if attempt >= max_attempts:
                return result
            wait = base_delay_s * (backoff ** (attempt - 1))
            logger.warning("attempt %d/%d returned HTTP %d — retry in %.0fs",
                           attempt, max_attempts, result, wait)
            if on_retry:
                on_retry(attempt, result)
            time.sleep(wait)
            continue

        return result

    # If we got here, fn returned a retryable status on the last attempt.
    return last  # type: ignore[return-value]
