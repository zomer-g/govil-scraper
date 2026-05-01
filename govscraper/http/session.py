"""cloudscraper session factory.

Extracted from `GovIlSession._init_cloudscraper`. Returns a fresh
cloudscraper.CloudScraper with one of four pre-defined browser fingerprints
(chrome/windows, chrome/linux, firefox/windows, chrome/darwin) — rotating
fingerprints on retry helps when Cloudflare bans a specific UA.

Headers are not pre-populated here so each scraper site can attach its own
Origin/Referer/Accept-Language. Pass them via `extra_headers=...` or update
the returned session's `.headers` after creation.
"""
from __future__ import annotations

from typing import Any

import cloudscraper

# Frozen list — order matters: GovIlSession indexes into it on retry.
BROWSER_CONFIGS: list[dict[str, Any]] = [
    {"browser": "chrome", "platform": "windows", "mobile": False},
    {"browser": "chrome", "platform": "linux", "mobile": False},
    {"browser": "firefox", "platform": "windows", "mobile": False},
    {"browser": "chrome", "platform": "darwin", "mobile": False},
]


def make_cloudscraper(
    *,
    config_index: int = 0,
    delay: int = 10,
    extra_headers: dict[str, str] | None = None,
) -> cloudscraper.CloudScraper:
    """Create a CloudScraper session.

    `config_index` selects the browser fingerprint (mod len). `delay` is the
    seconds-budget cloudscraper waits for the JS challenge — 10s is the
    upstream-recommended default. `extra_headers` are merged into the
    session headers immediately.
    """
    cfg = BROWSER_CONFIGS[config_index % len(BROWSER_CONFIGS)]
    scraper = cloudscraper.create_scraper(browser=cfg, delay=delay)
    if extra_headers:
        scraper.headers.update(extra_headers)
    return scraper
