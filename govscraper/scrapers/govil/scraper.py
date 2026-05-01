"""www.gov.il scraper — wraps the legacy scraper_engine.GovILScraper behind BaseScraper.

Phase C facade: the underlying scrape logic (DynamicCollector, Traditional
Collector, ContentPage) still lives in scraper_engine.py. This class
exposes the unified interface so the new Worker / registry can drive it.
The actual file split (dynamic.py / traditional.py / _fields.py) lands in
a later cleanup pass.
"""
from __future__ import annotations

from typing import ClassVar
from urllib.parse import urlparse

from ...types import ParsedURL, ProgressFn
from .._adapter import legacy_parsed_to_new, legacy_result_to_new
from ..base import BaseScraper, ScrapeResult
from ..registry import register


@register
class GovIlScraper(BaseScraper):
    id: ClassVar[str] = "govil"

    def __init__(self, session=None) -> None:
        # Lazy: a session is only created when fetch() is called.
        self._session = session

    @classmethod
    def parse_url(cls, url: str) -> ParsedURL | None:
        host = (urlparse(url).hostname or "").lower()
        if not host.endswith("gov.il"):
            return None
        # Exclude domains that have their own scraper.
        if host.endswith("nadlan.gov.il") or host.endswith("govmap.gov.il") or host == "data.gov.il":
            return None
        # Defer to the legacy parser for shape detection (DynamicCollector /
        # Traditional / ContentPage). It raises InvalidURLError for unsupported
        # paths — translate that to None so the registry can keep searching.
        try:
            from .legacy_engine import parse_gov_url
            legacy = parse_gov_url(url)
        except Exception:
            return None
        return legacy_parsed_to_new(legacy, scraper_id=cls.id)

    def fetch(self, parsed: ParsedURL, *, progress: ProgressFn) -> ScrapeResult:
        from .legacy_engine import GovILScraper, GovILSession

        legacy_parsed = parsed.params.get("_legacy")

        def progress_cb(**kw):
            from ...types import Progress
            progress(Progress(
                phase=kw.get("phase", "scrape"),
                current=int(kw.get("current", 0) or 0),
                total=int(kw.get("total", 0) or 0),
                message=str(kw.get("message", "") or ""),
            ))

        if self._session is None:
            self._session = GovILSession()

        legacy_scraper = GovILScraper(self._session, progress_callback=progress_cb)
        # The legacy scrape() takes the original URL string; re-derive it from
        # the new ParsedURL so we don't depend on the legacy ParsedURL caching.
        url = parsed.canonical_url or (legacy_parsed.original_url if legacy_parsed else "")
        legacy_result = legacy_scraper.scrape(url)
        return legacy_result_to_new(legacy_result, source_url=url)
