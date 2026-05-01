"""www.govmap.gov.il (WFS GIS) scraper — wraps legacy govmap_engine behind BaseScraper."""
from __future__ import annotations

from typing import ClassVar
from urllib.parse import urlparse

from ...types import ParsedURL, ProgressFn
from .._adapter import legacy_parsed_to_new, legacy_result_to_new
from ..base import BaseScraper, ScrapeResult
from ..registry import register


@register
class GovMapScraper(BaseScraper):
    id: ClassVar[str] = "govmap"

    def __init__(self, session=None) -> None:
        self._session = session

    @classmethod
    def parse_url(cls, url: str) -> ParsedURL | None:
        host = (urlparse(url).hostname or "").lower()
        if not host.endswith("govmap.gov.il"):
            return None
        try:
            from ..govil.legacy_engine import parse_gov_url
            legacy = parse_gov_url(url)
        except Exception:
            return None
        return legacy_parsed_to_new(legacy, scraper_id=cls.id)

    def fetch(self, parsed: ParsedURL, *, progress: ProgressFn) -> ScrapeResult:
        from .legacy_engine import scrape_govmap
        from ..govil.legacy_engine import GovILSession

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

        legacy_result = scrape_govmap(self._session, legacy_parsed, progress_callback=progress_cb)
        return legacy_result_to_new(legacy_result, source_url=parsed.canonical_url)
