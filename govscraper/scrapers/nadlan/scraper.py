"""www.nadlan.gov.il scraper — wraps legacy nadlan engines behind BaseScraper.

Today's nadlan scraping lives in two engines:
- nadlan_api.fetch_parcel_deals (single parcel, gush/chelka)
- nadlan_incremental_engine (settlement-level bulk + daily delta, used by
  over_worker for archive tasks)

This wrapper covers the common interactive case — single parcel via URL.
Bulk/incremental settlement runs continue to be driven directly through the
worker layer (over.org.il publisher + nadlan_incremental_engine) until
phase D consolidates them.
"""
from __future__ import annotations

from typing import ClassVar
from urllib.parse import parse_qs, urlparse

from ...types import ParsedURL, ProgressFn
from .._adapter import legacy_parsed_to_new, legacy_result_to_new
from ..base import BaseScraper, ScrapeResult
from ..registry import register


@register
class NadlanScraper(BaseScraper):
    id: ClassVar[str] = "nadlan"

    def __init__(self, session=None) -> None:
        self._session = session

    @classmethod
    def parse_url(cls, url: str) -> ParsedURL | None:
        host = (urlparse(url).hostname or "").lower()
        if not host.endswith("nadlan.gov.il"):
            return None
        try:
            from scraper_engine import parse_gov_url
            legacy = parse_gov_url(url)
        except Exception:
            return None
        return legacy_parsed_to_new(legacy, scraper_id=cls.id)

    def fetch(self, parsed: ParsedURL, *, progress: ProgressFn) -> ScrapeResult:
        from scraper_engine import GovILScraper, GovILSession

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

        legacy = GovILScraper(self._session, progress_callback=progress_cb)
        url = parsed.canonical_url
        legacy_result = legacy.scrape(url)
        return legacy_result_to_new(legacy_result, source_url=url)
