"""Unit tests for NadlanScraper.parse_url — covers parcel page form
(?view=kparcel_all&id=<gush>-<chelka>) and rejection of unrelated hosts.
"""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from govscraper.scrapers.nadlan.scraper import NadlanScraper  # noqa: E402


PARCEL_URLS = [
    "https://www.nadlan.gov.il/?view=kparcel_all&id=1234-5",
    "https://www.nadlan.gov.il/?view=kparcel_all&id=6909-1&page=deals",
    # nested host suffix should still match
    "https://nadlan.gov.il/?view=kparcel_all&id=42-7",
]

# These exist on nadlan.gov.il but are NOT supported by the current scraper —
# see scraper_engine.parse_gov_url which raises InvalidURLError for them.
UNSUPPORTED_NADLAN_URLS = [
    "https://www.nadlan.gov.il/?view=settlement&id=42",
    "https://www.nadlan.gov.il/?view=street&id=Hayarkon",
    "https://www.nadlan.gov.il/",  # no view param at all
]

NON_NADLAN_HOSTS = [
    "https://example.com/whatever",
    "https://www.gov.il/he/collectors/policies",
    "https://www.govmap.gov.il/?lay=X",
    "https://data.gov.il/dataset/foo",
]


def test_parcel_urls_parse():
    for url in PARCEL_URLS:
        p = NadlanScraper.parse_url(url)
        assert p is not None, f"failed to parse: {url}"
        assert p.scraper_id == "nadlan"


def test_gush_chelka_extracted():
    p = NadlanScraper.parse_url("https://www.nadlan.gov.il/?view=kparcel_all&id=6909-1")
    assert p is not None
    qp = p.params.get("query_params") or {}
    assert qp.get("gush") == "6909"
    assert qp.get("chelka") == "1"


def test_collector_name_uses_gush_chelka():
    p = NadlanScraper.parse_url("https://www.nadlan.gov.il/?view=kparcel_all&id=1234-5")
    assert p is not None
    assert p.params.get("collector_name") == "nadlan_1234_5"


def test_unsupported_nadlan_returns_none():
    """parse_gov_url raises InvalidURLError for unsupported nadlan shapes;
    NadlanScraper.parse_url is wired to swallow that and return None so the
    registry can keep searching (rather than blowing up dispatch())."""
    for url in UNSUPPORTED_NADLAN_URLS:
        p = NadlanScraper.parse_url(url)
        assert p is None, f"should be None for unsupported: {url}"


def test_non_nadlan_hosts_return_none():
    for url in NON_NADLAN_HOSTS:
        p = NadlanScraper.parse_url(url)
        assert p is None, f"nadlan should not claim: {url}"


def test_registry_dispatch_routes_nadlan():
    from govscraper.scrapers import dispatch
    hit = dispatch("https://www.nadlan.gov.il/?view=kparcel_all&id=1-2")
    assert hit is not None
    cls, parsed = hit
    assert cls.id == "nadlan"


if __name__ == "__main__":
    failures = 0
    for name in [n for n in globals() if n.startswith("test_")]:
        try:
            globals()[name]()
            print(f"OK  {name}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {name}: {e}")
        except Exception as e:
            failures += 1
            print(f"ERROR {name}: {type(e).__name__}: {e}")
    sys.exit(1 if failures else 0)
