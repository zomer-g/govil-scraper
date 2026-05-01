"""Unit tests for GovIlScraper.parse_url — covers DynamicCollector,
TraditionalCollector, ContentPage, and rejection of non-gov.il hosts.
"""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from govscraper.scrapers.govil.scraper import GovIlScraper  # noqa: E402


# Real URLs lifted from test_urls.py and the canonical engine. These are the
# shapes that must keep working — change parse_url and one of these breaks.

DYNAMIC_URLS = [
    "https://www.gov.il/he/Departments/DynamicCollectors/menifa?skip=0",
    "https://www.gov.il/he/Departments/DynamicCollectors/guidelines-state-attorney?skip=0",
    "https://www.gov.il/he/departments/dynamiccollectors/legal-advisor-guidelines?skip=0",
    # case-insensitive in the path
    "https://www.gov.il/HE/DEPARTMENTS/DYNAMICCOLLECTORS/foo",
]

TRADITIONAL_URLS = [
    "https://www.gov.il/he/collectors/policies?officeId=c3f24c3b-9940-45c2-82a1-c4be2087bf99",
    "https://www.gov.il/he/Collectors/something-else",
]

CONTENT_PAGES = [
    "https://www.gov.il/he/pages/some-content-page",
]

NON_GOVIL_HOSTS = [
    "https://example.com/whatever",
    "https://github.com/zomer-g/govil-scraper",
    "https://www.nadlan.gov.il/?view=kparcel_all&id=1234-5",
    "https://www.govmap.gov.il/?lay=220826",
    "https://data.gov.il/dataset/foo",
]


def test_dynamic_collector_urls():
    for url in DYNAMIC_URLS:
        p = GovIlScraper.parse_url(url)
        assert p is not None, f"failed to parse: {url}"
        assert p.scraper_id == "govil"
        assert p.params.get("page_type") in ("dynamic_collector", "DYNAMIC_COLLECTOR")


def test_traditional_collector_urls():
    for url in TRADITIONAL_URLS:
        p = GovIlScraper.parse_url(url)
        assert p is not None, f"failed to parse: {url}"
        assert p.scraper_id == "govil"
        assert p.params.get("page_type") in ("traditional_collector", "TRADITIONAL_COLLECTOR")


def test_office_id_extraction():
    p = GovIlScraper.parse_url(
        "https://www.gov.il/he/collectors/policies?officeId=c3f24c3b-9940-45c2-82a1-c4be2087bf99"
    )
    assert p is not None
    assert p.params["office_id"] == "c3f24c3b-9940-45c2-82a1-c4be2087bf99"


def test_content_page_urls():
    for url in CONTENT_PAGES:
        p = GovIlScraper.parse_url(url)
        assert p is not None, f"failed to parse: {url}"
        assert p.scraper_id == "govil"


def test_non_govil_hosts_return_none():
    for url in NON_GOVIL_HOSTS:
        # The govil scraper must yield only on www.gov.il content. Subdomain
        # peers (nadlan, govmap) and unrelated CKAN host (data.gov.il) are
        # handled by their own scrapers — see registry order in
        # govscraper/scrapers/__init__.py.
        p = GovIlScraper.parse_url(url)
        assert p is None, f"govil should not claim: {url}"


def test_query_params_preserved():
    """Filter params (Type=, publications_subject=, etc.) must survive parse,
    since the scraper passes them through to the upstream API.
    """
    p = GovIlScraper.parse_url(
        "https://www.gov.il/he/Departments/DynamicCollectors/menifa?Type=2&publications_subject=1"
    )
    assert p is not None
    qp = p.params.get("query_params") or {}
    assert qp.get("Type") == "2"
    assert qp.get("publications_subject") == "1"


def test_registry_dispatch_routes_govil():
    from govscraper.scrapers import dispatch
    hit = dispatch("https://www.gov.il/he/Departments/DynamicCollectors/menifa")
    assert hit is not None
    cls, parsed = hit
    assert cls.id == "govil"


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
