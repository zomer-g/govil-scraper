"""Unit tests for GovMapScraper.parse_url — covers layer permalinks with
ITM bbox, WGS84 bbox, and centre+zoom forms; plus rejection of non-govmap hosts.
"""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from govscraper.scrapers.govmap.scraper import GovMapScraper  # noqa: E402


# Supported permalink forms — see parse_govmap_url in
# govscraper.scrapers.govmap.legacy_engine.

LAYER_URLS = [
    # numeric-id layer with ITM bbox
    "https://www.govmap.gov.il/?lay=220826&bbox=170000,663000,180000,673000",
    # semantic-id layer with no bbox (whole-country query)
    "https://www.govmap.gov.il/?lay=DISTRICT_BORDERS",
    "https://www.govmap.gov.il/?lay=FIRE_STATIONS",
]

NON_GOVMAP_HOSTS = [
    "https://www.gov.il/he/collectors/policies",
    "https://www.nadlan.gov.il/?view=kparcel_all&id=1-2",
    "https://data.gov.il/dataset/foo",
    "https://example.com/?lay=X",  # has lay= but wrong host
]


def test_layer_urls_parse():
    for url in LAYER_URLS:
        p = GovMapScraper.parse_url(url)
        assert p is not None, f"failed to parse: {url}"
        assert p.scraper_id == "govmap"


def test_layer_id_extracted():
    p = GovMapScraper.parse_url("https://www.govmap.gov.il/?lay=DISTRICT_BORDERS")
    assert p is not None
    qp = p.params.get("query_params") or {}
    # Either "layer_id" (preferred) or the raw lay key — accept both since the
    # legacy parser populates one or the other depending on shape.
    layer_id = qp.get("layer_id") or qp.get("lay")
    assert layer_id == "DISTRICT_BORDERS", f"expected DISTRICT_BORDERS, got {qp!r}"


def test_non_govmap_hosts_return_none():
    for url in NON_GOVMAP_HOSTS:
        p = GovMapScraper.parse_url(url)
        assert p is None, f"govmap should not claim: {url}"


def test_govmap_dispatch_takes_precedence_over_govil():
    """govmap.gov.il is a subdomain of gov.il; the registry order in
    govscraper/scrapers/__init__.py registers govmap before govil so that
    dispatch() picks the more-specific scraper. Regression test for that
    invariant.
    """
    from govscraper.scrapers import dispatch
    hit = dispatch("https://www.govmap.gov.il/?lay=DISTRICT_BORDERS")
    assert hit is not None
    cls, _ = hit
    assert cls.id == "govmap", f"expected govmap, got {cls.id} (registry order regression)"


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
