"""Unit tests for DataGovIlScraper.parse_url — covers all three URL forms."""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from govscraper.scrapers.datagovil import DataGovIlScraper  # noqa: E402


def test_dataset_listing():
    p = DataGovIlScraper.parse_url("https://data.gov.il/dataset")
    assert p is not None
    assert p.scraper_id == "datagovil"
    assert p.params["form"] == "search"
    assert p.params["query"] == "*:*"


def test_dataset_search_with_q():
    p = DataGovIlScraper.parse_url("https://data.gov.il/dataset?q=health&rows=50&start=100")
    assert p is not None
    assert p.params["form"] == "search"
    assert p.params["query"] == "health"
    assert p.params["page_size"] == 50
    assert p.params["start"] == 100


def test_dataset_detail():
    p = DataGovIlScraper.parse_url("https://data.gov.il/dataset/covid19_data")
    assert p is not None
    assert p.params["form"] == "dataset"
    assert p.params["dataset_name"] == "covid19_data"
    assert p.params["fetch_datastore"] is False


def test_dataset_detail_fetch_datastore_flag():
    p = DataGovIlScraper.parse_url("https://data.gov.il/dataset/foo?fetch_datastore=true")
    assert p is not None
    assert p.params["fetch_datastore"] is True


def test_resource_uuid():
    p = DataGovIlScraper.parse_url(
        "https://data.gov.il/dataset/foo/resource/abc12345-6789-4abc-def0-1234567890ab"
    )
    assert p is not None
    assert p.params["form"] == "resource"
    assert p.params["dataset_name"] == "foo"
    assert p.params["resource_id"] == "abc12345-6789-4abc-def0-1234567890ab"


def test_resource_uuid_uppercase_normalised():
    p = DataGovIlScraper.parse_url(
        "https://data.gov.il/dataset/foo/resource/ABC12345-6789-4ABC-DEF0-1234567890AB"
    )
    assert p is not None
    assert p.params["resource_id"] == "abc12345-6789-4abc-def0-1234567890ab"


def test_unrelated_host_returns_none():
    assert DataGovIlScraper.parse_url("https://www.gov.il/he/foo") is None
    assert DataGovIlScraper.parse_url("https://example.com/dataset/foo") is None


def test_registry_dispatch():
    from govscraper.scrapers import dispatch
    hit = dispatch("https://data.gov.il/dataset/covid")
    assert hit is not None and hit[0].id == "datagovil"


if __name__ == "__main__":
    failed = False
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"OK  {name}")
            except AssertionError as e:
                failed = True
                print(f"FAIL {name}: {e}")
    sys.exit(1 if failed else 0)
