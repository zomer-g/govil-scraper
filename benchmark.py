#!/usr/bin/env python3
"""
Benchmark — Scraper correctness tests against real gov.il URLs.

Pulls 10 items from each URL and validates metadata, file attachments,
and content extraction.

Usage:
    python benchmark.py                    # metadata-only validation
    python benchmark.py --download-files   # also verify file download URLs
    python benchmark.py --max-items 5      # limit items per URL
"""

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import List, Optional

from scraper_engine import (
    GovILSession, GovILScraper, PageType,
    GovILScraperError, InvalidURLError, CloudflareBlockError,
)
from file_handler import FileHandler

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("benchmark")


# ---------------------------------------------------------------------------
# Test definitions
# ---------------------------------------------------------------------------

@dataclass
class TestCase:
    name: str
    url: str
    category: int          # 1=Traditional+PDFs, 2=Dynamic+PDFs, 3=Dynamic+Expandable
    expect_files: bool     # Whether file attachments are expected
    description: str = ""


TEST_CASES = [
    # Category 1: Traditional Collector with PDFs
    TestCase(
        name="policies (traditional+PDFs)",
        url="https://www.gov.il/he/collectors/policies?officeId=c3f24c3b-9940-45c2-82a1-c4be2087bf99",
        category=1,
        expect_files=True,
        description="Traditional collector — PDF files via content page API",
    ),

    # Category 2: Dynamic Collectors with PDFs + Metadata
    TestCase(
        name="menifa (custom API)",
        url="https://www.gov.il/he/Departments/DynamicCollectors/menifa?skip=0",
        category=2,
        expect_files=True,
        description="Dynamic collector — custom API with x-client-id",
    ),
    TestCase(
        name="guidelines-state-attorney",
        url="https://www.gov.il/he/Departments/DynamicCollectors/guidelines-state-attorney?skip=0",
        category=2,
        expect_files=True,
        description="Dynamic collector — standard GUID, BlobFolder file URLs",
    ),
    TestCase(
        name="legal-advisor-guidelines",
        url="https://www.gov.il/he/departments/dynamiccollectors/legal-advisor-guidelines?skip=0",
        category=2,
        expect_files=True,
        description="Dynamic collector — custom API with file attachments",
    ),

    # Category 3: Dynamic Collectors with Expandable Content/Tables
    TestCase(
        name="conditional-order",
        url="https://www.gov.il/he/Departments/DynamicCollectors/conditional-order?skip=0",
        category=3,
        expect_files=False,
        description="Dynamic collector — expandable content with nested tables",
    ),
    TestCase(
        name="hesdermutne",
        url="https://www.gov.il/he/departments/dynamiccollectors/hesdermutne?skip=0",
        category=3,
        expect_files=False,
        description="Dynamic collector — expandable content with nested data",
    ),
]


# ---------------------------------------------------------------------------
# Test result
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    name: str
    category: int
    status: str = "SKIP"   # PASS, FAIL, WARN, ERROR, SKIP
    items: int = 0
    fields: int = 0
    files: int = 0
    total_on_server: int = 0
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    nested_fields: List[str] = field(default_factory=list)  # fields with [N פריטים]
    sample_values: dict = field(default_factory=dict)        # field -> sample value
    download_ok: int = 0
    download_fail: int = 0
    csv_rows: int = 0
    csv_cols: int = 0
    elapsed_sec: float = 0.0


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

def _scrape_with_timeout(session, url, max_items, timeout_sec=120):
    """Run scrape in a thread with a timeout."""
    import threading
    result_holder = [None]
    error_holder = [None]

    def target():
        try:
            scraper = GovILScraper(session, page_size=max_items)
            result_holder[0] = scraper.scrape(url)
        except Exception as e:
            error_holder[0] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)
    if t.is_alive():
        raise TimeoutError(f"Scrape timed out after {timeout_sec}s")
    if error_holder[0]:
        raise error_holder[0]
    return result_holder[0]


def run_test(session: GovILSession, tc: TestCase, max_items: int,
             download_files: bool) -> TestResult:
    """Run a single benchmark test case."""
    result = TestResult(name=tc.name, category=tc.category)
    start = time.time()

    try:
        sr = _scrape_with_timeout(session, tc.url, max_items, timeout_sec=120)

        # Truncate to max_items
        items = sr.items[:max_items]
        attachments = [a for a in sr.file_attachments if a.item_index < max_items]

        result.items = len(items)
        result.fields = len(sr.column_headers)
        result.files = len(attachments)
        result.total_on_server = sr.total_count

        # --- Basic validations ---
        if sr.total_count <= 0:
            result.errors.append("total_count is 0")
        if len(items) < 1:
            result.errors.append("no items scraped")
        if len(sr.column_headers) < 3:
            result.warnings.append(f"only {len(sr.column_headers)} columns (expected >= 3)")
        if not sr.collector_name:
            result.warnings.append("collector_name is empty")

        # Check each item has at least 2 non-empty fields
        empty_items = 0
        for item in items:
            non_empty = sum(1 for v in item.values() if v and str(v).strip())
            if non_empty < 2:
                empty_items += 1
        if empty_items > 0:
            result.warnings.append(f"{empty_items} items with < 2 non-empty fields")

        # --- Category-specific validations ---

        if tc.category == 1:
            # Traditional: expect title/description fields
            if sr.page_type != PageType.TRADITIONAL_COLLECTOR:
                result.errors.append(f"expected TRADITIONAL_COLLECTOR, got {sr.page_type}")
            has_title = any("title" in item for item in items)
            if not has_title:
                result.warnings.append("no 'title' field found in items")

        if tc.category in (1, 2) and tc.expect_files:
            # Validate file attachments
            if len(attachments) < 1:
                result.errors.append("expected file attachments but found 0")
            else:
                bad_urls = [a for a in attachments if not a.url or not a.url.startswith("http")]
                if bad_urls:
                    result.errors.append(f"{len(bad_urls)} attachments with invalid URLs")
                no_name = [a for a in attachments if not a.filename]
                if no_name:
                    result.warnings.append(f"{len(no_name)} attachments with empty filenames")

        if tc.category == 3:
            # Check for nested content loss
            for item in items:
                for key, val in item.items():
                    val_str = str(val)
                    if "פריטים]" in val_str or "קבצים]" in val_str:
                        if key not in result.nested_fields:
                            result.nested_fields.append(key)
                    # Capture sample values for first item
                    if key.startswith("Data.") and key not in result.sample_values:
                        sample = str(val)[:100] if val else ""
                        if sample:
                            result.sample_values[key] = sample

        # --- CSV export validation ---
        try:
            tmp_dir = tempfile.mkdtemp(prefix="benchmark_")
            handler = FileHandler(session, output_dir=tmp_dir)
            # Create a minimal ScrapeResult for CSV export
            from scraper_engine import ScrapeResult
            mini_result = ScrapeResult(
                items=items,
                total_count=len(items),
                file_attachments=attachments,
                collector_name=sr.collector_name,
                page_type=sr.page_type,
                column_headers=sr.column_headers,
                warning=sr.warning,
            )
            csv_path = handler.export_csv(mini_result)
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                csv_headers = reader.fieldnames or []
                csv_data = list(reader)
                result.csv_cols = len(csv_headers)
                result.csv_rows = len(csv_data)
            if result.csv_rows != len(items):
                result.warnings.append(
                    f"CSV has {result.csv_rows} rows but scraped {len(items)} items")
            # Cleanup
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception as e:
            result.warnings.append(f"CSV export failed: {e}")

        # --- Optional: download file validation ---
        if download_files and attachments:
            test_atts = attachments[:2]  # Test first 2 files
            for att in test_atts:
                try:
                    resp = session.download_file(att.url)
                    if resp.status_code == 200 and len(resp.content) > 0:
                        result.download_ok += 1
                    else:
                        result.download_fail += 1
                        result.warnings.append(
                            f"Download returned {resp.status_code} for {att.filename}")
                except Exception as e:
                    result.download_fail += 1
                    result.warnings.append(f"Download failed for {att.filename}: {e}")

        # --- Determine final status ---
        if result.errors:
            result.status = "FAIL"
        elif result.nested_fields:
            result.status = "WARN"
            result.warnings.append(
                f"nested content as [N items] in: {', '.join(result.nested_fields)}")
        elif result.warnings:
            result.status = "WARN"
        else:
            result.status = "PASS"

    except (InvalidURLError, CloudflareBlockError, GovILScraperError) as e:
        result.status = "ERROR"
        result.errors.append(str(e))
    except Exception as e:
        result.status = "ERROR"
        result.errors.append(f"{type(e).__name__}: {e}")

    result.elapsed_sec = time.time() - start
    return result


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_results(results: List[TestResult], download_files: bool):
    """Print benchmark results as a formatted table."""

    print()
    print("=" * 90)
    print("BENCHMARK RESULTS")
    print("=" * 90)

    # Header
    header = f"{'Test':<35} {'Items':>5} {'Fields':>6} {'Files':>5} {'CSV':>7} {'Time':>6} {'Status'}"
    print(header)
    print("-" * 90)

    pass_count = 0
    warn_count = 0
    fail_count = 0
    error_count = 0

    for r in results:
        csv_info = f"{r.csv_rows}x{r.csv_cols}" if r.csv_rows else "-"
        time_str = f"{r.elapsed_sec:.1f}s"

        # Status with color hint
        if r.status == "PASS":
            status_str = "PASS"
            pass_count += 1
        elif r.status == "WARN":
            status_str = "WARN"
            warn_count += 1
        elif r.status == "FAIL":
            status_str = "FAIL"
            fail_count += 1
        else:
            status_str = "ERROR"
            error_count += 1

        line = f"{r.name:<35} {r.items:>5} {r.fields:>6} {r.files:>5} {csv_info:>7} {time_str:>6} {status_str}"
        print(line)

        # Download info
        if download_files and (r.download_ok or r.download_fail):
            print(f"  {'':35} Downloads: {r.download_ok} OK, {r.download_fail} failed")

    print("-" * 90)
    print(f"Total: {pass_count} PASS, {warn_count} WARN, {fail_count} FAIL, {error_count} ERROR")
    print("=" * 90)

    # Print details for non-PASS results
    for r in results:
        if r.status != "PASS":
            print(f"\n--- {r.name} ({r.status}) ---")
            if r.errors:
                for e in r.errors:
                    print(f"  ERROR: {e}")
            if r.warnings:
                for w in r.warnings:
                    print(f"  WARN:  {w}")
            if r.nested_fields:
                print(f"  Nested content fields: {', '.join(r.nested_fields)}")
            if r.sample_values:
                print(f"  Sample Data.* values (first item):")
                for k, v in list(r.sample_values.items())[:8]:
                    print(f"    {k}: {v}")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scraper correctness benchmark")
    parser.add_argument("--max-items", type=int, default=10,
                        help="Max items to scrape per URL (default: 10)")
    parser.add_argument("--download-files", action="store_true",
                        help="Also verify file download URLs (downloads first 2 per URL)")
    parser.add_argument("--category", type=int, choices=[1, 2, 3],
                        help="Run only tests for this category")
    args = parser.parse_args()

    cases = TEST_CASES
    if args.category:
        cases = [tc for tc in cases if tc.category == args.category]

    print(f"\nGov.il Scraper Benchmark")
    print(f"Tests: {len(cases)} URLs, max {args.max_items} items each")
    if args.download_files:
        print(f"File download verification: ON")
    print()

    # Create session and warm it once
    print("Warming session...")
    session = GovILSession(use_playwright_fallback=False)
    try:
        session.warm()
        print("Session ready.\n")
    except Exception as e:
        print(f"Failed to warm session: {e}")
        sys.exit(1)

    results = []
    for i, tc in enumerate(cases):
        print(f"[{i+1}/{len(cases)}] Testing: {tc.name} ...")
        print(f"  URL: {tc.url}")
        print(f"  Category {tc.category}: {tc.description}")

        r = run_test(session, tc, args.max_items, args.download_files)
        results.append(r)

        status_icon = {"PASS": "+", "WARN": "~", "FAIL": "!", "ERROR": "X"}.get(r.status, "?")
        print(f"  [{status_icon}] {r.status} — {r.items} items, {r.fields} fields, "
              f"{r.files} files, {r.elapsed_sec:.1f}s")

        # Brief pause between tests to be gentle on the API
        if i < len(cases) - 1:
            time.sleep(1)

    # Close session
    try:
        session.close()
    except Exception:
        pass

    print_results(results, args.download_files)

    # Exit code: 0 if all pass/warn, 1 if any fail/error
    if any(r.status in ("FAIL", "ERROR") for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
