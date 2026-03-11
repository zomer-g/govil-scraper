"""Quick test: validate all target URLs parse correctly and scrape successfully."""
import sys
import os
import time

# Fix Windows console encoding for Hebrew
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from scraper_engine import GovILSession, GovILScraper, parse_gov_url

URLS = [
    "https://www.gov.il/he/Departments/DynamicCollectors/menifa?skip=0",
    "https://www.gov.il/he/Departments/DynamicCollectors/guidelines-state-attorney?skip=0",
    "https://www.gov.il/he/departments/dynamiccollectors/legal-advisor-guidelines?skip=0",
    "https://www.gov.il/he/collectors/policies?officeId=c3f24c3b-9940-45c2-82a1-c4be2087bf99",
]

def main():
    print("=" * 60)
    print("STEP 1: URL PARSING")
    print("=" * 60)
    for url in URLS:
        parsed = parse_gov_url(url)
        print(f"  OK  {parsed.page_type.value:25s}  name={parsed.collector_name}  office={parsed.office_id}")

    print("\n" + "=" * 60)
    print("STEP 2: SESSION WARM-UP")
    print("=" * 60)
    session = GovILSession(use_playwright_fallback=True)
    session.warm()
    print("  Session warmed OK")

    print("\n" + "=" * 60)
    print("STEP 3: SCRAPE EACH URL (first page only)")
    print("=" * 60)
    scraper = GovILScraper(session, page_size=5)  # small page for quick test

    results = {}
    for url in URLS:
        name = url.split("/")[-1].split("?")[0]
        print(f"\n  Testing: {name}")
        try:
            result = scraper.scrape(url)
            print(f"    Total:       {result.total_count}")
            print(f"    Fetched:     {len(result.items)}")
            print(f"    Columns:     {result.column_headers[:5]}")
            print(f"    Attachments: {len(result.file_attachments)}")
            print(f"    Warning:     {result.warning}")
            results[name] = "PASS"
        except Exception as e:
            print(f"    FAILED: {e}")
            results[name] = f"FAIL: {e}"
        time.sleep(1)

    session.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_pass = True
    for name, status in results.items():
        icon = "PASS" if status == "PASS" else "FAIL"
        print(f"  [{icon}] {name}")
        if status != "PASS":
            all_pass = False

    sys.exit(0 if all_pass else 1)

if __name__ == "__main__":
    main()
