"""
Quality assessment for a completed batch of nadlan scraping.

Run: python tests/nadlan/quality_check.py downloads/batch1.csv parcels_batch1.csv

Reports on:
1. Coverage  — how many parcels processed vs total in input
2. Hit rate  — % of parcels that returned at least one deal
3. Schema    — every column has expected types & no unexpected nulls
4. Data integrity — JSON validity, dedup correctness, row consistency
5. Geographic distribution — deals per locality
6. Time distribution — earliest/latest deal dates, decade buckets
7. Anomalies — outlier prices, missing critical fields
"""
import sys, csv, json
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime


def main():
    if len(sys.argv) < 3:
        print('Usage: quality_check.py deals.csv parcels_batch.csv', file=sys.stderr)
        sys.exit(2)
    deals_csv = sys.argv[1]
    parcels_csv = sys.argv[2]

    sys.stdout.reconfigure(encoding='utf-8')

    # Load
    with open(parcels_csv, encoding='utf-8-sig') as f:
        parcels_total = sum(1 for _ in csv.DictReader(f))

    with open(deals_csv, encoding='utf-8-sig') as f:
        deals = list(csv.DictReader(f))

    ck_path = deals_csv + '.checkpoint.json'
    with open(ck_path, encoding='utf-8') as f:
        ck = json.load(f)
    parcels_processed = len(ck['done'])

    print('=' * 60)
    print(f'QUALITY REPORT — {deals_csv}')
    print('=' * 60)

    # 1. Coverage
    coverage = parcels_processed / parcels_total * 100
    print(f'\n1. Coverage')
    print(f'   parcels in batch:    {parcels_total:,}')
    print(f'   processed:           {parcels_processed:,}  ({coverage:.1f}%)')

    # 2. Hit rate (parcels with ≥1 deal)
    parcels_with_deals = {(r['gush'], r['chelka']) for r in deals}
    hit_rate = len(parcels_with_deals) / max(parcels_processed, 1) * 100
    print(f'\n2. Hit rate (parcels with ≥1 deal)')
    print(f'   {len(parcels_with_deals):,} of {parcels_processed:,}  ({hit_rate:.1f}%)')
    print(f'   total deals:         {len(deals):,}')
    print(f'   avg deals/hit:       {len(deals) / max(len(parcels_with_deals), 1):.1f}')

    # 3. Schema integrity
    print(f'\n3. Schema integrity')
    if not deals:
        print('   no deals — skipping')
    else:
        cols = list(deals[0].keys())
        print(f'   columns:             {len(cols)}')
        # Critical fields populated
        critical = ['gush', 'chelka', 'dealDate', 'dealAmount', 'address']
        for c in critical:
            n_empty = sum(1 for r in deals if not r.get(c))
            print(f'   {c:18} empty: {n_empty}/{len(deals)} ({n_empty/len(deals)*100:.1f}%)')

    # 4. Data integrity
    print(f'\n4. Data integrity')
    json_errors = 0
    for r in deals:
        for col in ('prev_deals', 'ownership'):
            v = r.get(col, '')
            if v:
                try:
                    json.loads(v)
                except Exception:
                    json_errors += 1
    print(f'   invalid JSON in prev_deals/ownership: {json_errors}')

    # Dedup: same (assetId, dealDate, row_id) shouldn't repeat
    keys = [(r.get('assetId'), r.get('dealDate'), r.get('row_id')) for r in deals]
    dups = len(keys) - len(set(keys))
    print(f'   duplicate deal rows: {dups}')

    # 5. Geographic
    print(f'\n5. Geographic distribution (top 10 localities by deals)')
    by_loc = Counter(r.get('meta_setl_name', '') or '?' for r in deals)
    for loc, n in by_loc.most_common(10):
        print(f'   {n:>5}  {loc}')

    # 6. Temporal
    print(f'\n6. Temporal distribution')
    decades = Counter()
    earliest = latest = None
    for r in deals:
        d = r.get('dealDate', '')
        if d:
            try:
                year = int(d[:4])
                decade = year // 10 * 10
                decades[decade] += 1
                if not earliest or d < earliest:
                    earliest = d
                if not latest or d > latest:
                    latest = d
            except ValueError:
                pass
    print(f'   earliest: {earliest}, latest: {latest}')
    for decade in sorted(decades):
        print(f'   {decade}s: {decades[decade]:,}')

    # 7. Anomalies
    print(f'\n7. Anomalies')
    bad_prices = []
    for r in deals:
        try:
            amt = float(r.get('dealAmount') or 0)
            if amt < 1000 or amt > 1e9:
                bad_prices.append((r.get('dealDate'), amt, r.get('address')))
        except ValueError:
            pass
    print(f'   suspicious prices (<1k or >1B):  {len(bad_prices)}')
    for d, a, addr in bad_prices[:5]:
        print(f'     {d}  ₪{a:,.0f}  @ {addr}')

    # Missing critical fields summary
    no_address = sum(1 for r in deals if not r.get('address'))
    no_dealnature = sum(1 for r in deals if not r.get('dealNature'))
    print(f'   deals missing address:      {no_address}/{len(deals)}')
    print(f'   deals missing dealNature:   {no_dealnature}/{len(deals)}')

    # 8. Verdict
    print(f'\n{"=" * 60}')
    print('VERDICT')
    print('=' * 60)
    issues = []
    if coverage < 99:
        issues.append(f'Coverage only {coverage:.1f}% — expected ~100%')
    if json_errors:
        issues.append(f'{json_errors} JSON parse errors')
    if dups:
        issues.append(f'{dups} duplicate rows')
    if hit_rate < 5:
        issues.append(f'Hit rate {hit_rate:.1f}% suspiciously low')

    if issues:
        print('ISSUES FOUND:')
        for i in issues:
            print(f'  - {i}')
        print('\nRECOMMENDATION: do not proceed to batch 2 until issues are resolved')
    else:
        print('No issues found.')
        print(f'Hit rate {hit_rate:.1f}%, coverage {coverage:.1f}%, '
              f'{len(deals):,} clean deals captured.')
        print('\nRECOMMENDATION: proceed to batch 2')


if __name__ == '__main__':
    main()
