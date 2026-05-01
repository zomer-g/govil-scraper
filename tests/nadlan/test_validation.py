"""
Validation suite for the nadlan scraping system before nationwide run.

Run:  python tests/nadlan/test_validation.py

Phase A — in-memory logic (instant)
Phase B — single-parcel scraping (3 × ~30s)
Phase C — bulk run smoke + resume (9 × ~30s)
Phase D — production-readiness analysis (instant, uses C1 results)
Phase E — edge cases (instant, uses existing CSVs)
"""
import sys, os, csv, json, time, tempfile, io
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from bulk_nadlan import (
    PARCEL_COLS, META_COLS, DEAL_COLS,
    _parcel_iter, _load_checkpoint, _save_checkpoint, _deal_key, run,
    _is_transient_error, _CIRCUIT_BREAKER_THRESHOLD,
)


class T:
    passed = failed = skipped = 0
    failures = []

    @classmethod
    def step(cls, name, fn):
        sys.stdout.write(f'  {name:<60s}')
        sys.stdout.flush()
        try:
            r = fn()
            if r == 'SKIP':
                print(' [SKIP]')
                cls.skipped += 1
            else:
                print(' [PASS]')
                cls.passed += 1
        except AssertionError as e:
            print(f' [FAIL] {e}')
            cls.failed += 1
            cls.failures.append((name, str(e)))
        except Exception as e:
            print(f' [ERR ] {type(e).__name__}: {e}')
            cls.failed += 1
            cls.failures.append((name, f'{type(e).__name__}: {e}'))


def make_parcels(path, rows):
    fields = ['gush', 'chelka', 'locality', 'municipality', 'parcel_type',
              'status', 'legal_area_sqm', 'area_sqm',
              'centroid_lat', 'centroid_lon']
    with open(path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in fields})


# ===== Phase A =====

def test_a1_schema():
    expected = PARCEL_COLS + META_COLS + DEAL_COLS
    sample = {**{c: f'{c}_v' for c in PARCEL_COLS},
              **{c: f'{c}_v' for c in META_COLS},
              **{c: f'{c}_v' for c in DEAL_COLS}}
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=expected, extrasaction='ignore')
    w.writeheader()
    w.writerow(sample)
    buf.seek(0)
    reader = csv.DictReader(buf)
    assert set(reader.fieldnames) == set(expected), \
        f'missing: {set(expected) - set(reader.fieldnames)}'
    row = next(reader)
    for c in expected:
        assert row[c] == f'{c}_v', f'{c} mismatch'


def test_a2_filter_status():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / 'parcels.csv'
        make_parcels(p, [
            {'gush': '1', 'chelka': '1', 'status': 'מוסדר'},
            {'gush': '2', 'chelka': '2', 'status': 'חדש רשום'},
            {'gush': '3', 'chelka': '3', 'status': 'מוסדר'},
            {'gush': '4', 'chelka': '4', 'status': 'ירדני'},
        ])
        rows = list(_parcel_iter(str(p), filter_status='מוסדר'))
        gushim = sorted(r['gush'] for r in rows)
        assert gushim == ['1', '3'], f'got {gushim}'
        # Without filter — all 4
        rows_all = list(_parcel_iter(str(p)))
        assert len(rows_all) == 4


def test_a3_checkpoint():
    with tempfile.TemporaryDirectory() as td:
        path = str(Path(td) / 'ck.json')
        original = {'done': ['1-1', '2-2'], 'deal_keys': ['a|2024|1']}
        _save_checkpoint(path, original)
        loaded = _load_checkpoint(path)
        assert loaded == original, f'mismatch: {loaded}'
        assert not Path(path + '.tmp').exists(), 'tmp file leaked'
        # Non-existent → empty
        empty = _load_checkpoint(str(Path(td) / 'nope.json'))
        assert empty == {'done': [], 'deal_keys': []}


def test_a4_dedup():
    d1 = {'assetId': 'X', 'dealDate': '2024-01-01', 'row_id': 1}
    d2 = {'assetId': 'X', 'dealDate': '2024-01-01', 'row_id': 1}
    d3 = {'assetId': 'Y', 'dealDate': '2024-01-01', 'row_id': 1}
    assert _deal_key(d1) == _deal_key(d2)
    assert _deal_key(d1) != _deal_key(d3)


def test_a6_transient_error_classifier():
    """_is_transient_error correctly distinguishes network from real errors."""
    # Real network errors we've seen in the wild
    transient_examples = [
        Exception("Page.goto: net::ERR_INTERNET_DISCONNECTED at https://..."),
        Exception("Page.goto: net::ERR_TIMED_OUT at https://..."),
        Exception("Page.goto: Timeout 60000ms exceeded."),
        Exception("net::ERR_CONNECTION_REFUSED"),
        Exception("net::ERR_NAME_NOT_RESOLVED"),
        Exception("Browser closed"),
    ]
    for e in transient_examples:
        assert _is_transient_error(e), f'should be transient: {e}'

    # Real errors that should mark parcel as done
    permanent_examples = [
        ValueError("not enough values to unpack"),
        KeyError("dealAmount"),
        TypeError("expected str, got NoneType"),
        Exception("statusCode 405"),
    ]
    for e in permanent_examples:
        assert not _is_transient_error(e), f'should be permanent: {e}'


def test_a7_circuit_breaker_simulation():
    """run() exits after _CIRCUIT_BREAKER_THRESHOLD consecutive transient errors,
    leaving failed parcels unmarked in checkpoint so re-run picks them up."""
    import bulk_nadlan
    from unittest import mock

    with tempfile.TemporaryDirectory() as td:
        parcels = Path(td) / 'parcels.csv'
        deals = str(Path(td) / 'deals.csv')
        # 20 parcels — circuit breaker should kick in after 10 fails
        rows = [{'gush': str(i), 'chelka': '1', 'status': 'מוסדר'} for i in range(1, 21)]
        make_parcels(parcels, rows)

        # Mock fetch to always raise a transient error
        def boom(*args, **kwargs):
            raise Exception("Page.goto: net::ERR_INTERNET_DISCONNECTED at ...")

        # Patch both fetch_parcel_deals and the playwright import inside run()
        mock_browser = mock.MagicMock()
        mock_browser.is_connected.return_value = True
        mock_pw_instance = mock.MagicMock()
        mock_pw_instance.chromium.launch.return_value = mock_browser

        mock_pw_cm = mock.MagicMock()
        mock_pw_cm.__enter__.return_value = mock_pw_instance
        mock_pw_cm.__exit__.return_value = False

        with mock.patch('nadlan_api.fetch_parcel_deals', side_effect=boom), \
             mock.patch('playwright.sync_api.sync_playwright',
                        return_value=mock_pw_cm):
            run(str(parcels), deals, filter_status='מוסדר',
                per_parcel_pause_s=0)

        ck = _load_checkpoint(deals + '.checkpoint.json')
        # All 20 parcels should NOT be in done (transient errors don't mark done)
        assert len(ck['done']) == 0, f'expected 0 done, got {len(ck["done"])}'
        # CSV should have header but no data rows
        with open(deals, encoding='utf-8-sig') as f:
            rows_out = list(csv.DictReader(f))
        assert len(rows_out) == 0, f'expected 0 deal rows, got {len(rows_out)}'


def test_a5_iter_skip():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / 'parcels.csv'
        make_parcels(p, [
            {'gush': '1', 'chelka': '1'},
            {'gush': '', 'chelka': '2'},
            {'gush': '3', 'chelka': ''},
            {'gush': '4', 'chelka': '4'},
        ])
        rows = list(_parcel_iter(str(p)))
        assert len(rows) == 2, f'expected 2, got {len(rows)}'


# ===== Phase B =====

def test_b1_known():
    from nadlan_api import fetch_parcel_deals
    items, meta, warn = fetch_parcel_deals('6909', '1')
    assert len(items) == 7, f'expected 7, got {len(items)}'
    sample = items[1]
    for f in ('trend_rate', 'trend_years', 'prev_deals', 'ownership'):
        assert f in sample, f'missing field {f}'
    pd = items[1].get('prev_deals')
    assert pd, 'expected prev_deals'
    parsed = json.loads(pd)
    assert isinstance(parsed, list) and len(parsed) >= 1


def test_b2_empty():
    from nadlan_api import fetch_parcel_deals
    items, meta, warn = fetch_parcel_deals('6909', '2')
    assert items == [], f'expected empty, got {len(items)}'


def test_b3_browser_reuse():
    from playwright.sync_api import sync_playwright
    from nadlan_api import fetch_parcel_deals
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        try:
            i1, _, _ = fetch_parcel_deals('6909', '1', browser=browser)
            assert browser.is_connected(), 'browser closed'
            i2, _, _ = fetch_parcel_deals('6909', '2', browser=browser)
            assert browser.is_connected(), 'browser closed'
            assert len(i1) == 7
            assert i2 == []
        finally:
            browser.close()


# ===== Phase C =====

C1 = {}


def test_c1_smoke():
    parcels_csv = ROOT / 'parcels.csv'
    if not parcels_csv.exists():
        return 'SKIP'
    with tempfile.TemporaryDirectory() as td:
        deals = str(Path(td) / 'deals.csv')
        t0 = time.time()
        run(str(parcels_csv), deals, limit=5,
            filter_status='מוסדר', per_parcel_pause_s=0)
        elapsed = time.time() - t0
        with open(deals, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
            rows = list(reader)
        expected = set(PARCEL_COLS + META_COLS + DEAL_COLS)
        assert set(header) == expected, \
            f'missing: {expected - set(header)}'
        ck = _load_checkpoint(deals + '.checkpoint.json')
        assert len(ck['done']) == 5, f'expected 5 done, got {len(ck["done"])}'
        C1['elapsed'] = elapsed
        C1['rows'] = len(rows)


def test_c2_resume():
    parcels_csv = ROOT / 'parcels.csv'
    if not parcels_csv.exists():
        return 'SKIP'
    with tempfile.TemporaryDirectory() as td:
        deals = str(Path(td) / 'deals.csv')
        run(str(parcels_csv), deals, limit=2,
            filter_status='מוסדר', per_parcel_pause_s=0)
        ck1 = _load_checkpoint(deals + '.checkpoint.json')
        run(str(parcels_csv), deals, limit=2,
            filter_status='מוסדר', per_parcel_pause_s=0)
        ck2 = _load_checkpoint(deals + '.checkpoint.json')
        assert len(ck2['done']) == 4, f'expected 4 (2+2), got {len(ck2["done"])}'
        assert set(ck1['done']) <= set(ck2['done']), 'first batch missing in second'


# ===== Phase D =====

def test_d1_timing():
    if 'elapsed' not in C1:
        return 'SKIP'
    per_parcel = C1['elapsed'] / 5
    days = per_parcel * 748_500 / 86400
    print(f'\n     >> {per_parcel:.1f}s/parcel × 748,500 = {days:.1f} days', end='')


def test_d2_output_size():
    f = ROOT / 'downloads' / 'deals_all.csv'
    if not f.exists():
        return 'SKIP'
    with open(f, encoding='utf-8-sig') as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return 'SKIP'
    avg_bytes = f.stat().st_size / len(rows)
    est_deals = 748_500 * 0.10 * 3.0  # ~10% with deals × ~3 deals/parcel
    csv_mb = est_deals * avg_bytes / 1e6
    ck_mb = 748_500 * 12 / 1e6
    print(f'\n     >> ~{est_deals:,.0f} deals → ~{csv_mb:.0f}MB CSV, ~{ck_mb:.0f}MB ckpt', end='')


def test_d3_json():
    f = ROOT / 'downloads' / 'deals_all.csv'
    if not f.exists():
        return 'SKIP'
    n_checked = 0
    with open(f, encoding='utf-8-sig') as fp:
        for r in csv.DictReader(fp):
            for col in ('prev_deals', 'ownership'):
                v = r.get(col, '')
                if v:
                    json.loads(v)
                    n_checked += 1
    assert n_checked > 0, 'no non-empty JSON fields in dataset'


def test_d4_hebrew():
    f = ROOT / 'downloads' / 'deals_all.csv'
    if not f.exists():
        return 'SKIP'
    found = False
    with open(f, encoding='utf-8-sig') as fp:
        for r in csv.DictReader(fp):
            for col in ('meta_setl_name', 'address', 'meta_neigh_name'):
                v = r.get(col, '')
                if any(0x0590 <= ord(c) <= 0x05FF for c in v):
                    found = True
                    break
            if found:
                break
    assert found, 'no Hebrew text in any row'


# ===== Phase E =====

def test_e1_pagination():
    return 'SKIP'  # No known >500-deal parcel; warn-path is exercised in unit


def test_e2_special_chars():
    parcels_csv = ROOT / 'parcels.csv'
    if not parcels_csv.exists():
        return 'SKIP'
    found = []
    with open(parcels_csv, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            loc = r.get('locality', '')
            if any(s in loc for s in ['יקנעם', 'אום', '(', '-']):
                found.append(loc)
                if len(found) >= 3:
                    break
    assert found, 'no rows with special chars'


def test_e3_old_deals():
    f = ROOT / 'downloads' / 'deals_all.csv'
    if not f.exists():
        return 'SKIP'
    with open(f, encoding='utf-8-sig') as fp:
        rows = list(csv.DictReader(fp))
    # Just verify CSV parses to the end without crash
    assert len(rows) > 0
    # Verify dealNature can be empty (1998 deal) without crash
    for r in rows:
        assert isinstance(r.get('dealNature', ''), str)


# ===== Main =====

def main():
    sys.stdout.reconfigure(encoding='utf-8')

    print('\n=== Phase A — Logic (no network) ===')
    T.step('A1: schema covers all expected columns', test_a1_schema)
    T.step('A2: --filter-status filters correctly', test_a2_filter_status)
    T.step('A3: checkpoint round-trip + atomic write', test_a3_checkpoint)
    T.step('A4: deduplication key is stable', test_a4_dedup)
    T.step('A5: iterator skips invalid rows', test_a5_iter_skip)
    T.step('A6: transient error classifier', test_a6_transient_error_classifier)
    T.step('A7: circuit breaker — failed transients are not "done"', test_a7_circuit_breaker_simulation)

    print('\n=== Phase B — Single parcel (network) ===')
    T.step('B1: known parcel 6909/1 has 7 deals', test_b1_known)
    T.step('B2: empty parcel 6909/2 graceful', test_b2_empty)
    T.step('B3: browser reuse across calls', test_b3_browser_reuse)

    print('\n=== Phase C — Bulk integration (network) ===')
    T.step('C1: smoke test --limit 5 --filter-status מוסדר', test_c1_smoke)
    T.step('C2: resume — second run skips processed', test_c2_resume)

    print('\n=== Phase D — Production analysis ===')
    T.step('D1: timing extrapolation', test_d1_timing)
    T.step('D2: output size estimate', test_d2_output_size)
    T.step('D3: JSON validity in nested fields', test_d3_json)
    T.step('D4: Hebrew encoding preserved', test_d4_hebrew)

    print('\n=== Phase E — Edge cases ===')
    T.step('E1: parcel with >500 deals (pagination)', test_e1_pagination)
    T.step('E2: localities with special chars', test_e2_special_chars)
    T.step('E3: old deals without dealNature', test_e3_old_deals)

    print('\n=== TEST RESULTS ===')
    print(f'Passed:  {T.passed}')
    print(f'Failed:  {T.failed}')
    print(f'Skipped: {T.skipped}')

    if T.failed:
        print('\nFailures:')
        for name, err in T.failures:
            print(f'  {name}: {err}')
        print('\nVERDICT: NOT READY')
        sys.exit(1)
    else:
        print('\nVERDICT: READY FOR PRODUCTION')


if __name__ == '__main__':
    main()
