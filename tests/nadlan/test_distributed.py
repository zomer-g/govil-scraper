"""
Integration tests for the distributed bulk-nadlan queue.

Covers the full path:
    bulk-queue (admin uploads parcels.csv)
    → bulk-claim (worker A) + bulk-claim (worker B) — atomic, no overlap
    → bulk-result (worker uploads deals)
    → bulk-status (counts add up)

Run:  python tests/nadlan/test_distributed.py
"""
import sys, os, csv, io, json, tempfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))


class T:
    passed = failed = 0
    failures = []

    @classmethod
    def step(cls, name, fn):
        sys.stdout.write(f'  {name:<60s}')
        sys.stdout.flush()
        try:
            fn()
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


def _mk_app(tmp_dir):
    """Build a fresh Flask app pointing at an isolated SQLite."""
    # Force the app to use this temp dir for state.
    os.environ['TEMP_DIR'] = tmp_dir
    # Clear any cached imports so app.py re-reads env.
    for m in ('app', 'storage', 'nadlan_api_routes'):
        sys.modules.pop(m, None)
    import app as app_mod  # noqa: F401
    # Mark the test client as admin so admin-only endpoints work.
    app_mod.app.config['TESTING'] = True
    return app_mod.app


def _make_parcels_csv(rows):
    buf = io.StringIO()
    fields = ['gush', 'chelka', 'locality', 'municipality', 'parcel_type',
              'status', 'legal_area_sqm', 'area_sqm',
              'centroid_lat', 'centroid_lon']
    w = csv.DictWriter(buf, fieldnames=fields)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k, '') for k in fields})
    return buf.getvalue().encode('utf-8-sig')


def _admin_headers():
    """Bypass admin check by patching is_admin to return True."""
    return {}  # combined with mock below


# ---------- tests ----------

def test_storage_queue_basic():
    from storage import CollectionStore
    with tempfile.TemporaryDirectory() as td:
        st = CollectionStore(td)
        # Insert 3
        res = st.nadlan_create_tasks([
            {'gush': '6909', 'chelka': '1', 'locality': 'תל אביב'},
            {'gush': '6909', 'chelka': '2', 'locality': 'תל אביב'},
            {'gush': '30012', 'chelka': '5', 'locality': 'ירושלים'},
        ])
        assert res['inserted'] == 3, f'inserted={res}'
        # Re-insert is idempotent
        res2 = st.nadlan_create_tasks([
            {'gush': '6909', 'chelka': '1', 'locality': 'תל אביב'},
            {'gush': '40234', 'chelka': '45', 'locality': 'אילת'},
        ])
        assert res2 == {'inserted': 1, 'skipped': 1}, f'res2={res2}'
        # Status
        s = st.nadlan_status()
        assert s['total'] == 4
        assert s['pending'] == 4
        assert s['done'] == 0


def test_atomic_claim_no_overlap():
    """Two workers claiming concurrently must never receive overlapping tasks."""
    from storage import CollectionStore
    with tempfile.TemporaryDirectory() as td:
        st = CollectionStore(td)
        st.nadlan_create_tasks([
            {'gush': str(i), 'chelka': '1'} for i in range(1, 11)
        ])
        # Worker A claims 4, Worker B claims 6 — together exactly 10
        a = st.nadlan_claim_tasks('worker-A', count=4)
        b = st.nadlan_claim_tasks('worker-B', count=6)
        assert len(a) == 4
        assert len(b) == 6
        a_ids = {t['parcel_id'] for t in a}
        b_ids = {t['parcel_id'] for t in b}
        assert a_ids.isdisjoint(b_ids), f'overlap: {a_ids & b_ids}'
        assert (a_ids | b_ids) == {f'{i}-1' for i in range(1, 11)}
        # No more tasks available
        c = st.nadlan_claim_tasks('worker-C', count=5)
        assert c == []


def test_complete_and_fail():
    from storage import CollectionStore
    with tempfile.TemporaryDirectory() as td:
        st = CollectionStore(td)
        st.nadlan_create_tasks([{'gush': '1', 'chelka': str(i)} for i in range(1, 5)])
        claimed = st.nadlan_claim_tasks('worker', count=4)
        assert len(claimed) == 4
        # Complete one
        st.nadlan_complete_task('1-1', deals_count=7)
        # Transient fail → back to pending
        st.nadlan_fail_task('1-2', 'Timeout 60000ms exceeded', transient=True)
        # Permanent fail → stays failed
        st.nadlan_fail_task('1-3', 'ValueError', transient=False)
        # Leave 1-4 claimed

        s = st.nadlan_status()
        assert s['done'] == 1, f'done={s["done"]}'
        assert s['pending'] == 1, f'pending={s["pending"]}'
        assert s['failed'] == 1, f'failed={s["failed"]}'
        assert s['claimed'] == 1, f'claimed={s["claimed"]}'
        assert s['deals_collected'] == 7


def test_stale_reset():
    """Tasks claimed but unresponsive must be returned to pending."""
    import time as _t
    from storage import CollectionStore
    with tempfile.TemporaryDirectory() as td:
        st = CollectionStore(td)
        st.nadlan_create_tasks([{'gush': '1', 'chelka': '1'}])
        st.nadlan_claim_tasks('worker-A', count=1)
        # Default timeout 600s — none stale yet
        n = st.nadlan_reset_stale(timeout_seconds=600)
        assert n == 0
        # Force stale: wait 1.1s so the claimed_at timestamp (1s precision)
        # is strictly older than the new cutoff.
        _t.sleep(1.5)
        # timeout_seconds=0 + 1s+ wall-clock delay means cutoff > claimed_at
        n = st.nadlan_reset_stale(timeout_seconds=0)
        assert n == 1, f'expected 1 reset, got {n}'
        # Now another worker can claim it
        again = st.nadlan_claim_tasks('worker-B', count=1)
        assert len(again) == 1


def test_http_endpoints():
    """End-to-end via Flask test client. Mocks is_admin so admin endpoints work."""
    from auth import is_admin  # noqa
    with tempfile.TemporaryDirectory() as td:
        os.environ['TEMP_DIR'] = td
        for m in ('app', 'storage', 'nadlan_api_routes'):
            sys.modules.pop(m, None)
        import app as app_mod
        client = app_mod.app.test_client()

        # Patch admin check globally for this test
        with mock.patch('nadlan_api_routes.is_admin', return_value=True):
            # 1. queue 3 parcels via multipart
            csv_bytes = _make_parcels_csv([
                {'gush': '6909', 'chelka': '1', 'locality': 'תל אביב', 'status': 'מוסדר'},
                {'gush': '6909', 'chelka': '2', 'locality': 'תל אביב', 'status': 'מוסדר'},
                {'gush': '99', 'chelka': '99', 'locality': 'X', 'status': 'חדש רשום'},
            ])
            r = client.post('/api/nadlan/bulk-queue',
                            data={'file': (io.BytesIO(csv_bytes), 'parcels.csv'),
                                  'filter_status': 'מוסדר'},
                            content_type='multipart/form-data')
            assert r.status_code == 200, f'queue failed: {r.status_code} {r.data}'
            j = r.get_json()
            assert j['inserted'] == 2, f'expected 2 (filter dropped 1), got {j}'

            # 2. status
            r = client.get('/api/nadlan/bulk-status')
            assert r.status_code == 200
            s = r.get_json()
            assert s['pending'] == 2

            # 3. worker claims 1
            r = client.post('/api/nadlan/bulk-claim',
                            json={'worker_id': 'host-1', 'count': 1})
            assert r.status_code == 200
            tasks = r.get_json()['tasks']
            assert len(tasks) == 1
            pid = tasks[0]['parcel_id']

            # 4. worker uploads 2 deal rows for that parcel
            deals_csv = io.BytesIO(
                ('gush,chelka,dealDate,dealAmount,address,worker_id,scraped_at\n'
                 f'{tasks[0]["gush"]},{tasks[0]["chelka"]},2019-01-01,1000000,'
                 'addr1,host-1,2025-01-01\n'
                 f'{tasks[0]["gush"]},{tasks[0]["chelka"]},2020-01-01,2000000,'
                 'addr2,host-1,2025-01-01\n').encode('utf-8-sig'))
            r = client.post(f'/api/nadlan/bulk-result/{pid}',
                            data={'file': (deals_csv, 'deals.csv'),
                                  'worker_id': 'host-1', 'deals_count': '2'},
                            content_type='multipart/form-data')
            assert r.status_code == 200, f'result failed: {r.data}'
            j = r.get_json()
            assert j['appended'] == 2

            # 5. status reflects done + 2 deals
            r = client.get('/api/nadlan/bulk-status')
            s = r.get_json()
            assert s['done'] == 1
            assert s['pending'] == 1
            assert s['deals_collected'] == 2

            # 6. central CSV has the 2 rows
            r = client.get('/api/nadlan/bulk-deals.csv')
            assert r.status_code == 200
            text = r.data.decode('utf-8-sig')
            assert 'addr1' in text and 'addr2' in text

            # 7. worker fails the second one (transient) — should go back to pending
            r = client.post('/api/nadlan/bulk-claim',
                            json={'worker_id': 'host-2', 'count': 1})
            tasks2 = r.get_json()['tasks']
            assert len(tasks2) == 1
            pid2 = tasks2[0]['parcel_id']
            r = client.post(f'/api/nadlan/bulk-fail/{pid2}',
                            data={'worker_id': 'host-2',
                                  'error': 'ERR_INTERNET_DISCONNECTED',
                                  'transient': 'true'})
            assert r.status_code == 200
            r = client.get('/api/nadlan/bulk-status')
            s = r.get_json()
            assert s['pending'] == 1, f'expected 1 pending after transient fail, got {s}'
            assert s['claimed'] == 0


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    print('=== Distributed bulk-nadlan tests ===')
    T.step('storage: create_tasks idempotent', test_storage_queue_basic)
    T.step('storage: atomic claim — no overlap between workers',
           test_atomic_claim_no_overlap)
    T.step('storage: complete + transient/permanent fail states',
           test_complete_and_fail)
    T.step('storage: reset_stale releases dead workers\' tasks',
           test_stale_reset)
    T.step('HTTP: queue → claim → upload → status round-trip',
           test_http_endpoints)
    print(f'\n{T.passed} pass, {T.failed} fail')
    if T.failed:
        for n, e in T.failures:
            print(f'  FAIL: {n}: {e}')
        sys.exit(1)
    print('\nVERDICT: distributed flow READY')


if __name__ == '__main__':
    main()
