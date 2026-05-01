"""Contract tests for over.org.il integration.

Goal: detect drift between the constants in
govscraper.worker.publishers._contract and what over_worker.py actually
sends. If either side changes one of these strings, both must change in
lockstep — and a contract-test failure is the canary.

We don't hit a live server here. Instead we statically scan over_worker.py
for the same literals, and we instantiate OverOrgPublisher against a
mocked OverWorkerClient to verify the intended call shape.
"""
from __future__ import annotations

import os
import re
import sys

# Ensure repo root is on sys.path when running this file directly
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from govscraper.worker.publishers import _contract as C  # noqa: E402


def _read_over_worker_source() -> str:
    with open(os.path.join(_ROOT, "over_worker.py"), encoding="utf-8") as f:
        return f.read()


def test_endpoint_paths_match_legacy_source():
    src = _read_over_worker_source()
    # Strip the {var} placeholders before searching, since over_worker.py
    # uses f-strings or plain concatenation.
    expected_paths = [
        C.PATH_POLL,
        C.PATH_PROGRESS.replace("/{task_id}", "/"),
        C.PATH_FAIL.replace("/{task_id}", "/"),
        C.PATH_UPLOAD_ZIP.replace("/{tracked_dataset_id}", "/"),
        C.PATH_UPLOAD_CSV.replace("/{tracked_dataset_id}", "/"),
        C.PATH_PUSH_VERSION,
    ]
    for p in expected_paths:
        assert p in src, f"endpoint {p!r} not found in over_worker.py"


def test_multipart_field_names_match_legacy_source():
    src = _read_over_worker_source()
    for name in [
        C.ZIP_FORM_VERSION_NUMBER,
        C.ZIP_FORM_ATTACHMENT_COUNT,
        C.ZIP_FORM_PART,
        C.ZIP_FORM_TOTAL_PARTS,
        C.CSV_FORM_VERSION_NUMBER,
        C.CSV_FORM_RESOURCE_NAME,
        C.CSV_FORM_ROW_COUNT,
        C.CSV_FORM_COMPRESSION,
        C.CSV_FORM_FIELDS_JSON,
    ]:
        assert f'"{name}"' in src, f"multipart field {name!r} missing from over_worker.py"


def test_push_version_payload_keys_match_legacy_source():
    src = _read_over_worker_source()
    for k in [
        C.PV_TRACKED_DATASET_ID,
        C.PV_METADATA_MODIFIED,
        C.PV_RESOURCES,
        C.PV_ATTACHMENTS,
        C.PV_SCRAPE_METADATA,
        C.PV_ZIP_RESOURCE_ID,
        C.PV_ZIP_RESOURCE_IDS,
        C.PV_CSV_RESOURCE_IDS,
        C.PV_SCRAPER_CONFIG_PATCH,
        C.PV_SKIP_VERSION,
        C.RES_NAME,
        C.RES_FORMAT,
        C.RES_RECORDS,
        C.RES_FIELDS,
        C.RES_ROW_COUNT,
        C.SM_SOURCE_URL,
        C.SM_SCRAPE_DURATION_SECONDS,
        C.SM_TOTAL_ITEMS,
        C.SM_TOTAL_FILES,
        C.SM_SCRAPER_VERSION,
    ]:
        assert f'"{k}"' in src, f"push-version key {k!r} missing from over_worker.py"


def test_hebrew_resource_name_matches_legacy():
    src = _read_over_worker_source()
    assert C.PRIMARY_RESOURCE_NAME in src, (
        "Hebrew resource name לנתוני הסורק must match what over_worker.py uses"
    )


def test_publisher_calls_legacy_client_with_correct_args():
    """OverOrgPublisher.publish() must call OverWorkerClient.push_version()
    with arguments that match the live contract."""
    from govscraper.scrapers.base import TabularResult
    from govscraper.types import Task
    from govscraper.worker.publishers.over_org import OverOrgPublisher

    captured: dict = {}

    class FakeClient:
        def push_version(self, **kwargs):
            captured.update(kwargs)
            return {"message": "ok"}

        def upload_csv(self, *a, **kw):
            return None

        def upload_zip(self, *a, **kw):
            return None

    pub = OverOrgPublisher(api_key="x", _client=FakeClient())
    task = Task(
        task_id="t1",
        source_url="https://www.gov.il/he/collectors/foo",
        tracked_dataset_id="ds-1",
    )
    rows = [{"a": 1, "b": "א"}, {"a": 2, "b": "ב"}]
    result = TabularResult(
        rows=rows,
        fields=[{"name": "a", "type": ""}, {"name": "b", "type": ""}],
        attachments=[],
        source_url=task.source_url,
        collector_name="foo",
    )
    outcome = pub.publish(task, result, duration_s=12.345)
    assert outcome.success
    assert captured["tracked_dataset_id"] == "ds-1"
    assert captured["source_url"] == task.source_url
    assert captured["records"] == rows
    assert captured["fields"] == [{"name": "a", "type": ""}, {"name": "b", "type": ""}]
    assert captured["duration_seconds"] == 12.345


if __name__ == "__main__":
    # Minimal runner so this works without pytest installed.
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
