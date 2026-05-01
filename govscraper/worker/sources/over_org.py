"""TaskSource for over.org.il.

Phase D facade: delegates the actual HTTP calls to the legacy
over_worker.OverWorkerClient so byte-identity with the live contract is
guaranteed. The shape exposed to Worker is the new TaskSource protocol.

Replacing the underlying client with a from-scratch implementation built on
worker.publishers._contract is a phase-G follow-up.
"""
from __future__ import annotations

from typing import Any

from ...types import Progress, Task
from ..publishers.base import ResultPublisher
from .base import TaskSource


def _task_from_raw(raw: dict[str, Any]) -> Task:
    """Convert an over.org.il task dict into our generic Task."""
    return Task(
        task_id=str(raw.get("task_id", "")),
        source_url=str(raw.get("source_url", "")),
        scraper_id=None,
        tracked_dataset_id=str(raw.get("tracked_dataset_id", "")) or None,
        config=dict(raw.get("scraper_config") or {}),
        raw=raw,
    )


class OverOrgSource:
    name = "over.org.il"

    def __init__(self, api_key: str, *, poll_interval: int = 30, publisher: ResultPublisher | None = None):
        # Lazy import keeps the legacy worker (and its requests/cloudscraper deps)
        # off the hot path for non-over deployments. Import the canonical path
        # directly rather than going through the over_worker.py shim.
        from govscraper.legacy.over_worker import OverWorkerClient
        self._client = OverWorkerClient(api_key, poll_interval=poll_interval)
        self._api_key = api_key
        self._publisher = publisher

    def poll(self) -> Task | None:
        raw = self._client.poll()
        if raw is None:
            return None
        return _task_from_raw(raw)

    def heartbeat(self, task: Task) -> None:
        # over_worker.execute_task runs its own heartbeat thread. Worker.runner
        # invokes this each beat; we forward as a progress ping with the last
        # known phase/current/total to keep the server's "task is alive" check
        # green even if no scrape progress fired since the last beat.
        self._client.report_progress(task.task_id, "heartbeat", 0, 0, "")

    def report_progress(self, task: Task, progress: Progress) -> None:
        self._client.report_progress(
            task.task_id,
            progress.phase or "scraping",
            progress.current,
            progress.total,
            progress.message,
        )

    def report_failure(self, task: Task, error: str, phase: str) -> None:
        self._client.report_failure(task.task_id, error, phase or "scraping")

    def default_publisher(self, task: Task) -> ResultPublisher:
        if self._publisher is None:
            from ..publishers.over_org import OverOrgPublisher
            self._publisher = OverOrgPublisher(self._api_key, _client=self._client)
        return self._publisher
