"""TaskSource that polls our own Flask server (the same /api/worker/poll
endpoint exposed in app.py). Wraps the legacy worker.WorkerClient so the
existing X-Worker-Key auth and progress/complete/fail/heartbeat flow keeps
working unchanged.
"""
from __future__ import annotations

from typing import Any

from ...types import Progress, Task
from ..publishers.base import ResultPublisher
from .base import TaskSource


def _task_from_raw(raw: dict[str, Any]) -> Task:
    return Task(
        task_id=str(raw.get("task_id") or raw.get("id") or ""),
        source_url=str(raw.get("url") or raw.get("source_url") or ""),
        scraper_id=None,
        config=dict(raw.get("config") or raw.get("scraper_config") or {}),
        raw=raw,
    )


class LocalServerSource:
    name = "local-server"

    def __init__(
        self,
        server_url: str,
        api_key: str,
        worker_id: str,
        *,
        poll_interval: int = 10,
        publisher: ResultPublisher | None = None,
    ):
        from govscraper.legacy.worker_client import WorkerClient
        self._client = WorkerClient(
            server_url=server_url,
            api_key=api_key,
            worker_id=worker_id,
            poll_interval=poll_interval,
        )
        self._publisher = publisher

    def poll(self) -> Task | None:
        raw = self._client.poll()
        return _task_from_raw(raw) if raw else None

    def heartbeat(self, task: Task) -> None:
        try:
            self._client.heartbeat()
        except Exception:
            pass

    def report_progress(self, task: Task, progress: Progress) -> None:
        self._client.report_progress(
            task.task_id,
            progress.phase or "scraping",
            progress.current,
            progress.total,
            progress.message,
        )

    def report_failure(self, task: Task, error: str, phase: str) -> None:
        try:
            self._client.report_failure(task.task_id, error)
        except AttributeError:
            # Older worker.py revisions called this method differently
            self._client._session.post(  # type: ignore[attr-defined]
                self._client._url(f"/api/worker/fail/{task.task_id}"),
                json={"error": error, "phase": phase},
                timeout=15,
            )

    def default_publisher(self, task: Task) -> ResultPublisher:
        if self._publisher is None:
            from ..publishers.local_collections import LocalCollectionsPublisher
            self._publisher = LocalCollectionsPublisher(client=self._client)
        return self._publisher
