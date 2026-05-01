"""ResultPublisher for the nadlan parcel-queue. Sends scraped deals back via
/api/nadlan/bulk-result/<parcel_id>.
"""
from __future__ import annotations

from typing import Any

from ...scrapers.base import ScrapeResult, TabularResult
from ...types import Task
from .base import PublishOutcome, ResultPublisher


class NadlanQueuePublisher:
    name = "nadlan-queue"

    def __init__(self, *, client: Any | None = None,
                 server_url: str | None = None, worker_id: str | None = None,
                 api_key: str | None = None):
        if client is None:
            from nadlan_worker import NadlanWorkerClient
            if not (server_url and worker_id):
                raise ValueError(
                    "NadlanQueuePublisher needs an existing client= or "
                    "server_url+worker_id"
                )
            client = NadlanWorkerClient(server_url=server_url, worker_id=worker_id, api_key=api_key)
        self._client = client

    def publish(self, task: Task, result: ScrapeResult, *, duration_s: float) -> PublishOutcome:
        if not isinstance(result, TabularResult):
            raise TypeError(
                f"NadlanQueuePublisher expects TabularResult, got {type(result).__name__}"
            )
        out = self._client.upload_result(task.task_id, list(result.rows))
        return PublishOutcome(
            success=True,
            message=f"uploaded {len(result.rows)} deals",
            refs=dict(out or {}),
        )
