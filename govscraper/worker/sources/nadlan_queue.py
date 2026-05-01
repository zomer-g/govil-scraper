"""TaskSource that claims parcel tasks from /api/nadlan/bulk-claim.

Phase D facade: delegates to nadlan_worker.NadlanWorkerClient. Each
"parcel" is a Task whose source_url is constructed from gush/chelka so the
unified Worker can dispatch it through the nadlan scraper registry entry.
"""
from __future__ import annotations

from typing import Any

from ...types import Progress, Task
from ..publishers.base import ResultPublisher
from .base import TaskSource


def _task_from_parcel(parcel: dict[str, Any]) -> Task:
    gush = str(parcel.get("gush") or "").strip()
    chelka = str(parcel.get("chelka") or "").strip()
    parcel_id = parcel.get("parcel_id") or f"{gush}-{chelka}"
    url = (
        f"https://www.nadlan.gov.il/?view=kparcel_all&id={gush}-{chelka}&page=deals"
    )
    return Task(
        task_id=str(parcel_id),
        source_url=url,
        scraper_id="nadlan",
        config={"parcel": parcel},
        raw=parcel,
    )


class NadlanQueueSource:
    name = "nadlan-queue"

    def __init__(
        self,
        server_url: str,
        worker_id: str,
        *,
        api_key: str | None = None,
        publisher: ResultPublisher | None = None,
    ):
        from nadlan_worker import NadlanWorkerClient
        self._client = NadlanWorkerClient(
            server_url=server_url,
            worker_id=worker_id,
            api_key=api_key,
        )
        self._publisher = publisher

    def poll(self) -> Task | None:
        tasks = self._client.claim(count=1)
        if not tasks:
            return None
        return _task_from_parcel(tasks[0])

    def heartbeat(self, task: Task) -> None:
        # nadlan queue has no heartbeat endpoint — task lease is short and
        # claim()/upload_result() bracket the work themselves.
        pass

    def report_progress(self, task: Task, progress: Progress) -> None:
        # No per-parcel progress endpoint on the bulk queue. Drop silently.
        pass

    def report_failure(self, task: Task, error: str, phase: str) -> None:
        transient = "timeout" in (error or "").lower() or "5xx" in (error or "").lower()
        self._client.report_failure(task.task_id, error, transient=transient)

    def default_publisher(self, task: Task) -> ResultPublisher:
        if self._publisher is None:
            from ..publishers.nadlan_queue import NadlanQueuePublisher
            self._publisher = NadlanQueuePublisher(client=self._client)
        return self._publisher
