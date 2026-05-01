"""ResultPublisher for our own server — uploads scrape ZIPs through
/api/collections/upload (the existing flow used by worker.py + local_scrape.py).

Phase D facade: delegates the ZIP packaging + multipart upload to the
legacy WorkerClient methods so behaviour is unchanged.
"""
from __future__ import annotations

from typing import Any

from ...scrapers.base import GeoFeatureResult, ScrapeResult, TabularResult
from ...types import Task
from .base import PublishOutcome, ResultPublisher


class LocalCollectionsPublisher:
    name = "local-collections"

    def __init__(self, *, client: Any | None = None, server_url: str | None = None,
                 api_key: str | None = None, worker_id: str | None = None):
        if client is None:
            from govscraper.legacy.worker_client import WorkerClient
            if not (server_url and api_key and worker_id):
                raise ValueError(
                    "LocalCollectionsPublisher needs either an existing client= or "
                    "server_url+api_key+worker_id"
                )
            client = WorkerClient(server_url=server_url, api_key=api_key, worker_id=worker_id)
        self._client = client

    def publish(self, task: Task, result: ScrapeResult, *, duration_s: float) -> PublishOutcome:
        # The legacy WorkerClient.execute_task does scrape+package+upload in
        # one shot, while our protocol gets here with a finished ScrapeResult.
        # Phase D delivers the seam; the actual packaging/upload split moves
        # into io/zip_packager + a thin client call in phase G.
        if not isinstance(result, (TabularResult, GeoFeatureResult)):
            raise NotImplementedError(
                f"LocalCollectionsPublisher does not yet handle {type(result).__name__}"
            )
        # For now, raise — Worker.run uses this only in tests until phase G
        # rewires app.py's mode=auto path through Worker.run_one().
        raise NotImplementedError(
            "LocalCollectionsPublisher.publish() is a phase-D stub. The legacy "
            "worker.WorkerClient.execute_task() flow remains the live path; phase "
            "G will move ZIP packaging into io/zip_packager and route here."
        )
