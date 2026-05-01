"""TaskSource protocol — the seam between Worker.run() and a queue."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from ...types import Progress, Task

if TYPE_CHECKING:
    from ..publishers.base import ResultPublisher


class TaskSource(Protocol):
    name: str

    def poll(self) -> Task | None:
        """Fetch one task or return None when idle. Should not block long."""

    def heartbeat(self, task: Task) -> None:
        """Called every ~30s while a task is in progress. May be a no-op."""

    def report_progress(self, task: Task, progress: Progress) -> None: ...

    def report_failure(self, task: Task, error: str, phase: str) -> None: ...

    def default_publisher(self, task: Task) -> "ResultPublisher":
        """Most sources have a single canonical publisher; the Worker uses
        this unless a per-task override is supplied."""
