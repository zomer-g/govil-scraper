"""ResultPublisher protocol — the seam between Worker.run() and result delivery."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from ...scrapers.base import ScrapeResult
from ...types import Task


@dataclass
class PublishOutcome:
    success: bool
    message: str = ""
    refs: dict[str, Any] = field(default_factory=dict)  # publisher-specific (resource_ids etc.)


class ResultPublisher(Protocol):
    name: str

    def publish(
        self,
        task: Task,
        result: ScrapeResult,
        *,
        duration_s: float,
    ) -> PublishOutcome: ...
