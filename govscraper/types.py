"""Cross-cutting types shared between scrapers, workers, publishers, and storage.

Kept dependency-free so any subpackage can import without cycles.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal


ScraperId = Literal["govil", "datagovil", "nadlan", "govmap"]


@dataclass
class ParsedURL:
    scraper_id: str
    canonical_url: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class FileAttachment:
    local_path: Path
    original_filename: str
    source_url: str
    size_bytes: int = 0
    sha256: str | None = None
    mime_type: str | None = None


@dataclass
class Progress:
    phase: str
    current: int = 0
    total: int = 0
    message: str = ""

    @property
    def percentage(self) -> int:
        if self.total <= 0:
            return 0
        return int(round(100 * self.current / self.total))


ProgressFn = Callable[[Progress], None]


@dataclass
class Checkpoint:
    cursor: str | None = None
    completed_count: int = 0
    state: dict[str, Any] = field(default_factory=dict)


@dataclass
class Task:
    """A unit of work pulled from a TaskSource.

    `task_id` is the source's identifier (over.org.il task id, internal queue
    row id, or nadlan parcel id). `tracked_dataset_id` is over.org.il-specific
    and is None for other sources. `scraper_id` is filled by the source after
    URL inspection so the worker can dispatch without re-parsing.
    """
    task_id: str
    source_url: str
    scraper_id: str | None = None
    tracked_dataset_id: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)
