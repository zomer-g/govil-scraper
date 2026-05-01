"""Worker — single main loop shared by every TaskSource.

Phase A: stub only; real implementation lands in phase D after the publisher
contract is captured. Existing worker.py / over_worker.py / nadlan_worker.py
remain authoritative until then.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Callable

from ..scrapers.base import ScrapeResult
from ..scrapers.registry import dispatch
from ..types import Progress, Task
from .publishers.base import PublishOutcome, ResultPublisher
from .sources.base import TaskSource


PublisherFor = Callable[[Task], ResultPublisher]


class _ThrottledProgress:
    """Forwards progress updates to the source no more than once per `min_interval` seconds."""
    def __init__(self, source: TaskSource, task: Task, *, min_interval: float = 5.0):
        self._source = source
        self._task = task
        self._min = min_interval
        self._last_at = 0.0
        self.last_phase = ""

    def __call__(self, progress: Progress) -> None:
        self.last_phase = progress.phase or self.last_phase
        now = time.monotonic()
        if now - self._last_at < self._min:
            return
        self._last_at = now
        try:
            self._source.report_progress(self._task, progress)
        except Exception:
            pass  # progress is best-effort


class _HeartbeatThread:
    """Calls source.heartbeat(task) every `every` seconds while in scope."""
    def __init__(self, source: TaskSource, task: Task, *, every: float = 30.0):
        self._source = source
        self._task = task
        self._every = every
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_HeartbeatThread":
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _loop(self) -> None:
        while not self._stop.wait(self._every):
            try:
                self._source.heartbeat(self._task)
            except Exception:
                pass


class Worker:
    def __init__(
        self,
        source: TaskSource,
        registry_dispatch: Callable[[str], object] = dispatch,
        publisher_for: PublisherFor | None = None,
        *,
        work_dir: Path,
        poll_interval: float = 5.0,
    ):
        self.source = source
        self.publisher_for = publisher_for
        self.work_dir = Path(work_dir)
        self.poll_interval = poll_interval
        self._dispatch = registry_dispatch
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            task = self._safe_poll()
            if task is None:
                self._stop.wait(self.poll_interval)
                continue
            self.run_one(task)

    def run_one(self, task: Task) -> PublishOutcome:
        progress = _ThrottledProgress(self.source, task)
        started_at = time.monotonic()
        with _HeartbeatThread(self.source, task):
            try:
                result = self._do_scrape(task, progress)
                publisher = self._publisher(task)
                outcome = publisher.publish(task, result, duration_s=time.monotonic() - started_at)
                return outcome
            except Exception as e:
                self.source.report_failure(task, str(e), phase=progress.last_phase or "scrape")
                return PublishOutcome(success=False, message=str(e))

    # internals

    def _safe_poll(self) -> Task | None:
        try:
            return self.source.poll()
        except Exception:
            return None

    def _do_scrape(self, task: Task, progress) -> ScrapeResult:
        hit = self._dispatch(task.source_url)
        if hit is None:
            raise ValueError(f"No scraper handles URL: {task.source_url}")
        scraper_cls, parsed = hit
        scraper = scraper_cls()  # type: ignore[call-arg]
        return scraper.fetch(parsed, progress=progress)

    def _publisher(self, task: Task) -> ResultPublisher:
        if self.publisher_for is not None:
            return self.publisher_for(task)
        return self.source.default_publisher(task)
