"""Abstract base for all four target-site scrapers."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Iterator, Literal, Union

from ..types import Checkpoint, FileAttachment, ParsedURL, ProgressFn


@dataclass
class TabularResult:
    """Row-level results — shape used by gov.il, nadlan, and CKAN-flattened CKAN resources."""
    rows: list[dict[str, Any]] = field(default_factory=list)
    fields: list[dict[str, str]] = field(default_factory=list)  # [{"name": ..., "type": ...}]
    attachments: list[FileAttachment] = field(default_factory=list)
    source_url: str = ""
    collector_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    warning: str | None = None
    kind: Literal["tabular"] = "tabular"


@dataclass
class CkanCatalogResult:
    """data.gov.il (CKAN) — dataset/resource catalogue plus optional inline rows."""
    datasets: list[dict[str, Any]] = field(default_factory=list)
    resources: list[dict[str, Any]] = field(default_factory=list)
    rows_by_resource: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    fields_by_resource: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    source_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    kind: Literal["ckan_catalog"] = "ckan_catalog"


@dataclass
class GeoFeatureResult:
    """govmap WFS — geometry features in WGS84 with optional ITM mirror."""
    features: list[dict[str, Any]] = field(default_factory=list)
    itm_features: list[dict[str, Any]] | None = None
    crs_wgs84: bool = True
    layer_id: str = ""
    layer_label: str = ""
    bbox_wgs84: tuple[float, float, float, float] | None = None
    bbox_itm: tuple[float, float, float, float] | None = None
    geometry_type: str = ""
    feature_count: int = 0
    attachments: list[FileAttachment] = field(default_factory=list)
    source_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    warning: str | None = None
    kind: Literal["geo"] = "geo"


ScrapeResult = Union[TabularResult, CkanCatalogResult, GeoFeatureResult]


class BaseScraper(ABC):
    """All target-site scrapers implement this interface.

    `id` is the registry key (e.g. "govil"). `parse_url` returns a ParsedURL
    when this scraper handles the URL, else None — the registry uses this for
    dispatch. `fetch` is the synchronous, single-shot scrape. Override
    `fetch_incremental` for sources that support resumable runs (settlements,
    archives); the default just yields the full fetch once.
    """
    id: ClassVar[str] = ""

    @classmethod
    @abstractmethod
    def parse_url(cls, url: str) -> ParsedURL | None:
        """Return a ParsedURL if this scraper handles `url`, else None."""

    @abstractmethod
    def fetch(self, parsed: ParsedURL, *, progress: ProgressFn) -> ScrapeResult:
        """Single-shot scrape. Must call `progress` at least once when complete."""

    def fetch_incremental(
        self,
        parsed: ParsedURL,
        checkpoint: Checkpoint | None,
        *,
        progress: ProgressFn,
    ) -> Iterator[tuple[ScrapeResult, Checkpoint]]:
        """Default: degenerate to a single fetch with an empty checkpoint."""
        yield self.fetch(parsed, progress=progress), Checkpoint()
