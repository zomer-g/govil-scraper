"""URL → scraper dispatch.

Scrapers self-register at import time via `register(cls)`. `dispatch(url)`
walks registered classes calling `parse_url` and returns the first hit.
"""
from __future__ import annotations

from typing import Type

from ..types import ParsedURL
from .base import BaseScraper


_REGISTRY: list[Type[BaseScraper]] = []


def register(scraper_cls: Type[BaseScraper]) -> Type[BaseScraper]:
    if not scraper_cls.id:
        raise ValueError(f"{scraper_cls.__name__} missing class-level `id`")
    if scraper_cls not in _REGISTRY:
        _REGISTRY.append(scraper_cls)
    return scraper_cls


def all_scrapers() -> list[Type[BaseScraper]]:
    return list(_REGISTRY)


def get_by_id(scraper_id: str) -> Type[BaseScraper]:
    for cls in _REGISTRY:
        if cls.id == scraper_id:
            return cls
    raise KeyError(f"No scraper registered with id={scraper_id!r}")


def dispatch(url: str) -> tuple[Type[BaseScraper], ParsedURL] | None:
    """Find the scraper that handles `url`. Returns (cls, parsed) or None."""
    for cls in _REGISTRY:
        parsed = cls.parse_url(url)
        if parsed is not None:
            return cls, parsed
    return None
