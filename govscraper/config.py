"""Centralised env-var access and defaults.

Phase A: skeleton only. Old modules continue reading os.environ directly until
phase F migrates them through here.
"""
from __future__ import annotations

import os
from pathlib import Path


def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    return val if val not in (None, "") else default


def work_dir() -> Path:
    raw = _env("TEMP_DIR") or _env("WORK_DIR") or "/tmp/govil_scraper"
    p = Path(raw)
    p.mkdir(parents=True, exist_ok=True)
    return p


def admin_emails() -> list[str]:
    raw = _env("ADMIN_EMAILS") or ""
    return [e.strip().lower() for e in raw.split(",") if e.strip()]


def worker_api_key() -> str | None:
    return _env("WORKER_API_KEY")


def over_api_key() -> str | None:
    return _env("OVER_API_KEY")


def disable_playwright() -> bool:
    return (_env("DISABLE_PLAYWRIGHT") or "").strip() in ("1", "true", "yes")


def govmap_layers_file() -> Path | None:
    raw = _env("GOVMAP_LAYERS_FILE")
    return Path(raw) if raw else None
