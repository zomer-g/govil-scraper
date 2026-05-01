"""Filesystem-safe filename helpers."""
from __future__ import annotations

import os
import re


def sanitize_filename(name: str, max_len: int = 200) -> str:
    """Strip OS-invalid characters and truncate to `max_len`.

    Aimed at Windows (the strictest of the platforms we run on). Falls back
    to "unnamed" if the result is empty.
    """
    if not name:
        return "unnamed"
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"_+", "_", name).strip("_. ")
    if len(name) > max_len:
        base, ext = os.path.splitext(name)
        name = base[: max_len - len(ext)] + ext
    return name or "unnamed"
