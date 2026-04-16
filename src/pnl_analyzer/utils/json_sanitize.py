from __future__ import annotations

import math
from typing import Any


def sanitize_for_json(value: Any) -> Any:
    """
    Postgres JSON does not allow NaN/Infinity. Recursively replaces non-finite floats with None.
    Also converts unknown objects to strings (best-effort).
    """
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, list):
        return [sanitize_for_json(v) for v in value]
    if isinstance(value, tuple):
        return [sanitize_for_json(v) for v in value]
    if isinstance(value, dict):
        return {str(k): sanitize_for_json(v) for k, v in value.items()}
    # Fallback: keep it representable.
    return str(value)

