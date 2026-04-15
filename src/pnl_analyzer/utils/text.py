from __future__ import annotations

import re
from rapidfuzz.fuzz import token_set_ratio


def normalize_query(text: str) -> str:
    t = text.lower()
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def fuzzy_score(a: str, b: str) -> float:
    aa = normalize_query(a)
    bb = normalize_query(b)
    if not aa or not bb:
        return 0.0
    return float(token_set_ratio(aa, bb)) / 100.0
