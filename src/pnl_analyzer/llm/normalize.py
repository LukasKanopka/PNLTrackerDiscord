from __future__ import annotations

import re


def _to_float(x) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def _norm_direction(x: str | None) -> str | None:
    if not x:
        return None
    v = str(x).strip().lower()
    if v in ("y", "yes", "true", "1", "long"):
        return "YES"
    if v in ("n", "no", "false", "0", "short"):
        return "NO"
    if v.upper() in ("YES", "NO"):
        return v.upper()
    return None


def _norm_platform(x: str | None) -> str | None:
    if not x:
        return None
    v = str(x).strip().lower()
    if "kalshi" in v:
        return "kalshi"
    if "polymarket" in v or v in ("poly", "pm"):
        return "polymarket"
    return v


_CENTS_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*c\b", re.IGNORECASE)
_PCT_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*%\b", re.IGNORECASE)


def _norm_price(x) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        v = float(x)
        if v > 1.0 and v <= 100.0:
            v = v / 100.0
        if 0.0 <= v <= 1.0:
            return v
        return None

    s = str(x).strip()
    if not s:
        return None

    m = _CENTS_RE.search(s)
    if m:
        v = _to_float(m.group("num"))
        if v is None:
            return None
        v = v / 100.0
        return v if 0.0 <= v <= 1.0 else None

    m = _PCT_RE.search(s)
    if m:
        v = _to_float(m.group("num"))
        if v is None:
            return None
        v = v / 100.0
        return v if 0.0 <= v <= 1.0 else None

    v = _to_float(s)
    if v is None:
        return None
    if v > 1.0 and v <= 100.0:
        v = v / 100.0
    return v if 0.0 <= v <= 1.0 else None


def normalize_bet_item(item: dict, messages: list[dict]) -> dict | None:
    """
    Tolerant normalization layer for LLM outputs.
    - Accepts cents/percent for quoted_price.
    - Accepts yes/no variants for position_direction.
    - Accepts platform variants.
    - Fills missing author/timestamp/market_intent from source_message_index when possible.
    """
    if not isinstance(item, dict):
        return None

    out = dict(item)

    # Key aliases
    if "position_direction" not in out:
        out["position_direction"] = out.get("direction") or out.get("side") or out.get("position")
    if "quoted_price" not in out:
        out["quoted_price"] = out.get("price") or out.get("entry_price") or out.get("entryPrice")
    if "market_intent" not in out:
        out["market_intent"] = out.get("market") or out.get("market_title") or out.get("question")

    out["position_direction"] = _norm_direction(out.get("position_direction")) or out.get("position_direction")
    out["platform"] = _norm_platform(out.get("platform")) or out.get("platform")

    qp = _norm_price(out.get("quoted_price"))
    if qp is not None:
        out["quoted_price"] = qp
    else:
        # Allow missing quoted_price; it can be filled from historical pricing.
        if out.get("quoted_price") is None:
            out["quoted_price"] = None

    # Bet size
    # Bet sizing (simplified): every bet uses the same sizing.
    # We intentionally ignore any model-provided per-bet sizing to keep runs consistent and avoid false positives.
    out["bet_size_units"] = 1.0

    # Fill provenance from source_message_index
    src_idx = out.get("source_message_index")
    if isinstance(src_idx, int) and 0 <= src_idx < len(messages):
        src = messages[src_idx]
        out.setdefault("author", src.get("author"))
        out.setdefault("timestamp_utc", src.get("timestamp_utc"))
        mi = out.get("market_intent")
        if not isinstance(mi, str) or not mi.strip():
            out["market_intent"] = src.get("text")

    # Basic sanity: must have these to be useful
    required = ("author", "timestamp_utc", "platform", "position_direction", "market_intent")
    for k in required:
        if out.get(k) is None or (isinstance(out.get(k), str) and not str(out[k]).strip()):
            return None

    return out
