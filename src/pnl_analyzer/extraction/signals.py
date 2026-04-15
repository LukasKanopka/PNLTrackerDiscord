from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse, unquote


_URL_RE = re.compile(r"https?://[^\s<>\"]+")
_PROXIED_HTTPS_RE = re.compile(r"(https://(?:kalshi\.com|polymarket\.com)/[^\s<>\"]+)")

_PLATFORM_RE = re.compile(r"\b(kalshi|polymarket|poly)\b", re.IGNORECASE)
_SIDE_RE = re.compile(r"\b(yes|no)\b", re.IGNORECASE)

_ACTION_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("BUY", re.compile(r"\b(buy|buying|bought|entry|in at|entered|opening)\b", re.IGNORECASE)),
    ("SELL", re.compile(r"\b(sell|selling|sold|exit|out at)\b", re.IGNORECASE)),
    ("ADD", re.compile(r"\b(adding|add|loaded|building a position|build(?:ing)? a position)\b", re.IGNORECASE)),
    ("TRIM", re.compile(r"\b(trim|trimmed|cut|cutting|reduce|reduced)\b", re.IGNORECASE)),
]

_CENTS_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*c\b", re.IGNORECASE)
_PCT_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*%\b", re.IGNORECASE)
_AT_RE = re.compile(r"@\s*(?P<num>\d+(?:\.\d+)?)\b", re.IGNORECASE)
_DECIMAL_RE = re.compile(r"\b0\.\d{1,3}\b")

_YES_BLOCK_RE = re.compile(r"\byes\s*:\s*(?P<val>\d+(?:\.\d+)?)\s*c\b", re.IGNORECASE)
_NO_BLOCK_RE = re.compile(r"\bno\s*:\s*(?P<val>\d+(?:\.\d+)?)\s*c\b", re.IGNORECASE)
_MY_BET_RE = re.compile(r"\bmy bet\s*:\s*(?P<side>yes|no)\b", re.IGNORECASE)

_DEICTIC_RE = re.compile(r"\b(this|above|here|that|it|the above)\b", re.IGNORECASE)

_USD_RE = re.compile(r"\$\s*(?P<num>\d[\d,]*(?:\.\d+)?)")

# Kalshi market tickers are typically uppercase alphanumerics with one-or-more '-' segments
# (e.g. KXMLBGAME-26APR091335ATHNYY, KXSERIEAGAME-26MAR08ACMINT-INT).
_KALSHI_TICKER_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9]{1,30}(?:-[A-Za-z0-9]{2,40})+)\b")


def _to_float(s: str) -> float | None:
    try:
        return float(s)
    except Exception:
        return None


def norm_price_to_prob(x: str | float | int | None) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        v = float(x)
        if v > 1.0 and v <= 100.0:
            v = v / 100.0
        return v if 0.0 <= v <= 1.0 else None

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

    m = _AT_RE.search(s)
    if m:
        v = _to_float(m.group("num"))
        if v is None:
            return None
        if v > 1.0 and v <= 100.0:
            v = v / 100.0
        return v if 0.0 <= v <= 1.0 else None

    m = _DECIMAL_RE.search(s)
    if m:
        # Avoid recursion on values like "0.31" which would re-match _DECIMAL_RE indefinitely.
        v = _to_float(m.group(0))
        return v if v is not None and 0.0 <= v <= 1.0 else None

    v = _to_float(s)
    if v is None:
        return None
    if v > 1.0 and v <= 100.0:
        v = v / 100.0
    return v if 0.0 <= v <= 1.0 else None


def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    urls = list(_URL_RE.findall(text))
    # Some Discord embeds proxy the target URL inside a CDN URL (e.g. ".../https/kalshi.com/api-app/preview/...").
    for u in list(urls):
        u2 = unquote(u)
        for m in _PROXIED_HTTPS_RE.findall(u2):
            urls.append(m)
    # Trim common trailing punctuation
    cleaned: list[str] = []
    for u in urls:
        u = u.rstrip(").,;")
        if u not in cleaned:
            cleaned.append(u)
    return cleaned


def parse_market_ref(url: str) -> dict | None:
    try:
        p = urlparse(url)
    except Exception:
        return None
    host = (p.netloc or "").lower()
    path = p.path or ""
    seg = [s for s in path.split("/") if s]

    if "polymarket.com" in host:
        if not seg:
            return None
        if seg[0] == "event" and len(seg) >= 2:
            event_slug = seg[1]
            market_slug = seg[2] if len(seg) >= 3 else None
            return {
                "platform": "polymarket",
                "url": url,
                "event_slug": event_slug,
                "market_slug": market_slug,
                "kind": "event_market" if market_slug else "event",
            }
        if seg[0] == "sports" and len(seg) >= 2:
            # Example: /sports/nba/games/week/3/nba-orl-sas-2026-02-01 (event slug is last segment)
            event_slug = seg[-1]
            return {
                "platform": "polymarket",
                "url": url,
                "event_slug": event_slug,
                "market_slug": None,
                "kind": "sports_event",
            }
        return None

    if "kalshi.com" in host:
        if not seg:
            return None
        # Direct preview embeds: /api-app/preview/<TICKER>
        if seg[:2] == ["api-app", "preview"] and len(seg) >= 3:
            ticker = seg[2].upper()
            return {"platform": "kalshi", "url": url, "ticker": ticker, "kind": "preview"}
        if seg[0] == "markets" and len(seg) >= 2:
            # Heuristic: last segment is sometimes the ticker (e.g. kxauctionpikachu-26).
            event_slug = seg[1]
            ticker = None
            page_slug = None
            if len(seg) >= 3:
                # Human-readable page slug(s) can help disambiguate series/event pages.
                # Example: /markets/kxmlbgame/professional-baseball-game/KXMLBGAME-...
                # Example: /markets/kxauctionpikachu/how-much-wil-.../
                page_slug = "/".join(seg[2:-1] if len(seg) >= 4 else seg[2:])
            for s in reversed(seg):
                if _KALSHI_TICKER_RE.fullmatch(s):
                    ticker = s.upper()
                    break
            return {
                "platform": "kalshi",
                "url": url,
                "event_slug": event_slug,
                "ticker": ticker,
                "page_slug": page_slug,
                "kind": "market" if ticker else "event",
            }
        return None

    return None


def extract_market_refs(text: str) -> list[dict]:
    refs: list[dict] = []
    for u in extract_urls(text or ""):
        mr = parse_market_ref(u)
        if mr is not None:
            refs.append(mr)
    return refs


def detect_platform(text: str) -> str | None:
    if not text:
        return None
    m = _PLATFORM_RE.search(text)
    if not m:
        return None
    v = m.group(1).lower()
    if v == "poly":
        return "polymarket"
    return v


def detect_side(text: str) -> str | None:
    if not text:
        return None
    m = _MY_BET_RE.search(text)
    if m:
        return "YES" if m.group("side").lower() == "yes" else "NO"
    m = re.search(r"\b(buying|bought|buy|loaded|adding|add|sell|sold|trim|cut)\b\s+(?P<side>yes|no)\b", text, re.IGNORECASE)
    if m:
        return "YES" if m.group("side").lower() == "yes" else "NO"
    m = _SIDE_RE.search(text)
    if m:
        return "YES" if m.group(1).lower() == "yes" else "NO"
    return None


def detect_action(text: str) -> str | None:
    if not text:
        return None
    for act, pat in _ACTION_PATTERNS:
        if pat.search(text):
            return act
    return None


@dataclass(frozen=True)
class OddsBlock:
    yes_price: float | None
    no_price: float | None


def extract_odds_block(text: str) -> OddsBlock | None:
    if not text:
        return None
    y = None
    n = None
    my = _YES_BLOCK_RE.search(text)
    if my:
        y = norm_price_to_prob(my.group("val") + "c")
    mn = _NO_BLOCK_RE.search(text)
    if mn:
        n = norm_price_to_prob(mn.group("val") + "c")
    if y is None and n is None:
        return None
    return OddsBlock(yes_price=y, no_price=n)


def extract_inline_price(text: str) -> float | None:
    if not text:
        return None
    # Prefer cents or percent explicit
    for pat in (_CENTS_RE, _PCT_RE, _AT_RE, _DECIMAL_RE):
        m = pat.search(text)
        if m:
            if "num" in m.groupdict():
                return norm_price_to_prob(m.group("num") + ("c" if pat is _CENTS_RE else "%" if pat is _PCT_RE else ""))
            return norm_price_to_prob(m.group(0))
    return None


def detect_deictic(text: str) -> bool:
    if not text:
        return False
    return bool(_DEICTIC_RE.search(text))


def extract_size_usd(text: str) -> float | None:
    if not text:
        return None
    m = _USD_RE.search(text)
    if not m:
        return None
    raw = m.group("num").replace(",", "")
    return _to_float(raw)
