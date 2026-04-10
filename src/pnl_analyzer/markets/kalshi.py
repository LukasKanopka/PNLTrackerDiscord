from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from pnl_analyzer.config import settings
from pnl_analyzer.markets.base import MarketClient, MarketMatch, PricePoint, VerifiedMarket
from pnl_analyzer.utils.retry import UpstreamHTTPError, with_retries
from pnl_analyzer.utils.text import fuzzy_score
from pnl_analyzer.utils.time import to_unix_seconds


def _load_private_key(pem: str) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("KALSHI_PRIVATE_KEY_PEM is not an RSA private key")
    return key


def _sign_pss(private_key: rsa.RSAPrivateKey, text: str) -> str:
    sig = private_key.sign(
        text.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


class KalshiClient(MarketClient):
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(base_url=settings.kalshi_base_url, timeout=20.0)
        self._prefix = settings.kalshi_api_prefix.rstrip("/")
        self._key_id = settings.kalshi_key_id

        pem = settings.kalshi_private_key_pem
        if self._key_id and not pem and settings.kalshi_private_key_path:
            key_path = Path(settings.kalshi_private_key_path)
            if key_path.exists():
                pem = key_path.read_text(encoding="utf-8")
        self._priv = _load_private_key(pem) if pem else None

    def _signed_headers(self, method: str, path_without_query: str) -> dict[str, str]:
        if not (self._key_id and self._priv):
            return {}
        ts_ms = str(int(time.time() * 1000))
        msg = ts_ms + method.upper() + path_without_query
        sig = _sign_pss(self._priv, msg)
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig,
        }

    async def _get(self, path: str, params: dict | None = None) -> dict:
        path_no_q = path.split("?")[0]
        full_path = f"{self._prefix}{path_no_q}" if not path_no_q.startswith(self._prefix) else path_no_q

        async def _do():
            headers = self._signed_headers("GET", full_path)
            r = await self._client.get(full_path, params=params, headers=headers)
            if r.status_code in (429, 500, 502, 503, 504):
                raise UpstreamHTTPError(r.status_code, f"Kalshi {full_path} retryable: {r.text}")
            if r.status_code >= 400:
                raise UpstreamHTTPError(r.status_code, f"Kalshi {full_path} failed: {r.text}")
            return r.json()

        return await with_retries(_do)

    async def match_market(self, intent: str, ts_utc: str) -> MarketMatch | None:
        # Kalshi doesn't expose a text search in v2 market listing; filter by close window, then fuzzy match.
        target = to_unix_seconds(ts_utc)
        min_close = max(0, target - 60 * 60 * 24 * 14)
        max_close = target + 60 * 60 * 24 * 60

        data = await self._get("/markets", params={"limit": 200, "min_close_ts": min_close, "max_close_ts": max_close})
        markets = data.get("markets") or []
        best: MarketMatch | None = None
        for m in markets:
            title = m.get("title") or m.get("subtitle") or m.get("ticker") or ""
            score = fuzzy_score(intent, title)
            if best is None or score > best.confidence:
                best = MarketMatch(market_id=m.get("ticker", ""), market_title=title, confidence=score)
        if best and best.market_id and best.confidence >= 0.35:
            return best
        return None

    async def get_verified_market(self, market_id: str) -> VerifiedMarket:
        data = await self._get(f"/markets/{market_id}")
        m = data.get("market") or data
        status = (m.get("status") or "").lower()
        resolved = status in ("settled", "determined")
        result = m.get("result") or m.get("determination") or None
        resolved_outcome = None
        if isinstance(result, str):
            if result.upper() in ("YES", "NO"):
                resolved_outcome = result.upper()
        return VerifiedMarket(
            market_id=market_id,
            market_title=m.get("title") or m.get("subtitle") or market_id,
            resolved=resolved,
            resolved_outcome=resolved_outcome,
            resolution_ts_utc=None,
        )

    async def get_price_near(self, market_id: str, side: str, ts_utc: str) -> PricePoint | None:
        # Use candlesticks around timestamp; requires series ticker, so first fetch market.
        data = await self._get(f"/markets/{market_id}")
        m = data.get("market") or data
        series_ticker = m.get("series_ticker") or m.get("seriesTicker") or m.get("series") or None
        if not series_ticker:
            return None
        target = to_unix_seconds(ts_utc)
        start_ts = max(0, target - 3600)
        end_ts = target + 3600

        cs = await self._get(
            f"/series/{series_ticker}/markets/{market_id}/candlesticks",
            params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": "1m"},
        )
        candles = cs.get("candlesticks") or cs.get("candles") or []
        if not candles:
            return None
        # Best-effort: use the close price of the last candle at/before target for requested side.
        best = None
        for c in candles:
            t = int(c.get("ts") or c.get("start_ts") or 0)
            if t <= target and (best is None or t > best[0]):
                best = (t, c)
        if best is None:
            best = (int(candles[0].get("ts") or start_ts), candles[0])
        candle = best[1]
        # Candles often have yes bid/ask fields; fall back to mid/close.
        price = candle.get("close") or candle.get("yes_close") or candle.get("yesClose") or None
        if price is None:
            return None
        try:
            p = float(price)
        except Exception:
            return None
        if p > 1.0:
            # Kalshi often uses cents (0-100)
            p = p / 100.0
        if side.upper() == "NO":
            p = 1.0 - p
        return PricePoint(ts_utc=ts_utc, price=p, source="kalshi_candlesticks_1m")
