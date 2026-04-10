from __future__ import annotations

from dataclasses import dataclass

import httpx

from pnl_analyzer.config import settings
from pnl_analyzer.markets.base import MarketClient, MarketMatch, PricePoint, VerifiedMarket
from pnl_analyzer.utils.retry import UpstreamHTTPError, with_retries
from pnl_analyzer.utils.text import fuzzy_score
from pnl_analyzer.utils.time import to_unix_seconds


@dataclass
class _GammaMarket:
    id: str
    question: str
    outcomes: list[str]
    outcome_prices: list[float]
    clob_token_ids: list[str] | None
    resolved: bool
    winning_outcome: str | None
    end_date_iso: str | None


class PolymarketClient(MarketClient):
    def __init__(self) -> None:
        self._gamma = httpx.AsyncClient(base_url=settings.polymarket_gamma_base_url, timeout=20.0)
        self._clob = httpx.AsyncClient(base_url=settings.polymarket_clob_base_url, timeout=20.0)

    async def _gamma_get(self, path: str, params: dict | None = None) -> dict | list:
        async def _do():
            r = await self._gamma.get(path, params=params)
            if r.status_code in (429, 500, 502, 503, 504):
                raise UpstreamHTTPError(r.status_code, f"Polymarket gamma {path} retryable: {r.text}")
            if r.status_code >= 400:
                raise UpstreamHTTPError(r.status_code, f"Polymarket gamma {path} failed: {r.text}")
            return r.json()

        return await with_retries(_do)

    async def _clob_get(self, path: str, params: dict | None = None) -> dict | list:
        async def _do():
            r = await self._clob.get(path, params=params)
            if r.status_code in (429, 500, 502, 503, 504):
                raise UpstreamHTTPError(r.status_code, f"Polymarket clob {path} retryable: {r.text}")
            if r.status_code >= 400:
                raise UpstreamHTTPError(r.status_code, f"Polymarket clob {path} failed: {r.text}")
            return r.json()

        return await with_retries(_do)

    async def match_market(self, intent: str, ts_utc: str) -> MarketMatch | None:
        # Use public search to get candidates. Gamma supports /public-search across markets.
        q = intent.strip()
        if not q:
            return None

        # Gamma public-search expects `q` (some docs/examples use `query`, so keep a fallback).
        try:
            data = await self._gamma_get("/public-search", params={"q": q, "limit": 25})
        except UpstreamHTTPError as e:
            if e.status_code in (400, 422):
                data = await self._gamma_get("/public-search", params={"query": q, "limit": 25})
            else:
                raise
        markets = data.get("markets") if isinstance(data, dict) else None
        if not markets:
            # fallback: list markets with a generic search param (best-effort)
            data = await self._gamma_get("/markets", params={"limit": 25, "search": q})
            markets = data if isinstance(data, list) else data.get("markets")
        if not markets:
            return None

        best: MarketMatch | None = None
        for m in markets:
            question = m.get("question") or m.get("title") or ""
            score = fuzzy_score(intent, question)
            if best is None or score > best.confidence:
                # Prefer YES token id if present (outcomes order is typically ["Yes","No"])
                clob_token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
                yes_token = None
                if isinstance(clob_token_ids, list) and clob_token_ids:
                    yes_token = clob_token_ids[0]
                best = MarketMatch(
                    market_id=str(m.get("id")),
                    market_title=question,
                    confidence=score,
                    side_token_id=yes_token,
                )
        if best and best.confidence >= 0.35:
            return best
        return None

    async def get_verified_market(self, market_id: str) -> VerifiedMarket:
        m = await self._gamma_get(f"/markets/{market_id}")
        resolved = bool(m.get("resolved"))
        winning_outcome = m.get("winningOutcome") or m.get("winning_outcome")
        end_date = m.get("endDate") or m.get("end_date")
        resolved_outcome = None
        if resolved and isinstance(winning_outcome, str):
            if winning_outcome.lower() in ("yes", "true"):
                resolved_outcome = "YES"
            elif winning_outcome.lower() in ("no", "false"):
                resolved_outcome = "NO"
        return VerifiedMarket(
            market_id=str(m.get("id")),
            market_title=m.get("question") or m.get("title") or "",
            resolved=resolved,
            resolved_outcome=resolved_outcome,
            resolution_ts_utc=end_date,
        )

    async def get_price_near(self, market_id: str, side: str, ts_utc: str) -> PricePoint | None:
        # Need token id for the specific side; fetch market and map outcomes -> clobTokenIds.
        m = await self._gamma_get(f"/markets/{market_id}")
        outcomes_raw = m.get("outcomes")
        token_ids_raw = m.get("clobTokenIds")
        if not (isinstance(outcomes_raw, str) and isinstance(token_ids_raw, str)):
            return None

        # gamma stores these as JSON-encoded strings
        import json

        outcomes = json.loads(outcomes_raw)
        token_ids = json.loads(token_ids_raw)
        if not (isinstance(outcomes, list) and isinstance(token_ids, list) and len(outcomes) == len(token_ids)):
            return None

        want = "yes" if side.upper() == "YES" else "no"
        token_id = None
        for o, tid in zip(outcomes, token_ids):
            if isinstance(o, str) and o.lower() == want:
                token_id = tid
                break
        if not token_id:
            return None

        target = to_unix_seconds(ts_utc)
        start_ts = max(0, target - 3600)
        end_ts = target + 3600

        hist = await self._clob_get(
            "/prices-history",
            params={"market": token_id, "startTs": start_ts, "endTs": end_ts, "interval": "1m"},
        )
        points = hist.get("history") if isinstance(hist, dict) else None
        if not points:
            return None
        # Choose last point at or before target
        best = None
        for p in points:
            t = int(p.get("t", 0))
            price = float(p.get("p", 0.0))
            if t <= target and (best is None or t > best[0]):
                best = (t, price)
        if best is None:
            t = int(points[0].get("t", target))
            price = float(points[0].get("p", 0.0))
            best = (t, price)
        return PricePoint(ts_utc=ts_utc, price=float(best[1]), source="polymarket_clob_prices_history_1m")
