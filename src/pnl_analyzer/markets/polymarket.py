from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import httpx

from pnl_analyzer.config import settings
from pnl_analyzer.markets.base import MarketClient, MarketMatch, PricePoint, VerifiedMarket
from pnl_analyzer.utils.retry import UpstreamHTTPError, with_retries
from pnl_analyzer.utils.text import fuzzy_score
from pnl_analyzer.utils.time import parse_utc, to_unix_seconds


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
        self._token_cache: dict[tuple[str, str], str] = {}  # (market_id, side) -> token_id

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
        # Use public search to get candidates. Gamma supports /public-search across events/markets.
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
        # The /public-search response is typically {"events":[{..., "markets":[...]}]}
        events = data.get("events") if isinstance(data, dict) else None
        markets: list[dict] = []
        if isinstance(events, list):
            for ev in events:
                ms = ev.get("markets")
                if isinstance(ms, list):
                    markets.extend(ms)
        if not markets:
            # Fallback: list markets with a generic search param (best-effort)
            data2 = await self._gamma_get("/markets", params={"limit": 25, "search": q})
            markets = data2 if isinstance(data2, list) else data2.get("markets") or []
        if not markets:
            return None

        target_dt = parse_utc(ts_utc)
        best: MarketMatch | None = None
        for m in markets:
            question = m.get("question") or m.get("title") or ""
            score = fuzzy_score(intent, question)
            # Timestamp-aware: prefer markets active around call time (small grace window).
            start = m.get("startDate") or m.get("start_date")
            end = m.get("endDate") or m.get("end_date")
            active_boost = 0.0
            try:
                if start and end:
                    sdt = parse_utc(str(start))
                    edt = parse_utc(str(end))
                    if sdt - timedelta(days=2) <= target_dt <= edt + timedelta(days=2):
                        active_boost = 0.05
            except Exception:
                active_boost = 0.0
            if best is None or score > best.confidence:
                # Prefer YES token id if present (outcomes order is typically ["Yes","No"])
                clob_token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
                yes_token = None
                if isinstance(clob_token_ids, list) and clob_token_ids:
                    yes_token = clob_token_ids[0]
                best = MarketMatch(
                    market_id=str(m.get("id")),
                    market_title=question,
                    confidence=min(1.0, score + active_boost),
                    side_token_id=yes_token,
                )
        if best and best.confidence >= 0.35:
            return best
        return None

    async def resolve_from_market_ref(self, market_ref: dict, *, intent: str, ts_utc: str) -> MarketMatch | None:
        """
        Deterministic resolution from a parsed Polymarket URL (event slug + optional market slug).
        """
        if not isinstance(market_ref, dict):
            return None
        if (market_ref.get("platform") or "").lower() != "polymarket":
            return None
        event_slug = market_ref.get("event_slug")
        if not isinstance(event_slug, str) or not event_slug:
            return None
        market_slug = market_ref.get("market_slug")

        data = await self._gamma_get("/events", params={"slug": event_slug})
        ev = None
        if isinstance(data, list) and data:
            ev = data[0]
        elif isinstance(data, dict):
            ev = data
        if not isinstance(ev, dict):
            return None
        markets = ev.get("markets")
        if not isinstance(markets, list) or not markets:
            return None

        target_dt = parse_utc(ts_utc)
        def _active(m: dict) -> bool:
            try:
                start = m.get("startDate") or m.get("start_date")
                end = m.get("endDate") or m.get("end_date")
                if not start or not end:
                    return True
                sdt = parse_utc(str(start))
                edt = parse_utc(str(end))
                return sdt - timedelta(days=2) <= target_dt <= edt + timedelta(days=2)
            except Exception:
                return True

        active_markets = [m for m in markets if _active(m)]
        if not active_markets:
            active_markets = markets

        if isinstance(market_slug, str) and market_slug:
            for m in active_markets:
                if str(m.get("slug") or "") == market_slug:
                    return MarketMatch(market_id=str(m.get("id")), market_title=str(m.get("question") or m.get("title") or ""), confidence=1.0)

        if len(active_markets) == 1:
            m = active_markets[0]
            return MarketMatch(market_id=str(m.get("id")), market_title=str(m.get("question") or m.get("title") or ""), confidence=0.95)

        best = None
        for m in active_markets:
            question = str(m.get("question") or m.get("title") or "")
            score = fuzzy_score(intent, question)
            if best is None or score > best[0]:
                best = (score, m)
        if best and best[0] >= 0.35:
            m = best[1]
            return MarketMatch(market_id=str(m.get("id")), market_title=str(m.get("question") or m.get("title") or ""), confidence=float(best[0]))
        return None

    async def get_verified_market(self, market_id: str) -> VerifiedMarket:
        m = await self._gamma_get(f"/markets/{market_id}")
        if not isinstance(m, dict):
            return VerifiedMarket(market_id=str(market_id), market_title="", resolved=False, resolved_outcome=None, resolution_ts_utc=None)
        # Gamma currently exposes UMA resolution status rather than a simple boolean.
        uma_status = str(m.get("umaResolutionStatus") or "").lower()
        resolved = uma_status == "resolved"
        end_date = m.get("closedTime") or m.get("umaEndDate") or m.get("endDate") or m.get("end_date")

        outcomes_raw = m.get("outcomes")
        prices_raw = m.get("outcomePrices") or m.get("outcome_prices")

        def _as_list(x):
            if isinstance(x, list):
                return x
            if isinstance(x, str) and x.strip():
                import json

                try:
                    v = json.loads(x)
                    return v if isinstance(v, list) else None
                except Exception:
                    return None
            return None

        outcomes = _as_list(outcomes_raw) or []
        prices = _as_list(prices_raw) or []

        resolved_outcome = None
        # Only support binary YES/NO for now.
        if resolved and outcomes and prices and len(outcomes) == len(prices):
            try:
                fprices = [float(p) for p in prices]
                # Determine winner by max price; for resolved markets this is typically 1/0.
                win_idx = max(range(len(fprices)), key=lambda i: fprices[i])
                win = str(outcomes[win_idx]).strip().lower()
                if win in ("yes", "true"):
                    resolved_outcome = "YES"
                elif win in ("no", "false"):
                    resolved_outcome = "NO"
            except Exception:
                resolved_outcome = None
        return VerifiedMarket(
            market_id=str(m.get("id")),
            market_title=m.get("question") or m.get("title") or "",
            resolved=resolved,
            resolved_outcome=resolved_outcome,
            resolution_ts_utc=end_date,
        )

    async def get_price_near(self, market_id: str, side: str, ts_utc: str) -> PricePoint | None:
        # Need token id for the specific side; fetch market and map outcomes -> clobTokenIds.
        key = (market_id, side.upper())
        token_id = self._token_cache.get(key)
        if token_id is None:
            m = await self._gamma_get(f"/markets/{market_id}")
            if not isinstance(m, dict):
                return None
            outcomes_raw = m.get("outcomes")
            token_ids_raw = m.get("clobTokenIds") or m.get("clob_token_ids")
            import json

            if isinstance(outcomes_raw, str) and outcomes_raw.strip():
                try:
                    outcomes = json.loads(outcomes_raw)
                except Exception:
                    outcomes = None
            else:
                outcomes = outcomes_raw

            if isinstance(token_ids_raw, str) and token_ids_raw.strip():
                try:
                    token_ids = json.loads(token_ids_raw)
                except Exception:
                    token_ids = None
            else:
                token_ids = token_ids_raw

            if not (isinstance(outcomes, list) and isinstance(token_ids, list) and len(outcomes) == len(token_ids)):
                return None

            want = "yes" if side.upper() == "YES" else "no"
            for o, tid in zip(outcomes, token_ids):
                if isinstance(o, str) and o.lower() == want and isinstance(tid, str) and tid:
                    token_id = tid
                    break
            if not token_id:
                return None
            self._token_cache[key] = token_id

        target = to_unix_seconds(ts_utc)
        # Start with 1h window; widen if empty.
        for window_s in (3600, 6 * 3600, 24 * 3600):
            start_ts = max(0, target - window_s)
            end_ts = target + window_s
            hist = await self._clob_get(
                "/prices-history",
                # CLOB currently enforces a minimum fidelity for 1m interval. Using 10 is the smallest allowed.
                params={"market": token_id, "startTs": start_ts, "endTs": end_ts, "interval": "1m", "fidelity": 10},
            )
            points = hist.get("history") if isinstance(hist, dict) else None
            if points:
                break
        else:
            points = None
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
