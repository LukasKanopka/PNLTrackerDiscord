from __future__ import annotations

import pytest

from pnl_analyzer.extraction.signals import extract_market_refs
from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.markets.base import MarketMatch, PricePoint, VerifiedMarket
from pnl_analyzer.markets.polymarket import PolymarketClient
from pnl_analyzer.pnl.engine import analyze_calls


class _StubPM(PolymarketClient):
    def __init__(self, event_payload: list[dict]) -> None:
        # Don't call parent __init__ (httpx clients)
        self._event_payload = event_payload
        self._token_cache = {}

    async def _gamma_get(self, path: str, params: dict | None = None):  # type: ignore[override]
        assert path == "/events"
        return self._event_payload


@pytest.mark.asyncio
async def test_polymarket_resolve_returns_market_id() -> None:
    ev = [
        {
            "id": "1",
            "slug": "foo-event",
            "markets": [
                {"id": "10", "slug": "a", "question": "A?"},
                {"id": "11", "slug": "b", "question": "B?"},
            ],
        }
    ]
    client = _StubPM(event_payload=ev)
    mr = {"platform": "polymarket", "event_slug": "foo-event", "market_slug": "b", "url": "https://polymarket.com/event/foo-event/b"}
    mm = await client.resolve_from_market_ref(mr, intent="whatever", ts_utc="2026-03-19T02:15:00Z")
    assert mm is not None
    assert mm.market_id == "11"


def test_kalshi_ticker_parses_from_market_url() -> None:
    refs = extract_market_refs("Link https://kalshi.com/markets/kxauctionpikachu/x/y/kxauctionpikachu-26")
    assert refs and refs[0]["platform"] == "kalshi"
    assert refs[0]["ticker"] == "KXAUCTIONPIKACHU-26"

def test_kalshi_ticker_parses_multi_dash_ticker() -> None:
    refs = extract_market_refs("Link https://kalshi.com/markets/kxmlbgame/professional-baseball-game/KXMLBGAME-26APR091335ATHNYY")
    assert refs and refs[0]["platform"] == "kalshi"
    assert refs[0]["ticker"] == "KXMLBGAME-26APR091335ATHNYY"


class _StubClient:
    def __init__(self, *, price: float | None) -> None:
        self._price = price

    async def match_market(self, intent: str, ts_utc: str):
        return MarketMatch(market_id="TEST", market_title=intent, confidence=1.0)

    async def get_verified_market(self, market_id: str):
        return VerifiedMarket(
            market_id=market_id,
            market_title="Test Market",
            resolved=True,
            resolved_outcome="YES",
            resolution_ts_utc="2026-03-20T00:00:00Z",
        )

    async def get_price_near(self, market_id: str, side: str, ts_utc: str):
        if self._price is None:
            return None
        return PricePoint(ts_utc=ts_utc, price=float(self._price), source="stub")


@pytest.mark.asyncio
async def test_pricing_fallback_historical_then_quoted_then_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure tests don't depend on local Postgres or DB cache side-effects.
    from pnl_analyzer.config import settings

    monkeypatch.setattr(settings, "database_url", None)
    call_hist = BetCall(
        author="a",
        timestamp_utc="2026-03-19T02:15:00Z",
        platform="polymarket",
        market_intent="Will X happen?",
        position_direction="YES",
        quoted_price=None,
        bet_size_units=1.0,
    )
    rep = await analyze_calls(calls=[call_hist], kalshi=_StubClient(price=0.5), polymarket=_StubClient(price=0.5), verify_prices=True)
    assert rep["bets"][0]["status"] == "OK"
    assert rep["bets"][0]["price"]["quality"] == "HISTORICAL"
    assert rep["bets"][0]["entry_price_used"] == pytest.approx(0.5)

    call_quoted = BetCall(
        author="b",
        timestamp_utc="2026-03-19T02:15:00Z",
        platform="polymarket",
        market_intent="Will X happen?",
        position_direction="YES",
        quoted_price=0.42,
        bet_size_units=1.0,
    )
    rep2 = await analyze_calls(calls=[call_quoted], kalshi=_StubClient(price=None), polymarket=_StubClient(price=None), verify_prices=True)
    assert rep2["bets"][0]["status"] == "OK"
    assert rep2["bets"][0]["price"]["quality"] == "QUOTED"
    assert rep2["bets"][0]["entry_price_used"] == pytest.approx(0.42)

    call_missing = BetCall(
        author="c",
        timestamp_utc="2026-03-19T02:15:00Z",
        platform="polymarket",
        market_intent="Will X happen?",
        position_direction="YES",
        quoted_price=None,
        bet_size_units=1.0,
    )
    rep3 = await analyze_calls(calls=[call_missing], kalshi=_StubClient(price=None), polymarket=_StubClient(price=None), verify_prices=True)
    assert rep3["bets"][0]["status"] == "ERROR_MISSING_ENTRY_PRICE"
