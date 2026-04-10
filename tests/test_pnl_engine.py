import pytest

from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.markets.base import MarketMatch, PricePoint, VerifiedMarket
from pnl_analyzer.pnl.engine import analyze_calls


class _StubClient:
    def __init__(self, platform: str) -> None:
        self.platform = platform

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
        return PricePoint(ts_utc=ts_utc, price=0.5, source=f"{self.platform}_stub")


@pytest.mark.asyncio
async def test_analyze_calls_computes_net_pnl() -> None:
    calls = [
        BetCall(
            author="alice",
            timestamp_utc="2026-03-19T02:15:00Z",
            platform="polymarket",
            market_intent="Will X happen?",
            position_direction="YES",
            quoted_price=0.48,
            bet_size_units=1.0,
        )
        ,
        BetCall(
            author="bob",
            timestamp_utc="2026-03-19T02:16:00Z",
            platform="kalshi",
            market_intent="Will Y happen?",
            position_direction="YES",
            quoted_price=0.48,
            bet_size_units=1.0,
        ),
    ]
    rep = await analyze_calls(calls=calls, kalshi=_StubClient("kalshi"), polymarket=_StubClient("poly"), verify_prices=True)
    bet_poly = rep["bets"][0]
    assert bet_poly["status"] == "OK"
    # Entry price overridden to 0.5; YES resolves YES so profit is (1-0.5)*100 = 50
    assert bet_poly["net_pnl_usd"] == pytest.approx(50.0)

    bet_kalshi = rep["bets"][1]
    assert bet_kalshi["status"] == "OK"
    # Kalshi fee (taker) at p=0.5 on 100 contracts: ceil(0.07*100*0.5*0.5) = 1.75
    assert bet_kalshi["fees_usd"] == pytest.approx(1.75)
    assert bet_kalshi["net_pnl_usd"] == pytest.approx(48.25)
