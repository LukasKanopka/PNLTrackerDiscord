from __future__ import annotations

from pnl_analyzer.markets.kalshi import KalshiClient
from pnl_analyzer.markets.polymarket import PolymarketClient


def build_market_clients() -> tuple[KalshiClient, PolymarketClient]:
    return KalshiClient(), PolymarketClient()

