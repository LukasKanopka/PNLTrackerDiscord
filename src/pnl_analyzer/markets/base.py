from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketMatch:
    market_id: str
    market_title: str
    confidence: float
    side_token_id: str | None = None  # polymarket token id (YES/NO token), when applicable
    candidates: list[dict] | None = None  # optional candidate list when ambiguous (best-effort)


@dataclass(frozen=True)
class VerifiedMarket:
    market_id: str
    market_title: str
    resolved: bool
    resolved_outcome: str | None  # YES|NO
    resolution_ts_utc: str | None


@dataclass(frozen=True)
class PricePoint:
    ts_utc: str
    price: float
    source: str


class MarketClient(ABC):
    @abstractmethod
    async def match_market(self, intent: str, ts_utc: str) -> MarketMatch | None:
        raise NotImplementedError

    @abstractmethod
    async def get_verified_market(self, market_id: str) -> VerifiedMarket:
        raise NotImplementedError

    @abstractmethod
    async def get_price_near(self, market_id: str, side: str, ts_utc: str) -> PricePoint | None:
        raise NotImplementedError
