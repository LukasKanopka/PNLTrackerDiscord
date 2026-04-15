from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy import select

from pnl_analyzer.db.models import PriceCache
from pnl_analyzer.db.session import session_scope


class PriceCacheStore(ABC):
    @abstractmethod
    async def get(self, *, platform: str, market_id: str, side: str, minute_ts: int) -> tuple[float, str | None] | None:
        raise NotImplementedError

    @abstractmethod
    async def set(self, *, platform: str, market_id: str, side: str, minute_ts: int, price: float, source: str | None) -> None:
        raise NotImplementedError


class InMemoryPriceCache(PriceCacheStore):
    def __init__(self) -> None:
        self._d: dict[tuple[str, str, str, int], tuple[float, str | None]] = {}

    async def get(self, *, platform: str, market_id: str, side: str, minute_ts: int) -> tuple[float, str | None] | None:
        return self._d.get((platform.lower(), market_id, side.upper(), int(minute_ts)))

    async def set(self, *, platform: str, market_id: str, side: str, minute_ts: int, price: float, source: str | None) -> None:
        self._d[(platform.lower(), market_id, side.upper(), int(minute_ts))] = (float(price), source)


class DBPriceCache(PriceCacheStore):
    def __init__(self) -> None:
        self._disabled = False

    async def get(self, *, platform: str, market_id: str, side: str, minute_ts: int) -> tuple[float, str | None] | None:
        if self._disabled:
            return None
        async for session in session_scope():
            try:
                res = await session.execute(
                    select(PriceCache.price, PriceCache.source).where(
                        PriceCache.platform == platform.lower(),
                        PriceCache.market_id == market_id,
                        PriceCache.side == side.upper(),
                        PriceCache.minute_ts == int(minute_ts),
                    )
                )
                row = res.first()
                if not row:
                    return None
                return float(row[0]), (row[1] if row[1] is None else str(row[1]))
            except Exception:
                # If DB is misconfigured/unavailable, degrade gracefully for this process.
                self._disabled = True
                return None

    async def set(self, *, platform: str, market_id: str, side: str, minute_ts: int, price: float, source: str | None) -> None:
        if self._disabled:
            return
        async for session in session_scope():
            try:
                # Upsert-ish: insert if missing, else ignore.
                existing = await session.execute(
                    select(PriceCache.id).where(
                        PriceCache.platform == platform.lower(),
                        PriceCache.market_id == market_id,
                        PriceCache.side == side.upper(),
                        PriceCache.minute_ts == int(minute_ts),
                    )
                )
                if existing.first():
                    return
                session.add(
                    PriceCache(
                        platform=platform.lower(),
                        market_id=market_id,
                        side=side.upper(),
                        minute_ts=int(minute_ts),
                        price=float(price),
                        source=source,
                    )
                )
                await session.commit()
            except Exception:
                self._disabled = True
                return
