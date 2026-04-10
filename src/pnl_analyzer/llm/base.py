from __future__ import annotations

from abc import ABC, abstractmethod

from pnl_analyzer.llm.types import BetCall


class BetExtractor(ABC):
    @abstractmethod
    async def extract_bets(self, messages: list[dict]) -> list[BetCall]:
        raise NotImplementedError

