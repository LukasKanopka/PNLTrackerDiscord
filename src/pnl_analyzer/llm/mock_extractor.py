from __future__ import annotations

from pnl_analyzer.llm.base import BetExtractor
from pnl_analyzer.llm.types import BetCall


class MockExtractor(BetExtractor):
    async def extract_bets(self, messages: list[dict]) -> list[BetCall]:
        # Deterministic placeholder: returns nothing.
        return []

