from __future__ import annotations

from pnl_analyzer.llm.base import BetExtractor
from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.extraction.candidates import deterministic_betcall_from_candidate, generate_call_candidates


class MockExtractor(BetExtractor):
    async def extract_bets(self, messages: list[dict]) -> list[BetCall]:
        # Deterministic-only extraction (no LLM calls). Useful for tests and offline iteration.
        out: list[BetCall] = []
        for c in generate_call_candidates(messages):
            call = deterministic_betcall_from_candidate(c)
            if call is not None:
                out.append(call)
        return out
