from __future__ import annotations

from pnl_analyzer.config import settings
from pnl_analyzer.llm.base import BetExtractor
from pnl_analyzer.llm.mock_extractor import MockExtractor


def build_extractor() -> BetExtractor:
    provider = (settings.llm_provider or "mock").lower()
    if provider == "mock":
        return MockExtractor()
    if provider == "openai":
        from pnl_analyzer.llm.openai_extractor import OpenAIBetExtractor

        return OpenAIBetExtractor()
    if provider == "openrouter":
        from pnl_analyzer.llm.openrouter_extractor import OpenRouterBetExtractor

        return OpenRouterBetExtractor()
    raise ValueError(f"Unknown LLM_PROVIDER: {settings.llm_provider}")
