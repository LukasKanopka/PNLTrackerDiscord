from __future__ import annotations

import pytest

from pnl_analyzer.extraction.candidates import deterministic_betcall_from_candidate, generate_call_candidates
from pnl_analyzer.llm.mock_extractor import MockExtractor
from pnl_analyzer.parsing.discord_txt import parse_discord_txt


def test_context_links_market_url_to_followup_call() -> None:
    content = (
        "[3/18/2026 10:15 PM] alice: Link to Market: https://polymarket.com/event/foo-event/foo-market\n"
        "[3/18/2026 10:16 PM] alice: buying NO here\n"
    )
    msgs = parse_discord_txt(content, export_timezone="America/New_York")
    cands = generate_call_candidates(msgs)
    # Candidate should be created for the second message and inherit the market ref from the first.
    followups = [c for c in cands if c.source_message_index == 1]
    assert followups
    c = followups[0]
    assert c.attached_from_context is True
    assert c.market_refs and c.market_refs[0]["platform"] == "polymarket"

    call = deterministic_betcall_from_candidate(c)
    assert call is not None
    assert call.platform == "polymarket"
    assert call.position_direction == "NO"
    assert isinstance(call.market_ref, dict)
    assert call.market_ref.get("event_slug") == "foo-event"
    assert call.market_ref.get("market_slug") == "foo-market"


@pytest.mark.asyncio
async def test_golden_slice_deterministic_extractor_is_stable() -> None:
    content = """[2/16/2026 12:40 AM] champtgram
Prediction: Will Logan Paul's Pikachu Illustrator sell for over $10M?

Odds:
Yes: 31 c
No: 69 c

My Bet: No
Link to bet: https://kalshi.com/markets/kxauctionpikachu/how-much-wil-logan-pauls-1997-illustrator-pikachu-be-/kxauctionpikachu-26

[2/28/2026 7:03 AM] redeyez: I am buying YES at 51c https://polymarket.com/event/us-strikes-iran-by/us-strikes-iran-by-march-1
"""
    msgs = parse_discord_txt(content, export_timezone="America/New_York")
    calls = await MockExtractor().extract_bets(msgs)
    # Keep this small and stable: two concrete calls.
    assert len(calls) == 2
    assert calls[0].platform == "kalshi"
    assert calls[0].position_direction == "NO"
    assert calls[0].quoted_price == pytest.approx(0.69)
    assert calls[1].platform == "polymarket"
    assert calls[1].position_direction == "YES"
    assert calls[1].quoted_price == pytest.approx(0.51)
