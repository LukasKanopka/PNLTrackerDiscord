from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any
import re

from pnl_analyzer.config import settings
from pnl_analyzer.extraction.signals import (
    OddsBlock,
    detect_action,
    detect_deictic,
    detect_platform,
    detect_side,
    extract_inline_price,
    extract_market_refs,
    extract_odds_block,
    extract_size_usd,
)
from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.utils.time import parse_utc


def _slim_intent(text: str) -> str:
    t = text or ""
    t = re.sub(r"https?://\\S+", "", t)
    t = re.sub(r"<@[^>]+>", "", t)
    t = re.sub(r"<a?:[^:>]+:\\d+>", "", t)
    t = re.sub(r"\\s+", " ", t).strip()
    # Keep enough context for fuzzy matching, but avoid enormous blobs.
    return t[:2000]


@dataclass(frozen=True)
class CallCandidate:
    source_message_index: int
    message: dict[str, Any]
    context_messages: list[dict[str, Any]]
    market_refs: list[dict[str, Any]]
    platform_hint: str | None
    side_hint: str | None
    action_hint: str | None
    odds_block: OddsBlock | None
    inline_price: float | None
    size_usd: float | None
    deictic: bool
    evidence: list[str]
    attached_from_context: bool


def _within_window(ts_a_utc: str, ts_b_utc: str, *, seconds: int) -> bool:
    try:
        a = parse_utc(ts_a_utc)
        b = parse_utc(ts_b_utc)
    except Exception:
        return False
    return abs((a - b).total_seconds()) <= seconds


def _candidate_reasons(text: str, market_refs: list[dict], platform: str | None, side: str | None, action: str | None, odds: OddsBlock | None, price: float | None) -> list[str]:
    reasons: list[str] = []
    if market_refs:
        reasons.append("has_market_url")
    if platform:
        reasons.append("has_platform")
    if side:
        reasons.append("has_side")
    if action:
        reasons.append("has_action")
    if odds is not None:
        reasons.append("has_odds_block")
    if price is not None:
        reasons.append("has_price")
    # Common structured posts
    if "prediction:" in (text or "").lower():
        reasons.append("has_prediction_label")
    if "my bet:" in (text or "").lower():
        reasons.append("has_my_bet_label")
    return reasons


def generate_call_candidates(messages: list[dict], *, context_window_seconds: int = 7200) -> list[CallCandidate]:
    """
    Deterministic high-recall candidate generator.
    - Scans every message (no caps).
    - Extracts market refs from URLs.
    - Links to prior market refs via per-author rolling context (default 2h).
    """
    indexed = [{"index": i, **m} for i, m in enumerate(messages)]

    # Context: last market ref per author + last market ref globally.
    last_by_author: dict[str, tuple[dict, dict]] = {}  # author -> (market_ref, source_message)
    last_global: tuple[dict, dict] | None = None

    out: list[CallCandidate] = []

    for m in indexed:
        idx = int(m["index"])
        author = str(m.get("author") or "")
        ts = str(m.get("timestamp_utc") or "")
        text = str(m.get("text") or "")

        # If a single Discord message contains multiple "call-ish" lines (common for stream recaps),
        # split it into multiple candidates to avoid conflating separate bets.
        raw_lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
        norm_lines = [re.sub(r"^[\\-\\*\\u2022\\s]+", "", ln).strip() for ln in raw_lines]
        callish_lines: list[str] = []
        for ln in norm_lines[:50]:
            low_ln = ln.lower()
            # Skip pure odds rows like "Yes: 31c" / "No: 69c" (they're context, not a call).
            if re.match(r"^(yes|no)\s*:\s*\d+(?:\.\d+)?\s*c\b", low_ln):
                continue

            side_ln = detect_side(ln)
            action_ln = detect_action(ln)
            price_ln = extract_inline_price(ln)

            labeled = any(k in low_ln for k in ("my bet:", "pick:", "bet:", "prediction:"))
            if labeled and (side_ln or action_ln or price_ln is not None):
                callish_lines.append(ln)
                continue

            # Typical compact stream pick: includes side + price on a single line, plus some market-specific tokens.
            if price_ln is not None and side_ln is not None:
                if any(k in low_ln for k in ("strike", "homer", "home run", "win", "wins", "spread", "points", "runs", "rbis", "total", "over", "under")):
                    callish_lines.append(ln)
                    continue

            if ("yes" in low_ln or "no" in low_ln) and action_ln is not None:
                callish_lines.append(ln)
        sub_texts = callish_lines[:8] if len(callish_lines) >= 2 else [text]

        # Message-level signals are still useful for sub-lines (links/odds are often only included once).
        message_market_refs = extract_market_refs(text)
        message_odds = extract_odds_block(text)
        market_refs = message_market_refs
        if market_refs:
            # Store only the most recent ref for each author; keep the first as canonical context.
            last_by_author[author] = (market_refs[0], m)
            last_global = (market_refs[0], m)

        # Compute message-level platform hint once.
        msg_platform_hint = detect_platform(text)

        for st in sub_texts:
            line_market_refs = extract_market_refs(st)
            platform_hint = detect_platform(st) or msg_platform_hint
            side_hint = detect_side(st)
            action_hint = detect_action(st)
            odds = extract_odds_block(st)
            inline_price = extract_inline_price(st)
            size_usd = extract_size_usd(st)
            deictic = detect_deictic(st)
            # If the call line is "My Bet/Pick" but odds were posted elsewhere in the same message,
            # carry the odds block through so we don't lose the quoted entry price.
            if odds is None and message_odds is not None:
                low = (st or "").lower()
                if "my bet" in low or low.startswith("pick:") or low.startswith("bet:"):
                    odds = message_odds

            # Candidate gating:
            low = (st or "").lower()
            refs = list(line_market_refs or message_market_refs)
            has_url = bool(refs)
            has_side = side_hint is not None
            has_action = action_hint is not None
            has_odds = odds is not None
            has_price = inline_price is not None
            has_prediction = "prediction:" in low
            has_my_bet = "my bet:" in low
            callish = has_action or has_odds or has_price or has_prediction or has_my_bet
            if not (callish or (has_url and has_side)):
                continue

            _ = _candidate_reasons(st, refs, platform_hint, side_hint, action_hint, odds, inline_price)

            attached_from_context = False
            context_messages: list[dict] = []

            # If no URL in-message/line, try per-author then global context.
            if not refs:
                allow_context_attach = deictic or len(_slim_intent(st)) <= 140
                if allow_context_attach and author in last_by_author:
                    ref, src_msg = last_by_author[author]
                    if _within_window(ts, str(src_msg.get("timestamp_utc") or ""), seconds=context_window_seconds):
                        refs = [ref]
                        context_messages.append(
                            {
                                "index": src_msg.get("index"),
                                "author": src_msg.get("author"),
                                "timestamp_utc": src_msg.get("timestamp_utc"),
                                "text": src_msg.get("text"),
                            }
                        )
                        attached_from_context = True
                if not refs and allow_context_attach and deictic and last_global is not None:
                    ref, src_msg = last_global
                    if _within_window(ts, str(src_msg.get("timestamp_utc") or ""), seconds=context_window_seconds):
                        refs = [ref]
                        context_messages.append(
                            {
                                "index": src_msg.get("index"),
                                "author": src_msg.get("author"),
                                "timestamp_utc": src_msg.get("timestamp_utc"),
                                "text": src_msg.get("text"),
                            }
                        )
                        attached_from_context = True

            # Include the immediate previous message as extra context when available.
            if idx > 0:
                prev = indexed[idx - 1]
                if prev.get("author") == author and _within_window(ts, str(prev.get("timestamp_utc") or ""), seconds=context_window_seconds):
                    context_messages.append(
                        {
                            "index": prev.get("index"),
                            "author": prev.get("author"),
                            "timestamp_utc": prev.get("timestamp_utc"),
                            "text": prev.get("text"),
                        }
                    )

            evidence = []
            if st != text:
                evidence.append(f"line:{_slim_intent(st)[:120]}")
            if refs:
                evidence.append(f"market_ref:{refs[0].get('url')}")
            if odds is not None:
                evidence.append(f"odds_block:yes={odds.yes_price} no={odds.no_price}")
            if inline_price is not None:
                evidence.append(f"inline_price:{inline_price}")
            if side_hint is not None:
                evidence.append(f"side:{side_hint}")
            if platform_hint is not None:
                evidence.append(f"platform:{platform_hint}")
            if action_hint is not None:
                evidence.append(f"action:{action_hint}")
            if size_usd is not None:
                evidence.append(f"size_usd:{size_usd}")

            out.append(
                CallCandidate(
                    source_message_index=idx,
                    message={"author": author, "timestamp_utc": ts, "text": st},
                    context_messages=context_messages[:2],
                    market_refs=refs,
                    platform_hint=platform_hint,
                    side_hint=side_hint,
                    action_hint=action_hint,
                    odds_block=odds,
                    inline_price=inline_price,
                    size_usd=size_usd,
                    deictic=deictic,
                    evidence=evidence[:6],
                    attached_from_context=attached_from_context,
                )
            )

    return out


def deterministic_betcall_from_candidate(c: CallCandidate) -> BetCall | None:
    """
    Build a BetCall without calling an LLM, when we have enough signal.
    Returns None if the candidate is too ambiguous (platform/side missing).
    """
    platform = c.platform_hint
    if not platform and c.market_refs:
        p = c.market_refs[0].get("platform")
        if isinstance(p, str) and p:
            platform = p.lower()
    side = c.side_hint
    if not platform or not side:
        return None

    # Prefer structured odds blocks over the first inline price match (which often grabs the YES line).
    quoted_price = None
    if c.odds_block is not None:
        if side == "YES":
            quoted_price = c.odds_block.yes_price
        else:
            quoted_price = c.odds_block.no_price
    if quoted_price is None:
        quoted_price = c.inline_price

    bet_units = None
    if c.size_usd is not None and settings.unit_notional_usd:
        bet_units = max(0.01, float(c.size_usd) / float(settings.unit_notional_usd))

    # Confidence: deterministic parse is higher, context-attached is lower.
    conf = 0.75
    if c.attached_from_context:
        conf = 0.6
    if quoted_price is None:
        conf -= 0.1

    market_ref: dict | None
    if not c.market_refs:
        market_ref = None
    elif len(c.market_refs) == 1:
        market_ref = c.market_refs[0]
    else:
        market_ref = {"options": c.market_refs}

    return BetCall(
        author=c.message["author"],
        timestamp_utc=c.message["timestamp_utc"],
        platform=platform,
        market_intent=_slim_intent(c.message["text"]),
        position_direction=side,
        quoted_price=quoted_price,
        bet_size_units=bet_units or 1.0,
        source_message_index=c.source_message_index,
        action=c.action_hint or "UNKNOWN",
        market_ref=market_ref,
        extraction_confidence=max(0.0, min(1.0, conf)),
        evidence=list(c.evidence),
    )
