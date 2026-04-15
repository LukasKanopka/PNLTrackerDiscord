from __future__ import annotations

import asyncio
import json
import logging
import time

from openai import AsyncOpenAI

from pnl_analyzer.config import settings
from pnl_analyzer.extraction.candidates import CallCandidate, deterministic_betcall_from_candidate, generate_call_candidates
from pnl_analyzer.llm.base import BetExtractor
from pnl_analyzer.llm.normalize import normalize_bet_item
from pnl_analyzer.llm.prompt import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from pnl_analyzer.llm.types import BetCall


class OpenAIBetExtractor(BetExtractor):
    def __init__(self) -> None:
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY is not set")
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)

    async def extract_bets(self, messages: list[dict]) -> list[BetCall]:
        log = logging.getLogger("pnl_analyzer")
        t0 = time.perf_counter()

        candidates = generate_call_candidates(messages)
        log.info("llm:openai start messages=%s candidates=%s model=%s", len(messages), len(candidates), settings.openai_model)

        seen: set[tuple[str, str, str, str, str]] = set()
        dropped = 0

        def _should_llm(c: CallCandidate, det: BetCall | None) -> bool:
            # Only pay for LLM when there is enough hard signal to be a real call.
            low = str((c.message or {}).get("text") or "").lower()
            has_side_signal = c.side_hint is not None or c.odds_block is not None or ("my bet:" in low)
            has_platform_signal = c.platform_hint is not None or any(isinstance(mr, dict) and mr.get("platform") for mr in (c.market_refs or []))
            callish = c.action_hint is not None or c.inline_price is not None or c.odds_block is not None or ("prediction:" in low) or ("my bet:" in low)
            if not (has_side_signal and has_platform_signal and callish):
                return False
            if det is None:
                return True
            return isinstance(det.market_ref, dict) and "options" in det.market_ref

        async def _llm_normalize(c: CallCandidate) -> BetCall | None:
            # Build deterministic if possible; use LLM only when needed.
            det = deterministic_betcall_from_candidate(c)
            if not _should_llm(c, det):
                return det

            market_ref_options = []
            for i, mr in enumerate(c.market_refs[:5]):
                market_ref_options.append({"id": i, **mr})

            payload = {
                "source_message_index": c.source_message_index,
                "message": {
                    "index": c.source_message_index,
                    "author": c.message.get("author"),
                    "timestamp_utc": c.message.get("timestamp_utc"),
                    "text": (c.message.get("text") or "")[: settings.llm_max_text_chars],
                },
                "context_messages": [
                    {
                        "index": cm.get("index"),
                        "author": cm.get("author"),
                        "timestamp_utc": cm.get("timestamp_utc"),
                        "text": (cm.get("text") or "")[: settings.llm_max_text_chars],
                    }
                    for cm in (c.context_messages or [])
                ],
                "market_ref_options": market_ref_options,
                "hints": {
                    "platform_hint": c.platform_hint,
                    "side_hint": c.side_hint,
                    "action_hint": c.action_hint,
                    "inline_price_hint": c.inline_price,
                },
            }

            user_prompt = USER_PROMPT_TEMPLATE.format(candidate_json=json.dumps(payload, ensure_ascii=False))
            resp = await self._client.chat.completions.create(
                model=settings.openai_model,
                messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_prompt}],
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            content = resp.choices[0].message.content or ""
            parsed = json.loads(content)
            bet = parsed.get("bet") if isinstance(parsed, dict) else None
            if bet is None:
                return det
            if isinstance(bet, dict) and "source_message_index" not in bet:
                bet["source_message_index"] = c.source_message_index
            if isinstance(bet, dict) and (not isinstance(bet.get("market_intent"), str) or not str(bet.get("market_intent") or "").strip()):
                bet["market_intent"] = c.message.get("text") or ""

            normalized = normalize_bet_item(bet if isinstance(bet, dict) else {}, messages)
            if normalized is None:
                return det
            try:
                call = BetCall.model_validate(normalized)
            except Exception:
                return det

            # Evidence + confidence: ensure present and stable.
            evidence = list(call.evidence or [])
            if not evidence:
                evidence = list(c.evidence or [])
                evidence.append("llm:normalized")
                call.evidence = evidence[:8]
            if call.extraction_confidence is None or call.extraction_confidence <= 0:
                call.extraction_confidence = 0.55
            if det is not None and det.platform.lower() == call.platform.lower() and det.position_direction == call.position_direction:
                call.extraction_confidence = min(1.0, float(call.extraction_confidence) + 0.1)
            if c.attached_from_context:
                call.extraction_confidence = max(0.0, float(call.extraction_confidence) - 0.05)
            return call

        sem = asyncio.Semaphore(max(1, int(getattr(settings, "llm_concurrency", 3))))

        async def _one(c: CallCandidate) -> BetCall | None:
            async with sem:
                try:
                    return await _llm_normalize(c)
                except Exception:
                    return deterministic_betcall_from_candidate(c)

        tasks = [_one(c) for c in candidates]
        results = await asyncio.gather(*tasks)

        out: list[BetCall] = []
        for call in results:
            if call is None:
                continue
            try:
                key = (
                    call.author,
                    call.timestamp_utc,
                    call.platform.lower(),
                    (call.market_intent or "").lower(),
                    call.position_direction.upper(),
                )
            except Exception:
                dropped += 1
                continue
            if key in seen:
                continue
            seen.add(key)
            out.append(call)

        log.info("llm:openai end bets=%s dropped_invalid=%s duration_ms=%s", len(out), dropped, int((time.perf_counter() - t0) * 1000))
        return out
