from __future__ import annotations

import asyncio
import json
import re
import logging
import time

import httpx

from pnl_analyzer.config import settings
from pnl_analyzer.extraction.candidates import CallCandidate, deterministic_betcall_from_candidate, generate_call_candidates
from pnl_analyzer.llm.base import BetExtractor
from pnl_analyzer.llm.normalize import normalize_bet_item
from pnl_analyzer.llm.prompt import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.utils.retry import UpstreamHTTPError, with_retries


def _extract_first_json_object(text: str) -> dict | None:
    if not text:
        return None
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _slim_text(text: str) -> str:
    t = text or ""
    t = re.sub(r"https?://\\S+", "", t)
    t = re.sub(r"<@[^>]+>", "", t)
    t = re.sub(r"<a?:[^:>]+:\\d+>", "", t)
    t = re.sub(r"\\s+", " ", t).strip()
    return t[: settings.llm_max_text_chars]


class OpenRouterBetExtractor(BetExtractor):
    def __init__(self) -> None:
        if not settings.openrouter_api_key:
            raise ValueError("OPENROUTER_API_KEY is not set")

        headers: dict[str, str] = {}
        if settings.openrouter_http_referer:
            headers["HTTP-Referer"] = settings.openrouter_http_referer
        if settings.openrouter_x_title:
            headers["X-Title"] = settings.openrouter_x_title

        headers["Authorization"] = f"Bearer {settings.openrouter_api_key}"
        headers["Content-Type"] = "application/json"
        self._client = httpx.AsyncClient(
            base_url=settings.openrouter_base_url,
            headers=headers,
            timeout=httpx.Timeout(60.0),
        )

    async def _chat(self, *, response_format: dict | None, system: str, user: str) -> str:
        log = logging.getLogger("pnl_analyzer")
        payload: dict = {
            "model": settings.openrouter_model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "temperature": 0.0,
        }
        if response_format is not None:
            payload["response_format"] = response_format

        async def _do() -> str:
            t0 = time.perf_counter()
            r = await self._client.post("/chat/completions", json=payload)
            dt_ms = int((time.perf_counter() - t0) * 1000)
            if r.status_code in (429, 500, 502, 503, 504):
                log.warning("llm:openrouter http=%s duration_ms=%s (retryable)", r.status_code, dt_ms)
                raise UpstreamHTTPError(r.status_code, f"OpenRouter retryable: {r.text}")
            if r.status_code >= 400:
                log.warning("llm:openrouter http=%s duration_ms=%s", r.status_code, dt_ms)
                raise UpstreamHTTPError(r.status_code, f"OpenRouter failed: {r.text}")
            data = r.json()
            log.info("llm:openrouter http=%s duration_ms=%s", r.status_code, dt_ms)
            return (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""

        return await with_retries(_do)

    async def extract_bets(self, messages: list[dict]) -> list[BetCall]:
        log = logging.getLogger("pnl_analyzer")
        t0 = time.perf_counter()
        candidates = generate_call_candidates(messages)
        log.info("llm:openrouter start messages=%s candidates=%s model=%s", len(messages), len(candidates), settings.openrouter_model)

        seen: set[tuple[str, str, str, str, str]] = set()
        dropped = 0

        def _should_llm(c: CallCandidate, det: BetCall | None) -> bool:
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
                    "text": _slim_text(c.message.get("text") or ""),
                },
                "context_messages": [
                    {
                        "index": cm.get("index"),
                        "author": cm.get("author"),
                        "timestamp_utc": cm.get("timestamp_utc"),
                        "text": _slim_text(cm.get("text") or ""),
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

            content: str = ""
            try:
                content = await self._chat(response_format={"type": "json_object"}, system=SYSTEM_PROMPT, user=user_prompt)
            except Exception:
                content = await self._chat(response_format=None, system=SYSTEM_PROMPT, user=user_prompt)

            parsed: dict | None
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = _extract_first_json_object(content)

            bet = parsed.get("bet") if isinstance(parsed, dict) else None
            if bet is None:
                return det
            if isinstance(bet, dict) and "source_message_index" not in bet:
                bet["source_message_index"] = c.source_message_index
            # If the LLM omits market_intent, prefer the candidate line text (not the full source message),
            # especially important when one Discord message contains multiple calls.
            if isinstance(bet, dict) and (not isinstance(bet.get("market_intent"), str) or not str(bet.get("market_intent") or "").strip()):
                bet["market_intent"] = c.message.get("text") or ""

            normalized = normalize_bet_item(bet if isinstance(bet, dict) else {}, messages)
            if normalized is None:
                return det
            try:
                call = BetCall.model_validate(normalized)
            except Exception:
                return det

            if not call.evidence:
                call.evidence = list(c.evidence or [])[:6] + ["llm:normalized"]
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

        results = await asyncio.gather(*[_one(c) for c in candidates])

        out: list[BetCall] = []
        for call in results:
            if call is None:
                continue
            key = (
                call.author,
                call.timestamp_utc,
                call.platform.lower(),
                (call.market_intent or "").lower(),
                call.position_direction.upper(),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(call)

        log.info("llm:openrouter end bets=%s dropped_invalid=%s duration_ms=%s", len(out), dropped, int((time.perf_counter() - t0) * 1000))
        return out
