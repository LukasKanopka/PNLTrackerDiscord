from __future__ import annotations

import json
import re
import logging
import time

import httpx

from pnl_analyzer.config import settings
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

        price_re = re.compile(r"(\\b0\\.\\d{1,3}\\b|\\b\\d{1,3}\\s*c\\b|\\b\\d{1,3}\\s*%\\b|@\\s*\\d{1,3})", re.IGNORECASE)
        platform_re = re.compile(r"\\b(kalshi|polymarket|poly)\\b", re.IGNORECASE)
        action_re = re.compile(r"\\b(buy|bought|sell|sold|loaded|adding|entry|in at|out at)\\b", re.IGNORECASE)
        side_re = re.compile(r"\\b(yes|no)\\b", re.IGNORECASE)

        indexed_all = [{"index": i, **m} for i, m in enumerate(messages)]
        candidates: list[dict] = []
        for m in indexed_all:
            t = (m.get("text") or "").strip()
            if not t:
                continue
            has_platform = bool(platform_re.search(t))
            has_price = bool(price_re.search(t))
            has_action = bool(action_re.search(t))
            has_side = bool(side_re.search(t))

            if has_platform and (has_action or has_price or has_side):
                candidates.append(m)
            elif has_price and (has_action or has_side):
                candidates.append(m)

        if not candidates:
            candidates = indexed_all[: settings.llm_max_candidates]
        else:
            candidates = candidates[: settings.llm_max_candidates]

        def chunked() -> list[list[dict]]:
            max_per = max(5, int(settings.llm_chunk_size))
            return [candidates[i : i + max_per] for i in range(0, len(candidates), max_per)]

        chunks = chunked()
        log.info("llm:openrouter start messages=%s candidates=%s chunks=%s model=%s", len(messages), len(candidates), len(chunks), settings.openrouter_model)

        out: list[BetCall] = []
        seen: set[tuple[str, str, str, str, str]] = set()
        dropped = 0

        for chunk_idx, chunk in enumerate(chunks, start=1):
            log.info("llm:openrouter chunk:start chunk=%s/%s items=%s", chunk_idx, len(chunks), len(chunk))
            chunk_for_llm = []
            for m in chunk:
                chunk_for_llm.append(
                    {
                        "index": m.get("index"),
                        "author": m.get("author"),
                        "timestamp_utc": m.get("timestamp_utc"),
                        "text": _slim_text(m.get("text") or ""),
                    }
                )

            messages_json = json.dumps(chunk_for_llm, ensure_ascii=False)
            user_prompt = USER_PROMPT_TEMPLATE.format(messages_json=messages_json)

            # Not all OpenRouter models support strict JSON mode; try it, then fallback.
            content: str = ""
            try:
                content = await self._chat(
                    response_format={"type": "json_object"},
                    system=SYSTEM_PROMPT,
                    user=user_prompt,
                )
            except Exception:
                content = await self._chat(response_format=None, system=SYSTEM_PROMPT, user=user_prompt)

            parsed: dict | None
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = _extract_first_json_object(content)

            items = None
            if isinstance(parsed, list):
                items = parsed
            elif isinstance(parsed, dict):
                items = parsed.get("bets")
            if not isinstance(items, list):
                snippet = (content or "").replace("\n", " ")[:240]
                log.warning("llm:openrouter chunk:parse_failed chunk=%s/%s snippet=%r", chunk_idx, len(chunks), snippet)
                log.warning("llm:openrouter chunk:end chunk=%s/%s bets_total=%s parsed_bets=0", chunk_idx, len(chunks), len(out))
                continue

            for item in items:
                if isinstance(item, dict) and "source_message_index" not in item and isinstance(item.get("index"), int):
                    item["source_message_index"] = item.get("index")

                normalized = normalize_bet_item(item if isinstance(item, dict) else {}, messages)
                if normalized is None:
                    dropped += 1
                    continue
                try:
                    call = BetCall.model_validate(normalized)
                except Exception:
                    dropped += 1
                    continue
                key = (
                    call.author,
                    call.timestamp_utc,
                    call.platform.lower(),
                    call.market_intent.lower(),
                    call.position_direction.upper(),
                )
                if key in seen:
                    continue
                seen.add(key)
                out.append(call)

            log.info(
                "llm:openrouter chunk:end chunk=%s/%s bets_total=%s parsed_bets=%s",
                chunk_idx,
                len(chunks),
                len(out),
                len(items),
            )

        log.info("llm:openrouter end bets=%s duration_ms=%s", len(out), int((time.perf_counter() - t0) * 1000))
        if dropped:
            log.info("llm:openrouter dropped_invalid=%s", dropped)
        return out
