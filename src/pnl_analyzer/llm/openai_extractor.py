from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from pnl_analyzer.config import settings
from pnl_analyzer.llm.base import BetExtractor
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

        import re

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
            candidates = indexed_all[:150]

        def chunked() -> list[list[dict]]:
            max_per = 50
            return [candidates[i : i + max_per] for i in range(0, len(candidates), max_per)]

        out: list[BetCall] = []
        seen: set[tuple[str, str, str, str, str]] = set()

        chunks = chunked()
        log.info("llm:openai start messages=%s candidates=%s chunks=%s model=%s", len(messages), len(candidates), len(chunks), settings.openai_model)

        for chunk_idx, chunk in enumerate(chunks, start=1):
            def slim_text(text: str) -> str:
                t = text or ""
                t = re.sub(r"https?://\\S+", "", t)
                t = re.sub(r"<@[^>]+>", "", t)
                t = re.sub(r"<a?:[^:>]+:\\d+>", "", t)
                t = re.sub(r"\\s+", " ", t).strip()
                return t[:500]

            chunk_for_llm = []
            for m in chunk:
                chunk_for_llm.append(
                    {
                        "index": m.get("index"),
                        "author": m.get("author"),
                        "timestamp_utc": m.get("timestamp_utc"),
                        "text": slim_text(m.get("text") or ""),
                    }
                )

            messages_json = json.dumps(chunk_for_llm, ensure_ascii=False)
            user_prompt = USER_PROMPT_TEMPLATE.format(messages_json=messages_json)

            resp = await self._client.chat.completions.create(
                model=settings.openai_model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            content = resp.choices[0].message.content or ""
            parsed = json.loads(content)
            items = parsed.get("bets") if isinstance(parsed, dict) else []
            if not isinstance(items, list):
                continue

            for item in items:
                if isinstance(item, dict):
                    if "source_message_index" not in item and isinstance(item.get("index"), int):
                        item["source_message_index"] = item.get("index")
                    src_idx = item.get("source_message_index")
                    if isinstance(src_idx, int) and 0 <= src_idx < len(messages):
                        src = messages[src_idx]
                        item.setdefault("author", src.get("author"))
                        item.setdefault("timestamp_utc", src.get("timestamp_utc"))
                        item.setdefault("market_intent", src.get("text"))

                try:
                    call = BetCall.model_validate(item)
                except Exception:
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

            if chunk_idx % 5 == 0:
                log.info("llm:openai progress chunk=%s/%s bets=%s", chunk_idx, len(chunks), len(out))

        log.info("llm:openai end bets=%s duration_ms=%s", len(out), int((time.perf_counter() - t0) * 1000))
        return out
