from __future__ import annotations

import json
import re

from openai import AsyncOpenAI

from pnl_analyzer.config import settings
from pnl_analyzer.llm.base import BetExtractor
from pnl_analyzer.llm.prompt import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from pnl_analyzer.llm.types import BetCall


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


class OpenRouterBetExtractor(BetExtractor):
    def __init__(self) -> None:
        if not settings.openrouter_api_key:
            raise ValueError("OPENROUTER_API_KEY is not set")

        headers: dict[str, str] = {}
        if settings.openrouter_http_referer:
            headers["HTTP-Referer"] = settings.openrouter_http_referer
        if settings.openrouter_x_title:
            headers["X-Title"] = settings.openrouter_x_title

        self._client = AsyncOpenAI(
            api_key=settings.openrouter_api_key,
            base_url=settings.openrouter_base_url,
            default_headers=headers or None,
        )

    async def extract_bets(self, messages: list[dict]) -> list[BetCall]:
        def chunked() -> list[list[dict]]:
            max_per = 120
            return [messages[i : i + max_per] for i in range(0, len(messages), max_per)]

        out: list[BetCall] = []
        seen: set[tuple[str, str, str, str, str]] = set()

        for chunk in chunked():
            messages_json = json.dumps(chunk, ensure_ascii=False)
            user_prompt = USER_PROMPT_TEMPLATE.format(messages_json=messages_json)

            # Not all OpenRouter models support strict JSON mode; try it, then fallback.
            content: str = ""
            try:
                resp = await self._client.chat.completions.create(
                    model=settings.openrouter_model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                )
                content = resp.choices[0].message.content or ""
            except Exception:
                resp = await self._client.chat.completions.create(
                    model=settings.openrouter_model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.0,
                )
                content = resp.choices[0].message.content or ""

            parsed: dict | None
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = _extract_first_json_object(content)

            items = (parsed or {}).get("bets")
            if not isinstance(items, list):
                continue

            for item in items:
                call = BetCall.model_validate(item)
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

        return out

