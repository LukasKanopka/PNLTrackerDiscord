from __future__ import annotations

import json

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
        def chunked() -> list[list[dict]]:
            max_per = 120
            chunks: list[list[dict]] = []
            i = 0
            while i < len(messages):
                chunks.append(messages[i : i + max_per])
                i += max_per
            return chunks

        out: list[BetCall] = []
        seen: set[tuple[str, str, str, str, str]] = set()

        for chunk in chunked():
            messages_json = json.dumps(chunk, ensure_ascii=False)
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
