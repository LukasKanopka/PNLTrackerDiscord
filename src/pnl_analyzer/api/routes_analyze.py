from __future__ import annotations

from fastapi import APIRouter, File, Form, UploadFile

from pnl_analyzer.config import settings
from pnl_analyzer.db.persist import persist_run
from pnl_analyzer.llm.factory import build_extractor
from pnl_analyzer.markets.factory import build_market_clients
from pnl_analyzer.parsing.discord_txt import parse_discord_txt
from pnl_analyzer.pnl.engine import analyze_calls

router = APIRouter(tags=["analyze"])


@router.post("/analyze")
async def analyze(
    file: UploadFile = File(...),
    export_timezone: str | None = Form(None),
    verify_prices: bool = Form(True),
) -> dict:
    content = (await file.read()).decode("utf-8", errors="replace")
    tz = export_timezone or settings.default_export_timezone

    messages = parse_discord_txt(content, export_timezone=tz)

    extractor = build_extractor()
    calls = await extractor.extract_bets(messages)

    kalshi, polymarket = build_market_clients()

    report = await analyze_calls(
        calls=calls,
        kalshi=kalshi,
        polymarket=polymarket,
        verify_prices=verify_prices,
    )

    response = {
        "source_filename": file.filename,
        "export_timezone": tz,
        "message_count": len(messages),
        "call_count": len(calls),
        "messages": messages,
        "calls": [c.model_dump() for c in calls],
        "report": report,
    }

    if settings.database_url:
        try:
            run_id = await persist_run(
                source_filename=file.filename,
                export_timezone=tz,
                verify_prices=verify_prices,
                messages=messages,
                calls=[c.model_dump() for c in calls],
                report=report,
            )
            response["run_id"] = run_id
        except Exception as e:
            response["persistence_error"] = str(e)

    return response
