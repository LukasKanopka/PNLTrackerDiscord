from __future__ import annotations

import logging
import time
import uuid

from fastapi import APIRouter, File, Form, UploadFile
from pydantic import BaseModel, ConfigDict

from pnl_analyzer.config import settings
from pnl_analyzer.db.persist import persist_raw_run, persist_run
from pnl_analyzer.db.queries import get_calls_for_run, get_run, list_runs, replace_results_for_run
from pnl_analyzer.llm.factory import build_extractor
from pnl_analyzer.markets.factory import build_market_clients
from pnl_analyzer.parsing.discord_txt import parse_discord_txt
from pnl_analyzer.pnl.engine import analyze_calls
from pnl_analyzer.utils.stages import stage

router = APIRouter(tags=["analyze"])
log = logging.getLogger("pnl_analyzer")


@router.get("/runs")
async def runs(limit: int = 20) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    return {"runs": await list_runs(limit=limit)}


class ImportPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_filename: str | None = None
    export_timezone: str = "America/New_York"
    verify_prices: bool = False
    timestamps_are_utc: bool = True
    messages: list[dict] = []
    calls: list[dict] = []
    report: dict | None = None


@router.post("/import")
async def import_report(payload: ImportPayload) -> dict:
    """
    Imports a previously generated JSON payload (for example, the output of /v1/analyze)
    and stores it as a DB run. If `report` is provided, per-call results are persisted too.
    """
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}

    if not payload.calls:
        return {"error": "calls is required to import into DB"}

    request_id = uuid.uuid4().hex[:10]
    messages = payload.messages
    calls = payload.calls

    if not payload.timestamps_are_utc:
        from pnl_analyzer.utils.time import reinterpret_as_local_then_to_utc

        def fix_ts(obj: dict) -> dict:
            o = dict(obj)
            if "timestamp_utc" in o and isinstance(o["timestamp_utc"], str):
                o["timestamp_utc"] = reinterpret_as_local_then_to_utc(o["timestamp_utc"], payload.export_timezone)
            return o

        messages = [fix_ts(m) for m in messages]
        calls = [fix_ts(c) for c in calls]

    with stage(log, "import_persist", request_id, calls=len(payload.calls), messages=len(payload.messages), has_report=bool(payload.report)):
        if payload.report is None:
            run_id = await persist_raw_run(
                source_filename=payload.source_filename,
                export_timezone=payload.export_timezone,
                messages=messages,
                calls=calls,
            )
            return {"run_id": run_id, "imported": True, "message_count": len(payload.messages), "call_count": len(payload.calls)}

        run_id = await persist_run(
            source_filename=payload.source_filename,
            export_timezone=payload.export_timezone,
            verify_prices=payload.verify_prices,
            messages=messages,
            calls=calls,
            report=payload.report,
        )
        return {"run_id": run_id, "imported": True, "message_count": len(payload.messages), "call_count": len(payload.calls)}


@router.post("/analyze")
async def analyze(
    file: UploadFile = File(...),
    export_timezone: str | None = Form(None),
    verify_prices: bool = Form(True),
) -> dict:
    request_id = uuid.uuid4().hex[:10]
    t0 = time.perf_counter()
    content = (await file.read()).decode("utf-8", errors="replace")
    tz = export_timezone or settings.default_export_timezone

    with stage(log, "parse", request_id, filename=file.filename, tz=tz):
        messages = parse_discord_txt(content, export_timezone=tz)
        log.info("[%s] parse:messages count=%s", request_id, len(messages))

    with stage(log, "extract", request_id, provider=settings.llm_provider):
        extractor = build_extractor()
        calls = await extractor.extract_bets(messages)
        log.info("[%s] extract:calls count=%s", request_id, len(calls))

    kalshi, polymarket = build_market_clients()

    with stage(log, "analyze", request_id, verify_prices=verify_prices, calls=len(calls)):
        report = await analyze_calls(
            calls=calls,
            kalshi=kalshi,
            polymarket=polymarket,
            verify_prices=verify_prices,
            logger=log,
            request_id=request_id,
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
        with stage(log, "persist", request_id):
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

    log.info("[%s] done duration_ms=%s", request_id, int((time.perf_counter() - t0) * 1000))
    return response


@router.post("/ingest")
async def ingest(
    file: UploadFile = File(...),
    export_timezone: str | None = Form(None),
    extract_calls: bool = Form(True),
) -> dict:
    """
    Stores parsed messages + extracted calls into Postgres without market matching or PnL.
    Useful to avoid heavy external API traffic during iteration.
    """
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}

    request_id = uuid.uuid4().hex[:10]
    content = (await file.read()).decode("utf-8", errors="replace")
    tz = export_timezone or settings.default_export_timezone
    with stage(log, "parse", request_id, filename=file.filename, tz=tz):
        messages = parse_discord_txt(content, export_timezone=tz)

    calls = []
    if extract_calls:
        with stage(log, "extract", request_id, provider=settings.llm_provider):
            extractor = build_extractor()
            calls = await extractor.extract_bets(messages)

    with stage(log, "persist_raw", request_id, messages=len(messages), calls=len(calls)):
        run_id = await persist_raw_run(
            source_filename=file.filename,
            export_timezone=tz,
            messages=messages,
            calls=[c.model_dump() for c in calls],
        )
    return {"run_id": run_id, "message_count": len(messages), "call_count": len(calls)}


@router.post("/runs/{run_id}/analyze")
async def analyze_run(
    run_id: str,
    verify_prices: bool = Form(True),
) -> dict:
    """
    Computes PnL against Kalshi/Polymarket for an existing DB run (no re-ingestion).
    Persists per-call results into `call_results`.
    """
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}

    request_id = uuid.uuid4().hex[:10]
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}

    with stage(log, "load_calls", request_id, run_id=run_id):
        call_rows = await get_calls_for_run(run_id)
    from pnl_analyzer.llm.types import BetCall

    calls = [
        BetCall(
            author=c.author,
            timestamp_utc=c.timestamp_utc,
            platform=c.platform,
            market_intent=c.market_intent,
            position_direction=c.position_direction,
            quoted_price=c.quoted_price,
            bet_size_units=c.bet_size_units,
            source_message_index=c.source_message_index,
        )
        for c in call_rows
    ]

    kalshi, polymarket = build_market_clients()
    with stage(log, "analyze", request_id, verify_prices=verify_prices, calls=len(calls)):
        report = await analyze_calls(
            calls=calls,
            kalshi=kalshi,
            polymarket=polymarket,
            verify_prices=verify_prices,
            logger=log,
            request_id=request_id,
        )

    with stage(log, "persist_results", request_id, run_id=run_id):
        await replace_results_for_run(run_id, report)
    return {"run_id": run_id, "call_count": len(calls), "verify_prices": verify_prices, "report": report}
