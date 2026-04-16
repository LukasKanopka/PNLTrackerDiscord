from __future__ import annotations

import uuid
from sqlalchemy import delete
from sqlalchemy import select

from pnl_analyzer.db.models import Call, CallIssue, CallResult, Message, Run, Upload
from pnl_analyzer.db.session import session_scope
from pnl_analyzer.utils.json_sanitize import sanitize_for_json


async def ensure_schema() -> None:
    # Lightweight dev-mode schema creation (v1). For production, use Alembic.
    from pnl_analyzer.db.session import get_engine
    from pnl_analyzer.db.base import Base

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Minimal "auto-migration" for dev: add newer columns if they don't exist.
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS original_filename varchar(512);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS content_sha256 varchar(64);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS byte_size integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS mime_type varchar(128);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS storage_path varchar(1024);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS text_preview text;")

        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS upload_id uuid;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS status varchar(32) DEFAULT 'INGESTED';")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS error_text text;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS app_version varchar(64);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS settings_snapshot json;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS parse_ms integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS extract_ms integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS analyze_ms integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS runs ADD COLUMN IF NOT EXISTS metrics_json json;")

        # Allow long-form market intents (some calls include the full analysis text).
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ALTER COLUMN market_intent TYPE text;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS source_message_index integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS call_index integer DEFAULT 0;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ALTER COLUMN quoted_price DROP NOT NULL;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS action varchar(16);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS market_ref json;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS extraction_confidence double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS evidence json;")

        await conn.exec_driver_sql("ALTER TABLE IF EXISTS messages ADD COLUMN IF NOT EXISTS message_index integer DEFAULT 0;")

        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS contracts double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS fees_usd double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS net_pnl_usd double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS roi double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS match_method varchar(32);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS price_quality varchar(32);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS price_ts_utc varchar(32);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS debug_json json;")

        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_issues ADD COLUMN IF NOT EXISTS issue_type varchar(64);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_issues ADD COLUMN IF NOT EXISTS details_json json;")


async def persist_run(
    *,
    source_filename: str | None,
    export_timezone: str,
    verify_prices: bool,
    messages: list[dict],
    calls: list[dict],
    report: dict | None,
    upload_id: str | None = None,
    status: str | None = None,
    app_version: str | None = None,
    settings_snapshot: dict | None = None,
    parse_ms: int | None = None,
    extract_ms: int | None = None,
    analyze_ms: int | None = None,
    metrics_json: dict | None = None,
) -> str:
    async for session in session_scope():
        up_id = None
        if upload_id:
            try:
                up_id = uuid.UUID(str(upload_id))
            except Exception:
                up_id = None
        run = Run(
            source_filename=source_filename,
            export_timezone=export_timezone,
            verify_prices=verify_prices,
            upload_id=up_id,
            status=status or ("DONE" if report is not None else "EXTRACTED"),
            app_version=app_version,
            settings_snapshot=sanitize_for_json(settings_snapshot) if settings_snapshot is not None else None,
            parse_ms=parse_ms,
            extract_ms=extract_ms,
            analyze_ms=analyze_ms,
            metrics_json=sanitize_for_json(metrics_json) if metrics_json is not None else None,
        )
        session.add(run)
        await session.flush()

        message_rows = [Message(run_id=run.id, message_index=i, **m) for i, m in enumerate(messages)]
        call_rows = [Call(run_id=run.id, call_index=i, **c) for i, c in enumerate(calls)]
        session.add_all(message_rows)
        session.add_all(call_rows)
        await session.flush()

        if report is not None:
            # Attach results by call index order (best-effort)
            bet_results = report.get("bets") or []
            # Avoid async lazy-loading relationships (can raise greenlet_spawn errors).
            for call_row, bet_row in zip(call_rows, bet_results):
                status = bet_row.get("status") or "ERROR"
                match = bet_row.get("match") or {}
                market = bet_row.get("market") or {}
                pp = bet_row.get("price_point") or {}
                price = bet_row.get("price") or {}
                debug_json = bet_row if isinstance(bet_row, dict) else {"raw": str(bet_row)}
                session.add(
                    CallResult(
                        call_id=call_row.id,
                        status=status,
                        matched_market_id=(match.get("market_id") or market.get("id")),
                        matched_market_title=(match.get("market_title") or market.get("title")),
                        match_confidence=(match.get("confidence") or market.get("confidence")),
                        match_method=match.get("method"),
                        resolved_outcome=bet_row.get("resolved_outcome"),
                        entry_price_used=bet_row.get("entry_price_used"),
                        price_source=(price.get("source") or pp.get("source")),
                        price_quality=price.get("quality"),
                        price_ts_utc=price.get("ts_used"),
                        contracts=bet_row.get("contracts"),
                        fees_usd=bet_row.get("fees_usd"),
                        net_pnl_usd=bet_row.get("net_pnl_usd"),
                        roi=bet_row.get("roi"),
                        debug_json=debug_json,
                    )
                )

        await session.commit()
        return str(run.id)


async def persist_raw_run(
    *,
    source_filename: str | None,
    export_timezone: str,
    messages: list[dict],
    calls: list[dict],
) -> str:
    return await persist_run(
        source_filename=source_filename,
        export_timezone=export_timezone,
        verify_prices=False,
        messages=messages,
        calls=calls,
        report=None,
    )


async def persist_upload(
    *,
    original_filename: str | None,
    content_sha256: str,
    byte_size: int,
    mime_type: str | None,
    storage_path: str,
    text_preview: str | None,
) -> str:
    async for session in session_scope():
        up = Upload(
            original_filename=original_filename,
            content_sha256=content_sha256,
            byte_size=int(byte_size),
            mime_type=mime_type,
            storage_path=storage_path,
            text_preview=text_preview,
        )
        session.add(up)
        await session.commit()
        return str(up.id)


async def set_run_status(
    run_id: str,
    *,
    status: str,
    error_text: str | None = None,
    analyze_ms: int | None = None,
    metrics_json: dict | None = None,
) -> None:
    async for session in session_scope():
        try:
            rid = uuid.UUID(run_id)
        except Exception:
            return
        run = await session.get(Run, rid)
        if run is None:
            return
        run.status = status
        run.error_text = error_text
        if analyze_ms is not None:
            run.analyze_ms = analyze_ms
        if metrics_json is not None:
            run.metrics_json = sanitize_for_json(metrics_json)
        await session.commit()


async def replace_results_for_run_with_debug(run_id: str, report: dict) -> None:
    """
    Replace call_results for a run, and persist per-row debug_json.
    """
    from pnl_analyzer.db.queries import replace_results_for_run

    # Backward-compatible wrapper; the core implementation now stores debug_json.
    await replace_results_for_run(run_id, report)


async def replace_issues_for_run(run_id: str, issues: list[dict]) -> None:
    async for session in session_scope():
        try:
            rid = uuid.UUID(run_id)
        except Exception:
            return
        run = await session.get(Run, rid)
        if run is None:
            return
        await session.execute(delete(CallIssue).where(CallIssue.run_id == rid))
        for it in issues:
            session.add(
                CallIssue(
                    run_id=rid,
                    call_id=it.get("call_id"),
                    issue_type=str(it.get("issue_type") or "UNKNOWN"),
                    details_json=it.get("details_json"),
                )
            )
        await session.commit()
