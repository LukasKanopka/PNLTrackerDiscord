from __future__ import annotations

from sqlalchemy import select

from pnl_analyzer.db.models import Call, CallResult, Message, Run
from pnl_analyzer.db.session import session_scope


async def ensure_schema() -> None:
    # Lightweight dev-mode schema creation (v1). For production, use Alembic.
    from pnl_analyzer.db.session import get_engine
    from pnl_analyzer.db.base import Base

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Minimal "auto-migration" for dev: add newer columns if they don't exist.
        # Allow long-form market intents (some calls include the full analysis text).
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ALTER COLUMN market_intent TYPE text;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS source_message_index integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ALTER COLUMN quoted_price DROP NOT NULL;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS action varchar(16);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS market_ref json;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS extraction_confidence double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS evidence json;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS contracts double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS fees_usd double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS net_pnl_usd double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS roi double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS match_method varchar(32);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS price_quality varchar(32);")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS price_ts_utc varchar(32);")


async def persist_run(
    *,
    source_filename: str | None,
    export_timezone: str,
    verify_prices: bool,
    messages: list[dict],
    calls: list[dict],
    report: dict | None,
) -> str:
    async for session in session_scope():
        run = Run(source_filename=source_filename, export_timezone=export_timezone, verify_prices=verify_prices)
        session.add(run)
        await session.flush()

        message_rows = [Message(run_id=run.id, **m) for m in messages]
        call_rows = [Call(run_id=run.id, **c) for c in calls]
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
