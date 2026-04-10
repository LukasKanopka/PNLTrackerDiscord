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
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS calls ADD COLUMN IF NOT EXISTS source_message_index integer;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS contracts double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS fees_usd double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS net_pnl_usd double precision;")
        await conn.exec_driver_sql("ALTER TABLE IF EXISTS call_results ADD COLUMN IF NOT EXISTS roi double precision;")


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

        session.add_all([Message(run_id=run.id, **m) for m in messages])
        session.add_all([Call(run_id=run.id, **c) for c in calls])
        await session.flush()

        if report is not None:
            # Attach results by call index order (best-effort)
            bet_results = report.get("bets") or []
            for call_row, bet_row in zip(run.calls, bet_results):
                status = bet_row.get("status") or "ERROR"
                market = bet_row.get("market") or {}
                pp = bet_row.get("price_point") or {}
                session.add(
                    CallResult(
                        call_id=call_row.id,
                        status=status,
                        matched_market_id=market.get("id"),
                        matched_market_title=market.get("title"),
                        match_confidence=market.get("confidence"),
                        resolved_outcome=bet_row.get("resolved_outcome"),
                        entry_price_used=bet_row.get("entry_price_used"),
                        price_source=pp.get("source"),
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
