from __future__ import annotations

import uuid

from sqlalchemy import delete, select

from pnl_analyzer.db.models import Call, CallResult, Run
from pnl_analyzer.db.session import session_scope


async def get_run(run_id: str) -> Run | None:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        res = await session.execute(select(Run).where(Run.id == rid))
        return res.scalar_one_or_none()


async def get_calls_for_run(run_id: str) -> list[Call]:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        res = await session.execute(select(Call).where(Call.run_id == rid).order_by(Call.id.asc()))
        return list(res.scalars().all())


async def replace_results_for_run(run_id: str, report: dict) -> None:
    rid = uuid.UUID(run_id)
    bet_results = report.get("bets") or []

    async for session in session_scope():
        res = await session.execute(select(Call.id).where(Call.run_id == rid).order_by(Call.id.asc()))
        call_ids = list(res.scalars().all())

        # Drop existing results for those calls, then insert fresh.
        if call_ids:
            await session.execute(delete(CallResult).where(CallResult.call_id.in_(call_ids)))

        for call_id, bet_row in zip(call_ids, bet_results):
            status = bet_row.get("status") or "ERROR"
            market = bet_row.get("market") or {}
            pp = bet_row.get("price_point") or {}
            session.add(
                CallResult(
                    call_id=call_id,
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

