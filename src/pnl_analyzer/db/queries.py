from __future__ import annotations

import uuid

from sqlalchemy import delete, select
from sqlalchemy import func
from sqlalchemy.orm import selectinload

from pnl_analyzer.db.models import Call, CallIssue, CallResult, Message, Run
from pnl_analyzer.db.session import session_scope


async def get_run(run_id: str) -> Run | None:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        res = await session.execute(select(Run).options(selectinload(Run.upload)).where(Run.id == rid))
        return res.scalar_one_or_none()


async def get_calls_for_run(run_id: str) -> list[Call]:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        res = await session.execute(select(Call).where(Call.run_id == rid).order_by(Call.call_index.asc(), Call.id.asc()))
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
            match = bet_row.get("match") or {}
            market = bet_row.get("market") or {}
            pp = bet_row.get("price_point") or {}
            price = bet_row.get("price") or {}
            session.add(
                CallResult(
                    call_id=call_id,
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
                    debug_json=bet_row if isinstance(bet_row, dict) else {"raw": str(bet_row)},
                )
            )

        await session.commit()


async def list_runs(limit: int = 20) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    async for session in session_scope():
        res = await session.execute(select(Run).order_by(Run.created_at.desc()).limit(limit))
        runs = list(res.scalars().all())

        out: list[dict] = []
        for r in runs:
            msg_count = await session.scalar(select(func.count()).select_from(Message).where(Message.run_id == r.id))
            call_count = await session.scalar(select(func.count()).select_from(Call).where(Call.run_id == r.id))
            out.append(
                {
                    "run_id": str(r.id),
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "source_filename": r.source_filename,
                    "export_timezone": r.export_timezone,
                    "verify_prices": r.verify_prices,
                    "upload_id": str(getattr(r, "upload_id", None)) if getattr(r, "upload_id", None) else None,
                    "status": getattr(r, "status", None),
                    "error_text": getattr(r, "error_text", None),
                    "parse_ms": getattr(r, "parse_ms", None),
                    "extract_ms": getattr(r, "extract_ms", None),
                    "analyze_ms": getattr(r, "analyze_ms", None),
                    "message_count": int(msg_count or 0),
                    "call_count": int(call_count or 0),
                }
            )
        return out


async def get_run_counts(run_id: str) -> dict | None:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        run = await session.get(Run, rid)
        if run is None:
            return None
        msg_count = await session.scalar(select(func.count()).select_from(Message).where(Message.run_id == rid))
        call_count = await session.scalar(select(func.count()).select_from(Call).where(Call.run_id == rid))
        return {"message_count": int(msg_count or 0), "call_count": int(call_count or 0)}


async def list_bets_for_run(
    run_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
    author: str | None = None,
    platform: str | None = None,
    status: str | None = None,
    min_ts_utc: str | None = None,
    max_ts_utc: str | None = None,
    sort: str = "ts_desc",
) -> dict:
    rid = uuid.UUID(run_id)
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))

    async for session in session_scope():
        q = (
            select(Call, CallResult)
            .join(CallResult, CallResult.call_id == Call.id, isouter=True)
            .where(Call.run_id == rid)
        )
        if author:
            q = q.where(Call.author == author)
        if platform:
            q = q.where(func.lower(Call.platform) == platform.lower())
        if status:
            q = q.where(CallResult.status == status)  # type: ignore[comparison-overlap]
        if min_ts_utc:
            q = q.where(Call.timestamp_utc >= min_ts_utc)
        if max_ts_utc:
            q = q.where(Call.timestamp_utc <= max_ts_utc)

        if sort == "net_pnl_desc":
            q = q.order_by(CallResult.net_pnl_usd.desc().nullslast(), Call.timestamp_utc.desc())
        elif sort == "net_pnl_asc":
            q = q.order_by(CallResult.net_pnl_usd.asc().nullslast(), Call.timestamp_utc.desc())
        elif sort == "ts_asc":
            q = q.order_by(Call.timestamp_utc.asc())
        else:
            q = q.order_by(Call.timestamp_utc.desc())

        total = await session.scalar(select(func.count()).select_from(q.subquery()))
        res = await session.execute(q.limit(limit).offset(offset))

        rows: list[dict] = []
        for c, r in res.all():
            rows.append(
                {
                    "call_id": int(c.id),
                    "call": {
                        "author": c.author,
                        "timestamp_utc": c.timestamp_utc,
                        "platform": c.platform,
                        "market_intent": c.market_intent,
                        "position_direction": c.position_direction,
                        "quoted_price": c.quoted_price,
                        "bet_size_units": c.bet_size_units,
                        "source_message_index": c.source_message_index,
                        "action": c.action,
                        "market_ref": c.market_ref,
                        "extraction_confidence": c.extraction_confidence,
                        "evidence": c.evidence,
                    },
                    "result": None
                    if r is None
                    else {
                        "status": r.status,
                        "matched_market_id": r.matched_market_id,
                        "matched_market_title": r.matched_market_title,
                        "match_confidence": r.match_confidence,
                        "match_method": r.match_method,
                        "resolved_outcome": r.resolved_outcome,
                        "entry_price_used": r.entry_price_used,
                        "price_source": r.price_source,
                        "price_quality": r.price_quality,
                        "price_ts_utc": r.price_ts_utc,
                        "contracts": r.contracts,
                        "fees_usd": r.fees_usd,
                        "net_pnl_usd": r.net_pnl_usd,
                        "roi": r.roi,
                        "debug_json": r.debug_json,
                    },
                }
            )

        return {"total": int(total or 0), "limit": limit, "offset": offset, "bets": rows}


async def list_issues_for_run(run_id: str) -> dict:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        q = (
            select(CallIssue, Call)
            .join(Call, Call.id == CallIssue.call_id, isouter=True)
            .where(CallIssue.run_id == rid)
            .order_by(CallIssue.id.asc())
        )
        res = await session.execute(q)
        items: list[dict] = []
        counts: dict[str, int] = {}
        for iss, call in res.all():
            t = str(getattr(iss, "issue_type", None) or "UNKNOWN")
            counts[t] = int(counts.get(t, 0) + 1)
            items.append(
                {
                    "issue_id": int(iss.id),
                    "issue_type": t,
                    "call_id": int(iss.call_id) if iss.call_id is not None else None,
                    "details_json": iss.details_json,
                    "call": None
                    if call is None
                    else {
                        "author": call.author,
                        "timestamp_utc": call.timestamp_utc,
                        "platform": call.platform,
                        "market_intent": call.market_intent,
                        "position_direction": call.position_direction,
                    },
                }
            )
        return {"counts": counts, "issues": items}


async def fetch_call_results_for_run(run_id: str) -> list[tuple[Call, CallResult | None]]:
    rid = uuid.UUID(run_id)
    async for session in session_scope():
        q = (
            select(Call, CallResult)
            .join(CallResult, CallResult.call_id == Call.id, isouter=True)
            .where(Call.run_id == rid)
            .order_by(Call.timestamp_utc.asc())
        )
        res = await session.execute(q)
        return [(c, r) for (c, r) in res.all()]
