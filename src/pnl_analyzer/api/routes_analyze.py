from __future__ import annotations

import logging
import os
import time
import uuid

from fastapi import APIRouter, BackgroundTasks, File, Form, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from pnl_analyzer.config import settings
from pnl_analyzer.db.persist import persist_raw_run, persist_run, persist_upload, replace_issues_for_run, set_run_status
from pnl_analyzer.db.queries import (
    delete_run_and_maybe_upload,
    fetch_call_results_for_run,
    get_calls_for_run,
    get_run,
    get_run_counts,
    list_bets_for_run,
    list_issues_for_run,
    list_runs,
    replace_results_for_run,
)
from pnl_analyzer.extraction.candidates import candidate_reasons, deterministic_betcall_from_candidate, generate_call_candidates
from pnl_analyzer.llm.factory import build_extractor
from pnl_analyzer.markets.factory import build_market_clients
from pnl_analyzer.parsing.discord_txt import parse_discord_txt
from pnl_analyzer.pnl.engine import analyze_calls
from pnl_analyzer.metrics.run_metrics import (
    compute_analysis_metrics,
    compute_pre_analysis_metrics,
    compute_user_stats_from_report,
    compute_user_stats_from_rows,
    equity_curve_from_report,
    equity_curve_from_rows,
)
from pnl_analyzer.uploads.store import store_upload_bytes
from pnl_analyzer.utils.stages import stage
from pnl_analyzer.utils.json_sanitize import sanitize_for_json
from pnl_analyzer.db.session import session_scope
from pnl_analyzer.db.models import Call, CallResult, Run

router = APIRouter(tags=["analyze"])
log = logging.getLogger("pnl_analyzer")


@router.get("/runs")
async def runs(limit: int = 20) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    return {"runs": await list_runs(limit=limit)}

@router.get("/runs/{run_id}")
async def run_detail(run_id: str) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}
    counts = await get_run_counts(run_id)
    return {
        "run_id": str(run.id),
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "source_filename": run.source_filename,
        "export_timezone": run.export_timezone,
        "verify_prices": run.verify_prices,
        "upload_id": str(run.upload_id) if getattr(run, "upload_id", None) else None,
        "status": getattr(run, "status", None),
        "error_text": getattr(run, "error_text", None),
        "parse_ms": getattr(run, "parse_ms", None),
        "extract_ms": getattr(run, "extract_ms", None),
        "analyze_ms": getattr(run, "analyze_ms", None),
        "metrics": getattr(run, "metrics_json", None),
        "settings_snapshot": getattr(run, "settings_snapshot", None),
        "message_count": (counts or {}).get("message_count"),
        "call_count": (counts or {}).get("call_count"),
    }


@router.get("/runs/{run_id}/metrics")
async def run_metrics(run_id: str) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}
    return {"run_id": str(run.id), "status": getattr(run, "status", None), "metrics": getattr(run, "metrics_json", None)}


@router.get("/runs/{run_id}/upload_preview")
async def run_upload_preview(run_id: str) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}
    # Prefer DB preview if present; fall back to file read.
    try:
        upload = getattr(run, "upload", None)
        if upload is not None and getattr(upload, "text_preview", None):
            return {"run_id": str(run.id), "preview": upload.text_preview}
        if upload is not None and getattr(upload, "storage_path", None):
            p = str(upload.storage_path)
            if os.path.exists(p):
                with open(p, "rb") as f:
                    raw = f.read(max(1, int(settings.upload_preview_chars)))
                return {"run_id": str(run.id), "preview": raw.decode("utf-8", errors="replace")}
    except Exception:
        pass
    return {"run_id": str(run.id), "preview": None}


@router.get("/runs/{run_id}/upload")
async def run_upload_download(run_id: str):
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}
    upload = getattr(run, "upload", None)
    if upload is None:
        return {"error": "upload not found for run", "run_id": run_id}
    p = str(getattr(upload, "storage_path", "") or "")
    if not p or not os.path.exists(p):
        return {"error": "upload file missing on disk", "run_id": run_id}
    fn = getattr(upload, "original_filename", None) or getattr(run, "source_filename", None) or "discord_export.txt"
    return FileResponse(path=p, media_type="text/plain", filename=fn)


@router.delete("/runs/{run_id}")
async def delete_run(run_id: str, delete_upload: bool = True) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    res = await delete_run_and_maybe_upload(run_id, delete_upload=delete_upload)
    if not res.get("deleted"):
        return {"error": res.get("error") or "delete failed", "run_id": run_id}

    storage_path = res.get("storage_path")
    if storage_path and isinstance(storage_path, str):
        try:
            if os.path.exists(storage_path):
                os.remove(storage_path)
        except Exception:
            # Best-effort file cleanup; DB is already consistent.
            pass
    return {"ok": True, "run_id": run_id, "upload_deleted": bool(res.get("upload_deleted"))}


async def _analyze_and_persist_run(*, run_id: str, verify_prices: bool) -> None:
    """
    Background analysis job: load calls from DB, run analysis, persist results + issues + metrics.
    """
    if not settings.database_url:
        return

    t0 = time.perf_counter()
    await set_run_status(run_id, status="ANALYZING", error_text=None)
    run = await get_run(run_id)
    if run is None:
        return

    try:
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
                source_message_index=getattr(c, "source_message_index", None),
                action=getattr(c, "action", None),
                market_ref=getattr(c, "market_ref", None),
                extraction_confidence=getattr(c, "extraction_confidence", 0.5) or 0.5,
                evidence=getattr(c, "evidence", None) or [],
            )
            for c in call_rows
        ]

        kalshi, polymarket = build_market_clients()
        snap = getattr(run, "settings_snapshot", None) or {}
        unit_notional_usd = None
        default_bet_units = None
        try:
            if isinstance(snap, dict):
                if snap.get("unit_notional_usd") is not None:
                    unit_notional_usd = float(snap.get("unit_notional_usd"))
                if snap.get("default_bet_units") is not None:
                    default_bet_units = float(snap.get("default_bet_units"))
        except Exception:
            unit_notional_usd = None
            default_bet_units = None
        report = await analyze_calls(
            calls=calls,
            kalshi=kalshi,
            polymarket=polymarket,
            verify_prices=verify_prices,
            unit_notional_usd=unit_notional_usd,
            default_bet_units=default_bet_units,
            logger=log,
            request_id=uuid.uuid4().hex[:10],
        )

        await replace_results_for_run(run_id, report)

        # Issues (review queue) keyed by DB call id order.
        bet_rows = report.get("bets") or []
        issues: list[dict] = []
        for call_row, bet_row in zip(call_rows, bet_rows):
            if not isinstance(bet_row, dict):
                continue
            st = str(bet_row.get("status") or "UNKNOWN")
            if st == "OK":
                continue
            issue_type = st
            if st == "ERROR":
                issue_type = "UPSTREAM_ERROR"
            issues.append({"call_id": int(call_row.id), "issue_type": issue_type, "details_json": bet_row})
        await replace_issues_for_run(run_id, issues)

        # Metrics merge (pre-analysis + analysis).
        metrics = dict(getattr(run, "metrics_json", None) or {})
        metrics["analysis"] = compute_analysis_metrics(report)
        # Helpful derived views for UI
        metrics["user_stats"] = compute_user_stats_from_report(report)
        metrics["equity_curve"] = equity_curve_from_report(report)

        await set_run_status(
            run_id,
            status="DONE",
            error_text=None,
            analyze_ms=int((time.perf_counter() - t0) * 1000),
            metrics_json=sanitize_for_json(metrics),
        )
    except Exception as e:
        await set_run_status(
            run_id,
            status="ERROR",
            error_text=str(e),
            analyze_ms=int((time.perf_counter() - t0) * 1000),
        )


@router.post("/runs/{run_id}/analyze_async")
async def analyze_async(
    run_id: str,
    background: BackgroundTasks,
    verify_prices: bool = Form(True),
    unit_notional_usd: float | None = Form(None),
    default_bet_units: float | None = Form(None),
) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}

    # Update sizing settings (used by background analyzer).
    snap = dict(getattr(run, "settings_snapshot", None) or {})
    if unit_notional_usd is not None:
        if float(unit_notional_usd) <= 0:
            return {"error": "unit_notional_usd must be > 0"}
        snap["unit_notional_usd"] = float(unit_notional_usd)
    if default_bet_units is not None:
        if float(default_bet_units) <= 0:
            return {"error": "default_bet_units must be > 0"}
        snap["default_bet_units"] = float(default_bet_units)
    if unit_notional_usd is not None or default_bet_units is not None:
        try:
            from pnl_analyzer.db.models import Run
            from pnl_analyzer.db.session import session_scope
            import uuid as _uuid

            rid = _uuid.UUID(run_id)
            async for session in session_scope():
                rr = await session.get(Run, rid)
                if rr is not None:
                    rr.settings_snapshot = sanitize_for_json(snap)
                    rr.verify_prices = bool(verify_prices)
                    await session.commit()
        except Exception:
            pass

    background.add_task(_analyze_and_persist_run, run_id=run_id, verify_prices=verify_prices)
    return {"run_id": run_id, "queued": True, "status": "ANALYZING"}


@router.post("/runs/{run_id}/rescale")
async def rescale_run_pnl(
    run_id: str,
    unit_notional_usd: float = Form(...),
    default_bet_units: float = Form(1.0),
) -> dict:
    """
    Fast path for changing "amount per bet" without re-running upstream analysis:
    - No LLM calls
    - No market matching / resolution lookups
    - No historical price queries

    Recomputes contracts/fees/net_pnl/roi for already-OK bets using stored entry_price/outcome.
    """
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}

    if float(unit_notional_usd) <= 0:
        return {"error": "unit_notional_usd must be > 0"}
    if float(default_bet_units) <= 0:
        return {"error": "default_bet_units must be > 0"}

    rid = uuid.UUID(run_id)
    unit_notional_usd_f = float(unit_notional_usd)
    default_bet_units_f = float(default_bet_units)

    # Local imports to avoid exporting internals broadly.
    from pnl_analyzer.pnl.engine import _kalshi_fee_usd, _pnl_for_binary_call

    async for session in session_scope():
        run = await session.get(Run, rid)
        if run is None:
            return {"error": "run not found", "run_id": run_id}

        snap = dict(getattr(run, "settings_snapshot", None) or {})
        snap["unit_notional_usd"] = unit_notional_usd_f
        snap["default_bet_units"] = default_bet_units_f
        run.settings_snapshot = sanitize_for_json(snap)

        q = (
            select(Call, CallResult)
            .join(CallResult, CallResult.call_id == Call.id, isouter=True)
            .where(Call.run_id == rid)
            .order_by(Call.id.asc())
        )
        res = await session.execute(q)

        updated = 0
        norm_rows: list[tuple[dict, dict | None]] = []

        for call_row, result_row in res.all():
            call_d = {
                "author": call_row.author,
                "timestamp_utc": call_row.timestamp_utc,
                "platform": call_row.platform,
                "market_intent": call_row.market_intent,
                "position_direction": call_row.position_direction,
            }

            if result_row is None:
                norm_rows.append((call_d, None))
                continue

            # Only OK rows have all data necessary to rescale accurately.
            if str(result_row.status or "") == "OK" and result_row.entry_price_used is not None and result_row.resolved_outcome:
                entry_price = float(result_row.entry_price_used)
                resolved_outcome = str(result_row.resolved_outcome)
                side = str(call_row.position_direction or "")

                contracts_new = unit_notional_usd_f * float(default_bet_units_f)

                gross = _pnl_for_binary_call(entry_price, resolved_outcome, side, float(contracts_new))

                fees_new = 0.0
                platform = str(call_row.platform or "").lower()
                if platform == "kalshi":
                    ticker = result_row.matched_market_id
                    if ticker:
                        fees_new = _kalshi_fee_usd(market_ticker=str(ticker), price=entry_price, contracts=float(contracts_new))
                    else:
                        # Best-effort: scale previous fees if we can't recompute (missing ticker).
                        try:
                            if result_row.contracts and result_row.fees_usd is not None and float(result_row.contracts) > 0:
                                fees_new = float(result_row.fees_usd) * (float(contracts_new) / float(result_row.contracts))
                        except Exception:
                            fees_new = 0.0
                elif settings.polymarket_fee_bps:
                    fees_new = (float(settings.polymarket_fee_bps) / 10000.0) * (entry_price * float(contracts_new))

                net = gross - float(fees_new)
                denom = entry_price * float(contracts_new)
                roi = (net / denom) if denom > 0 else None

                result_row.contracts = float(contracts_new)
                result_row.fees_usd = float(fees_new)
                result_row.net_pnl_usd = float(net)
                result_row.roi = None if roi is None else float(roi)

                if isinstance(result_row.debug_json, dict):
                    dj = dict(result_row.debug_json)
                    dj["contracts"] = float(contracts_new)
                    dj["fees_usd"] = float(fees_new)
                    dj["net_pnl_usd"] = float(net)
                    dj["roi"] = None if roi is None else float(roi)
                    result_row.debug_json = sanitize_for_json(dj)

                updated += 1

            res_d = {
                "status": result_row.status,
                "resolved_outcome": result_row.resolved_outcome,
                "net_pnl_usd": result_row.net_pnl_usd,
                "roi": result_row.roi,
            }
            norm_rows.append((call_d, res_d))

        metrics = dict(getattr(run, "metrics_json", None) or {})
        metrics["user_stats"] = compute_user_stats_from_rows(norm_rows)
        metrics["equity_curve"] = equity_curve_from_rows(norm_rows)
        run.metrics_json = sanitize_for_json(metrics)

        await session.commit()
        return {
            "ok": True,
            "run_id": run_id,
            "updated_results": updated,
            "unit_notional_usd": unit_notional_usd_f,
            "default_bet_units": default_bet_units_f,
        }


@router.get("/runs/{run_id}/issues")
async def run_issues(run_id: str) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}
    data = await list_issues_for_run(run_id)
    return {"run_id": run_id, "status": getattr(run, "status", None), **data}


@router.get("/runs/{run_id}/bets")
async def run_bets(
    run_id: str,
    limit: int = 50,
    offset: int = 0,
    author: str | None = None,
    platform: str | None = None,
    status: str | None = None,
    min_ts_utc: str | None = None,
    max_ts_utc: str | None = None,
    sort: str = "ts_desc",
) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}
    data = await list_bets_for_run(
        run_id,
        limit=limit,
        offset=offset,
        author=author,
        platform=platform,
        status=status,
        min_ts_utc=min_ts_utc,
        max_ts_utc=max_ts_utc,
        sort=sort,
    )
    return {"run_id": run_id, "status": getattr(run, "status", None), **data}


@router.get("/runs/{run_id}/report")
async def run_report(run_id: str, min_bets: int = 0) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}

    rows = await fetch_call_results_for_run(run_id)
    snap = getattr(run, "settings_snapshot", None) or {}
    unit_notional = settings.unit_notional_usd
    try:
        if isinstance(snap, dict) and snap.get("unit_notional_usd") is not None:
            unit_notional = float(snap.get("unit_notional_usd"))
    except Exception:
        unit_notional = settings.unit_notional_usd
    # Normalize to dict rows for metrics helpers.
    norm_rows: list[tuple[dict, dict | None]] = []
    for c, r in rows:
        call_d = {
            "author": c.author,
            "timestamp_utc": c.timestamp_utc,
            "platform": c.platform,
            "market_intent": c.market_intent,
            "position_direction": c.position_direction,
        }
        res_d = None
        if r is not None:
            res_d = {
                "status": r.status,
                "resolved_outcome": r.resolved_outcome,
                "net_pnl_usd": r.net_pnl_usd,
                "roi": r.roi,
            }
        norm_rows.append((call_d, res_d))

    user_stats = compute_user_stats_from_rows(norm_rows)
    if min_bets > 0:
        user_stats = [u for u in user_stats if int(u.get("bets") or 0) >= int(min_bets)]

    leaderboard = [
        {
            "author": u["author"],
            "bets": u["bets"],
            "wins": u["wins"],
            "win_rate": u["win_rate"],
            "net_pnl_usd": u["net_pnl_usd"],
            "net_units": (float(u["net_pnl_usd"]) / float(unit_notional)) if unit_notional else None,
        }
        for u in user_stats
    ]

    # Aggregate
    resolved_bets = sum(int(u.get("bets") or 0) for u in user_stats)
    total_wins = sum(int(u.get("wins") or 0) for u in user_stats)
    total_net = sum(float(u.get("net_pnl_usd") or 0.0) for u in user_stats)
    equity = equity_curve_from_rows(norm_rows)
    # Compute max drawdown from equity curve points
    peak = 0.0
    max_dd = 0.0
    for pt in equity:
        cum = float(pt.get("cum_net_pnl_usd") or 0.0)
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    aggregate = {
        "resolved_bets": resolved_bets,
        "win_rate": (total_wins / resolved_bets) if resolved_bets else None,
        "total_net_pnl_usd": total_net,
        "total_net_units": (total_net / float(unit_notional)) if unit_notional else None,
        "max_drawdown_usd": max_dd,
    }

    return {
        "run_id": run_id,
        "status": getattr(run, "status", None),
        "aggregate": aggregate,
        "leaderboard": leaderboard,
        "user_stats": user_stats,
        "equity_curve": equity,
    }


@router.get("/runs/{run_id}/users")
async def run_users(run_id: str, min_bets: int = 0) -> dict:
    rep = await run_report(run_id, min_bets=min_bets)
    if "error" in rep:
        return rep
    return {"run_id": run_id, "status": rep.get("status"), "user_stats": rep.get("user_stats")}


@router.get("/runs/{run_id}/users/{author}")
async def run_user_detail(run_id: str, author: str, limit: int = 50, offset: int = 0) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}
    run = await get_run(run_id)
    if run is None:
        return {"error": "run not found", "run_id": run_id}

    rows = await fetch_call_results_for_run(run_id)
    norm_rows: list[tuple[dict, dict | None]] = []
    for c, r in rows:
        call_d = {
            "author": c.author,
            "timestamp_utc": c.timestamp_utc,
            "platform": c.platform,
            "market_intent": c.market_intent,
            "position_direction": c.position_direction,
        }
        res_d = None
        if r is not None:
            res_d = {"status": r.status, "resolved_outcome": r.resolved_outcome, "net_pnl_usd": r.net_pnl_usd, "roi": r.roi}
        norm_rows.append((call_d, res_d))

    user_stats = [u for u in compute_user_stats_from_rows(norm_rows) if u.get("author") == author]
    equity = equity_curve_from_rows(norm_rows, author=author)
    bets = await list_bets_for_run(run_id, limit=limit, offset=offset, author=author, sort="ts_desc")
    return {
        "run_id": run_id,
        "status": getattr(run, "status", None),
        "author": author,
        "user": user_stats[0] if user_stats else None,
        "equity_curve": equity,
        **bets,
    }


@router.post("/runs")
async def create_run(
    background: BackgroundTasks,
    file: UploadFile = File(...),
    export_timezone: str | None = Form(None),
    verify_prices: bool = Form(True),
    auto_analyze: bool = Form(True),
) -> dict:
    if not settings.database_url:
        return {"error": "DATABASE_URL not set"}

    t0 = time.perf_counter()
    content_bytes = await file.read()
    stored = store_upload_bytes(upload_store_dir=settings.upload_store_dir, original_filename=file.filename, content=content_bytes)
    preview = content_bytes[: max(0, int(settings.upload_preview_chars))].decode("utf-8", errors="replace") if settings.upload_preview_chars else None
    upload_id = await persist_upload(
        original_filename=file.filename,
        content_sha256=stored.sha256,
        byte_size=stored.byte_size,
        mime_type=file.content_type,
        storage_path=stored.storage_path,
        text_preview=preview,
    )

    tz = export_timezone or settings.default_export_timezone

    parse_t0 = time.perf_counter()
    content = content_bytes.decode("utf-8", errors="replace")
    messages = parse_discord_txt(content, export_timezone=tz)
    parse_ms = int((time.perf_counter() - parse_t0) * 1000)

    extract_t0 = time.perf_counter()
    candidates = generate_call_candidates(messages)
    reason_counts: dict[str, int] = {}
    for c in candidates:
        try:
            rs = candidate_reasons(
                str(getattr(c, "message", {}).get("text") or ""),
                list(getattr(c, "market_refs", None) or []),
                getattr(c, "platform_hint", None),
                getattr(c, "side_hint", None),
                getattr(c, "action_hint", None),
                getattr(c, "odds_block", None),
                getattr(c, "inline_price", None),
            )
            for r in rs:
                reason_counts[r] = reason_counts.get(r, 0) + 1
        except Exception:
            continue

    extractor = build_extractor()
    calls = await extractor.extract_bets(messages)
    extract_ms = int((time.perf_counter() - extract_t0) * 1000)

    metrics = compute_pre_analysis_metrics(
        raw_text=content,
        export_timezone=tz,
        messages=messages,
        candidates=candidates,
        calls=calls,
        candidate_reason_counts=reason_counts,
    )

    # Store messages/calls; analysis is a separate job.
    settings_snapshot = {
        "llm_provider": settings.llm_provider,
        "openai_model": settings.openai_model,
        "openrouter_model": settings.openrouter_model,
        "verify_prices": verify_prices,
        "export_timezone": tz,
        "upstream_concurrency": settings.upstream_concurrency,
        "llm_concurrency": settings.llm_concurrency,
        "unit_notional_usd": settings.unit_notional_usd,
        "default_bet_units": 1.0,
    }

    run_id = await persist_run(
        source_filename=file.filename,
        export_timezone=tz,
        verify_prices=verify_prices,
        messages=messages,
        calls=[c.model_dump() for c in calls],
        report=None,
        upload_id=upload_id,
        status="EXTRACTED",
        app_version="0.1.0",
        settings_snapshot=settings_snapshot,
        parse_ms=parse_ms,
        extract_ms=extract_ms,
        metrics_json=metrics,
    )

    if auto_analyze:
        background.add_task(_analyze_and_persist_run, run_id=run_id, verify_prices=verify_prices)
        status = "ANALYZING"
    else:
        status = "EXTRACTED"

    return {
        "run_id": run_id,
        "upload_id": upload_id,
        "status": status,
        "message_count": len(messages),
        "call_count": len(calls),
        "duration_ms": int((time.perf_counter() - t0) * 1000),
    }


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
            action=getattr(c, "action", None),
            market_ref=getattr(c, "market_ref", None),
            extraction_confidence=getattr(c, "extraction_confidence", 0.5) or 0.5,
            evidence=getattr(c, "evidence", None) or [],
        )
        for c in call_rows
    ]

    kalshi, polymarket = build_market_clients()
    snap = getattr(run, "settings_snapshot", None) or {}
    unit_notional_usd = None
    default_bet_units = None
    try:
        if isinstance(snap, dict):
            if snap.get("unit_notional_usd") is not None:
                unit_notional_usd = float(snap.get("unit_notional_usd"))
            if snap.get("default_bet_units") is not None:
                default_bet_units = float(snap.get("default_bet_units"))
    except Exception:
        unit_notional_usd = None
        default_bet_units = None
    with stage(log, "analyze", request_id, verify_prices=verify_prices, calls=len(calls)):
        report = await analyze_calls(
            calls=calls,
            kalshi=kalshi,
            polymarket=polymarket,
            verify_prices=verify_prices,
            unit_notional_usd=unit_notional_usd,
            default_bet_units=default_bet_units,
            logger=log,
            request_id=request_id,
        )

    with stage(log, "persist_results", request_id, run_id=run_id):
        await replace_results_for_run(run_id, report)
    return {"run_id": run_id, "call_count": len(calls), "verify_prices": verify_prices, "report": report}
