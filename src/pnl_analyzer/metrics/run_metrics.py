from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Iterable


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _max_drawdown(ts_pnls: list[tuple[str, float]]) -> float:
    if not ts_pnls:
        return 0.0
    ts_pnls.sort(key=lambda t: t[0])
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for _, pnl in ts_pnls:
        cum += float(pnl)
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    return float(max_dd)


def compute_pre_analysis_metrics(
    *,
    raw_text: str,
    export_timezone: str,
    messages: list[dict],
    candidates: list[Any],
    calls: list[Any],
    candidate_reason_counts: dict[str, int] | None = None,
) -> dict:
    raw_lines = raw_text.splitlines()

    # Parse metrics: we keep these simple/cheap; deeper parsing diagnostics can be added later.
    parse_metrics = {
        "raw_line_count": len(raw_lines),
        "parsed_message_count": len(messages),
        "attachments_block_count": raw_text.count("{Attachments}"),
        "reactions_block_count": raw_text.count("{Reactions}"),
    }

    # Candidate metrics.
    attached = 0
    source_idxs: set[int] = set()
    for c in candidates:
        try:
            if getattr(c, "attached_from_context", False):
                attached += 1
            source_idxs.add(int(getattr(c, "source_message_index")))
        except Exception:
            continue

    market_url_msgs = 0
    try:
        from pnl_analyzer.extraction.signals import extract_market_refs

        for i, m in enumerate(messages):
            if extract_market_refs(str(m.get("text") or "")):
                market_url_msgs += 1
    except Exception:
        market_url_msgs = None  # type: ignore[assignment]

    missed_with_url = None
    if isinstance(market_url_msgs, int):
        missed_with_url = 0
        try:
            from pnl_analyzer.extraction.signals import extract_market_refs

            for i, m in enumerate(messages):
                if i in source_idxs:
                    continue
                if extract_market_refs(str(m.get("text") or "")):
                    missed_with_url += 1
        except Exception:
            missed_with_url = None

    cand_metrics = {
        "candidate_count": len(candidates),
        "context_attached_count": attached,
        "candidate_reason_counts": candidate_reason_counts or {},
        "messages_with_market_url": market_url_msgs,
        "messages_with_market_url_but_no_candidate": missed_with_url,
    }

    # Extraction metrics.
    llm_norm = 0
    for call in calls:
        try:
            evidence = getattr(call, "evidence", None) or []
            if any(str(e) == "llm:normalized" for e in evidence):
                llm_norm += 1
        except Exception:
            continue
    extract_metrics = {
        "extracted_call_count": len(calls),
        "llm_normalized_call_count": llm_norm,
    }

    return {"parse": parse_metrics, "candidates": cand_metrics, "extraction": extract_metrics, "analysis": None}


def compute_analysis_metrics(report: dict) -> dict:
    bets = report.get("bets") or []
    status_counts = Counter()
    match_method = Counter()
    price_quality = Counter()
    upstream_errors = 0

    confidences: list[float] = []
    for b in bets:
        if not isinstance(b, dict):
            continue
        status = str(b.get("status") or "UNKNOWN")
        status_counts[status] += 1
        if status == "ERROR":
            upstream_errors += 1

        m = b.get("match") or {}
        if isinstance(m, dict):
            method = m.get("method")
            if method:
                match_method[str(method)] += 1
            c = _safe_float(m.get("confidence"))
            if c is not None and 0.0 <= c <= 1.0 and not math.isnan(c):
                confidences.append(c)

        p = b.get("price") or {}
        if isinstance(p, dict):
            q = p.get("quality")
            if q:
                price_quality[str(q)] += 1

    return {
        "status_counts": dict(status_counts),
        "match_method_counts": dict(match_method),
        "price_quality_counts": dict(price_quality),
        "match_confidence": {
            "count": len(confidences),
            "min": min(confidences) if confidences else None,
            "max": max(confidences) if confidences else None,
        },
        "upstream_error_count": upstream_errors,
    }


def compute_user_stats_from_report(report: dict) -> list[dict]:
    bets = report.get("bets") or []
    by_user: dict[str, list[dict]] = defaultdict(list)
    for b in bets:
        if not isinstance(b, dict):
            continue
        call = b.get("call") or {}
        if not isinstance(call, dict):
            continue
        author = str(call.get("author") or "")
        if not author:
            continue
        by_user[author].append(b)

    out: list[dict] = []
    for author, rows in by_user.items():
        ok = [r for r in rows if str(r.get("status") or "") == "OK"]
        pnls: list[float] = []
        rois: list[float] = []
        ts_pnls: list[tuple[str, float]] = []
        wins = 0
        for r in ok:
            pnl = _safe_float(r.get("net_pnl_usd"))
            if pnl is not None:
                pnls.append(float(pnl))
            roi = _safe_float(r.get("roi"))
            if roi is not None:
                rois.append(float(roi))
            call = r.get("call") or {}
            ts = str((call.get("timestamp_utc") if isinstance(call, dict) else "") or "")
            if ts and pnl is not None:
                ts_pnls.append((ts, float(pnl)))

            resolved = str(r.get("resolved_outcome") or "").upper()
            side = str(((call.get("position_direction") if isinstance(call, dict) else None) or "")).upper()
            if resolved and side and resolved == side:
                wins += 1

        profits = sum(p for p in pnls if p > 0)
        losses = sum(-p for p in pnls if p < 0)
        profit_factor = None
        if losses > 0:
            profit_factor = profits / losses
        elif profits > 0 and losses == 0 and pnls:
            profit_factor = float("inf")

        out.append(
            {
                "author": author,
                "bets": len(ok),
                "wins": wins,
                "win_rate": (wins / len(ok)) if ok else None,
                "net_pnl_usd": float(sum(pnls)) if pnls else 0.0,
                "avg_pnl_per_bet": (float(sum(pnls)) / len(ok)) if ok else None,
                "median_pnl_usd": _median(pnls),
                "profit_factor": profit_factor,
                "avg_roi": (sum(rois) / len(rois)) if rois else None,
                "max_drawdown_usd": _max_drawdown(ts_pnls),
            }
        )

    out.sort(key=lambda r: float(r.get("net_pnl_usd") or 0.0), reverse=True)
    return out


def equity_curve_from_report(report: dict, *, author: str | None = None) -> list[dict]:
    bets = report.get("bets") or []
    rows: list[tuple[str, float]] = []
    for b in bets:
        if not isinstance(b, dict):
            continue
        if str(b.get("status") or "") != "OK":
            continue
        call = b.get("call") or {}
        if not isinstance(call, dict):
            continue
        if author and str(call.get("author") or "") != author:
            continue
        ts = str(call.get("timestamp_utc") or "")
        pnl = _safe_float(b.get("net_pnl_usd"))
        if not ts or pnl is None:
            continue
        rows.append((ts, float(pnl)))

    rows.sort(key=lambda t: t[0])
    cum = 0.0
    out: list[dict] = []
    for ts, pnl in rows:
        cum += pnl
        out.append({"timestamp_utc": ts, "net_pnl_usd": pnl, "cum_net_pnl_usd": cum})
    return out


def compute_user_stats_from_rows(rows: Iterable[tuple[dict, dict | None]]) -> list[dict]:
    """
    rows: iterable of ({call}, {result}|None) dict-ish.
    Only includes status==OK rows for PnL-derived stats.
    """
    by_user: dict[str, list[tuple[str, float, float | None, str, str]]] = defaultdict(list)
    # (ts, pnl, roi, resolved_outcome, side)
    for call, res in rows:
        if not isinstance(call, dict) or not isinstance(res, dict):
            continue
        if str(res.get("status") or "") != "OK":
            continue
        author = str(call.get("author") or "")
        if not author:
            continue
        ts = str(call.get("timestamp_utc") or "")
        pnl = _safe_float(res.get("net_pnl_usd"))
        if pnl is None:
            continue
        roi = _safe_float(res.get("roi"))
        by_user[author].append(
            (
                ts,
                float(pnl),
                float(roi) if roi is not None else None,
                str(res.get("resolved_outcome") or "").upper(),
                str(call.get("position_direction") or "").upper(),
            )
        )

    out: list[dict] = []
    for author, vals in by_user.items():
        pnls = [v[1] for v in vals]
        rois = [v[2] for v in vals if v[2] is not None]
        wins = sum(1 for (_, _, _, resolved, side) in vals if resolved and side and resolved == side)

        profits = sum(p for p in pnls if p > 0)
        losses = sum(-p for p in pnls if p < 0)
        profit_factor = None
        if losses > 0:
            profit_factor = profits / losses
        elif profits > 0 and losses == 0 and pnls:
            profit_factor = float("inf")

        ts_pnls = [(v[0], v[1]) for v in vals]

        out.append(
            {
                "author": author,
                "bets": len(vals),
                "wins": wins,
                "win_rate": (wins / len(vals)) if vals else None,
                "net_pnl_usd": float(sum(pnls)),
                "avg_pnl_per_bet": (float(sum(pnls)) / len(vals)) if vals else None,
                "median_pnl_usd": _median(pnls),
                "profit_factor": profit_factor,
                "avg_roi": (sum(rois) / len(rois)) if rois else None,
                "max_drawdown_usd": _max_drawdown(ts_pnls),
            }
        )

    out.sort(key=lambda r: float(r.get("net_pnl_usd") or 0.0), reverse=True)
    return out


def equity_curve_from_rows(rows: Iterable[tuple[dict, dict | None]], *, author: str | None = None) -> list[dict]:
    pts: list[tuple[str, float]] = []
    for call, res in rows:
        if not isinstance(call, dict) or not isinstance(res, dict):
            continue
        if str(res.get("status") or "") != "OK":
            continue
        if author and str(call.get("author") or "") != author:
            continue
        ts = str(call.get("timestamp_utc") or "")
        pnl = _safe_float(res.get("net_pnl_usd"))
        if not ts or pnl is None:
            continue
        pts.append((ts, float(pnl)))
    pts.sort(key=lambda t: t[0])
    cum = 0.0
    out: list[dict] = []
    for ts, pnl in pts:
        cum += pnl
        out.append({"timestamp_utc": ts, "net_pnl_usd": pnl, "cum_net_pnl_usd": cum})
    return out
