from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_CEILING
import time
import logging
import asyncio

from pnl_analyzer.config import settings
from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.markets.base import MarketClient
from pnl_analyzer.utils.retry import UpstreamHTTPError
from pnl_analyzer.prices.cache import DBPriceCache, InMemoryPriceCache
from pnl_analyzer.utils.time import to_unix_minute


def _pnl_for_binary_call(entry_price: float, resolved_outcome: str, side: str, notional: float) -> float:
    """
    Contract pays $1 if correct, $0 if wrong. Cost is entry_price per $1 payout.
    """
    win = resolved_outcome.upper() == side.upper()
    payout = notional if win else 0.0
    cost = entry_price * notional
    return payout - cost


def _ceil_to_cent(x: float) -> float:
    d = Decimal(str(x))
    return float(d.quantize(Decimal("0.01"), rounding=ROUND_CEILING))


def _kalshi_fee_usd(*, market_ticker: str, price: float, contracts: float) -> float:
    # Fee schedule (Feb 2026) is multiplier * C * P * (1-P), rounded up to next cent.
    # We assume taker fills unless configured otherwise.
    liquidity = (settings.kalshi_assume_liquidity or "taker").lower()
    if market_ticker.upper().startswith(("INX", "NASDAQ100")):
        mult = settings.kalshi_index_fee_multiplier
    elif liquidity == "maker":
        mult = settings.kalshi_maker_fee_multiplier
    else:
        mult = settings.kalshi_taker_fee_multiplier
    # Use Decimal to avoid float "1.7500000002" -> 1.76 artifacts when ceiling.
    p = Decimal(str(price))
    c = Decimal(str(contracts))
    m = Decimal(str(mult))
    fee = m * c * p * (Decimal("1") - p)
    return float(fee.quantize(Decimal("0.01"), rounding=ROUND_CEILING))


async def _resolve_match(
    *,
    call: BetCall,
    kalshi: MarketClient,
    polymarket: MarketClient,
) -> tuple[dict | None, str | None]:
    """
    Returns (match_dict, method) where match_dict matches MarketMatch-ish shape:
    {platform, market_id, market_title, confidence, method}
    """
    platform = (call.platform or "").lower()
    client = kalshi if platform == "kalshi" else polymarket

    # Deterministic from market_ref when possible.
    mr = call.market_ref
    if isinstance(mr, dict) and mr.get("options") and isinstance(mr.get("options"), list):
        # Try options in order; prefer exact-platform matches.
        opts = [o for o in mr.get("options") if isinstance(o, dict)]
        mr = None
        for o in opts:
            if (o.get("platform") or "").lower() == platform:
                mr = o
                break
        if mr is None and opts:
            mr = opts[0]

    if isinstance(mr, dict):
        if platform == "kalshi" and hasattr(client, "resolve_from_market_ref"):
            try:
                resolved = await getattr(client, "resolve_from_market_ref")(mr, intent=call.market_intent, ts_utc=call.timestamp_utc)
                if resolved:
                    method = "ticker" if isinstance(mr.get("ticker"), str) and mr.get("ticker") else "url"
                    match_dict = (
                        {
                            "platform": "kalshi",
                            "market_id": resolved.market_id,
                            "market_title": resolved.market_title,
                            "confidence": resolved.confidence,
                            "candidates": getattr(resolved, "candidates", None),
                        },
                        method,
                    )
                    # If the URL/ticker resolution is ambiguous, attempt a timestamp-aware search fallback
                    # and accept it only if it is clearly higher confidence.
                    cands = match_dict[0].get("candidates")
                    if isinstance(cands, list) and len(cands) > 1 and float(match_dict[0].get("confidence") or 0.0) < 0.35:
                        fb = await client.match_market(call.market_intent, call.timestamp_utc)
                        if fb and fb.market_id and float(fb.confidence) >= 0.5 and float(fb.confidence) > float(match_dict[0].get("confidence") or 0.0) + 0.15:
                            return (
                                {"platform": "kalshi", "market_id": fb.market_id, "market_title": fb.market_title, "confidence": fb.confidence},
                                "search",
                            )
                    return match_dict
            except Exception:
                pass
        if platform == "polymarket" and hasattr(client, "resolve_from_market_ref"):
            try:
                resolved = await getattr(client, "resolve_from_market_ref")(mr, intent=call.market_intent, ts_utc=call.timestamp_utc)
                if resolved:
                    return (
                        {
                            "platform": "polymarket",
                            "market_id": resolved.market_id,
                            "market_title": resolved.market_title,
                            "confidence": resolved.confidence,
                            "candidates": getattr(resolved, "candidates", None),
                        },
                        "url",
                    )
            except Exception:
                pass

    # Fallback fuzzy search
    match = await client.match_market(call.market_intent, call.timestamp_utc)
    if not match:
        return None, None
    return (
        {
            "platform": platform,
            "market_id": match.market_id,
            "market_title": match.market_title,
            "confidence": match.confidence,
            "candidates": getattr(match, "candidates", None),
        },
        "search",
    )


async def analyze_calls(
    calls: list[BetCall],
    kalshi: MarketClient,
    polymarket: MarketClient,
    verify_prices: bool,
    logger: logging.Logger | None = None,
    request_id: str | None = None,
) -> dict:
    t0 = time.perf_counter()
    unit_notional = settings.unit_notional_usd
    price_cache = DBPriceCache() if settings.database_url else InMemoryPriceCache()
    upstream_sem = asyncio.Semaphore(max(1, int(settings.upstream_concurrency or 10)))

    per_bet: list[dict] = []
    by_user = defaultdict(lambda: {"bets": 0, "wins": 0, "net_pnl": 0.0})
    resolved_rows: list[tuple[str, float]] = []

    if logger is not None and request_id is not None:
        logger.info("[%s] pnl:start calls=%s verify_prices=%s", request_id, len(calls), verify_prices)

    async def _process_one(call_idx: int, call: BetCall) -> tuple[int, dict]:
        async with upstream_sem:
            platform = (call.platform or "").lower()
            client = kalshi if platform == "kalshi" else polymarket

            try:
                match_dict, method = await _resolve_match(call=call, kalshi=kalshi, polymarket=polymarket)
            except UpstreamHTTPError as e:
                return call_idx, {"call": call.model_dump(), "status": "ERROR", "error": f"match upstream {e.status_code}: {e}"}
            except Exception as e:
                return call_idx, {"call": call.model_dump(), "status": "ERROR", "error": f"match error: {e}"}

            if not match_dict:
                return call_idx, {"call": call.model_dump(), "status": "UNMATCHED", "match": None, "price": None}

            # If the resolver surfaced multiple candidates but couldn't disambiguate confidently,
            # preserve the candidate set and avoid computing PnL (prevents wrong tickers/side selection).
            cands = match_dict.get("candidates")
            if isinstance(cands, list) and len(cands) > 1 and float(match_dict.get("confidence") or 0.0) < 0.35:
                return (
                    call_idx,
                    {
                        "call": call.model_dump(),
                        "match": {**match_dict, "method": method},
                        "status": "AMBIGUOUS_MARKET",
                    },
                )

            market_id = str(match_dict.get("market_id") or "")
            try:
                verified = await client.get_verified_market(market_id)
            except UpstreamHTTPError as e:
                # Kalshi URLs/tickers can refer to a market family/event; if direct lookup 404s, fall back to fuzzy search.
                if platform == "kalshi" and e.status_code == 404 and method in ("ticker", "url"):
                    try:
                        fb = await client.match_market(call.market_intent, call.timestamp_utc)
                        if fb:
                            match_dict = {
                                "platform": "kalshi",
                                "market_id": fb.market_id,
                                "market_title": fb.market_title,
                                "confidence": fb.confidence,
                            }
                            method = "search"
                            market_id = fb.market_id
                            verified = await client.get_verified_market(market_id)
                        else:
                            raise e
                    except Exception:
                        return (
                            call_idx,
                            {
                                "call": call.model_dump(),
                                "match": {**match_dict, "method": method},
                                "status": "ERROR",
                                "error": f"get_verified_market upstream {e.status_code}: {e}",
                            },
                        )
                else:
                    return (
                        call_idx,
                        {
                            "call": call.model_dump(),
                            "match": {**match_dict, "method": method},
                            "status": "ERROR",
                            "error": f"get_verified_market upstream {e.status_code}: {e}",
                        },
                    )
            except Exception as e:
                return (
                    call_idx,
                    {
                        "call": call.model_dump(),
                        "match": {**match_dict, "method": method},
                        "status": "ERROR",
                        "error": f"get_verified_market error: {e}",
                    },
                )

            if not verified.resolved or not verified.resolved_outcome:
                return (
                    call_idx,
                    {
                        "call": call.model_dump(),
                        "match": {**match_dict, "method": method, "market_title": verified.market_title or match_dict.get("market_title")},
                        "status": "PENDING",
                    },
                )

            # Entry price selection: historical (cached) -> quoted -> missing.
            side = call.position_direction
            minute_ts = to_unix_minute(call.timestamp_utc)
            entry_price = None
            price_point = None
            price_quality = "MISSING"
            price_source = None

            if verify_prices:
                cached = await price_cache.get(platform=platform, market_id=market_id, side=side, minute_ts=minute_ts)
                if cached is not None:
                    entry_price, price_source = cached
                    price_quality = "HISTORICAL"
                else:
                    try:
                        price_point = await client.get_price_near(market_id, side, call.timestamp_utc)
                        if price_point is not None:
                            entry_price = float(price_point.price)
                            price_source = price_point.source
                            price_quality = "APPROXIMATE" if "approx" in (price_source or "").lower() else "HISTORICAL"
                            await price_cache.set(platform=platform, market_id=market_id, side=side, minute_ts=minute_ts, price=entry_price, source=price_source)
                    except UpstreamHTTPError as e:
                        if logger is not None and request_id is not None:
                            logger.warning("[%s] price_verify_failed market=%s upstream=%s", request_id, market_id, e.status_code)
                    except Exception as e:
                        if logger is not None and request_id is not None:
                            logger.warning("[%s] price_verify_failed market=%s error=%s", request_id, market_id, e)

            if entry_price is None and call.quoted_price is not None:
                entry_price = float(call.quoted_price)
                price_quality = "QUOTED"
                price_source = "quoted"

            if entry_price is None:
                return (
                    call_idx,
                    {
                        "call": call.model_dump(),
                        "match": {**match_dict, "method": method, "market_title": verified.market_title or match_dict.get("market_title")},
                        "resolved_outcome": verified.resolved_outcome,
                        "price": {"entry_price": None, "source": None, "ts_used": call.timestamp_utc, "quality": "MISSING"},
                        "status": "ERROR_MISSING_ENTRY_PRICE",
                    },
                )

            contracts = unit_notional * float(call.bet_size_units or settings.default_bet_units)
            gross_pnl = _pnl_for_binary_call(entry_price, verified.resolved_outcome, side, contracts)
            fees = 0.0
            if platform == "kalshi":
                fees = _kalshi_fee_usd(market_ticker=market_id, price=entry_price, contracts=contracts)
            elif settings.polymarket_fee_bps:
                fees = (float(settings.polymarket_fee_bps) / 10000.0) * (entry_price * contracts)
            net_pnl = gross_pnl - fees

            return (
                call_idx,
                {
                    "call": call.model_dump(),
                    "match": {**match_dict, "method": method, "market_title": verified.market_title or match_dict.get("market_title")},
                    "market": {"id": market_id, "title": verified.market_title or match_dict.get("market_title"), "confidence": match_dict.get("confidence")},  # legacy
                    "resolved_outcome": verified.resolved_outcome,
                    "price": {"entry_price": entry_price, "source": price_source, "ts_used": call.timestamp_utc, "quality": price_quality},
                    "entry_price_used": entry_price,  # legacy
                    "price_point": None if price_point is None else price_point.__dict__,
                    "contracts": contracts,
                    "fees_usd": fees,
                    "net_pnl_usd": net_pnl,
                    "roi": net_pnl / (entry_price * contracts) if entry_price > 0 else None,
                    "status": "OK",
                },
            )

    results = await asyncio.gather(*[_process_one(i, c) for i, c in enumerate(calls)])
    results.sort(key=lambda t: t[0])

    ok = pending = unmatched = 0
    for idx, row in results:
        per_bet.append(row)
        status = row.get("status")
        if status == "OK":
            ok += 1
            call = calls[idx]
            verified_outcome = row.get("resolved_outcome") or ""
            net_pnl = float(row.get("net_pnl_usd") or 0.0)
            user = by_user[call.author]
            user["bets"] += 1
            if str(verified_outcome).upper() == str(call.position_direction).upper():
                user["wins"] += 1
            user["net_pnl"] += net_pnl
            resolved_rows.append((call.timestamp_utc, net_pnl))
        elif status == "PENDING":
            pending += 1
        elif status == "UNMATCHED":
            unmatched += 1

        if logger is not None and request_id is not None and (idx + 1) % 25 == 0:
            logger.info("[%s] pnl:progress processed=%s ok=%s pending=%s unmatched=%s", request_id, idx + 1, ok, pending, unmatched)

    total_resolved = sum(v["bets"] for v in by_user.values())
    total_wins = sum(v["wins"] for v in by_user.values())
    total_net_pnl = sum(v["net_pnl"] for v in by_user.values())

    # Max drawdown on aggregate equity curve (sequential by message timestamp).
    resolved_rows.sort(key=lambda x: x[0])
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for _, pnl in resolved_rows:
        cum += pnl
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    aggregate = {
        "resolved_bets": total_resolved,
        "win_rate": (total_wins / total_resolved) if total_resolved else None,
        "total_net_pnl_usd": total_net_pnl,
        "total_net_units": (total_net_pnl / unit_notional) if unit_notional else None,
        "max_drawdown_usd": max_dd,
    }
    leaderboard = [
        {
            "author": k,
            "bets": v["bets"],
            "wins": v["wins"],
            "win_rate": (v["wins"] / v["bets"]) if v["bets"] else None,
            "net_pnl_usd": v["net_pnl"],
            "net_units": (v["net_pnl"] / unit_notional) if unit_notional else None,
        }
        for k, v in sorted(by_user.items(), key=lambda kv: kv[1]["net_pnl"], reverse=True)
    ]
    if logger is not None and request_id is not None:
        logger.info(
            "[%s] pnl:end ok=%s pending=%s unmatched=%s duration_ms=%s",
            request_id,
            ok,
            pending,
            unmatched,
            int((time.perf_counter() - t0) * 1000),
        )
    return {"aggregate": aggregate, "leaderboard": leaderboard, "bets": per_bet}
