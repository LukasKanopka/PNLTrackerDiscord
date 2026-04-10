from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_CEILING

from pnl_analyzer.config import settings
from pnl_analyzer.llm.types import BetCall
from pnl_analyzer.markets.base import MarketClient


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


async def analyze_calls(
    calls: list[BetCall],
    kalshi: MarketClient,
    polymarket: MarketClient,
    verify_prices: bool,
) -> dict:
    unit_notional = settings.unit_notional_usd

    per_bet: list[dict] = []
    by_user = defaultdict(lambda: {"bets": 0, "wins": 0, "net_pnl": 0.0})
    resolved_rows: list[tuple[str, float]] = []

    for call in calls:
        client = kalshi if call.platform.lower() == "kalshi" else polymarket
        match = await client.match_market(call.market_intent, call.timestamp_utc)
        if not match:
            per_bet.append(
                {
                    "call": call.model_dump(),
                    "status": "UNMATCHED",
                }
            )
            continue

        verified = await client.get_verified_market(match.market_id)
        if not verified.resolved or not verified.resolved_outcome:
            per_bet.append(
                {
                    "call": call.model_dump(),
                    "market": {"id": match.market_id, "title": match.market_title, "confidence": match.confidence},
                    "status": "PENDING",
                }
            )
            continue

        entry_price = call.quoted_price
        price_point = None
        if verify_prices:
            price_point = await client.get_price_near(match.market_id, call.position_direction, call.timestamp_utc)
            if price_point is not None:
                entry_price = price_point.price

        contracts = unit_notional * float(call.bet_size_units or settings.default_bet_units)
        gross_pnl = _pnl_for_binary_call(entry_price, verified.resolved_outcome, call.position_direction, contracts)
        fees = 0.0
        if call.platform.lower() == "kalshi":
            fees = _kalshi_fee_usd(market_ticker=match.market_id, price=entry_price, contracts=contracts)
        net_pnl = gross_pnl - fees

        user = by_user[call.author]
        user["bets"] += 1
        if verified.resolved_outcome.upper() == call.position_direction.upper():
            user["wins"] += 1
        user["net_pnl"] += net_pnl
        resolved_rows.append((call.timestamp_utc, net_pnl))

        per_bet.append(
            {
                "call": call.model_dump(),
                "market": {"id": match.market_id, "title": verified.market_title, "confidence": match.confidence},
                "resolved_outcome": verified.resolved_outcome,
                "entry_price_used": entry_price,
                "price_point": None if price_point is None else price_point.__dict__,
                "contracts": contracts,
                "fees_usd": fees,
                "net_pnl_usd": net_pnl,
                "roi": net_pnl / (entry_price * contracts) if entry_price > 0 else None,
                "status": "OK",
            }
        )

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
    return {"aggregate": aggregate, "leaderboard": leaderboard, "bets": per_bet}
