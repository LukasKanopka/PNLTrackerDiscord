from __future__ import annotations

import re
import json
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


async def _llm_disambiguate(
    intent: str,
    side: str,
    candidates: list[dict],
    client: MarketClient,
) -> dict | None:
    """
    Use LLM to pick the correct market from ambiguous candidates.
    Takes the bet intent, side (YES/NO), and list of candidate markets,
    returns the selected candidate.
    """
    if not candidates or len(candidates) <= 1:
        return None
    
    if not settings.openrouter_api_key:
        return None
    
    cand_text = "\n".join([
        f"- {c.get('ticker')}: {c.get('title', '')}" 
        for c in candidates[:5]
    ])
    
    prompt = f"""You are a market disambiguation assistant. Given a bet prediction and a list of candidate markets, pick the ONE correct market.

User's bet prediction:
- Side: {side}
- Intent: {intent[:300]}

Candidate markets:
{cand_text}

Return ONLY the ticker of the correct market, or "UNKNOWN" if you cannot determine.

Examples:
- If user says "Bet on Spurs to win" and candidates are ["KXNBAGAME-26MAR12DENSAS-SAS", "KXNBAGAME-26MAR12DENSAS-DEN"], return KXNBAGAME-26MAR12DENSAS-SAS (San Antonio = SAS)
- If user says "Bet on OKC" and candidates are ["KXNBAGAME-26MAR25OKCBOS-OKC", "KXNBAGAME-26MAR25OKCBOS-BOS"], return KXNBAGAME-26MAR25OKCBOS-OKC

Answer:"""

    import httpx
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.post(
                f"{settings.openrouter_base_url}/chat/completions",
                headers=headers,
                json={
                    "model": settings.openrouter_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                }
            )
            if resp.status_code == 200:
                result = resp.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                # Parse the response - look for a ticker
                for c in candidates:
                    if c.get("ticker", "").upper() in content.upper():
                        return c
    except Exception as e:
        logging.getLogger("pnl_analyzer").warning(f"llm disambiguate failed: {e}")
    
    return None


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
    if isinstance(mr, dict) and mr.get("options"):
        opts = mr.get("options", [])
        if isinstance(opts, list):
            # Try options in order; prefer exact-platform matches.
            opts = [o for o in opts if isinstance(o, dict)]
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
            # try one more disambiguation pass using pick phrase from market_intent.
            cands = match_dict.get("candidates")
            if isinstance(cands, list) and len(cands) > 1 and float(match_dict.get("confidence") or 0.0) < 0.35:
                # Special handling for BTTS (Both Teams to Score) - search for BTTS markets
                intent_lower = (call.market_intent or "").lower()
                is_btts = "both teams to score" in intent_lower or " btts " in intent_lower or intent_lower.endswith("btts")
                
                if is_btts and platform == "kalshi":
                    # Search specifically for BTTS markets
                    btts_search = re.sub(r"(both teams to score|btts)", "both score goals", intent_lower)
                    btts_match = await client.match_market(f"Both Teams to Score {call.market_intent[:200]}", call.timestamp_utc)
                    if btts_match and btts_match.market_id:
                        match_dict = {
                            "platform": platform,
                            "market_id": btts_match.market_id,
                            "market_title": btts_match.market_title,
                            "confidence": 0.6,
                            "method": "search-btts",
                        }
                    else:
                        return (
                            call_idx,
                            {
                                "call": call.model_dump(),
                                "match": {**match_dict, "method": method},
                                "status": "AMBIGUOUS_MARKET",
                            },
                        )
                else:
                    # Extract pick phrase from market_intent (e.g., "My Bet: Yes on Spurs" -> "Spurs")
                    pick_phrase = None
                    side_taken = (call.position_direction or "").upper()
                    intent_lower = (call.market_intent or "").lower()
                    
                    # Try multiple patterns to extract team/pick from intent
                    for label in ("my bet:", "bet:", "pick:"):
                        idx = intent_lower.find(label)
                        if idx >= 0:
                            pick_text = call.market_intent[idx + len(label):].strip()
                            pick_text = re.sub(r"^(yes|no|on)\s+", "", pick_text, flags=re.IGNORECASE)
                            pick_text = re.sub(r"[@\d%].*$", "", pick_text).strip()
                            if pick_text:
                                pick_phrase = pick_text.lower()
                            break
                    
                    # If no pick phrase found, try extracting from various formats
                    if not pick_phrase:
                        # Pattern: "My Bet: No on Spurs (45c)" -> extract "Spurs"
                        m = re.search(r"(?:my bet|bet|pick):\s*(?:yes|no)\s+on\s+([a-zA-Z\s]+?)(?:\s*\()", intent_lower)
                        if m:
                            pick_phrase = m.group(1).strip().lower()
                        # Pattern: "Buy YES on San Antonio (64c)" or "BUY YES - Oklahoma City Wins"
                        if not pick_phrase:
                            m = re.search(r"(?:buy|playing)\s+(?:yes|no)\s+(?:on\s+)?([a-zA-Z\s]+?)(?:\s*\(|\s*-)", intent_lower)
                            if m:
                                pick_phrase = m.group(1).strip().lower()
                        # Pattern: "Prediction: Detroit 59c Vs Atlanta" -> extract "Detroit"
                        if not pick_phrase:
                            m = re.search(r"prediction:\s*([a-zA-Z0-9]+)\s+\d+[c%]?\s+vs", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Prediction: ** Detroit 59c Vs Atlanta" (with markdown **)
                        if not pick_phrase:
                            m = re.search(r"prediction:\s*\*+\s*([a-zA-Z0-9]+)\s+\d+[c%]?\s+vs", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Spurs VS Nuggets" in title
                        if not pick_phrase:
                            m = re.search(r"([a-zA-Z]+)\s+vs\s+[a-zA-Z]+", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Team +7.5" (spread bet)
                        if not pick_phrase:
                            m = re.search(r"([a-zA-Z]+)\s+\+\d+\.?\d*", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "for Purdue"
                        if not pick_phrase:
                            m = re.search(r"\bfor\s+([a-zA-Z]{3,})\b", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Oklahoma City at Los Angeles Clippers" -> extract first team
                        if not pick_phrase:
                            m = re.search(r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+at\s+", intent_lower)
                            if m:
                                pick_phrase = m.group(1).strip().lower()
                        # Pattern: "Prediction: ** Detroit 59c Vs Atlanta" (with markdown **)
                        if not pick_phrase:
                            m = re.search(r"prediction:\s*\*+\s*([a-zA-Z0-9]+)\s+\d+[c%]?\s+vs", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Prediction: OKC 56c Vs Boston" (team abbreviation)
                        if not pick_phrase:
                            m = re.search(r"prediction:\s*\*+\s*([A-Z]{2,5})\s+\d+[c%]?\s+vs", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Team +7.5" (spread bet)
                        if not pick_phrase:
                            m = re.search(r"([a-zA-Z]{3,})\s+\+\d+\.?\d*", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "for Purdue" -> extract "Purdue"
                        if not pick_phrase:
                            m = re.search(r"\bfor\s+([a-zA-Z]{3,})\b", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Pelicans +7.5" (team with +line)
                        if not pick_phrase:
                            m = re.search(r"([a-zA-Z]+)\s+\+\d+\.?\d*", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Prediction: OKC 56c Vs Boston" (team abbreviation)
                        if not pick_phrase:
                            m = re.search(r"prediction:\s*([A-Z]{2,5})\s+\d+[c%]?\s+vs", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "Team +7.5" (spread bet)
                        if not pick_phrase:
                            m = re.search(r"([a-zA-Z]{3,})\s+\+\d+\.?\d*", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()
                        # Pattern: "for Purdue" -> extract "Purdue"
                        if not pick_phrase:
                            m = re.search(r"\bfor\s+([a-zA-Z]{3,})\b", intent_lower)
                            if m:
                                pick_phrase = m.group(1).lower()

                    # Try to match pick_phrase against candidate titles/tickers
                    selected = None
                    pick_abbrev = None
                    if pick_phrase:
                        # Comprehensive team name to abbreviation mapping
                        team_to_abbrev = {
                            "detroit": "DET", "atlanta": "ATL", "okc": "OKC", "boston": "BOS",
                            "pelicans": "NOP", "toronto": "TOR", "purdue": "PUR", "arizona": "ARIZ",
                            "san antonio": "SAS", "spurs": "SAS", 
                            "miami": "MIA", "heat": "MIA", 
                            "lakers": "LAL", "clippers": "LAC", 
                            "los angeles": "LAC", "los angeles clippers": "LAC",
                            "houston": "HOU", "knicks": "NYK", "nuggets": "DEN",
                            "oklahoma city": "OKC", "thunder": "OKC",
                            "new orleans": "NOP", "golden state": "GSW", "warriors": "GSW",
                        }
                        
                        pick_abbrev = team_to_abbrev.get(pick_phrase, pick_phrase[:3].upper() if len(pick_phrase) >= 3 else pick_phrase.upper())
                        
                        for c in cands:
                            ticker = c.get("ticker", "")
                            title = c.get("title", "")
                            ticker_suffix = ticker.split("-")[-1].upper() if "-" in ticker else ""
                            
                            is_our_team = pick_abbrev.upper() == ticker_suffix
                            
                            if side_taken == "YES":
                                if is_our_team:
                                    selected = c
                                    break
                                if pick_phrase.lower() in title.lower():
                                    selected = c
                                    break
                            elif side_taken == "NO":
                                if is_our_team:
                                    continue
                                selected = c
                                break
                    
                    # Fallback: if we have pick_phrase but couldn't match, use first candidate (best effort)
                    if not selected and pick_phrase and cands:
                        selected = cands[0]
                    
                    # If still no match, try LLM-based disambiguation
                    if not selected and cands:
                        selected = await _llm_disambiguate(
                            intent=call.market_intent,
                            side=side_taken,
                            candidates=cands,
                            client=client,
                        )

                    if selected:
                        match_dict = {
                            "platform": match_dict.get("platform"),
                            "market_id": selected.get("ticker"),
                            "market_title": selected.get("title"),
                            "confidence": 0.5,
                            "method": method,
                        }
                    else:
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
