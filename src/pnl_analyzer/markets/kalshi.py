from __future__ import annotations

import base64
import time
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
import re

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from dateutil import tz

from pnl_analyzer.config import settings
from pnl_analyzer.markets.base import MarketClient, MarketMatch, PricePoint, VerifiedMarket
from pnl_analyzer.utils.retry import UpstreamHTTPError, with_retries
from pnl_analyzer.utils.text import fuzzy_score
from pnl_analyzer.utils.time import to_unix_seconds


def _load_private_key(pem: str) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("KALSHI_PRIVATE_KEY_PEM is not an RSA private key")
    return key


def _sign_pss(private_key: rsa.RSAPrivateKey, text: str) -> str:
    sig = private_key.sign(
        text.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


def _event_ts_hint_from_ticker(ticker: str) -> int | None:
    """
    Many sports tickers contain an ET datetime chunk like 26APR071840.
    Parse that as America/New_York local time and convert to unix seconds (UTC).
    """
    if not ticker:
        return None
    import re

    m = re.search(r"-(?P<dt>\d{2}[A-Z]{3}\d{2}\d{2}\d{2})", ticker.upper())
    if not m:
        return None
    dt_raw = m.group("dt")
    try:
        dt_local = datetime.strptime(dt_raw, "%y%b%d%H%M")
    except Exception:
        return None
    et = tz.gettz("America/New_York")
    if et is None:
        return None
    dt_local = dt_local.replace(tzinfo=et)
    return int(dt_local.timestamp())


class KalshiClient(MarketClient):
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(base_url=settings.kalshi_base_url, timeout=20.0)
        self._prefix = settings.kalshi_api_prefix.rstrip("/")
        self._key_id = settings.kalshi_key_id

        pem = settings.kalshi_private_key_pem
        if self._key_id and not pem and settings.kalshi_private_key_path:
            key_path = Path(settings.kalshi_private_key_path)
            if key_path.exists():
                pem = key_path.read_text(encoding="utf-8")
        self._priv = _load_private_key(pem) if pem else None

    def _signed_headers(self, method: str, path_without_query: str) -> dict[str, str]:
        if not (self._key_id and self._priv):
            return {}
        ts_ms = str(int(time.time() * 1000))
        msg = ts_ms + method.upper() + path_without_query
        sig = _sign_pss(self._priv, msg)
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig,
        }

    async def _get(self, path: str, params: dict | None = None) -> dict:
        path_no_q = path.split("?")[0]
        full_path = f"{self._prefix}{path_no_q}" if not path_no_q.startswith(self._prefix) else path_no_q

        async def _do():
            headers = self._signed_headers("GET", full_path)
            r = await self._client.get(full_path, params=params, headers=headers)
            if r.status_code in (429, 500, 502, 503, 504):
                raise UpstreamHTTPError(r.status_code, f"Kalshi {full_path} retryable: {r.text}")
            if r.status_code >= 400:
                raise UpstreamHTTPError(r.status_code, f"Kalshi {full_path} failed: {r.text}")
            return r.json()

        return await with_retries(_do)

    async def match_market(self, intent: str, ts_utc: str) -> MarketMatch | None:
        # Kalshi doesn't expose a text search in v2 market listing; filter by close window, then fuzzy match.
        target = to_unix_seconds(ts_utc)
        min_close = max(0, target - 60 * 60 * 24 * 14)
        # Many "futures" markets resolve months out; use a wider window.
        max_close = target + 60 * 60 * 24 * 365
        markets: list[dict] = []
        cursor: str | None = None
        # Best-effort pagination: stop early once we have enough candidates.
        for _ in range(5):
            params = {"limit": 200, "min_close_ts": min_close, "max_close_ts": max_close}
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/markets", params=params)
            batch = data.get("markets") or []
            if isinstance(batch, list):
                markets.extend(batch)
            cursor = data.get("cursor") or data.get("next_cursor") or None
            has_more = bool(data.get("has_more") or data.get("hasMore") or False)
            if not cursor or not has_more or len(markets) >= 800:
                break
        best: MarketMatch | None = None
        for m in markets:
            title = m.get("title") or m.get("subtitle") or m.get("ticker") or ""
            score = fuzzy_score(intent, title)
            if best is None or score > best.confidence:
                best = MarketMatch(market_id=m.get("ticker", ""), market_title=title, confidence=score)
        if best and best.market_id and best.confidence >= 0.35:
            return best
        return None

    async def resolve_from_market_ref(self, market_ref: dict, *, intent: str, ts_utc: str) -> MarketMatch | None:
        """
        Resolve a Kalshi market deterministically from a kalshi.com URL reference.
        - If a full market ticker is present, prefer it.
        - Otherwise, use the URL event slug as a series ticker and list markets, then fuzzy match.
        """
        if not isinstance(market_ref, dict):
            return None
        if (market_ref.get("platform") or "").lower() != "kalshi":
            return None

        page_slug = market_ref.get("page_slug")
        page_text = None
        if isinstance(page_slug, str) and page_slug:
            page_text = page_slug.replace("/", " ").replace("-", " ")

        ticker = market_ref.get("ticker")
        if isinstance(ticker, str) and ticker:
            ticker_u = ticker.upper()
            # Trust but verify: some kalshi.com URLs include an incomplete/legacy ticker.
            try:
                vm = await self.get_verified_market(ticker_u)
                if vm.market_title:
                    return MarketMatch(market_id=ticker_u, market_title=vm.market_title, confidence=1.0)
            except Exception:
                pass

            def _score_title(title: str) -> float:
                s1 = fuzzy_score(intent, title)
                if page_text:
                    s2 = fuzzy_score(page_text, title)
                    return max(s1, min(1.0, s2 + 0.05))
                return s1

            async def _match_event_markets(event_ticker: str) -> MarketMatch | None:
                try:
                    evd = await self._get(f"/events/{event_ticker}")
                except UpstreamHTTPError as e:
                    if e.status_code == 404:
                        return None
                    raise
                markets = evd.get("markets") if isinstance(evd, dict) else None
                if not isinstance(markets, list) or not markets:
                    return None

                cleaned_intent = re.sub(r"[*_`]+", " ", intent or "")
                cleaned_intent = re.sub(r"\s+", " ", cleaned_intent).strip()
                key_half_points = set(re.findall(r"\b\d{1,3}\.5\b", cleaned_intent))

                def _key_numbers_and_mode() -> tuple[set[int], str]:
                    # 1) Dollar-million thresholds like "$10M" or "10 million" (common for auction/price markets).
                    million_nums: set[int] = set()
                    for m in re.finditer(r"\$\s*(\d{1,3})(?:\.\d+)?\s*m\b", cleaned_intent, flags=re.IGNORECASE):
                        million_nums.add(int(m.group(1)))
                    for m in re.finditer(r"\b(\d{1,3})\s*(?:million|mm)\b", cleaned_intent, flags=re.IGNORECASE):
                        million_nums.add(int(m.group(1)))
                    if million_nums:
                        return million_nums, "million"

                    # 2) Explicit thresholds (e.g. "7+") and common O/U half-point lines (e.g. "u6.5" -> 7+).
                    nums: set[int] = set()
                    for m in re.finditer(r"\b(\d{1,3})\s*\+", cleaned_intent):
                        nums.add(int(m.group(1)))
                    for m in re.finditer(r"\b(?:o|over|u|under)\s*(\d{1,3})\s*\.\s*5\b", cleaned_intent, flags=re.IGNORECASE):
                        nums.add(int(m.group(1)) + 1)
                    if nums:
                        return nums, "threshold"

                    # 3) Fallback: use standalone integers if nothing else exists (avoid cents and tiny numbers).
                    for m in re.finditer(r"\b(\d{2,3})\b(?!\s*c\b)", cleaned_intent, flags=re.IGNORECASE):
                        nums.add(int(m.group(1)))
                    return nums, "generic" if nums else "none"

                key_nums, key_mode = _key_numbers_and_mode()

                def _score_text(txt: str) -> float:
                    s = _score_title(txt)
                    # Half-point line matching (important for spreads like "over 1.5 points").
                    if key_half_points:
                        txt_half = set(re.findall(r"\b\d{1,3}\.5\b", txt))
                        if txt_half & key_half_points:
                            s = min(1.0, s + 0.12)
                        elif txt_half:
                            s = max(0.0, s - 0.05)
                    if key_nums:
                        title_nums: set[int] = set()
                        for m in re.finditer(r"\b(\d{1,3})\s*\+", txt):
                            title_nums.add(int(m.group(1)))
                        for m in re.finditer(r"\b(\d{1,3})\s*\.\s*5\b", txt):
                            title_nums.add(int(m.group(1)))
                        if key_mode == "million":
                            for m in re.finditer(r"\b(\d{1,3})(?:\.\d+)?\s*(?:million|mm)\b", txt, flags=re.IGNORECASE):
                                title_nums.add(int(m.group(1)))
                        else:
                            # Also include plain integers for rule blobs like "at least 10 million".
                            for m in re.finditer(r"\b(\d{1,3})\b(?!\s*c\b)", txt, flags=re.IGNORECASE):
                                title_nums.add(int(m.group(1)))
                        if title_nums & key_nums:
                            s = min(1.0, s + 0.12)
                        elif title_nums:
                            s = max(0.0, s - 0.05)
                    return s

                scored: list[tuple[float, dict]] = []
                for m in markets:
                    if not isinstance(m, dict):
                        continue
                    t = str(m.get("title") or m.get("subtitle") or m.get("ticker") or "")
                    scored.append((_score_text(t), m))
                if not scored:
                    return None
                scored.sort(key=lambda x: x[0], reverse=True)
                top_score, top_m = scored[0]
                second = scored[1][0] if len(scored) > 1 else 0.0
                top_ticker = str(top_m.get("ticker") or "")
                top_title = str(top_m.get("title") or top_m.get("subtitle") or top_ticker)
                candidates = [{"ticker": str(m.get("ticker") or ""), "title": str(m.get("title") or m.get("subtitle") or "")} for _, m in scored[:10]]

                # If ambiguous, pull richer market details for better disambiguation (rules often include thresholds).
                if len(scored) > 1 and (top_score - second) < 0.05:
                    # Extract a "pick" phrase from the message when available to disambiguate multi-market events.
                    def _pick_phrase() -> str | None:
                        low = cleaned_intent.lower()
                        for label in ("my bet:", "pick:", "prediction:", "bet:"):
                            i = low.find(label)
                            if i >= 0:
                                frag = cleaned_intent[i + len(label) :].strip()
                                frag = frag.split("\\n", 1)[0].strip()
                                # Cut common trailing sections.
                                frag = re.split(r"\b(odds|analysis|kalshi link|polymarket link)\b", frag, flags=re.IGNORECASE)[0].strip()
                                return frag[:120] if frag else None
                        return None

                    picked = _pick_phrase()

                    detailed: list[tuple[float, dict, str]] = []
                    for _, m in scored[:10]:
                        mt = str(m.get("ticker") or "")
                        if not mt:
                            continue
                        try:
                            md = await self._get(f"/markets/{mt}")
                            mm = md.get("market") or md
                            blob = " ".join(
                                [
                                    str(mm.get("title") or ""),
                                    str(mm.get("subtitle") or ""),
                                    str(mm.get("rules_primary") or mm.get("rulesPrimary") or ""),
                                ]
                            )
                            s_blob = _score_text(blob)
                            s_title = _score_text(str(m.get("title") or ""))
                            # When we have key numeric thresholds (e.g. 7+, $10M), prefer the rule/blob score;
                            # titles are often identical across sibling markets and can mask numeric mismatches.
                            s = s_blob if key_nums else max(s_blob, s_title)
                            # Extra boost when the message explicitly names the chosen side (team/person/threshold).
                            if picked:
                                subj = None
                                rm = str(mm.get("rules_primary") or mm.get("rulesPrimary") or "")
                                msub = re.match(r"\s*If\s+(?P<subj>[^,]+?)\s+(?:wins|is|are|will|sells|scores)\b", rm, flags=re.IGNORECASE)
                                if msub:
                                    subj = msub.group("subj").strip()
                                if subj:
                                    s = min(1.0, s + 0.15 * fuzzy_score(picked, subj))
                                # Also boost when the pick contains a short team code matching the ticker suffix (e.g. OKC/BOS/DET).
                                suff = mt.split("-")[-1].upper()
                                if 2 <= len(suff) <= 5 and suff.isalnum() and suff in str(picked).upper():
                                    s = min(1.0, s + 0.12)
                            detailed.append((s, m, blob))
                        except Exception:
                            detailed.append((_score_title(str(m.get("title") or "")), m, ""))
                    detailed.sort(key=lambda x: x[0], reverse=True)
                    if detailed:
                        top_score = float(detailed[0][0])
                        top_m = detailed[0][1]
                        top_ticker = str(top_m.get("ticker") or "")
                        top_title = str(top_m.get("title") or top_m.get("subtitle") or top_ticker)
                        second = float(detailed[1][0]) if len(detailed) > 1 else 0.0
                        candidates = [{"ticker": str(m.get("ticker") or ""), "title": str(m.get("title") or m.get("subtitle") or "")} for _, m, _ in detailed[:10]]

                conf = float(top_score)
                if len(scored) > 1 and (top_score - second) < 0.05:
                    conf = min(conf, 0.2)
                return MarketMatch(market_id=top_ticker, market_title=top_title, confidence=conf, candidates=candidates if conf < 0.35 else None)

            # If the URL points at an event family (e.g., KXMLBGAME) but the intent is clearly a prop market,
            # attempt a cross-series event lookup by suffix (common Kalshi convention: <SERIES>-<SUFFIX>).
            base_series = ticker_u.split("-", 1)[0].upper() if "-" in ticker_u else ticker_u
            suffix = ticker_u.split("-", 1)[1].upper() if "-" in ticker_u else ""
            low_intent = (intent or "").lower()
            alt_series: list[str] = []
            if base_series == "KXMLBGAME":
                if "strikeout" in low_intent:
                    alt_series.append("KXMLBKS")
                if "home run" in low_intent or "homerun" in low_intent or " hr " in f" {low_intent} ":
                    alt_series.append("KXMLBHR")
            if base_series == "KXNCAAMBGAME":
                if "+" in low_intent or "-" in low_intent or "spread" in low_intent:
                    alt_series.append("KXNCAAMBSPREAD")
            if base_series == "KXNBAGAME":
                if "wins by" in low_intent or "spread" in low_intent or re.search(r"[\\+\\-]\\s*\\d", low_intent):
                    alt_series.append("KXNBASPREAD")

            # First try treating the URL ticker as an event ticker (Kalshi often uses event pages with multiple markets).
            base_mm = await _match_event_markets(ticker_u)

            best_alt: MarketMatch | None = None
            for s in alt_series:
                if not suffix:
                    continue
                alt_event_ticker = f"{s}-{suffix}"
                mm2 = await _match_event_markets(alt_event_ticker)
                if mm2 is not None and mm2.market_id:
                    if best_alt is None or float(mm2.confidence) > float(best_alt.confidence):
                        best_alt = mm2

            if best_alt is not None and best_alt.market_id:
                # Prefer the alt-series match when it is clearly more confident than the base event,
                # or when the base event is ambiguous/low-confidence.
                if base_mm is None:
                    return best_alt
                base_conf = float(base_mm.confidence or 0.0)
                alt_conf = float(best_alt.confidence or 0.0)
                if base_conf < 0.35 and alt_conf >= 0.25:
                    return best_alt
                if alt_conf >= 0.35 and alt_conf > base_conf + 0.1:
                    return best_alt
            if base_mm is not None and base_mm.market_id:
                return base_mm

        event_slug = market_ref.get("event_slug")
        series_ticker = None
        if isinstance(event_slug, str) and event_slug:
            series_ticker = event_slug.upper()
        elif isinstance(ticker, str) and "-" in ticker:
            series_ticker = ticker.split("-", 1)[0].upper()
        if not series_ticker:
            return None

        target = to_unix_seconds(ts_utc)
        # Default close window around call time.
        min_close = max(0, target - 60 * 60 * 24 * 14)
        max_close = target + 60 * 60 * 24 * 365
        # If the URL ticker contains an explicit event datetime chunk, use it to narrow the listing window.
        if isinstance(ticker, str) and ticker:
            hinted = _event_ts_hint_from_ticker(ticker)
            if hinted:
                min_close = max(0, hinted - 60 * 60 * 24)
                max_close = hinted + 60 * 60 * 24 * 2

        markets: list[dict] = []
        cursor: str | None = None
        for _ in range(8):
            params = {"limit": 200, "series_ticker": series_ticker, "min_close_ts": min_close, "max_close_ts": max_close}
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/markets", params=params)
            batch = data.get("markets") or []
            if isinstance(batch, list):
                markets.extend(batch)
            cursor = data.get("cursor") or data.get("next_cursor") or None
            has_more = bool(data.get("has_more") or data.get("hasMore") or False)
            if not cursor or not has_more:
                break
            if len(markets) >= 1500:
                break

        if not markets:
            return None

        def _score(m: dict) -> float:
            title = m.get("title") or m.get("subtitle") or m.get("ticker") or ""
            s1 = fuzzy_score(intent, title)
            if page_text:
                s2 = fuzzy_score(page_text, title)
                return max(s1, min(1.0, s2 + 0.05))  # slight boost for URL slug alignment
            return s1

        # If the URL included a non-resolving "event key" ticker (often missing outcome suffix),
        # attempt to resolve to a concrete market ticker by prefix match.
        event_key = ticker.upper() if isinstance(ticker, str) and ticker else None
        if event_key:
            prefix = event_key + "-"
            pref = [m for m in markets if str(m.get("ticker") or "").upper().startswith(prefix)]
            if pref:
                scored = []
                for m in pref:
                    title = m.get("title") or m.get("subtitle") or m.get("ticker") or ""
                    scored.append((_score(m), m, title))
                scored.sort(key=lambda x: x[0], reverse=True)
                top_score, top_m, top_title = scored[0]
                # Ambiguity: if the top two are very close, expose candidates and return low-confidence.
                second = scored[1][0] if len(scored) > 1 else 0.0
                candidates = [{"ticker": str(m.get("ticker") or ""), "title": str(t)} for _, m, t in scored[:10]]
                conf = float(top_score)
                if len(scored) > 1 and (top_score - second) < 0.05:
                    conf = min(conf, 0.2)
                return MarketMatch(
                    market_id=str(top_m.get("ticker") or ""),
                    market_title=str(top_title),
                    confidence=conf,
                    candidates=candidates,
                )

        best: MarketMatch | None = None
        for m in markets:
            title = m.get("title") or m.get("subtitle") or m.get("ticker") or ""
            score = _score(m)
            # Boost if ticker shares the same series prefix.
            mt = str(m.get("ticker") or "")
            if mt.upper().startswith(series_ticker + "-"):
                score = min(1.0, score + 0.05)
            if best is None or score > best.confidence:
                best = MarketMatch(market_id=mt, market_title=title, confidence=score)
        if best and best.market_id and best.confidence >= 0.25:
            return best
        return None

    async def get_verified_market(self, market_id: str) -> VerifiedMarket:
        data = await self._get(f"/markets/{market_id}")
        m = data.get("market") or data
        status = (m.get("status") or "").lower()
        resolved = status in ("settled", "determined")
        result = m.get("result") or m.get("determination") or None
        resolved_outcome = None
        if isinstance(result, str):
            if result.upper() in ("YES", "NO"):
                resolved_outcome = result.upper()
        return VerifiedMarket(
            market_id=market_id,
            market_title=m.get("title") or m.get("subtitle") or market_id,
            resolved=resolved,
            resolved_outcome=resolved_outcome,
            resolution_ts_utc=None,
        )

    async def get_price_near(self, market_id: str, side: str, ts_utc: str) -> PricePoint | None:
        # Use candlesticks around timestamp; requires series ticker, so first fetch market.
        data = await self._get(f"/markets/{market_id}")
        m = data.get("market") or data
        series_ticker = m.get("series_ticker") or m.get("seriesTicker") or m.get("series") or None
        if not series_ticker:
            return None
        target = to_unix_seconds(ts_utc)
        start_ts = max(0, target - 3600)
        end_ts = target + 3600

        cs = await self._get(
            f"/series/{series_ticker}/markets/{market_id}/candlesticks",
            params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": "1m"},
        )
        candles = cs.get("candlesticks") or cs.get("candles") or []
        if not candles:
            return None
        # Best-effort: use the close price of the last candle at/before target for requested side.
        best = None
        for c in candles:
            t = int(c.get("ts") or c.get("start_ts") or 0)
            if t <= target and (best is None or t > best[0]):
                best = (t, c)
        if best is None:
            best = (int(candles[0].get("ts") or start_ts), candles[0])
        candle = best[1]
        # Prefer side-specific close/mid fields when available; avoid NO=1-YES unless necessary.
        side_u = side.upper()
        candidates = []
        if side_u == "YES":
            candidates = [
                candle.get("yes_close"),
                candle.get("yesClose"),
                candle.get("yes_mid"),
                candle.get("yesMid"),
                candle.get("close"),
            ]
        else:
            candidates = [
                candle.get("no_close"),
                candle.get("noClose"),
                candle.get("no_mid"),
                candle.get("noMid"),
            ]
            # Last resort: derive NO from close (assumed YES) and mark as approximate.
            candidates.append(candle.get("close"))

        picked = None
        picked_from_close = False
        for v in candidates:
            if v is None:
                continue
            picked = v
            picked_from_close = (v == candle.get("close"))
            break
        if picked is None:
            return None

        try:
            p = float(picked)
        except Exception:
            return None
        if p > 1.0:
            p = p / 100.0
        if not (0.0 <= p <= 1.0):
            return None

        if side_u == "NO" and picked_from_close:
            p = 1.0 - p
            return PricePoint(ts_utc=ts_utc, price=p, source="kalshi_candlesticks_1m_approx_no_from_yes_close")
        return PricePoint(ts_utc=ts_utc, price=p, source="kalshi_candlesticks_1m")
