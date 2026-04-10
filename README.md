# Prediction Market PnL Analyzer
Uploads a Discord `.txt` export, extracts prediction-market calls (Kalshi/Polymarket), verifies markets + historical prices, and computes per-bet and per-user PnL/ROI.

## Quickstart
1. Start Postgres (optional but recommended):
   - `docker compose up -d db`
2. Create a venv + install:
   - `python3 -m venv .venv && source .venv/bin/activate`
   - `pip install -e .`
3. Configure env:
   - `cp .env.example .env` and fill in keys as needed
4. Run API:
   - `uvicorn pnl_analyzer.main:app --reload`

Then open:
- `http://127.0.0.1:8000/docs`

## Primary Endpoint
`POST /v1/analyze` (multipart form upload)
- file: Discord export `.txt`
- export_timezone (optional): e.g. `America/New_York`
- verify_prices (optional): `true|false` (defaults true)

Returns:
- parsed messages
- extracted bet calls
- market matches + verified entry prices (best-effort)
- per-bet PnL and aggregate stats

## Notes on accuracy
- Polymarket price verification uses the public CLOB `/prices-history` endpoint (1m by default).
- Kalshi requests can be signed using `KALSHI_KEY_ID` + `KALSHI_PRIVATE_KEY_PEM` (RSA-PSS SHA256).
- Recommended: store the Kalshi PEM in `secrets/kalshi_private_key.pem` and set `KALSHI_PRIVATE_KEY_PATH=secrets/kalshi_private_key.pem`.
- Kalshi fees are computed using the published fee schedule formula and rounded up to the next cent; maker/taker is configurable.
- Matching is fuzzy; if you have a known market ticker/ID mapping, you can supply it in v2 (TODO).
- LLM extraction supports `LLM_PROVIDER=openrouter` (recommended for model choice) or `LLM_PROVIDER=openai`.
