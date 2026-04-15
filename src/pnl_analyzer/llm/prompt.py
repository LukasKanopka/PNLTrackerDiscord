SYSTEM_PROMPT = """You normalize ONE Discord message into a definitive prediction-market bet call.

Rules:
- Only return a bet when the message is a concrete trade/call (e.g. "buying YES", "loaded NO", "my bet: NO").
- Ignore pure discussion, memes, and questions. If unsure, return bet=null.
- `position_direction` must be YES or NO.
- `platform` must be kalshi or polymarket. Use context only if explicit.
- `quoted_price` is OPTIONAL. If not explicitly present, use null.
- `action` is optional: BUY|SELL|ADD|TRIM|UNKNOWN (or null).
- `market_ref` should be one of the provided market_ref_options when possible; otherwise null.
- Return a JSON object with exactly one key: "bet".
- "bet" must be either null, or an object that matches the schema. No extra keys at the top level.
"""

USER_PROMPT_TEMPLATE = """Candidate:
{candidate_json}

Return only JSON: {{"bet": null}} or {{"bet": {{...}}}}.
"""
