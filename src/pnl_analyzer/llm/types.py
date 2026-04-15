from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic import ConfigDict


class BetCall(BaseModel):
    model_config = ConfigDict(extra="ignore")

    author: str
    timestamp_utc: str
    platform: str = Field(description="kalshi|polymarket")
    market_intent: str
    position_direction: str = Field(description="YES|NO")
    quoted_price: float | None = Field(default=None, ge=0.0, le=1.0, description="Optional; may be filled from historical pricing later")
    bet_size_units: float = Field(default=1.0, gt=0.0)
    source_message_index: int | None = Field(default=None, ge=0)
    action: str | None = Field(default=None, description="BUY|SELL|ADD|TRIM|UNKNOWN")
    market_ref: dict | None = Field(default=None, description="Parsed market reference derived from URLs/tickers when available")
    extraction_confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list, description="Short snippets supporting the extracted call")
