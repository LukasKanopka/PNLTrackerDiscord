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
    quoted_price: float = Field(ge=0.0, le=1.0)
    bet_size_units: float = Field(default=1.0, gt=0.0)
    source_message_index: int | None = Field(default=None, ge=0)
