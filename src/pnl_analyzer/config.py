from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "dev"
    log_level: str = "INFO"
    default_export_timezone: str = "America/New_York"

    # Sizing
    default_bet_units: float = 1.0
    unit_notional_usd: float = 100.0

    # DB
    database_url: str | None = None

    # LLM
    llm_provider: str = "mock"  # openai|openrouter|mock
    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"
    openrouter_api_key: str | None = None
    openrouter_model: str = "openai/gpt-4o-mini"
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_http_referer: str | None = None
    openrouter_x_title: str | None = None

    # Kalshi
    kalshi_base_url: str = "https://demo-api.kalshi.co"
    kalshi_api_prefix: str = "/trade-api/v2"
    kalshi_key_id: str | None = None
    kalshi_private_key_pem: str | None = None
    kalshi_private_key_path: str | None = None
    kalshi_assume_liquidity: str = "taker"  # taker|maker
    kalshi_taker_fee_multiplier: float = 0.07
    kalshi_maker_fee_multiplier: float = 0.0175
    kalshi_index_fee_multiplier: float = 0.035

    # Polymarket
    polymarket_gamma_base_url: str = "https://gamma-api.polymarket.com"
    polymarket_clob_base_url: str = "https://clob.polymarket.com"


settings = Settings()
