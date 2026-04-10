from __future__ import annotations

import logging

from fastapi import FastAPI

from pnl_analyzer.config import settings
from pnl_analyzer.api.routes_analyze import router as analyze_router
from pnl_analyzer.logging_setup import configure_logging


configure_logging()

app = FastAPI(title="Prediction Market PnL Analyzer", version="0.1.0")
app.include_router(analyze_router, prefix="/v1")


@app.on_event("startup")
async def _startup() -> None:
    if settings.database_url:
        from pnl_analyzer.db.persist import ensure_schema

        try:
            await ensure_schema()
        except Exception as e:
            logging.getLogger("pnl_analyzer").warning("DB disabled (startup connect failed): %s", e)


@app.get("/health")
async def health() -> dict:
    return {"ok": True}
