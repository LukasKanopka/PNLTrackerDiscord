from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

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
            # Disable DB for the running process to avoid repeated connection attempts during requests.
            settings.database_url = None


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


# --- Optional React UI (served from ui/dist if present) ---
_REPO_ROOT = Path(__file__).resolve().parents[2]
_UI_DIST = _REPO_ROOT / "ui" / "dist"

if _UI_DIST.exists() and (_UI_DIST / "index.html").exists():

    @app.get("/")
    async def _ui_index() -> FileResponse:
        return FileResponse(_UI_DIST / "index.html")

    @app.get("/{path:path}")
    async def _ui_assets_or_spa(path: str) -> FileResponse:
        # Let API/docs routes win (they are declared earlier); this is a fallback.
        if path.startswith("v1") or path in ("docs", "openapi.json", "health"):
            # If this is hit, it's because the route didn't exist; still return 404-ish via index fallback.
            return FileResponse(_UI_DIST / "index.html")
        p = (_UI_DIST / path).resolve()
        try:
            if _UI_DIST in p.parents and p.is_file():
                return FileResponse(p)
        except Exception:
            pass
        return FileResponse(_UI_DIST / "index.html")
