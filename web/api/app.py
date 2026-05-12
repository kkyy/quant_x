"""FastAPI application factory."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager

# Ensure both quant_ex root (for web.api imports) and its parent (for quant_ex.* imports)
_project_root = str(Path(__file__).resolve().parent.parent.parent)      # quant_ex/
_project_parent = str(Path(__file__).resolve().parent.parent.parent.parent)  # strategy/
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _project_parent not in sys.path:
    sys.path.insert(0, _project_parent)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="quant_ex Dashboard",
        version="0.1.0",
        lifespan=lifespan,
    )

    _cors_origins = os.environ.get(
        "CORS_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173",
    ).split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from web.api.routers import system, data, models, backtest, signals, factors, config as config_router

    app.include_router(system.router, prefix="/api/system", tags=["system"])
    app.include_router(data.router, prefix="/api/data", tags=["data"])
    app.include_router(models.router, prefix="/api/models", tags=["models"])
    app.include_router(backtest.router, prefix="/api/backtest", tags=["backtest"])
    app.include_router(signals.router, prefix="/api/signals", tags=["signals"])
    app.include_router(factors.router, prefix="/api/factors", tags=["factors"])
    app.include_router(config_router.router, prefix="/api/config", tags=["config"])

    static_dir = Path(__file__).resolve().parent.parent / "frontend" / "dist"
    if static_dir.is_dir():
        assets_dir = static_dir / "assets"
        if assets_dir.is_dir():
            app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

        index_file = static_dir / "index.html"

        @app.get("/", include_in_schema=False)
        async def serve_spa_index():
            return FileResponse(index_file)

        @app.get("/{full_path:path}", include_in_schema=False)
        async def serve_spa_or_static(full_path: str):
            if full_path.startswith("api/"):
                raise HTTPException(status_code=404, detail="Not Found")

            candidate = (static_dir / full_path).resolve()
            if candidate.is_file() and candidate.is_relative_to(static_dir.resolve()):
                return FileResponse(candidate)
            return FileResponse(index_file)

    return app


app = create_app()
