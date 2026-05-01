import json
from datetime import datetime, date as date_mod
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from web.api.deps import CACHE_DIR, get_config
from web.api.services.task_manager import get_task_manager
from web.api.routers.system import stream_task

router = APIRouter()


class FetchRequest(BaseModel):
    type: str
    scope: str = "all"
    symbols: Optional[list[str]] = None
    universe: Optional[str] = None
    ttl: Optional[int] = None
    force: bool = False


class CacheStatus(BaseModel):
    type: str
    file_count: int
    total_size_mb: float
    latest: Optional[str]
    ttl_days: int


def _get_fetcher_registry():
    from quant_ex.run_fetch_data import _FETCHER_REGISTRY
    return _FETCHER_REGISTRY


@router.get("/cache-status")
async def cache_status():
    registry = _get_fetcher_registry()
    results = []
    for name, (cls_name, cache_dir, ttl) in registry.items():
        d = Path(cache_dir)
        if not d.exists():
            results.append(CacheStatus(type=name, file_count=0, total_size_mb=0.0, latest=None, ttl_days=ttl))
            continue
        files = list(d.glob("*.csv"))
        total_size = sum(f.stat().st_size for f in files)
        latest = max((f.stat().st_mtime for f in files), default=0)
        results.append(CacheStatus(
            type=name,
            file_count=len(files),
            total_size_mb=round(total_size / 1024 / 1024, 2),
            latest=datetime.fromtimestamp(latest).isoformat() if latest else None,
            ttl_days=ttl,
        ))
    return results


@router.post("/fetch")
async def start_fetch(req: FetchRequest):
    tm = get_task_manager()

    def _fetch():
        from quant_ex.run_fetch_data import _get_fetcher_cls, fetch_generic, _FETCHER_REGISTRY
        registry = _FETCHER_REGISTRY
        if req.type == "all":
            types_to_fetch = list(registry.keys())
        else:
            types_to_fetch = [req.type]

        results = {}
        for t in types_to_fetch:
            cls_name, cache_dir, ttl = registry[t]
            ttl = req.ttl or ttl
            try:
                fetch_generic(t, symbols=[], cache_dir=cache_dir, ttl_days=ttl)
                results[t] = "done"
            except Exception as exc:
                results[t] = f"error: {exc}"
        return results

    task_id = await tm.start_sync_task("data_fetch", _fetch)
    return {"task_id": task_id}


@router.get("/fetch/{task_id}/stream")
async def stream_fetch(task_id: str):
    return await stream_task(task_id)


@router.delete("/cache/{data_type}/expired")
async def delete_expired(data_type: str):
    registry = _get_fetcher_registry()
    if data_type not in registry:
        return {"error": f"Unknown type: {data_type}"}
    _, cache_dir, ttl = registry[data_type]
    d = Path(cache_dir)
    if not d.exists():
        return {"deleted": 0}
    deleted = 0
    for f in d.glob("*.csv"):
        mtime = date_mod.fromtimestamp(f.stat().st_mtime)
        if (date_mod.today() - mtime).days >= ttl:
            f.unlink()
            deleted += 1
    return {"deleted": deleted}


@router.get("/stock-lookup/{symbol}")
async def stock_lookup(symbol: str):
    from quant_ex.data.utils import load_stock_names
    names = load_stock_names()
    matched = {k: v for k, v in names.items() if symbol.upper() in k or symbol.lower() in v.lower()}
    if not matched:
        return {"symbol": symbol, "matches": []}

    registry = _get_fetcher_registry()
    result = []
    for sym, name in matched.items():
        cache_files = []
        for dtype, (_, cache_dir, _) in registry.items():
            d = Path(cache_dir)
            if d.exists():
                bare = sym[2:]
                for f in d.glob(f"*{bare}*"):
                    cache_files.append({
                        "type": dtype,
                        "file": f.name,
                        "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                    })
        result.append({"symbol": sym, "name": name, "cache_files": cache_files})
    return {"symbol": symbol, "matches": result}
