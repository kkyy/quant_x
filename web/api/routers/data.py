import json
from datetime import datetime, date as date_mod, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
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


def _normalize_sector_symbol(code: str) -> str:
    raw = str(code).strip().upper()
    if not raw:
        return raw
    if raw.startswith(("SH", "SZ", "BJ")):
        return raw
    if raw.startswith(("6", "9")):
        return f"SH{raw}"
    if raw.startswith(("8", "4")):
        return f"BJ{raw}"
    return f"SZ{raw}"


def _load_sector_groups() -> dict[str, dict]:
    """Load sectors from the checked-in sector map, with crawler cache fallback."""
    sector_map_path = CACHE_DIR / "sector_map.json"
    if sector_map_path.exists():
        with open(sector_map_path, encoding="utf-8") as f:
            sector_map = json.load(f)
        groups: dict[str, dict] = {}
        for symbol, sector_name in sector_map.items():
            sector_id = str(sector_name)
            group = groups.setdefault(
                sector_id,
                {"sector_id": sector_id, "sector_name": sector_id, "stocks": []},
            )
            group["stocks"].append(_normalize_sector_symbol(symbol))
        for group in groups.values():
            group["stocks"] = sorted(set(group["stocks"]))
        return groups

    crawler_path = Path(__file__).resolve().parents[3] / "crawler" / "data" / "sector_stocks.json"
    if not crawler_path.exists():
        return {}

    with open(crawler_path, encoding="utf-8") as f:
        crawler_data = json.load(f)

    groups = {}
    for category_data in crawler_data.values():
        if not isinstance(category_data, dict):
            continue
        for sector_id, payload in category_data.items():
            if not isinstance(payload, dict):
                continue
            stocks = [
                _normalize_sector_symbol(item.get("code", ""))
                for item in payload.get("stocks", [])
                if item.get("code")
            ]
            groups[sector_id] = {
                "sector_id": sector_id,
                "sector_name": payload.get("name") or sector_id,
                "stocks": sorted(set(stocks)),
            }
    return groups


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
        raise HTTPException(status_code=400, detail=f"Unknown type: {data_type}")
    _, cache_dir, ttl = registry[data_type]
    d = Path(cache_dir)
    if not d.exists():
        return {"deleted": 0}
    deleted = 0
    for f in d.glob("*.csv"):
        mtime = date_mod.fromtimestamp(f.stat().st_mtime)
        if (date_mod.today() - mtime).days > ttl:
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


@router.get("/stock/search")
async def stock_search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    from web.api.services.data_service import search_stocks
    return search_stocks(q, limit)


@router.get("/stock/{symbol}/quotes")
async def stock_quotes(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fields: Optional[str] = None,
):
    from web.api.services.data_service import get_stock_quotes
    field_list = fields.split(",") if fields else None
    return get_stock_quotes(symbol, start or "2020-01-01", end, field_list)


@router.get("/sectors")
async def list_sectors():
    groups = _load_sector_groups()
    return sorted(
        [
            {
                "sector_id": group["sector_id"],
                "sector_name": group["sector_name"],
                "stock_count": len(group["stocks"]),
            }
            for group in groups.values()
        ],
        key=lambda item: (-item["stock_count"], item["sector_name"]),
    )


@router.get("/sectors/rotation")
async def sector_rotation(windows: str = Query("1,5,20")):
    from web.api.services.data_service import _qlib_loader, _cached

    window_list = [int(w) for w in windows.split(",") if w.strip().isdigit()]

    def _compute():
        sector_data = _load_sector_groups()

        if not sector_data:
            return []

        loader = _qlib_loader()
        if loader is None:
            return []

        today = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=max(window_list) + 30)).strftime("%Y-%m-%d")

        results = []
        for sector_id, group in sector_data.items():
            stocks = group["stocks"]
            if not stocks:
                continue
            qlib_instruments = stocks[:50]

            try:
                price_data = loader.load(
                    instruments=qlib_instruments,
                    start_time=start,
                    end_time=today,
                    fields=["$close"],
                )
                if price_data is None or price_data.empty:
                    continue

                close = price_data["$close"].unstack(level=0) if hasattr(price_data["$close"], "unstack") else None
                if close is None or close.empty:
                    continue

                sector_mean = close.mean(axis=1)
                returns = {}
                for w in window_list:
                    if len(sector_mean) > w:
                        ret = (sector_mean.iloc[-1] / sector_mean.iloc[-w - 1] - 1) if len(sector_mean) > w + 1 else 0.0
                        returns[f"{w}d"] = round(float(ret), 4)

                results.append({
                    "sector_id": sector_id,
                    "sector_name": group["sector_name"],
                    "returns": returns,
                })
            except Exception:
                continue

        return results

    cache_key = f"sector_rotation_{windows}"
    return _cached(cache_key, ttl=14400.0, factory=_compute)


@router.get("/sectors/{sector_id}/stocks")
async def sector_stocks(sector_id: str):
    groups = _load_sector_groups()
    group = groups.get(sector_id)
    if group is None:
        return {"sector_id": sector_id, "sector_name": sector_id, "stocks": []}
    from quant_ex.data.utils import load_stock_names
    names = load_stock_names()
    return {
        "sector_id": sector_id,
        "sector_name": group["sector_name"],
        "stocks": [{"symbol": s, "name": names.get(s, s)} for s in group["stocks"]],
    }


@router.get("/alt-data/{data_type}")
async def alt_data(
    data_type: str,
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    cache_dir = CACHE_DIR / data_type
    if not cache_dir.exists():
        return {"type": data_type, "columns": [], "rows": [], "total": 0, "has_more": False}

    import pandas as pd
    csv_files = sorted(cache_dir.glob("*.csv"))
    if not csv_files:
        return {"type": data_type, "columns": [], "rows": [], "total": 0, "has_more": False}

    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            if symbol and "symbol" in df.columns:
                df = df[df["symbol"].str.contains(symbol, case=False, na=False)]
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return {"type": data_type, "columns": [], "rows": [], "total": 0, "has_more": False}

    combined = pd.concat(dfs, ignore_index=True)

    if start and "date" in combined.columns:
        combined = combined[combined["date"] >= start]
    if end and "date" in combined.columns:
        combined = combined[combined["date"] <= end]

    total = len(combined)
    has_more = total > limit
    combined = combined.head(limit)

    columns = combined.columns.tolist()
    rows = combined.to_dict(orient="records")
    for row in rows:
        for k, v in row.items():
            if pd.isna(v):
                row[k] = None

    return {"type": data_type, "columns": columns, "rows": rows, "total": total, "has_more": has_more}
