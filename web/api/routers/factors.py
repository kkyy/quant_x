from typing import Optional

import pandas as pd
from fastapi import APIRouter, Query
from pydantic import BaseModel

from web.api.deps import get_config
from web.api.services.factor_service import compute_factor_values, compute_ic_analysis
from web.api.services.task_manager import get_task_manager

router = APIRouter()


class EvaluateRequest(BaseModel):
    name: str


class MineRequest(BaseModel):
    min_ic: float = 0.03
    min_icir: float = 0.4
    top_n: int = 30


@router.get("")
async def list_factors():
    from quant_ex.features.base import FactorRegistry
    try:
        from quant_ex.models import trainer
    except Exception:
        pass
    factors = []
    for name in FactorRegistry.list():
        cls = FactorRegistry.get(name)
        factors.append({"name": name, "class": cls.__name__})
    return factors


@router.get("/library")
async def factor_library():
    from quant_ex.features.base import FactorRegistry
    config = get_config()
    try:
        from quant_ex.models import trainer
    except Exception:
        pass

    enabled = set()
    for fc in config.get("model", {}).get("features", {}).get("factors", []):
        enabled.add(fc.get("name"))

    result = []
    for name in FactorRegistry.list():
        cls = FactorRegistry.get(name)
        result.append({"name": name, "class": cls.__name__, "enabled": name in enabled})
    return result


@router.post("/evaluate")
async def evaluate_factor(req: EvaluateRequest):
    tm = get_task_manager()
    def _eval():
        return {"factor": req.name, "message": "evaluation not yet implemented in web mode"}
    task_id = await tm.start_sync_task("factor_eval", _eval)
    return {"task_id": task_id}


@router.post("/mine")
async def mine_factors(req: MineRequest):
    tm = get_task_manager()
    def _mine():
        import subprocess, sys
        cmd = [sys.executable, "run_factor_mining.py",
               "--min-ic", str(req.min_ic),
               "--min-icir", str(req.min_icir),
               "--top-n", str(req.top_n)]
        subprocess.run(cmd, check=False)
        return {"status": "completed"}
    task_id = await tm.start_sync_task("factor_mine", _mine)
    return {"task_id": task_id}


@router.get("/values")
async def factor_values(
    factors: str = Query(..., description="Comma-separated factor names"),
    symbols: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    factor_list = [f.strip() for f in factors.split(",")]
    symbol_list = [s.strip() for s in symbols.split(",")] if symbols else None
    return compute_factor_values(factor_list, symbol_list, start, end)


@router.get("/ic-analysis")
async def ic_analysis(
    factor: str = Query(...),
    horizon: int = Query(5, ge=1, le=60),
    window: int = Query(20, ge=5, le=120),
):
    return compute_ic_analysis(factor, horizon, window)


@router.get("/heatmap")
async def factor_heatmap(
    factors: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    factor_list = [f.strip() for f in factors.split(",")]
    result = compute_factor_values(factor_list, start=start, end=end)
    if not result["data"]:
        return {"factors": factor_list, "matrix": []}
    df = pd.DataFrame(result["data"])
    numeric_cols = [c for c in df.columns if c not in ("symbol", "date", "instrument")]
    if len(numeric_cols) < 2:
        return {"factors": factor_list, "matrix": [[1.0]]}
    corr = df[numeric_cols].corr().fillna(0).values.tolist()
    corr = [[round(float(v), 4) for v in row] for row in corr]
    return {"factors": numeric_cols, "matrix": corr}
