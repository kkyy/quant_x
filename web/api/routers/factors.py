from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel
from web.api.deps import get_config
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
