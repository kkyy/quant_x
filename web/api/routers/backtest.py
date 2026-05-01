from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import FileResponse
from pydantic import BaseModel

from web.api.deps import get_config, BACKTEST_RESULTS_DIR
from web.api.services.task_manager import get_task_manager
from web.api.routers.system import stream_task

router = APIRouter()


class GridSearchRequest(BaseModel):
    model_path: str
    topk: list[int] = [5, 10, 15, 20]
    n_drop: list[int] = [1, 3, 5]
    hold_thresh: list[int] = [3, 5, 10]
    start: Optional[str] = None
    end: Optional[str] = None
    market: str = "csi300"
    multi_seed: bool = False


@router.post("/grid")
async def start_grid_search(req: GridSearchRequest):
    tm = get_task_manager()

    def _grid():
        import subprocess, sys
        argv = [sys.executable, "run_backtest.py",
                "--model-path", req.model_path,
                "--topk", ",".join(str(x) for x in req.topk),
                "--n-drop", ",".join(str(x) for x in req.n_drop),
                "--hold-thresh", ",".join(str(x) for x in req.hold_thresh),
                "--market", req.market]
        if req.start:
            argv.extend(["--start", req.start])
        if req.end:
            argv.extend(["--end", req.end])
        if req.multi_seed:
            argv.append("--seeds")
        subprocess.run(argv, check=False)
        return {"status": "completed"}

    task_id = await tm.start_sync_task("grid_search", _grid)
    return {"task_id": task_id}


@router.get("/grid/{task_id}/stream")
async def stream_grid(task_id: str):
    return await stream_task(task_id)


@router.get("/results")
async def list_results():
    if not BACKTEST_RESULTS_DIR.exists():
        return []
    results = []
    for f in sorted(BACKTEST_RESULTS_DIR.glob("*.csv"), reverse=True):
        results.append({
            "filename": f.name,
            "size_kb": round(f.stat().st_size / 1024, 1),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    return results


@router.get("/results/{filename}")
async def get_result(filename: str):
    import pandas as pd
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {"error": "Not found"}
    df = pd.read_csv(path)
    return {"columns": list(df.columns), "rows": df.to_dict(orient="records")[:200]}


@router.get("/charts/{filename}")
async def get_chart(filename: str):
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {"error": "Not found"}
    return FileResponse(str(path), media_type="image/png")


class WFVRequest(BaseModel):
    train_universes: list[str] = ["csi300"]
    eval_market: str = "csi300"
    topk: list[int] = [5, 15, 20]
    n_drop: list[int] = [1, 3]
    hold_thresh: list[int] = [5, 8, 10]
    workers: int = 1


@router.post("/walk-forward")
async def start_wfv(req: WFVRequest):
    tm = get_task_manager()

    def _wfv():
        import subprocess, sys
        cmd = [sys.executable, "run_walk_forward_validation.py",
               "--train-universes", ",".join(req.train_universes),
               "--eval-market", req.eval_market,
               "--topk", ",".join(str(x) for x in req.topk),
               "--n-drop", ",".join(str(x) for x in req.n_drop),
               "--hold-thresh", ",".join(str(x) for x in req.hold_thresh),
               "--workers", str(req.workers)]
        subprocess.run(cmd, check=False)
        return {"status": "completed"}

    task_id = await tm.start_sync_task("wfv", _wfv)
    return {"task_id": task_id}
