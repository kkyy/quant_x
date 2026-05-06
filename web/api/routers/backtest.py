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
    optimize: bool = False
    n_iters: int = 3
    grid_workers: int = 1
    output_csv: Optional[str] = None
    slippage_multipliers: Optional[list[float]] = None
    markets: Optional[list[str]] = None


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
        if req.optimize:
            argv.append("--optimize")
        if req.n_iters != 3:
            argv.extend(["--n-iters", str(req.n_iters)])
        if req.grid_workers != 1:
            argv.extend(["--grid-workers", str(req.grid_workers)])
        if req.output_csv:
            argv.extend(["--output-csv", req.output_csv])
        if req.slippage_multipliers:
            argv.extend(["--slippage-multipliers", ",".join(str(x) for x in req.slippage_multipliers)])
        if req.markets:
            argv.extend(["--markets", ",".join(req.markets)])
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
    seeds: bool = False
    run_id: Optional[str] = None
    grid_workers: int = 1
    robust_weights: Optional[dict] = None
    folds_config: Optional[str] = None
    train_config: Optional[str] = None


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
        if req.seeds:
            cmd.append("--seeds")
        if req.run_id:
            cmd.extend(["--run-id", req.run_id])
        if req.grid_workers != 1:
            cmd.extend(["--grid-workers", str(req.grid_workers)])
        if req.robust_weights:
            import json
            cmd.extend(["--robust-weights", json.dumps(req.robust_weights)])
        if req.folds_config:
            cmd.extend(["--folds-config", req.folds_config])
        if req.train_config:
            cmd.extend(["--train-config", req.train_config])
        subprocess.run(cmd, check=False)
        return {"status": "completed"}

    task_id = await tm.start_sync_task("wfv", _wfv)
    return {"task_id": task_id}
