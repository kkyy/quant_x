import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from web.api.deps import MODELS_DIR, get_config

router = APIRouter()


@router.get("")
async def list_models():
    if not MODELS_DIR.exists():
        return []
    models = []
    for pkl in sorted(MODELS_DIR.glob("*.pkl")):
        meta_path = MODELS_DIR / f"{pkl.stem}_meta.json"
        meta = {}
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
        models.append({
            "filename": pkl.name,
            "size_mb": round(pkl.stat().st_size / 1024 / 1024, 2),
            "modified": datetime.fromtimestamp(pkl.stat().st_mtime).isoformat(),
            "meta": meta,
        })
    return models


@router.get("/registry")
async def model_registry():
    from quant_ex.models.base import ModelRegistry
    from quant_ex.features.base import FactorRegistry

    try:
        from quant_ex.models import trainer
    except Exception:
        pass

    return {
        "models": [{"name": n} for n in ModelRegistry.list()],
        "factors": [{"name": n} for n in FactorRegistry.list()],
    }


@router.get("/{filename}/meta")
async def get_meta(filename: str):
    meta_path = MODELS_DIR / f"{Path(filename).stem}_meta.json"
    if not meta_path.exists():
        return {}
    with open(meta_path) as f:
        return json.load(f)


@router.get("/{filename}/importance")
async def get_importance(filename: str):
    imp_path = MODELS_DIR / f"{Path(filename).stem}_feature_importance.json"
    if not imp_path.exists():
        return {}
    with open(imp_path) as f:
        return json.load(f)


class TrainRequest(BaseModel):
    model: str = "lgbm"
    tag: Optional[str] = None
    factors: list[str] = []
    fit_start: Optional[str] = None
    fit_end: Optional[str] = None
    qlib_native: bool = False


@router.post("/train")
async def start_training(req: TrainRequest):
    from web.api.services.task_manager import get_task_manager
    tm = get_task_manager()

    def _train():
        from quant_ex.utils.config import load_config
        from quant_ex.data.loader import DataLoader
        from quant_ex.models.trainer import ModelTrainer
        from quant_ex.features.base import FactorPipeline

        cfg = load_config()
        loader = DataLoader(cfg)
        trainer = ModelTrainer(cfg, loader)

        factor_pipeline = None
        if req.factors:
            factor_configs = [{"name": f} for f in req.factors]
            factor_pipeline = FactorPipeline.from_config(factor_configs)

        kwargs = {}
        if req.fit_start:
            kwargs["fit_start"] = req.fit_start
        if req.fit_end:
            kwargs["fit_end"] = req.fit_end

        model, dataset, recorder_id = trainer.train(
            model_name=req.model,
            tag=req.tag,
            factor_pipeline=factor_pipeline,
            qlib_native=req.qlib_native,
            **kwargs,
        )
        return {"recorder_id": recorder_id}

    task_id = await tm.start_sync_task("model_train", _train)
    return {"task_id": task_id}
