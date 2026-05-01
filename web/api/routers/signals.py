from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from web.api.deps import SIGNALS_DIR, get_config

router = APIRouter()


@router.get("/regime")
async def get_regime():
    config = get_config()
    try:
        from quant_ex.strategy.regime_switch import RegimeStrategySwitch
        rss = RegimeStrategySwitch.from_config(config)
        if rss is None:
            return {"enabled": False, "regime": None, "label": None}
        return {"enabled": True, "regime": None, "label": "requires_price_data"}
    except Exception as exc:
        return {"enabled": False, "error": str(exc)}


class GenerateSignalRequest(BaseModel):
    model_path: str
    account: float = 1000000
    positions: Optional[str] = None
    dry_run: bool = True


@router.post("/generate")
async def generate_signal(req: GenerateSignalRequest):
    from web.api.services.task_manager import get_task_manager
    tm = get_task_manager()

    def _generate():
        from quant_ex.run_daily import main as daily_main

        positions = {}
        if req.positions:
            for pair in req.positions.split(","):
                sym, qty = pair.strip().split(":")
                positions[sym] = float(qty)

        daily_main(
            model_path=req.model_path,
            account=req.account,
            current_positions=positions if positions else None,
            dry_run=req.dry_run,
        )
        return {"status": "completed"}

    task_id = await tm.start_sync_task("signal_generate", _generate)
    return {"task_id": task_id}


@router.get("/history")
async def signal_history():
    if not SIGNALS_DIR.exists():
        return []
    from datetime import datetime
    results = []
    for f in sorted(SIGNALS_DIR.glob("signal_*.txt"), reverse=True):
        results.append({
            "filename": f.name,
            "size_kb": round(f.stat().st_size / 1024, 1),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    return results


@router.get("/history/{filename}")
async def get_signal(filename: str):
    path = SIGNALS_DIR / filename
    if not path.exists():
        return {"error": "Not found"}
    return {"content": path.read_text(encoding="utf-8")}
