from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from web.api.deps import CONFIG_DIR

router = APIRouter()

_VALID_CONFIGS = {"base", "model", "notify", "strategy_candidates"}


@router.get("/{name}")
async def read_config(name: str):
    if name not in _VALID_CONFIGS:
        raise HTTPException(404, f"Unknown config: {name}")
    path = CONFIG_DIR / f"{name}.yaml"
    if not path.exists():
        return {"content": "", "exists": False}
    return {"content": path.read_text(encoding="utf-8"), "exists": True}


class ConfigUpdate(BaseModel):
    content: str


@router.put("/{name}")
async def write_config(name: str, body: ConfigUpdate):
    if name not in _VALID_CONFIGS:
        raise HTTPException(404, f"Unknown config: {name}")
    path = CONFIG_DIR / f"{name}.yaml"
    path.write_text(body.content, encoding="utf-8")
    return {"saved": True}


@router.get("/daily-presets/list")
async def list_daily_presets():
    presets = []
    for f in sorted(CONFIG_DIR.glob("daily_*.yaml")):
        presets.append({"filename": f.name})
    return presets
