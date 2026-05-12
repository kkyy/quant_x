import copy
import logging
import subprocess
import sys
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from web.api.deps import PROJECT_ROOT, SIGNALS_DIR, get_config

logger = logging.getLogger(__name__)

router = APIRouter()


def _safe_path(base_dir, filename: str):
    """Prevent path traversal."""
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=403, detail="Invalid filename")
    return base_dir / filename


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
    universe: Optional[str] = None
    refresh_cache: bool = False
    config: Optional[str] = None
    position_date: Optional[str] = None
    min_action_value: Optional[float] = None


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
            config_path=req.config,
            model_path=req.model_path,
            account=req.account,
            current_positions=positions if positions else None,
            dry_run=req.dry_run,
        )
        return {"status": "completed"}

    task_id = await tm.start_sync_task("signal_generate", _generate)
    return {"task_id": task_id}


class RebalanceRequest(BaseModel):
    mock: bool = True
    dry_run: bool = True
    config: Optional[str] = None
    positions: Optional[str] = None
    position_date: Optional[str] = None
    min_action_value: Optional[float] = None
    skip_update: bool = True
    force: bool = False
    notify_channel: Optional[str] = None


def _build_rebalance_cmd(req: RebalanceRequest) -> list[str]:
    cmd = [sys.executable, str(PROJECT_ROOT / "run_scheduled_rebalance.py")]
    if req.mock:
        cmd.append("--mock")
    if req.dry_run:
        cmd.append("--dry-run")
    if req.config:
        cmd.extend(["--config", req.config])
    if req.positions:
        cmd.extend(["--positions", req.positions])
    if req.position_date:
        cmd.extend(["--position-date", req.position_date])
    if req.min_action_value is not None:
        cmd.extend(["--min-action-value", str(req.min_action_value)])
    if req.skip_update:
        cmd.append("--skip-update")
    if req.force:
        cmd.append("--force")
    if req.notify_channel:
        cmd.extend(["--notify-channel", req.notify_channel])
    return cmd


@router.post("/rebalance")
async def run_rebalance(req: RebalanceRequest):
    from web.api.services.task_manager import get_task_manager

    tm = get_task_manager()

    def _run():
        cmd = _build_rebalance_cmd(req)
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f"Rebalance failed (exit {result.returncode}): {result.stderr[-500:]}")
        return {"stdout": result.stdout[-2000:], "returncode": result.returncode}

    task_id = await tm.start_sync_task("rebalance", _run)
    return {"task_id": task_id}


class NotifyTestRequest(BaseModel):
    title: str
    content: str
    channel: Optional[str] = None
    dry_run: bool = True
    confirm_send: bool = False


_NOTIFY_CHANNELS = {"bark", "pushplus", "dingtalk", "serverchan", "wechat_mp", "all"}


def _enabled_notify_channels(config: dict) -> list[str]:
    notify_cfg = config.get("notify") or {
        key: config.get(key, {})
        for key in ("bark", "pushplus", "dingtalk", "serverchan", "wechat_mp")
        if key in config
    }
    return [
        name
        for name, cfg in notify_cfg.items()
        if isinstance(cfg, dict) and cfg.get("enabled", False)
    ]


def _notify_config_for_channel(config: dict, channel: Optional[str]) -> dict:
    if not channel or channel == "all":
        return config
    if channel not in _NOTIFY_CHANNELS:
        raise HTTPException(status_code=400, detail=f"Unknown notification channel: {channel}")

    patched = copy.deepcopy(config)
    notify_cfg = patched.get("notify")
    if not notify_cfg:
        notify_cfg = {
            key: patched.get(key, {})
            for key in ("bark", "pushplus", "dingtalk", "serverchan", "wechat_mp")
            if key in patched
        }
        patched["notify"] = notify_cfg

    for name, cfg in notify_cfg.items():
        if isinstance(cfg, dict):
            cfg["enabled"] = name == channel
    return patched


@router.post("/notify-test")
async def send_notify_test(req: NotifyTestRequest):
    config = get_config()
    selected_channel = req.channel or "all"
    if selected_channel not in _NOTIFY_CHANNELS:
        raise HTTPException(status_code=400, detail=f"Unknown notification channel: {selected_channel}")

    enabled_channels = _enabled_notify_channels(_notify_config_for_channel(config, selected_channel))
    if req.dry_run:
        return {
            "success": True,
            "dry_run": True,
            "sent": False,
            "channels": enabled_channels,
        }
    if not req.confirm_send:
        raise HTTPException(
            status_code=400,
            detail="Real notification requires confirm_send=true.",
        )

    try:
        from quant_ex.notify.pusher import NotificationPusher
        pusher = NotificationPusher(_notify_config_for_channel(config, selected_channel))
        results = pusher.send(title=req.title, content=req.content)
        return {"success": all(results.values()) if results else False, "dry_run": False, "sent": True, "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    path = _safe_path(SIGNALS_DIR, filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Signal file not found")
    return {"content": path.read_text(encoding="utf-8")}
