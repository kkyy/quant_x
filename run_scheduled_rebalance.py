#!/usr/bin/env python3
"""After-close qlib update, backtest replay and Bark rebalance notification."""
from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT.parent))

from quant_ex.backtest.engine import BacktestEngine
from quant_ex.data.loader import DataLoader
from quant_ex.data.sector import SectorDataProvider
from quant_ex.data.universe import UniverseFilter
from quant_ex.notify.pusher import NotificationPusher
from quant_ex.signals.postprocess import postprocess_requires_price_data, postprocess_signal
from quant_ex.utils.config import load_config
from quant_ex.utils.logger import setup_logger
from quant_ex.utils.qlib_utils import load_recorder_model

logger = setup_logger("run_scheduled_rebalance")

SIGNAL_DATE_START_ALIASES = {"signal_date", "trade_date", "today"}
PREVIOUS_DATE_START_ALIASES = {"previous_trade_date", "previous_trading_day", "yesterday"}


@dataclass
class RebalanceAction:
    action: str
    instrument: str
    shares: float
    price: float
    value: float


def _resolve_path(path: str | Path) -> Path:
    p = Path(path).expanduser()
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=str, default=None, help="Optional YAML config override.")
    parser.add_argument("--model-path", type=str, default=None, help="Override daily_rebalance.model_path.")
    parser.add_argument("--start-date", type=str, default=None, help="Fixed replay start date, e.g. 2024-01-01.")
    parser.add_argument("--today", type=str, default=None, help="Override current date for testing.")
    parser.add_argument("--market", type=str, default=None, help="Default: daily_rebalance.market or csi1000.")
    parser.add_argument("--topk", type=int, default=None)
    parser.add_argument("--n-drop", type=int, default=None)
    parser.add_argument("--hold-thresh", type=int, default=None)
    parser.add_argument("--account", type=float, default=None)
    parser.add_argument("--mock", action="store_true", help="Skip data update/backtest and send a mock signal.")
    parser.add_argument("--remind", action="store_true", help="Send cached previous signal for today's execution.")
    parser.add_argument(
        "--reminder-label",
        choices=["open", "close"],
        default="open",
        help="Label used in reminder title/content.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print report without sending notification.")
    parser.add_argument("--force", action="store_true", help="Run even if today is not in the trading calendar.")
    parser.add_argument("--skip-update", action="store_true", help="Skip run_update_qlib_data.py in real mode.")
    parser.add_argument(
        "--no-reminder-rebuild",
        action="store_true",
        help="Do not rebuild a missing/stale cache during reminder mode.",
    )
    parser.add_argument("--create-update-tarball", action="store_true", help="Let update script create qlib_bin.tar.gz.")
    parser.add_argument("--notify-skip", action="store_true", help="Notify when skipped on a non-trading day.")
    parser.add_argument(
        "--notify-channel",
        choices=["bark", "all"],
        default=None,
        help="Default: daily_rebalance.notify_channel or bark.",
    )
    return parser.parse_args()


def _daily_cfg(config: dict, args: argparse.Namespace) -> Dict[str, Any]:
    cfg = copy.deepcopy(config.get("daily_rebalance", {}))
    cfg["market"] = args.market or cfg.get("market", "csi1000")
    cfg["topk"] = args.topk if args.topk is not None else int(cfg.get("topk", 15))
    cfg["n_drop"] = args.n_drop if args.n_drop is not None else int(cfg.get("n_drop", 3))
    cfg["hold_thresh"] = (
        args.hold_thresh if args.hold_thresh is not None else int(cfg.get("hold_thresh", 5))
    )
    start_date = args.start_date or cfg.get("start_date") or config.get("backtest", {}).get("start_time")
    cfg["start_date"] = start_date
    cfg["_start_date_raw"] = start_date
    cfg["account"] = args.account if args.account is not None else float(cfg.get("account", 1_000_000))
    cfg["model_path"] = args.model_path if args.model_path is not None else cfg.get("model_path", "")
    cfg["notify_channel"] = args.notify_channel or cfg.get("notify_channel", "bark")
    cfg["notify_on_skip"] = bool(args.notify_skip or cfg.get("notify_on_skip", False))
    cfg["create_update_tarball"] = bool(args.create_update_tarball or cfg.get("create_update_tarball", False))
    cfg["cache_dir"] = cfg.get("cache_dir") or "signals/daily_rebalance_cache"
    cfg["reminder_rebuild_on_miss"] = bool(
        cfg.get("reminder_rebuild_on_miss", True) and not args.no_reminder_rebuild
    )
    if not cfg["start_date"]:
        raise ValueError("daily_rebalance.start_date is required.")
    return cfg


def _resolve_start_date(
    value: str,
    trade_date: pd.Timestamp,
    calendar: List[pd.Timestamp],
) -> Tuple[str, bool]:
    token = str(value).strip().lower()
    if token in SIGNAL_DATE_START_ALIASES:
        return trade_date.strftime("%Y-%m-%d"), True
    if token in PREVIOUS_DATE_START_ALIASES:
        return _previous_trading_day(trade_date, calendar)
    return str(value), True


def _resolve_cfg_start_date(
    cfg: Dict[str, Any],
    trade_date: pd.Timestamp,
    calendar: List[pd.Timestamp],
) -> Dict[str, Any]:
    resolved = copy.deepcopy(cfg)
    raw_start = resolved.get("_start_date_raw") or resolved.get("start_date")
    start_date, exact = _resolve_start_date(str(raw_start), trade_date, calendar)
    if not exact:
        logger.warning("未在交易日历中找到动态回测起点，暂按工作日推断: %s", start_date)
    resolved["start_date"] = start_date
    return resolved


def _apply_strategy_config(config: dict, cfg: Dict[str, Any]) -> dict:
    config = copy.deepcopy(config)
    config.setdefault("market", {})["name"] = cfg["market"]
    config.setdefault("strategy", {}).setdefault("topk_dropout", {})
    config["strategy"]["topk_dropout"].update(
        {
            "topk": int(cfg["topk"]),
            "n_drop": int(cfg["n_drop"]),
            "hold_thresh": int(cfg["hold_thresh"]),
        }
    )
    config.setdefault("backtest", {})["account"] = float(cfg["account"])
    config["backtest"]["start_time"] = cfg["start_date"]
    return config


def _calendar_files(config: dict) -> Tuple[Path, Path]:
    provider_uri = _resolve_path(config.get("qlib", {}).get("provider_uri", "./qlib_data/qlib_bin"))
    calendar_dir = provider_uri / "calendars"
    return calendar_dir / "day.txt", calendar_dir / "day_future.txt"


def _read_calendar(path: Path) -> List[pd.Timestamp]:
    if not path.exists():
        return []
    values = pd.read_csv(path, header=None).iloc[:, 0]
    dates = pd.to_datetime(values, errors="coerce").dropna().dt.normalize()
    return sorted(pd.Timestamp(day).normalize() for day in dates.unique())


def _trading_calendar(config: dict) -> Tuple[List[pd.Timestamp], List[pd.Timestamp]]:
    day_file, future_file = _calendar_files(config)
    actual = _read_calendar(day_file)
    future = _read_calendar(future_file)
    return actual, future or actual


def _next_trading_day(target: pd.Timestamp, calendar: List[pd.Timestamp]) -> Tuple[str, bool]:
    for day in calendar:
        if day > target:
            return day.strftime("%Y-%m-%d"), True
    next_bday = target + pd.offsets.BDay(1)
    return pd.Timestamp(next_bday).strftime("%Y-%m-%d"), False


def _previous_trading_day(target: pd.Timestamp, calendar: List[pd.Timestamp]) -> Tuple[str, bool]:
    for day in reversed(calendar):
        if day < target:
            return day.strftime("%Y-%m-%d"), True
    previous_bday = target - pd.offsets.BDay(1)
    return pd.Timestamp(previous_bday).strftime("%Y-%m-%d"), False


def _run_update(config_path: Optional[str], create_tarball: bool) -> None:
    cmd = [sys.executable, str(PROJECT_ROOT / "run_update_qlib_data.py")]
    if config_path:
        cmd.extend(["--config", config_path])
    if not create_tarball:
        cmd.append("--no-tarball")
    logger.info("更新 qlib 数据: %s", " ".join(cmd))
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def _load_model(config: dict, model_path: str = ""):
    if model_path:
        from quant_ex.models.base import BaseAlphaModel

        logger.info("加载模型: %s", model_path)
        return BaseAlphaModel.load(model_path)

    exp_cfg = config.get("experiment", {})
    recorder_id = exp_cfg.get("latest_recorder_id", "")
    if not recorder_id:
        raise RuntimeError(
            "未配置模型。请设置 daily_rebalance.model_path，或填写 experiment.latest_recorder_id。"
        )
    logger.info("加载 qlib recorder 模型: %s / %s", exp_cfg.get("name"), recorder_id)
    return load_recorder_model(exp_cfg.get("name", "tutorial_exp"), recorder_id)


def _predict_for_replay(
    model,
    data_loader: DataLoader,
    universe_filter: UniverseFilter,
    config: dict,
    instruments: str,
    start: str,
    end: str,
):
    tcfg = config.get("training", {})
    segments = {
        "train": (tcfg.get("fit_start", "2015-01-01"), tcfg.get("fit_end", "2021-12-31")),
        "valid": (tcfg.get("valid_start", "2022-01-01"), tcfg.get("valid_end", "2023-12-31")),
        "test": (start, end),
    }
    dataset = data_loader.build_dataset(segments=segments, instruments=instruments)

    price_data = None
    needs_full_price_data = (
        getattr(model, "factor_pipeline", None) is not None
        or postprocess_requires_price_data(config)
    )
    if needs_full_price_data:
        price_data = data_loader.load_price_data(
            instruments=instruments,
            start_time=tcfg.get("fit_start", "2015-01-01"),
            end_time=end,
        )

    if getattr(model, "factor_pipeline", None) is not None:
        logger.info("模型含有 factor_pipeline，为 %s 重新计算额外因子", instruments)
        model.refresh_extra_factors(price_data)

    pred = model.predict(dataset, segment="test")
    if universe_filter.requires_price_data():
        if price_data is None:
            price_data = data_loader.load_price_data(instruments=instruments, start_time=start, end_time=end)
        pred = universe_filter.filter(pred, price_data=price_data)
    else:
        pred = universe_filter.filter(pred)

    post_cfg = config.get("signal", {}).get("postprocess", {})
    sector_provider = (
        SectorDataProvider(config)
        if (
            post_cfg.get("industry_neutralize", False)
            or post_cfg.get("stock_vs_sector_filter", {}).get("enabled", False)
        )
        else None
    )
    sector_map = sector_provider.get_map() if sector_provider is not None else None
    return postprocess_signal(
        pred,
        config=config,
        sector_map=sector_map,
        price_data=price_data,
    )


def _position_payload(position_obj: Any) -> Dict[str, Any]:
    if hasattr(position_obj, "position"):
        return position_obj.position
    if isinstance(position_obj, dict) and "position" in position_obj:
        return position_obj["position"]
    if isinstance(position_obj, dict):
        return position_obj
    return {}


def _snapshot(position_obj: Any, lot_size: int = 100) -> Dict[str, Dict[str, float]]:
    payload = _position_payload(position_obj)
    result: Dict[str, Dict[str, float]] = {}
    for inst, info in payload.items():
        if not isinstance(info, dict) or "amount" not in info:
            continue
        shares = int(float(info.get("amount", 0)) / lot_size) * lot_size
        if shares <= 0:
            continue
        price = float(info.get("price", 0) or 0)
        result[inst] = {"shares": float(shares), "price": price, "value": shares * price}
    return result


def _source_csv_path(instrument: str) -> Path:
    return PROJECT_ROOT / "qlib_data" / "qlib_source" / f"{instrument}.csv"


def _load_actual_close(instrument: str, trade_date: str) -> Optional[float]:
    path = _source_csv_path(instrument)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, usecols=["tradedate", "close"])
    except Exception:
        return None
    dates = pd.to_datetime(df["tradedate"], errors="coerce")
    target = pd.Timestamp(trade_date).normalize()
    matched = df.loc[dates == target, "close"]
    if matched.empty:
        matched = df.loc[dates <= target, "close"].tail(1)
    if matched.empty:
        return None
    price = pd.to_numeric(matched, errors="coerce").dropna()
    if price.empty:
        return None
    value = float(price.iloc[-1])
    return value if value > 0 else None


def _convert_snapshot_to_actual_prices(
    snapshot: Dict[str, Dict[str, float]],
    trade_date: str,
    lot_size: int = 100,
) -> Dict[str, Dict[str, float]]:
    converted: Dict[str, Dict[str, float]] = {}
    for inst, info in snapshot.items():
        target_value = float(info.get("value", 0) or 0)
        actual_price = _load_actual_close(inst, trade_date)
        if actual_price is None or target_value <= 0:
            converted[inst] = dict(info)
            continue
        shares = int(target_value / actual_price / lot_size) * lot_size
        if shares <= 0:
            continue
        converted[inst] = {
            "shares": float(shares),
            "price": actual_price,
            "value": shares * actual_price,
            "raw_target_value": target_value,
        }
    return converted


def _sorted_position_items(positions: dict) -> List[Tuple[pd.Timestamp, Any]]:
    items = [(pd.to_datetime(date), value) for date, value in positions.items()]
    return sorted(items, key=lambda item: item[0])


def _diff_positions(
    previous: Dict[str, Dict[str, float]],
    target: Dict[str, Dict[str, float]],
) -> List[RebalanceAction]:
    actions: List[RebalanceAction] = []
    instruments = sorted(set(previous) | set(target))
    for inst in instruments:
        old_shares = previous.get(inst, {}).get("shares", 0)
        new_shares = target.get(inst, {}).get("shares", 0)
        diff = new_shares - old_shares
        if abs(diff) < 1:
            continue
        info = target.get(inst) or previous.get(inst) or {}
        price = float(info.get("price", 0) or 0)
        action = "buy" if diff > 0 else ("reduce" if inst in target else "sell")
        actions.append(
            RebalanceAction(
                action=action,
                instrument=inst,
                shares=abs(diff),
                price=price,
                value=abs(diff) * price,
            )
        )
    return actions


def _run_real_rebalance(config: dict, cfg: Dict[str, Any], trade_date: str, next_trade_date: str) -> str:
    model = _load_model(config, cfg.get("model_path", ""))
    data_loader = DataLoader(config)
    universe_filter = UniverseFilter(config.get("strategy", {}))
    pred = _predict_for_replay(
        model=model,
        data_loader=data_loader,
        universe_filter=universe_filter,
        config=config,
        instruments=cfg["market"],
        start=cfg["start_date"],
        end=trade_date,
    )
    engine = BacktestEngine(config)
    report, positions = engine.run(
        pred=pred,
        strategy_params={
            "topk": cfg["topk"],
            "n_drop": cfg["n_drop"],
            "hold_thresh": cfg["hold_thresh"],
        },
        start_time=cfg["start_date"],
        end_time=trade_date,
        account=cfg["account"],
        universe_filter=None,
    )
    position_items = _sorted_position_items(positions)
    if not position_items:
        raise RuntimeError("回测没有返回 position 数据。")
    latest_dt, latest_obj = position_items[-1]
    prev_obj = position_items[-2][1] if len(position_items) > 1 else {}
    target = _convert_snapshot_to_actual_prices(_snapshot(latest_obj), trade_date)
    previous = _convert_snapshot_to_actual_prices(_snapshot(prev_obj), trade_date)
    actions = _diff_positions(previous, target)
    metrics = _last_metrics(report)
    name_map = _load_stock_names()
    sector_map = _load_sector_map(config)
    return _format_report(
        trade_date=trade_date,
        next_trade_date=next_trade_date,
        latest_position_date=latest_dt.strftime("%Y-%m-%d"),
        cfg=cfg,
        target=target,
        actions=actions,
        metrics=metrics,
        mock=False,
        name_map=name_map,
        sector_map=sector_map,
    )


def _last_metrics(report: pd.DataFrame) -> Dict[str, float]:
    if report is None or report.empty:
        return {}
    cols = [c for c in ("return", "cost", "bench") if c in report.columns]
    if not cols:
        return {}
    row = report.iloc[-1]
    return {col: float(row[col]) for col in cols if pd.notna(row[col])}


def _mock_report(cfg: Dict[str, Any], trade_date: str, next_trade_date: str) -> str:
    previous = {
        "SH600216": {"shares": 800, "price": 12.40, "value": 9_920},
        "SZ002050": {"shares": 1200, "price": 8.80, "value": 10_560},
        "SZ300014": {"shares": 600, "price": 18.50, "value": 11_100},
        "SH603197": {"shares": 0, "price": 29.30, "value": 0},
    }
    target = {
        "SH600216": {"shares": 500, "price": 12.40, "value": 6_200},
        "SZ002050": {"shares": 1500, "price": 8.80, "value": 13_200},
        "SZ300014": {"shares": 600, "price": 18.50, "value": 11_100},
        "SH603197": {"shares": 1000, "price": 29.30, "value": 29_300},
    }
    actions = _diff_positions(previous, target)
    name_map = _load_stock_names()
    sector_map = _load_sector_map({})
    return _format_report(
        trade_date=trade_date,
        next_trade_date=next_trade_date,
        latest_position_date=trade_date,
        cfg=cfg,
        target=target,
        actions=actions,
        metrics={"return": 0.0031, "cost": 0.0004},
        mock=True,
        name_map=name_map,
        sector_map=sector_map,
    )


def _cache_dir(cfg: Dict[str, Any]) -> Path:
    return _resolve_path(cfg.get("cache_dir", "signals/daily_rebalance_cache"))


def _cache_paths(cfg: Dict[str, Any], trade_date: str) -> Tuple[Path, Path]:
    cache_dir = _cache_dir(cfg)
    return cache_dir / f"rebalance_{trade_date}.json", cache_dir / "latest.json"


def _save_signal_cache(
    cfg: Dict[str, Any],
    trade_date: str,
    next_trade_date: str,
    report: str,
    mock: bool,
) -> Path:
    cache_path, latest_path = _cache_paths(cfg, trade_date)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "trade_date": trade_date,
        "next_trade_date": next_trade_date,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "mock": mock,
        "strategy": {
            "market": cfg["market"],
            "topk": cfg["topk"],
            "n_drop": cfg["n_drop"],
            "hold_thresh": cfg["hold_thresh"],
            "start_date": cfg["start_date"],
        },
        "report": report,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    cache_path.write_text(text + "\n", encoding="utf-8")
    latest_path.write_text(text + "\n", encoding="utf-8")
    logger.info("调仓信号缓存已保存: %s", cache_path)
    return cache_path


def _load_latest_signal_cache(cfg: Dict[str, Any]) -> Dict[str, Any]:
    latest_path = _cache_dir(cfg) / "latest.json"
    if not latest_path.exists():
        raise FileNotFoundError(f"找不到调仓信号缓存: {latest_path}")
    return json.loads(latest_path.read_text(encoding="utf-8"))


def _cache_matches_today(payload: Dict[str, Any], today: pd.Timestamp) -> bool:
    next_trade_date = payload.get("next_trade_date")
    if not next_trade_date:
        return False
    return pd.Timestamp(next_trade_date).normalize() == today


def _reminder_title(label: str, next_trade_date: str) -> str:
    prefix = "开盘前" if label == "open" else "收盘前"
    return f"{prefix}调仓提醒 {next_trade_date}"


def _format_cached_reminder(payload: Dict[str, Any], label: str) -> str:
    prefix = "开盘前提醒" if label == "open" else "收盘前提醒"
    return "\n".join(
        [
            f"{prefix}: 请按昨日缓存信号执行/检查调仓",
            f"信号日: {payload.get('trade_date')}  执行日: {payload.get('next_trade_date')}",
            "",
            str(payload.get("report", "")).strip(),
        ]
    ).strip()


def _send_cached_reminder(
    config: dict,
    cfg: Dict[str, Any],
    config_path: Optional[str],
    today: pd.Timestamp,
    label: str,
    dry_run: bool,
    force: bool,
    skip_update: bool,
    mock: bool,
) -> None:
    today_str = today.strftime("%Y-%m-%d")
    payload = _load_reminder_payload(
        config=config,
        cfg=cfg,
        config_path=config_path,
        today=today,
        force=force,
        skip_update=skip_update,
        mock=mock,
    )

    if payload is None:
        return

    _, trading_calendar = _trading_calendar(config)
    if trading_calendar and today not in trading_calendar and not force:
        logger.info("%s 不是交易日，跳过提醒。", today_str)
        return

    report = _format_cached_reminder(payload, label)
    print(report)
    if dry_run:
        logger.info("dry-run 模式，跳过通知推送")
        return

    pusher = NotificationPusher(_notify_config(config, cfg["notify_channel"]))
    results = pusher.send(_reminder_title(label, today_str), report)
    for name, ok in results.items():
        logger.info("%s %s", "ok" if ok else "failed", name)
    if results and not all(results.values()):
        raise RuntimeError(f"提醒推送失败: {results}")


def _load_reminder_payload(
    config: dict,
    cfg: Dict[str, Any],
    config_path: Optional[str],
    today: pd.Timestamp,
    force: bool,
    skip_update: bool,
    mock: bool,
) -> Optional[Dict[str, Any]]:
    try:
        payload = _load_latest_signal_cache(cfg)
    except FileNotFoundError as exc:
        logger.warning("%s", exc)
    else:
        if _cache_matches_today(payload, today) or force:
            return payload
        logger.warning(
            "缓存执行日为 %s，今天是 %s，将尝试重新生成。",
            payload.get("next_trade_date"),
            today.strftime("%Y-%m-%d"),
        )

    if not cfg.get("reminder_rebuild_on_miss", True):
        logger.info("reminder_rebuild_on_miss=false，跳过缓存补救。")
        return None

    return _rebuild_signal_for_reminder(
        config=config,
        cfg=cfg,
        config_path=config_path,
        today=today,
        force=force,
        skip_update=skip_update,
        mock=mock,
    )


def _rebuild_signal_for_reminder(
    config: dict,
    cfg: Dict[str, Any],
    config_path: Optional[str],
    today: pd.Timestamp,
    force: bool,
    skip_update: bool,
    mock: bool,
) -> Optional[Dict[str, Any]]:
    today_str = today.strftime("%Y-%m-%d")
    if not skip_update and not mock:
        _run_update(config_path, create_tarball=cfg["create_update_tarball"])

    actual_calendar, trading_calendar = _trading_calendar(config)
    if trading_calendar and today not in trading_calendar and not force:
        logger.info("%s 不是交易日，跳过提醒补救。", today_str)
        return None

    signal_date, exact_previous = _previous_trading_day(today, trading_calendar)
    if not exact_previous:
        logger.warning("未在交易日历中找到上一交易日，暂按上一个工作日推断: %s", signal_date)

    if not mock:
        latest_actual = max(actual_calendar) if actual_calendar else None
        if latest_actual is None:
            raise RuntimeError("无法读取 qlib 实际交易日历 calendars/day.txt。")
        signal_ts = pd.Timestamp(signal_date).normalize()
        if latest_actual < signal_ts and not force:
            raise RuntimeError(
                f"qlib 数据尚未更新到 {signal_date}，当前最新数据日为 {latest_actual:%Y-%m-%d}。"
            )

    logger.info("提醒补救: 重新生成 %s -> %s 的调仓信号。", signal_date, today_str)
    signal_ts = pd.Timestamp(signal_date).normalize()
    run_cfg = _resolve_cfg_start_date(cfg, signal_ts, trading_calendar)
    run_config = _apply_strategy_config(config, run_cfg)
    report = (
        _mock_report(run_cfg, signal_date, today_str)
        if mock
        else _run_real_rebalance(run_config, run_cfg, signal_date, today_str)
    )
    _save_signal_cache(run_cfg, signal_date, today_str, report, mock=mock)
    return _load_latest_signal_cache(run_cfg)


def _to_qlib_code(code: str) -> str:
    code = str(code).strip()
    if len(code) != 6 or not code.isdigit():
        return code
    prefix = int(code[0])
    if prefix in (0, 2, 3):
        return f"SZ{code}"
    if prefix in (6, 9):
        return f"SH{code}"
    if prefix in (4, 8):
        return f"BJ{code}"
    return code


def _load_stock_names() -> Dict[str, str]:
    path = PROJECT_ROOT / "crawler" / "data" / "sector_stocks.json"
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        names = {}
        for category in data.values():
            for sector in category.values():
                for stock in sector.get("stocks", []):
                    code = stock.get("code", "")
                    name = stock.get("name", "")
                    if code and name:
                        names[_to_qlib_code(code)] = name
        return names
    except Exception:
        return {}


def _load_sector_map(config: dict) -> Dict[str, str]:
    try:
        provider = SectorDataProvider(config)
        return provider.get_map() or {}
    except Exception:
        return {}


def _format_report(
    trade_date: str,
    next_trade_date: str,
    latest_position_date: str,
    cfg: Dict[str, Any],
    target: Dict[str, Dict[str, float]],
    actions: Iterable[RebalanceAction],
    metrics: Dict[str, float],
    mock: bool,
    name_map: Optional[Dict[str, str]] = None,
    sector_map: Optional[Dict[str, str]] = None,
) -> str:
    title = "量化调仓信号"
    if mock:
        title += " MOCK"
    lines = [
        title,
        f"信号日: {trade_date}  执行日: {next_trade_date}",
        f"策略: {cfg['market']} / topk={cfg['topk']} / n_drop={cfg['n_drop']} / hold={cfg['hold_thresh']}",
        f"固定回测起点: {cfg['start_date']}  position日: {latest_position_date}",
        "价格/股数口径: 未复权收盘价，按目标市值折算为100股整数手",
    ]
    if metrics:
        daily_ret = metrics.get("return")
        cost = metrics.get("cost")
        parts = []
        if daily_ret is not None:
            parts.append(f"当日收益 {daily_ret:.2%}")
        if cost is not None:
            parts.append(f"交易成本 {cost:.2%}")
        if parts:
            lines.append(" | ".join(parts))

    actions = list(actions)
    lines.append("")
    lines.append("次交易日调仓动作:")
    if not actions:
        lines.append("无调仓动作，持仓保持不变。")
    else:
        label = {"buy": "买入", "reduce": "减仓", "sell": "卖出"}
        for item in actions:
            sign = "+" if item.action == "buy" else "-"
            price = f" @ {item.price:.2f}" if item.price > 0 else ""
            value = f" 约{item.value:,.0f}元" if item.value > 0 else ""
            name = name_map.get(item.instrument, "") if name_map else ""
            name_str = f" {name}" if name else ""
            lines.append(
                f"{label[item.action]} {item.instrument}{name_str}: {sign}{item.shares:.0f}股{price}{value}"
            )

    lines.append("")
    lines.append("目标持仓摘要:")
    if not target:
        lines.append("无目标持仓。")
    else:
        for inst, info in sorted(target.items()):
            name = name_map.get(inst, "") if name_map else ""
            sector = sector_map.get(inst, "") if sector_map else ""
            name_str = f" {name}" if name else ""
            sec_str = f" [{sector}]" if sector else ""
            lines.append(
                f"{inst}{name_str}{sec_str}: {info['shares']:.0f}股 约{info['value']:,.0f}元"
            )
    return "\n".join(lines)


def _notify_config(config: dict, channel: str) -> dict:
    if channel == "all":
        return config

    patched = copy.deepcopy(config)
    notify_cfg = patched.get("notify")
    if not notify_cfg:
        notify_cfg = {
            key: patched.get(key, {})
            for key in ("bark", "pushplus", "dingtalk", "serverchan", "wechat_mp")
            if key in patched
        }
        patched["notify"] = notify_cfg
    for name in ("pushplus", "dingtalk", "serverchan", "wechat_mp"):
        if isinstance(notify_cfg.get(name), dict):
            notify_cfg[name]["enabled"] = False
    if isinstance(notify_cfg.get("bark"), dict):
        notify_cfg["bark"]["enabled"] = True
    return patched


def _send_report(config: dict, report: str, trade_date: str, dry_run: bool, channel: str) -> None:
    print(report)
    if dry_run:
        logger.info("dry-run 模式，跳过通知推送")
        return
    pusher = NotificationPusher(_notify_config(config, channel))
    results = pusher.send(f"量化调仓信号 {trade_date}", report)
    for name, ok in results.items():
        logger.info("%s %s", "ok" if ok else "failed", name)
    if results and not all(results.values()):
        raise RuntimeError(f"通知推送失败: {results}")


def main() -> None:
    args = _parse_args()
    raw_config = load_config(args.config)
    base_cfg = _daily_cfg(raw_config, args)

    trade_date = pd.Timestamp(args.today or datetime.now().strftime("%Y-%m-%d")).normalize()
    trade_date_str = trade_date.strftime("%Y-%m-%d")
    _, initial_trading_calendar = _trading_calendar(raw_config)
    cfg = _resolve_cfg_start_date(base_cfg, trade_date, initial_trading_calendar)
    config = _apply_strategy_config(raw_config, cfg)

    # ── Regime-aware parameter switching (optional) ────────────────────────────
    try:
        from quant_ex.strategy.regime_switch import RegimeStrategySwitch

        regime_switch = RegimeStrategySwitch.from_config(config)
        if regime_switch is not None:
            # Need price_data to detect regime; reuse data_loader
            dl = DataLoader(config)
            instruments = cfg.get("market", config.get("market", {}).get("name", "csi300"))
            price_data = dl.load_price_data(
                instruments=instruments,
                start_time=cfg["start_date"],
                end_time=trade_date_str,
            )
            regime_label = regime_switch.detect_regime(price_data)
            cfg = regime_switch.adjust_cfg(cfg, regime_label)
            config = _apply_strategy_config(raw_config, cfg)
    except Exception as exc:
        logger.warning("Regime switch integration skipped: %s", exc)

    # ── 刷新外部数据缓存 ─────────────────────────────────────────────────────────
    try:
        from quant_ex.data.fetchers import NorthboundFetcher, FinancialFetcher
        feat_cfg = config.get("model", {}).get("features", {})
        factor_names = [f.get("name") for f in feat_cfg.get("factors", []) if f.get("name")]

        if "northbound" in factor_names:
            NorthboundFetcher(cache_dir="./cache/northbound", cache_ttl_days=1).refresh_cache([])
            logger.info("北向资金缓存已刷新")

        if "fundamental" in factor_names:
            fund_cfg = next((f for f in feat_cfg.get("factors", []) if f.get("name") == "fundamental"), {})
            metrics = fund_cfg.get("metrics", ["valuation"])
            if any(m not in ("pe_ttm", "pb", "ps_ttm", "dyr", "valuation") for m in metrics):
                FinancialFetcher(cache_dir="./cache/financial", cache_ttl_days=7).refresh_cache([])
                logger.info("财务数据缓存已刷新")
    except Exception as exc:
        logger.warning(f"外部数据缓存刷新跳过: {exc}")

    logger.info("=== 收盘后调仓任务 %s ===", trade_date_str)

    if args.remind:
        _send_cached_reminder(
            config=config,
            cfg=cfg,
            config_path=args.config,
            today=trade_date,
            label=args.reminder_label,
            dry_run=args.dry_run,
            force=args.force,
            skip_update=args.skip_update,
            mock=args.mock,
        )
        return

    if args.mock:
        _, calendar = _trading_calendar(config)
        next_trade_date, _ = _next_trading_day(trade_date, calendar)
        report = _mock_report(cfg, trade_date_str, next_trade_date)
        _save_signal_cache(cfg, trade_date_str, next_trade_date, report, mock=True)
        _send_report(config, report, trade_date_str, args.dry_run, cfg["notify_channel"])
        return

    if not args.skip_update:
        _run_update(args.config, create_tarball=cfg["create_update_tarball"])

    actual_calendar, trading_calendar = _trading_calendar(config)
    if trade_date not in trading_calendar and not args.force:
        msg = f"{trade_date_str} 不是交易日，跳过调仓任务。"
        logger.info(msg)
        if cfg["notify_on_skip"]:
            _send_report(config, msg, trade_date_str, args.dry_run, cfg["notify_channel"])
        return

    latest_actual = max(actual_calendar) if actual_calendar else None
    if latest_actual is None:
        raise RuntimeError("无法读取 qlib 实际交易日历 calendars/day.txt。")
    if latest_actual < trade_date and not args.force:
        raise RuntimeError(
            f"qlib 数据尚未更新到 {trade_date_str}，当前最新数据日为 {latest_actual:%Y-%m-%d}。"
        )

    next_trade_date, exact_next = _next_trading_day(trade_date, trading_calendar)
    if not exact_next:
        logger.warning("未在交易日历中找到下一交易日，暂按下一个工作日推断: %s", next_trade_date)

    report = _run_real_rebalance(config, cfg, trade_date_str, next_trade_date)
    _save_signal_cache(cfg, trade_date_str, next_trade_date, report, mock=False)
    _send_report(config, report, trade_date_str, args.dry_run, cfg["notify_channel"])


if __name__ == "__main__":
    main()
