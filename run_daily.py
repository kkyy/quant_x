#!/usr/bin/env python3
"""
每日信号生成脚本 — 收盘后运行，推送次日持仓建议。

用法:
    python run_daily.py                        # 使用默认配置
    python run_daily.py --config my.yaml       # 自定义配置文件
    python run_daily.py --dry-run              # 不推送通知，只打印
    python run_daily.py --account 500000       # 指定账户资金
    python run_daily.py --positions SH600000:500,SZ000001:300   # 当前持仓
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime
from pathlib import Path

# 确保包路径正确
sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.utils.logger import setup_logger
from quant_ex.utils.config import load_config
from quant_ex.utils.qlib_utils import load_recorder_model
from quant_ex.data.loader import DataLoader
from quant_ex.data.universe import UniverseFilter
from quant_ex.data.sector import SectorDataProvider
from quant_ex.signals.generator import SignalGenerator
from quant_ex.notify.pusher import NotificationPusher

logger = setup_logger("run_daily")


def parse_positions(s: str) -> dict:
    """Parse 'SH600000:500,SZ000001:300' → {'SH600000': 500, ...}"""
    if not s:
        return {}
    result = {}
    for pair in s.split(","):
        inst, shares = pair.strip().split(":")
        result[inst.strip()] = float(shares.strip())
    return result


def _load_model(config: dict, model_path: str = None):
    """Load model from .pkl path (custom) or MLflow recorder (qlib-native)."""
    if model_path:
        from quant_ex.models.base import BaseAlphaModel
        logger.info(f"加载模型: {model_path}")
        return BaseAlphaModel.load(model_path)

    exp_cfg = config.get("experiment", {})
    rid = exp_cfg.get("latest_recorder_id", "")
    if not rid:
        logger.error(
            "未配置模型路径。请使用 --model-path 指定 .pkl 文件，\n"
            "或在 config/base.yaml 中填写 experiment.latest_recorder_id"
        )
        sys.exit(1)
    logger.info(f"加载模型: {exp_cfg.get('name')} / {rid}")
    return load_recorder_model(exp_cfg.get("name", "tutorial_exp"), rid)


def main(
    config_path: str = None,
    model_path: str = None,
    dry_run: bool = False,
    account: float = None,
    current_positions: dict = None,
):
    config = load_config(config_path)
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"=== 每日选股信号 {today} ===")

    # ── 初始化组件 ────────────────────────────────────────────────────────────
    data_loader = DataLoader(config)
    universe_filter = UniverseFilter(config.get("strategy", {}))
    sector_provider = SectorDataProvider(config)
    pusher = NotificationPusher(config)

    # ── 加载模型 ──────────────────────────────────────────────────────────────
    model = _load_model(config, model_path)

    # ── 构建数据集 ────────────────────────────────────────────────────────────
    tcfg = config.get("training", {})
    dataset = data_loader.build_dataset(
        segments={
            "train": (tcfg.get("fit_start", "2015-01-01"), tcfg.get("fit_end", "2021-12-31")),
            "valid": (tcfg.get("valid_start", "2022-01-01"), tcfg.get("valid_end", "2023-12-31")),
            "test":  (tcfg.get("test_start", "2024-01-01"), today),
        },
        instruments=config.get("market", {}).get("name", "csi300"),
    )

    # ── 生成信号 ──────────────────────────────────────────────────────────────
    acct = account or config.get("backtest", {}).get("account", 1_000_000)
    sig_gen = SignalGenerator(config, data_loader, universe_filter, sector_provider)
    data = sig_gen.generate(
        model=model,
        dataset=dataset,
        current_positions=current_positions or {},
        account_value=acct,
        trade_date=today,
    )

    report = sig_gen.format_report(data)
    print(report)

    # ── 保存到文件 ────────────────────────────────────────────────────────────
    sig_dir = Path(config.get("paths", {}).get("signal_dir", "./signals"))
    sig_dir.mkdir(parents=True, exist_ok=True)
    out = sig_dir / f"signal_{today}.txt"
    out.write_text(report, encoding="utf-8")
    logger.info(f"信号已保存 → {out}")

    # ── 推送通知 ──────────────────────────────────────────────────────────────
    if not dry_run:
        results = pusher.send(f"量化选股信号 {today}", report)
        for ch, ok in results.items():
            logger.info(f"  {'✅' if ok else '❌'} {ch}")
    else:
        logger.info("dry-run 模式，跳过通知推送")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="每日量化选股信号生成")
    parser.add_argument("--config",      type=str, default=None)
    parser.add_argument("--model-path",  type=str, default=None,
                        help="直接加载 .pkl 模型文件，例如 models/lgbm_sector_full_20260308_143021.pkl")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--account", type=float, default=None)
    parser.add_argument(
        "--positions",
        type=str,
        default="",
        help="当前持仓，格式: 'SH600000:500,SZ000001:300'",
    )
    args = parser.parse_args()
    main(
        config_path=args.config,
        model_path=args.model_path,
        dry_run=args.dry_run,
        account=args.account,
        current_positions=parse_positions(args.positions),
    )
