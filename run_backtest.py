#!/usr/bin/env python3
"""
批量回测 & 参数网格搜索脚本。

用法:
    # 使用默认网格搜索
    python run_backtest.py

    # 自定义参数范围
    python run_backtest.py --topk 5,10,15,20 --n-drop 1,3,5 --hold-thresh 3,5,10

    # 使用 AI Agent 自动迭代优化（需要 ANTHROPIC_API_KEY）
    python run_backtest.py --optimize --n-iters 3

    # 指定回测区间
    python run_backtest.py --start 2024-01-01 --end 2025-12-31
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.utils.logger import setup_logger
from quant_ex.utils.config import load_config
from quant_ex.utils.qlib_utils import load_recorder_model
from quant_ex.data.loader import DataLoader
from quant_ex.data.universe import UniverseFilter
from quant_ex.backtest.engine import BacktestEngine
from quant_ex.backtest.grid_search import GridSearchBacktest
from quant_ex.backtest.metrics import format_metrics

logger = setup_logger("run_backtest")


def parse_ints(s: str) -> list:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def main(
    config_path: str = None,
    topk_vals: list = None,
    n_drop_vals: list = None,
    hold_thresh_vals: list = None,
    start: str = None,
    end: str = None,
    optimize: bool = False,
    n_iters: int = 3,
):
    config = load_config(config_path)
    today = datetime.now().strftime("%Y-%m-%d")

    logger.info("=== 批量回测 ===")

    # ── 加载模型 ──────────────────────────────────────────────────────────────
    exp_cfg = config.get("experiment", {})
    rid = exp_cfg.get("latest_recorder_id", "")
    if not rid:
        logger.error("未配置 experiment.latest_recorder_id，请先训练模型并填写")
        sys.exit(1)

    model = load_recorder_model(exp_cfg.get("name", "tutorial_exp"), rid)

    # ── 构建数据集 ────────────────────────────────────────────────────────────
    data_loader = DataLoader(config)
    universe_filter = UniverseFilter(config.get("strategy", {}))
    tcfg = config.get("training", {})
    dataset = data_loader.build_dataset(
        segments={
            "train": (tcfg.get("fit_start", "2015-01-01"), tcfg.get("fit_end", "2021-12-31")),
            "valid": (tcfg.get("valid_start", "2022-01-01"), tcfg.get("valid_end", "2023-12-31")),
            "test":  (tcfg.get("test_start", "2024-01-01"), today),
        },
        instruments=config.get("market", {}).get("name", "csi300"),
    )

    # ── 生成预测 ──────────────────────────────────────────────────────────────
    pred = model.predict(dataset, segment="test")
    pred = universe_filter.filter(pred)

    # ── 构建参数网格 ──────────────────────────────────────────────────────────
    param_grid = {
        "topk":        topk_vals        or [5, 10, 15, 20],
        "n_drop":      n_drop_vals      or [1, 3, 5],
        "hold_thresh": hold_thresh_vals or [3, 5, 10],
    }

    engine = BacktestEngine(config)
    out_dir = Path(config.get("paths", {}).get("backtest_results_dir", "./backtest_results"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 执行 ──────────────────────────────────────────────────────────────────
    if optimize:
        from quant_ex.agent.auto_optimizer import AutoOptimizer
        logger.info("启动 AI 优化 Agent …")
        optimizer = AutoOptimizer()
        records = optimizer.run_loop(
            backtest_engine=engine,
            pred=pred,
            initial_grid=param_grid,
            n_iterations=n_iters,
            save_dir=str(out_dir),
            universe_filter=universe_filter,
        )
        print("\n=== 优化汇总 ===")
        for r in records:
            print(f"  第{r['iteration']}轮最优: {r.get('best_params', {})}")
    else:
        searcher = GridSearchBacktest(engine, pred, config)
        results_df = searcher.run(
            param_grid=param_grid,
            start_time=start,
            end_time=end,
            universe_filter=universe_filter,
        )

        print("\n=== 网格搜索结果 ===")
        print(results_df.to_string())

        best = GridSearchBacktest.best_params(results_df)
        print(f"\n✅ 最优参数: {best}")

        csv_path = out_dir / f"grid_{today}.csv"
        results_df.to_csv(csv_path, index=False)
        logger.info(f"结果已保存 → {csv_path}")

        # 单独展示最优参数的详细指标
        if not results_df.empty and "sharpe" in results_df.columns:
            top_row = results_df.iloc[0]
            m = {k: top_row[k] for k in
                 ["cum_return","annual_return","annual_vol","sharpe",
                  "max_drawdown","calmar","win_rate","sortino","n_days"]
                 if k in top_row}
            print("\n最优参数详细指标:")
            print(format_metrics(m))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="量化策略批量回测")
    parser.add_argument("--config",      type=str, default=None)
    parser.add_argument("--topk",        type=str, default=None, help="e.g. 5,10,15")
    parser.add_argument("--n-drop",      type=str, default=None)
    parser.add_argument("--hold-thresh", type=str, default=None)
    parser.add_argument("--start",       type=str, default=None)
    parser.add_argument("--end",         type=str, default=None)
    parser.add_argument("--optimize",    action="store_true", help="使用 AI Agent 迭代优化")
    parser.add_argument("--n-iters",     type=int, default=3)
    args = parser.parse_args()

    main(
        config_path=args.config,
        topk_vals=parse_ints(args.topk) if args.topk else None,
        n_drop_vals=parse_ints(args.n_drop) if args.n_drop else None,
        hold_thresh_vals=parse_ints(args.hold_thresh) if args.hold_thresh else None,
        start=args.start,
        end=args.end,
        optimize=args.optimize,
        n_iters=args.n_iters,
    )
