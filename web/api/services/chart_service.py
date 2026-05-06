"""Parse backtest result CSVs into chart-ready data."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd

from web.api.deps import BACKTEST_RESULTS_DIR

logger = logging.getLogger(__name__)


def parse_equity_curve(filename: str) -> Dict:
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {"dates": [], "portfolio": [], "benchmark": [], "excess": []}

    df = pd.read_csv(path)
    date_col = "date" if "date" in df.columns else "datetime" if "datetime" in df.columns else None
    if date_col is None or "return" not in df.columns:
        return {"dates": [], "portfolio": [], "benchmark": [], "excess": []}

    dates = df[date_col].astype(str).tolist()

    # Portfolio: cumulative from daily returns
    returns = df["return"].fillna(0)
    portfolio = (1 + returns).cumprod().tolist()

    # Benchmark
    if "benchmark_return" in df.columns:
        bench_returns = df["benchmark_return"].fillna(0)
        benchmark = (1 + bench_returns).cumprod().tolist()
    else:
        benchmark = [1.0] * len(dates)

    excess = [p - b for p, b in zip(portfolio, benchmark)]

    return {
        "dates": dates,
        "portfolio": [round(v, 6) for v in portfolio],
        "benchmark": [round(v, 6) for v in benchmark],
        "excess": [round(v, 6) for v in excess],
    }


def parse_metrics(filename: str) -> Dict:
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {}

    df = pd.read_csv(path)
    if "return" not in df.columns:
        return {}

    try:
        from quant_ex.backtest.metrics import compute_metrics
        metrics = compute_metrics(df)
        return {k: round(v, 6) if isinstance(v, float) else v for k, v in metrics.items()}
    except Exception as e:
        logger.warning("compute_metrics failed for %s: %s", filename, e)
        return {}


def parse_drawdown(filename: str) -> Dict:
    curve = parse_equity_curve(filename)
    if not curve["portfolio"]:
        return {"dates": [], "drawdown": []}

    portfolio = curve["portfolio"]
    peak = portfolio[0]
    drawdown = []
    for v in portfolio:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak != 0 else 0
        drawdown.append(round(dd, 6))

    return {"dates": curve["dates"], "drawdown": drawdown}


def compare_runs(filenames: List[str]) -> Dict:
    colors = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"]
    runs = []
    for i, fn in enumerate(filenames):
        equity = parse_equity_curve(fn)
        dd = parse_drawdown(fn)
        metrics = parse_metrics(fn)
        label = fn.replace(".csv", "").replace("grid_search_", "")
        runs.append({
            "filename": fn,
            "label": label,
            "color": colors[i % len(colors)],
            "equity_curve": equity,
            "drawdown": dd,
            "metrics": metrics,
        })
    all_dates = max([r["equity_curve"]["dates"] for r in runs if r["equity_curve"]["dates"]], key=len, default=[])
    return {"runs": runs, "dates": all_dates}
