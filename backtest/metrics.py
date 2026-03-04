"""Standard backtest performance metrics."""
from __future__ import annotations
from typing import Dict

import numpy as np
import pandas as pd


def compute_metrics(report: pd.DataFrame, annual_factor: int = 252) -> Dict[str, float]:
    """
    Compute annualised performance metrics from a qlib backtest report.

    Args:
        report:        qlib backtest report DataFrame (must have a 'return' column)
        annual_factor: trading days per year

    Returns:
        dict with: cum_return, annual_return, annual_vol, sharpe,
                   max_drawdown, calmar, win_rate, sortino, n_days
    """
    if report is None or len(report) == 0:
        return {}

    rets = (report["return"] if "return" in report.columns else report.iloc[:, 0]).dropna()
    if len(rets) == 0:
        return {}

    n = len(rets)
    cum = (1 + rets).prod() - 1
    ann_ret = (1 + cum) ** (annual_factor / n) - 1
    ann_vol = rets.std() * np.sqrt(annual_factor)
    sharpe = ann_ret / (ann_vol + 1e-8)

    nav = (1 + rets).cumprod()
    dd = (nav - nav.cummax()) / nav.cummax()
    max_dd = float(dd.min())
    calmar = ann_ret / (abs(max_dd) + 1e-8)

    win_rate = float((rets > 0).mean())
    down = rets[rets < 0]
    downside_std = down.std() * np.sqrt(annual_factor) if len(down) > 0 else 1e-8
    sortino = ann_ret / (downside_std + 1e-8)

    return {
        "cum_return":    round(cum, 4),
        "annual_return": round(ann_ret, 4),
        "annual_vol":    round(ann_vol, 4),
        "sharpe":        round(sharpe, 4),
        "max_drawdown":  round(max_dd, 4),
        "calmar":        round(calmar, 4),
        "win_rate":      round(win_rate, 4),
        "sortino":       round(sortino, 4),
        "n_days":        n,
    }


def format_metrics(m: Dict[str, float]) -> str:
    """Human-readable metrics table."""
    lines = [
        "═" * 42,
        "  回测绩效指标",
        "═" * 42,
        f"  累计收益:    {m.get('cum_return', 0):.2%}",
        f"  年化收益:    {m.get('annual_return', 0):.2%}",
        f"  年化波动:    {m.get('annual_vol', 0):.2%}",
        f"  夏普比率:    {m.get('sharpe', 0):.3f}",
        f"  最大回撤:    {m.get('max_drawdown', 0):.2%}",
        f"  卡玛比率:    {m.get('calmar', 0):.3f}",
        f"  胜率:        {m.get('win_rate', 0):.2%}",
        f"  索提诺比率:  {m.get('sortino', 0):.3f}",
        f"  交易天数:    {m.get('n_days', 0)}",
        "═" * 42,
    ]
    return "\n".join(lines)
