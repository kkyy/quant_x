"""Standard backtest performance metrics."""
from __future__ import annotations
from typing import Dict, Optional

import numpy as np
import pandas as pd


def compute_metrics(
    report: pd.DataFrame,
    annual_factor: int = 252,
    benchmark_rets: Optional[pd.Series] = None,
    positions: Optional[dict] = None,
) -> Dict[str, float]:
    """
    Compute annualised performance metrics from a qlib backtest report.

    qlib reports trading cost in a separate ``cost`` column.  Portfolio
    performance should be computed from net returns, so when ``cost`` exists
    this function uses ``return - cost``.

    Args:
        report:          qlib backtest report DataFrame (must have a 'return' column)
        annual_factor:   trading days per year (default 252)
        benchmark_rets:  optional daily benchmark return Series (same index as report)
                         when provided, computes alpha / information_ratio / tracking_error
        positions:       optional dict of {date: {instrument: weight}} for turnover calc

    Returns:
        dict with: cum_return, annual_return, annual_vol, sharpe,
                   max_drawdown, calmar, win_rate, sortino, n_days,
                   and if benchmark_rets provided:
                     excess_annual_return, information_ratio, tracking_error, beta, alpha
                   and if positions provided:
                     avg_turnover
    """
    if report is None or len(report) == 0:
        return {}

    if "return" in report.columns:
        rets = report["return"].copy()
        if "cost" in report.columns:
            rets = rets - report["cost"].reindex(rets.index).fillna(0)
    else:
        rets = report.iloc[:, 0].copy()
    rets = rets.dropna()
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

    result = {
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

    # ── Benchmark-relative metrics ─────────────────────────────────────────────
    if benchmark_rets is not None and not benchmark_rets.empty:
        bm = benchmark_rets.reindex(rets.index).fillna(0)
        alpha_daily = rets.values - bm.values

        bm_cum = (1 + bm).prod() - 1
        bm_ann = (1 + bm_cum) ** (annual_factor / len(bm)) - 1
        excess_ann = ann_ret - bm_ann

        te = float(np.std(alpha_daily, ddof=1)) * np.sqrt(annual_factor)
        ir = float(np.mean(alpha_daily)) * annual_factor / (te + 1e-8)

        bm_var = float(np.var(bm.values, ddof=1))
        beta = float(np.cov(rets.values, bm.values)[0, 1] / (bm_var + 1e-8))
        alpha_ann = ann_ret - beta * bm_ann

        result.update({
            "excess_annual_return": round(excess_ann, 4),
            "information_ratio":    round(ir, 4),
            "tracking_error":       round(te, 4),
            "beta":                 round(beta, 4),
            "alpha":                round(alpha_ann, 4),
        })

    # ── Turnover ───────────────────────────────────────────────────────────────
    if positions is not None:
        result["avg_turnover"] = round(_compute_turnover(positions), 4)

    return result


def _compute_turnover(positions: dict) -> float:
    """Average daily one-way turnover from a dict of {date: {inst: weight}}."""
    dates = sorted(positions.keys())
    if len(dates) < 2:
        return 0.0
    turnovers = []
    prev = positions[dates[0]]
    for d in dates[1:]:
        curr = positions[d]
        all_insts = set(prev) | set(curr)
        daily_to = sum(abs(curr.get(i, 0) - prev.get(i, 0)) for i in all_insts) / 2
        turnovers.append(daily_to)
        prev = curr
    return float(np.mean(turnovers)) if turnovers else 0.0


def format_metrics(m: Dict[str, float]) -> str:
    """Human-readable metrics table."""
    lines = [
        "═" * 48,
        "  回测绩效指标",
        "═" * 48,
        f"  累计收益:        {m.get('cum_return', 0):.2%}",
        f"  年化收益:        {m.get('annual_return', 0):.2%}",
        f"  年化波动:        {m.get('annual_vol', 0):.2%}",
        f"  夏普比率:        {m.get('sharpe', 0):.3f}",
        f"  最大回撤:        {m.get('max_drawdown', 0):.2%}",
        f"  卡玛比率:        {m.get('calmar', 0):.3f}",
        f"  胜率:            {m.get('win_rate', 0):.2%}",
        f"  索提诺比率:      {m.get('sortino', 0):.3f}",
        f"  交易天数:        {m.get('n_days', 0)}",
    ]

    if "information_ratio" in m:
        lines += [
            "─" * 48,
            "  超额收益（相对基准）",
            "─" * 48,
            f"  超额年化收益:    {m.get('excess_annual_return', 0):.2%}",
            f"  信息比率(IR):    {m.get('information_ratio', 0):.3f}",
            f"  跟踪误差:        {m.get('tracking_error', 0):.2%}",
            f"  Beta:            {m.get('beta', 0):.3f}",
            f"  Alpha(年化):     {m.get('alpha', 0):.2%}",
        ]

    if "avg_turnover" in m:
        lines += [
            "─" * 48,
            f"  平均日换手率:    {m.get('avg_turnover', 0):.2%}",
        ]

    lines.append("═" * 48)
    return "\n".join(lines)
