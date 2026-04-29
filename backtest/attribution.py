"""
Performance attribution — sector-level Brinson decomposition.

Decomposes portfolio return vs benchmark into:
  - Allocation effect   : did we over/underweight the right sectors?
  - Selection effect    : did we pick better stocks within each sector?
  - Interaction effect  : combined allocation + selection

Usage
-----
    from quant_ex.backtest.attribution import brinson_attribution, format_attribution

    # portfolio_weights: Series[float], index=(instrument, datetime)  — daily position weights
    # benchmark_weights: Series[float], same structure (e.g. equal-weight index)
    # returns:           Series[float], daily instrument returns, same index
    # sector_map:        dict {instrument: sector_name}

    result = brinson_attribution(portfolio_weights, benchmark_weights, returns, sector_map)
    print(format_attribution(result))

The function returns a dict with:
    "by_sector"  — DataFrame with allocation/selection/interaction per sector
    "total"      — dict with total attribution components
    "summary"    — one-row DataFrame for quick display
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def brinson_attribution(
    portfolio_weights: pd.Series,
    benchmark_weights: pd.Series,
    returns: pd.Series,
    sector_map: Dict[str, str],
) -> Dict:
    """
    Sector-level Brinson-Hood-Beebower attribution.

    Parameters
    ----------
    portfolio_weights : pd.Series
        Position weights with (instrument, datetime) MultiIndex.
        Each (instrument, date) value is the fraction of portfolio NAV.
    benchmark_weights : pd.Series
        Benchmark weights, same structure.
        Use equal-weight index membership if you don't have market-cap weights.
    returns : pd.Series
        Daily instrument returns, same MultiIndex.
        Typically ``price_data["real_close"].pct_change()``.
    sector_map : dict
        ``{instrument: sector_name}`` mapping.

    Returns
    -------
    dict with keys:
        ``by_sector``  — DataFrame (sector × [bp_weight, bm_weight,
                          bp_return, bm_return, allocation, selection, interaction, total])
        ``total``      — dict summing allocation, selection, interaction, total
        ``summary``    — single-row DataFrame
    """
    if portfolio_weights.empty or benchmark_weights.empty or returns.empty:
        logger.warning("attribution: empty inputs; returning zero attribution")
        return _empty_result()

    # Align all series to a common index
    common_idx = portfolio_weights.index.intersection(benchmark_weights.index).intersection(
        returns.index
    )
    if common_idx.empty:
        logger.warning("attribution: no common (instrument, datetime) index; returning zero")
        return _empty_result()

    pw = portfolio_weights.reindex(common_idx).fillna(0.0)
    bw = benchmark_weights.reindex(common_idx).fillna(0.0)
    ret = returns.reindex(common_idx).fillna(0.0)

    # Attach sector labels
    instruments = common_idx.get_level_values("instrument")
    sectors = instruments.map(lambda i: sector_map.get(i, "Unknown"))

    df = pd.DataFrame({
        "pw": pw.values,
        "bw": bw.values,
        "ret": ret.values,
        "sector": sectors,
    }, index=common_idx)

    # Aggregate to (date × sector)
    grp = df.groupby([df.index.get_level_values("datetime"), "sector"])

    # Sector-level portfolio weight, benchmark weight, and return (weighted avg)
    sec_pw  = grp["pw"].sum().unstack("sector").fillna(0.0)    # dates × sectors
    sec_bw  = grp["bw"].sum().unstack("sector").fillna(0.0)
    # Weighted average return within sector for portfolio and benchmark
    def _wavg(sub: pd.DataFrame, weight_col: str) -> pd.Series:
        w = sub[weight_col]
        total_w = w.sum()
        if total_w == 0:
            return 0.0
        return (sub["ret"] * w).sum() / total_w

    sec_pr = grp.apply(lambda g: _wavg(g, "pw")).unstack("sector").fillna(0.0)
    sec_br = grp.apply(lambda g: _wavg(g, "bw")).unstack("sector").fillna(0.0)

    # Benchmark total return per date
    bm_total_ret = (sec_bw * sec_br).sum(axis=1)  # dates

    # Brinson components — broadcast dates scalar to sector columns
    bm_total_mat = pd.DataFrame(
        np.tile(bm_total_ret.values[:, None], (1, sec_pw.shape[1])),
        index=sec_pw.index,
        columns=sec_pw.columns,
    )

    allocation   = (sec_pw - sec_bw) * (sec_br - bm_total_mat)
    selection    = sec_bw * (sec_pr - sec_br)
    interaction  = (sec_pw - sec_bw) * (sec_pr - sec_br)
    total_effect = allocation + selection + interaction

    # Average over time
    by_sector = pd.DataFrame({
        "bp_weight":    sec_pw.mean(),
        "bm_weight":    sec_bw.mean(),
        "bp_return":    sec_pr.mean(),
        "bm_return":    sec_br.mean(),
        "allocation":   allocation.mean(),
        "selection":    selection.mean(),
        "interaction":  interaction.mean(),
        "total":        total_effect.mean(),
    }).sort_values("total", ascending=False)

    total = {
        "allocation":  float(allocation.values.sum() / max(1, len(allocation))),
        "selection":   float(selection.values.sum()  / max(1, len(selection))),
        "interaction": float(interaction.values.sum() / max(1, len(interaction))),
        "total":       float(total_effect.values.sum() / max(1, len(total_effect))),
    }

    summary = pd.DataFrame([{
        "allocation_bps":   round(total["allocation"] * 10000, 2),
        "selection_bps":    round(total["selection"]  * 10000, 2),
        "interaction_bps":  round(total["interaction"] * 10000, 2),
        "total_alpha_bps":  round(total["total"]       * 10000, 2),
    }])

    return {"by_sector": by_sector, "total": total, "summary": summary}


def format_attribution(result: Dict, top_n: int = 10) -> str:
    """Return a human-readable attribution report string."""
    if not result or result.get("by_sector") is None:
        return "Attribution: no data"

    lines = [
        "=" * 70,
        "  Brinson-Hood-Beebower Performance Attribution",
        "=" * 70,
        f"  {'Sector':<22} {'BP Wt':>7} {'BM Wt':>7} {'BP Ret':>8} {'BM Ret':>8}"
        f" {'Alloc':>8} {'Select':>8} {'Interact':>8} {'Total':>8}",
        "-" * 70,
    ]
    df = result["by_sector"].head(top_n)
    for sec, row in df.iterrows():
        lines.append(
            f"  {str(sec)[:22]:<22}"
            f" {row['bp_weight']:>7.2%}"
            f" {row['bm_weight']:>7.2%}"
            f" {row['bp_return']:>8.2%}"
            f" {row['bm_return']:>8.2%}"
            f" {row['allocation']:>8.4f}"
            f" {row['selection']:>8.4f}"
            f" {row['interaction']:>8.4f}"
            f" {row['total']:>8.4f}"
        )
    t = result["total"]
    lines += [
        "-" * 70,
        f"  {'TOTAL':<22}"
        f" {'':>7} {'':>7} {'':>8} {'':>8}"
        f" {t['allocation']:>8.4f}"
        f" {t['selection']:>8.4f}"
        f" {t['interaction']:>8.4f}"
        f" {t['total']:>8.4f}",
        "=" * 70,
        f"  Alpha breakdown (bps/day):  "
        f"Allocation={t['allocation']*1e4:.2f}  "
        f"Selection={t['selection']*1e4:.2f}  "
        f"Interaction={t['interaction']*1e4:.2f}  "
        f"Total={t['total']*1e4:.2f}",
        "=" * 70,
    ]
    return "\n".join(lines)


def build_equal_weight_benchmark(
    instruments_by_date: pd.Series,
) -> pd.Series:
    """Helper: construct equal-weight benchmark weights from a membership Series.

    Parameters
    ----------
    instruments_by_date : pd.Series
        MultiIndex (instrument, datetime) with value 1.0 for each member.

    Returns
    -------
    pd.Series of equal-weight benchmark weights (sum=1 per date).
    """
    counts = instruments_by_date.groupby(level="datetime").transform("sum")
    return instruments_by_date / counts.replace(0, np.nan)


def _empty_result() -> Dict:
    empty_df = pd.DataFrame(columns=[
        "bp_weight", "bm_weight", "bp_return", "bm_return",
        "allocation", "selection", "interaction", "total",
    ])
    return {
        "by_sector": empty_df,
        "total": {"allocation": 0.0, "selection": 0.0, "interaction": 0.0, "total": 0.0},
        "summary": pd.DataFrame([{"allocation_bps": 0, "selection_bps": 0,
                                   "interaction_bps": 0, "total_alpha_bps": 0}]),
    }
