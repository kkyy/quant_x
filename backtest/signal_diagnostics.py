"""Signal quality diagnostics such as IC and RankIC."""
from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def compute_signal_ic(
    pred: pd.Series,
    price_data: pd.DataFrame,
    horizon: int = 5,
) -> Dict[str, float]:
    """Compute daily IC/RankIC between signal and future returns."""
    if pred is None or pred.empty or price_data is None or price_data.empty:
        return {}

    price_col = "real_close" if "real_close" in price_data.columns else "$close"
    if price_col not in price_data.columns:
        return {}

    close = price_data[price_col].sort_index()
    future_ret = close.groupby(level="instrument").transform(
        lambda s: s.shift(-horizon) / s - 1
    )
    if (
        isinstance(pred.index, pd.MultiIndex)
        and isinstance(future_ret.index, pd.MultiIndex)
        and set(pred.index.names) == set(future_ret.index.names)
        and pred.index.names != future_ret.index.names
    ):
        future_ret = future_ret.reorder_levels(pred.index.names)
    aligned = pd.concat(
        [pred.rename("score"), future_ret.rename("future_ret")],
        axis=1,
        join="inner",
    ).dropna()
    if aligned.empty:
        return {}

    ic_values = []
    rank_ic_values = []
    min_obs = 10
    for _, day_df in aligned.groupby(level="datetime"):
        if len(day_df) < min_obs:
            continue
        ic = day_df["score"].corr(day_df["future_ret"], method="pearson")
        rank_ic = day_df["score"].corr(day_df["future_ret"], method="spearman")
        if pd.notna(ic):
            ic_values.append(float(ic))
        if pd.notna(rank_ic):
            rank_ic_values.append(float(rank_ic))

    return _summary(ic_values, rank_ic_values)


def _summary(ic_values: list[float], rank_ic_values: list[float]) -> Dict[str, float]:
    def mean(values: list[float]) -> float:
        return float(np.mean(values)) if values else 0.0

    def std(values: list[float]) -> float:
        return float(np.std(values, ddof=1)) if len(values) > 1 else 0.0

    ic_mean = mean(ic_values)
    rank_ic_mean = mean(rank_ic_values)
    ic_std = std(ic_values)
    rank_ic_std = std(rank_ic_values)
    return {
        "ic": round(ic_mean, 4),
        "icir": round(ic_mean / (ic_std + 1e-8), 4),
        "rank_ic": round(rank_ic_mean, 4),
        "rank_icir": round(rank_ic_mean / (rank_ic_std + 1e-8), 4),
        "ic_days": len(ic_values),
    }
