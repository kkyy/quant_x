"""Signal quality diagnostics: IC/RankIC and IC decay analysis."""
from __future__ import annotations

from typing import Dict, List, Optional

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


def compute_ic_decay(
    pred: pd.Series,
    price_data: pd.DataFrame,
    horizons: Optional[List[int]] = None,
    min_obs: int = 10,
) -> pd.DataFrame:
    """Compute RankIC at multiple prediction horizons to reveal signal decay.

    Args:
        pred:       Signal scores, (instrument, datetime) MultiIndex.
        price_data: OHLCV DataFrame with same MultiIndex.
        horizons:   List of forward-return horizons in trading days.
                    Default: [1, 2, 3, 5, 10, 15, 20].
        min_obs:    Minimum cross-sectional observations per date.

    Returns:
        DataFrame with columns [horizon, mean_rank_ic, rank_icir, n_days].
        Useful for choosing hold_thresh and diagnosing signal half-life.

    Example::

        decay = compute_ic_decay(pred, price_data)
        print(decay)
        # horizon  mean_rank_ic  rank_icir  n_days
        #       1        0.045      1.82     240
        #       5        0.038      1.54     240
        #      10        0.021      0.85     240
        #      20        0.006      0.21     240
    """
    if horizons is None:
        horizons = [1, 2, 3, 5, 10, 15, 20]

    price_col = "real_close" if "real_close" in price_data.columns else "$close"
    if price_col not in price_data.columns:
        return pd.DataFrame(columns=["horizon", "mean_rank_ic", "rank_icir", "n_days"])

    close = price_data[price_col].sort_index()

    # Align pred to close index
    if (
        isinstance(pred.index, pd.MultiIndex)
        and isinstance(close.index, pd.MultiIndex)
        and set(pred.index.names) == set(close.index.names)
        and pred.index.names != close.index.names
    ):
        close = close.reorder_levels(pred.index.names)

    rows = []
    for h in horizons:
        future_ret = close.groupby(level="instrument").transform(
            lambda s: s.shift(-h) / s - 1
        )
        aligned = pd.concat(
            [pred.rename("score"), future_ret.rename("future_ret")],
            axis=1,
            join="inner",
        ).dropna()
        if aligned.empty:
            rows.append({"horizon": h, "mean_rank_ic": 0.0, "rank_icir": 0.0, "n_days": 0})
            continue

        rank_ic_values = []
        for _, day_df in aligned.groupby(level="datetime"):
            if len(day_df) < min_obs:
                continue
            ric = day_df["score"].corr(day_df["future_ret"], method="spearman")
            if pd.notna(ric):
                rank_ic_values.append(float(ric))

        if rank_ic_values:
            mean_ric = float(np.mean(rank_ic_values))
            std_ric = float(np.std(rank_ic_values, ddof=1))
            icir = mean_ric / (std_ric + 1e-8)
        else:
            mean_ric, icir = 0.0, 0.0

        rows.append({
            "horizon":      h,
            "mean_rank_ic": round(mean_ric, 5),
            "rank_icir":    round(icir, 4),
            "n_days":       len(rank_ic_values),
        })

    return pd.DataFrame(rows)


def compute_rolling_ic(
    pred: pd.Series,
    price_data: pd.DataFrame,
    horizon: int = 5,
    window: int = 20,
    min_obs: int = 10,
) -> pd.DataFrame:
    """Compute a rolling window RankIC time series for model monitoring.

    Args:
        pred:       Signal scores.
        price_data: Price data.
        horizon:    Forward-return horizon in days.
        window:     Rolling window length in trading days.
        min_obs:    Minimum cross-sectional size per date.

    Returns:
        DataFrame with columns [datetime, rolling_rank_ic, rolling_rank_icir].
        A sustained drop in rolling_rank_ic vs historical mean signals model decay.
    """
    price_col = "real_close" if "real_close" in price_data.columns else "$close"
    if price_col not in price_data.columns:
        return pd.DataFrame()

    close = price_data[price_col].sort_index()
    if (
        isinstance(pred.index, pd.MultiIndex)
        and isinstance(close.index, pd.MultiIndex)
        and pred.index.names != close.index.names
        and set(pred.index.names) == set(close.index.names)
    ):
        close = close.reorder_levels(pred.index.names)

    future_ret = close.groupby(level="instrument").transform(
        lambda s: s.shift(-horizon) / s - 1
    )
    aligned = pd.concat(
        [pred.rename("score"), future_ret.rename("future_ret")],
        axis=1, join="inner",
    ).dropna()
    if aligned.empty:
        return pd.DataFrame()

    daily_ic: Dict[pd.Timestamp, float] = {}
    for dt, day_df in aligned.groupby(level="datetime"):
        if len(day_df) < min_obs:
            continue
        ric = day_df["score"].corr(day_df["future_ret"], method="spearman")
        if pd.notna(ric):
            daily_ic[dt] = float(ric)

    if not daily_ic:
        return pd.DataFrame()

    ic_series = pd.Series(daily_ic).sort_index()
    rolling_mean = ic_series.rolling(window, min_periods=max(1, window // 2)).mean()
    rolling_std  = ic_series.rolling(window, min_periods=max(1, window // 2)).std()
    rolling_icir = rolling_mean / (rolling_std + 1e-8)

    return pd.DataFrame({
        "datetime":        ic_series.index,
        "daily_rank_ic":   ic_series.values,
        "rolling_rank_ic": rolling_mean.values,
        "rolling_icir":    rolling_icir.values,
    }).reset_index(drop=True)


def _summary(ic_values: list, rank_ic_values: list) -> Dict[str, float]:
    def mean(values: list) -> float:
        return float(np.mean(values)) if values else 0.0

    def std(values: list) -> float:
        return float(np.std(values, ddof=1)) if len(values) > 1 else 0.0

    ic_mean = mean(ic_values)
    rank_ic_mean = mean(rank_ic_values)
    ic_std = std(ic_values)
    rank_ic_std = std(rank_ic_values)
    return {
        "ic":         round(ic_mean, 4),
        "icir":       round(ic_mean / (ic_std + 1e-8), 4),
        "rank_ic":    round(rank_ic_mean, 4),
        "rank_icir":  round(rank_ic_mean / (rank_ic_std + 1e-8), 4),
        "ic_days":    len(ic_values),
    }
