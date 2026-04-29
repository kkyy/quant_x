"""Prediction signal post-processing utilities."""
from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def postprocess_signal(
    pred: pd.Series,
    config: dict,
    sector_map: Optional[Dict[str, str]] = None,
    size_data: Optional[pd.Series] = None,
) -> pd.Series:
    """Apply configured cross-sectional signal transforms.

    Args:
        pred:        Raw prediction series with (instrument, datetime) MultiIndex.
        config:      Strategy config dict.
        sector_map:  {instrument: sector_name} for industry neutralization.
        size_data:   Optional log-market-cap Series (same MultiIndex) for
                     size neutralization. Only used when
                     ``signal.postprocess.size_neutralize`` is True.
    """
    cfg = config.get("signal", {}).get("postprocess", {})
    if pred is None or pred.empty or not cfg.get("enabled", True):
        return pred

    signal = pred.dropna().copy()
    if cfg.get("industry_neutralize", False):
        signal = neutralize_by_group(
            signal,
            sector_map=sector_map,
            min_group_size=int(cfg.get("min_group_size", 5)),
        )

    if cfg.get("size_neutralize", False):
        signal = neutralize_by_size(signal, size_data=size_data)

    method = cfg.get("daily_transform", "rank")
    if method == "rank":
        signal = daily_rank(signal, pct=bool(cfg.get("rank_pct", True)))
    elif method == "zscore":
        signal = daily_zscore(signal)
    elif method in ("none", None):
        pass
    else:
        logger.warning("Unknown signal daily_transform=%s; skipped", method)

    return signal.sort_index(kind="mergesort")


def daily_rank(pred: pd.Series, pct: bool = True) -> pd.Series:
    """Rank scores within each trading day."""
    return pred.groupby(level="datetime", group_keys=False).rank(
        method="average",
        pct=pct,
    )


def daily_zscore(pred: pd.Series) -> pd.Series:
    """Z-score scores within each trading day."""
    grouped = pred.groupby(level="datetime", group_keys=False)
    mean = grouped.transform("mean")
    std = grouped.transform("std").replace(0, pd.NA)
    return ((pred - mean) / std).fillna(0.0)


def neutralize_by_group(
    pred: pd.Series,
    sector_map: Optional[Dict[str, str]],
    min_group_size: int = 5,
) -> pd.Series:
    """Subtract same-day group mean from each score."""
    if not sector_map:
        logger.warning("industry_neutralize enabled but sector_map is empty; skipped")
        return pred

    frame = pred.rename("score").reset_index()
    frame["group"] = frame["instrument"].map(sector_map).fillna("Unknown")
    group_sizes = frame.groupby(["datetime", "group"])["score"].transform("size")
    group_mean = frame.groupby(["datetime", "group"])["score"].transform("mean")
    day_mean = frame.groupby("datetime")["score"].transform("mean")
    frame["score"] = frame["score"] - group_mean.where(group_sizes >= min_group_size, day_mean)
    return frame.set_index(list(pred.index.names))["score"]


def neutralize_by_size(
    pred: pd.Series,
    size_data: Optional[pd.Series] = None,
) -> pd.Series:
    """Remove the linear size (log-market-cap) exposure from each score.

    This is done via cross-sectional OLS within each trading day:
    residual = score - β * log_mktcap

    When *size_data* is None the function tries to use Alpha158's
    ``$market_cap`` qlib field. If that also fails the original signal
    is returned unchanged.

    Parameters
    ----------
    pred :       Signal Series with (instrument, datetime) MultiIndex.
    size_data :  Optional log-market-cap Series, same MultiIndex.
                 If not provided, falls back to qlib D.features lookup.

    Returns
    -------
    Residualised signal Series (same index as *pred*).
    """
    if size_data is None:
        try:
            from qlib.config import C as _C
            if not getattr(_C, "provider_uri", None):
                raise RuntimeError("qlib not initialized")
            from qlib.data import D
            instruments = pred.index.get_level_values("instrument").unique().tolist()
            datetimes = pred.index.get_level_values("datetime")
            df = D.features(
                instruments,
                ["$market_cap"],
                start_time=str(datetimes.min())[:10],
                end_time=str(datetimes.max())[:10],
            )
            s = df.iloc[:, 0]
            s.index.names = ["instrument", "datetime"]
            # Use log to reduce skew; add 1 to avoid log(0)
            size_data = np.log1p(s.clip(lower=0))
        except Exception as exc:
            logger.warning("size_neutralize: could not load size data: %s — skipped", exc)
            return pred

    frame = pd.concat([pred.rename("score"), size_data.rename("size")], axis=1, join="inner").dropna()
    if frame.empty:
        return pred

    def _resid(grp: pd.DataFrame) -> pd.Series:
        y = grp["score"].values
        x = grp["size"].values
        if x.std() < 1e-9:
            return pd.Series(y, index=grp.index.get_level_values("instrument"))
        beta = np.cov(y, x)[0, 1] / np.var(x)
        resid = y - beta * x
        return pd.Series(resid, index=grp.index.get_level_values("instrument"))

    residuals = (
        frame
        .groupby(level="datetime", group_keys=False)
        .apply(_resid)
    )
    # Re-attach datetime level
    residuals.index = frame.index
    return pred.copy().where(~pred.index.isin(frame.index)).fillna(
        residuals.reindex(pred.index)
    )
