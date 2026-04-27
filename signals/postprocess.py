"""Prediction signal post-processing utilities."""
from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def postprocess_signal(
    pred: pd.Series,
    config: dict,
    sector_map: Optional[Dict[str, str]] = None,
) -> pd.Series:
    """Apply configured cross-sectional signal transforms."""
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
