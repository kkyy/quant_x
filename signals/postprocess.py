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
    price_data: Optional[pd.DataFrame] = None,
) -> pd.Series:
    """Apply configured cross-sectional signal transforms.

    Args:
        pred:        Raw prediction series with (instrument, datetime) MultiIndex.
        config:      Strategy config dict.
        sector_map:  {instrument: sector_name} for industry neutralization.
        size_data:   Optional log-market-cap Series (same MultiIndex) for
                     size neutralization. Only used when
                     ``signal.postprocess.size_neutralize`` is True.
        price_data:  Optional price DataFrame used by configured relative
                     strength filters.
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

    signal = apply_stock_vs_sector_filter(
        signal,
        config=config,
        sector_map=sector_map,
        price_data=price_data,
    )

    return signal.sort_index(kind="mergesort")


def postprocess_requires_price_data(config: dict) -> bool:
    """Return True when post-processing needs a price DataFrame from callers."""
    cfg = config.get("signal", {}).get("postprocess", {})
    if not cfg.get("enabled", True):
        return False
    return bool(
        cfg.get("stock_vs_sector_filter", {}).get("enabled", False)
    )


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


def _compute_market_drawdown(
    price_data: pd.DataFrame,
    drawdown_window: int = 120,
) -> pd.Series:
    """Compute equal-weight index drawdown from rolling high.

    Returns pd.Series indexed by datetime, values <= 0.
    """
    close_col = "real_close" if "real_close" in price_data.columns else "$close"
    close = (
        price_data[close_col]
        .reset_index()
        .pivot(index="datetime", columns="instrument", values=close_col)
        .sort_index()
    )
    idx_level = close.mean(axis=1)
    roll_high = idx_level.rolling(drawdown_window, min_periods=1).max()
    drawdown = (idx_level / roll_high - 1.0).clip(upper=0.0)
    return drawdown


def _apply_svs_filter_core(
    pred: pd.Series,
    factor_rank: pd.Series,
    effective_mode: str,
    keep_top_pct: float,
    effective_weight: float,
) -> pd.Series:
    """Core SVS filter logic, extracted for reuse by drawdown gating."""
    if effective_mode == "hard_filter":
        threshold = 1.0 - keep_top_pct
        keep = factor_rank.reindex(pred.index) >= threshold
        filtered = pred[keep.fillna(False)]
        return filtered if not filtered.empty else pred

    svs_rank_aligned = factor_rank.reindex(pred.index)

    if effective_mode == "multiplicative_weight":
        blended = pred * (1.0 - effective_weight + effective_weight * svs_rank_aligned)
        return daily_rank(blended, pct=True)

    if effective_mode == "residual_add":
        blended = pred + effective_weight * svs_rank_aligned
        return daily_rank(blended, pct=True)

    # Fallback to hard_filter
    threshold = 1.0 - keep_top_pct
    keep = factor_rank.reindex(pred.index) >= threshold
    filtered = pred[keep.fillna(False)]
    return filtered if not filtered.empty else pred


def apply_stock_vs_sector_filter(
    pred: pd.Series,
    config: dict,
    sector_map: Optional[Dict[str, str]],
    price_data: Optional[pd.DataFrame],
    mode: Optional[str] = None,
    weight_strength: Optional[float] = None,
) -> pd.Series:
    """Apply stock-vs-sector relative strength overlay to predictions.

    Three modes are supported:

    * ``hard_filter`` (default) — drop all stocks below the ``keep_top_pct``
      threshold, keeping only the top fraction by SVS rank.
    * ``multiplicative_weight`` — blend model score with SVS rank as a
      multiplicative weight:
      ``score = pred * (1 - w + w * svs_rank)``, then re-rank to [0,1].
    * ``residual_add`` — add a weighted SVS rank to the model score:
      ``score = pred + w * svs_rank``, then re-rank to [0,1].

    Config shape::

        signal:
          postprocess:
            stock_vs_sector_filter:
              enabled: true
              window: 20
              keep_top_pct: 0.4
              mode: "hard_filter"            # or "multiplicative_weight" / "residual_add"
              weight_strength: 0.5           # blend strength for soft modes
              drawdown_threshold: -0.10      # disable SVS when market drawdown exceeds this
              drawdown_window: 120           # rolling high window for drawdown calculation

    Parameters
    ----------
    pred : pd.Series
        Prediction series with (instrument, datetime) MultiIndex.
    config : dict
        Strategy config dict (contains ``signal.postprocess.stock_vs_sector_filter``).
    sector_map : dict or None
        {instrument: sector_name} mapping.
    price_data : pd.DataFrame or None
        Price data for computing SVS factors.
    mode : str or None
        Override filter mode.  If None, reads from config (default: ``"hard_filter"``).
    weight_strength : float or None
        Override blend strength for soft modes.  If None, reads from config
        (default: ``0.5``).
    """
    cfg = (
        config.get("signal", {})
        .get("postprocess", {})
        .get("stock_vs_sector_filter", {})
    )
    if not cfg.get("enabled", False):
        return pred

    if pred is None or pred.empty:
        return pred
    if price_data is None or price_data.empty:
        logger.warning("stock_vs_sector_filter enabled but price_data is empty; skipped")
        return pred
    if not sector_map:
        logger.warning("stock_vs_sector_filter enabled but sector_map is empty; skipped")
        return pred

    window = int(cfg.get("window", 20))
    keep_top_pct = float(cfg.get("keep_top_pct", 0.4))
    if not 0 < keep_top_pct <= 1:
        logger.warning(
            "stock_vs_sector_filter keep_top_pct=%s is invalid; skipped",
            keep_top_pct,
        )
        return pred

    # Resolve mode and weight_strength: explicit params override config
    effective_mode = mode if mode is not None else cfg.get("mode", "hard_filter")
    effective_weight = (
        weight_strength if weight_strength is not None
        else float(cfg.get("weight_strength", 0.5))
    )

    if effective_mode not in ("hard_filter", "multiplicative_weight", "residual_add"):
        logger.warning(
            "stock_vs_sector_filter mode=%s is unknown; falling back to hard_filter",
            effective_mode,
        )
        effective_mode = "hard_filter"

    try:
        from quant_ex.features.sector_factors import SectorFactorEngine

        factor_engine = SectorFactorEngine(
            sector_map=sector_map,
            concept_map={},
            include_sector_momentum=False,
            include_sector_relative=False,
            include_stock_vs_sector=True,
            stock_vs_sector_windows=[window],
            include_sector_reversal=False,
            include_sector_volatility=False,
            include_sector_id=False,
            include_concept=False,
            include_concept_id=False,
        )
        factors = factor_engine.compute(price_data)
        if factors is None or factors.empty:
            logger.warning("stock_vs_sector_filter produced no factors; skipped")
            return pred

        col = f"stock_vs_sector_{window}d"
        if col not in factors.columns:
            logger.warning("stock_vs_sector_filter missing factor column %s; skipped", col)
            return pred

        factor_rank = daily_rank(factors[col], pct=True)
        if (
            isinstance(factor_rank.index, pd.MultiIndex)
            and isinstance(pred.index, pd.MultiIndex)
            and set(factor_rank.index.names) == set(pred.index.names)
            and factor_rank.index.names != pred.index.names
        ):
            factor_rank = factor_rank.reorder_levels(pred.index.names)

        # ── Drawdown-gated SVS: per-day switching ────────────────────────────
        drawdown_threshold = cfg.get("drawdown_threshold", None)
        if drawdown_threshold is not None:
            dd_window = int(cfg.get("drawdown_window", 120))
            dd_series = _compute_market_drawdown(price_data, dd_window)
            dd_threshold = float(drawdown_threshold)

            strong_dates = set(dd_series[dd_series > dd_threshold].index)
            weak_dates = set(dd_series[dd_series <= dd_threshold].index)

            pred_dates = pred.index.get_level_values("datetime")
            # Dates not in drawdown series (warmup) default to strong
            missing = set(pred_dates.unique()) - strong_dates - weak_dates
            if missing:
                strong_dates.update(missing)

            pred_strong = pred[pred_dates.isin(strong_dates)]
            pred_weak = pred[pred_dates.isin(weak_dates)]

            n_strong = len(pred_strong)
            n_weak = len(pred_weak)
            logger.info(
                "drawdown_gated_svs: %d strong-market rows, %d weak-market rows "
                "(threshold=%.0f%%, window=%d)",
                n_strong, n_weak, dd_threshold * 100, dd_window,
            )

            if pred_strong.empty:
                logger.info("drawdown_gated_svs: all dates in drawdown, returning unfiltered signal")
                return pred

            result_strong = _apply_svs_filter_core(
                pred_strong, factor_rank, effective_mode, keep_top_pct, effective_weight,
            )

            if pred_weak.empty:
                logger.info(
                    "stock_vs_sector_filter kept %d/%d (drawdown_gated, no weak dates)",
                    len(result_strong), len(pred),
                )
                return result_strong

            result = pd.concat([result_strong, pred_weak]).sort_index(kind="mergesort")
            logger.info(
                "stock_vs_sector_filter: %d strong (SVS) + %d weak (baseline) = %d total "
                "(drawdown_gated, window=%s, keep_top_pct=%.2f, mode=%s)",
                len(result_strong), len(pred_weak), len(result),
                window, keep_top_pct, effective_mode,
            )
            return result

        # ── No drawdown gating: apply SVS uniformly ──────────────────────────
        result = _apply_svs_filter_core(
            pred, factor_rank, effective_mode, keep_top_pct, effective_weight,
        )
        if effective_mode == "hard_filter":
            logger.info(
                "stock_vs_sector_filter kept %d/%d signal rows "
                "(window=%s, keep_top_pct=%.2f, mode=hard_filter)",
                len(result), len(pred),
                window, keep_top_pct,
            )
        else:
            logger.info(
                "stock_vs_sector_filter applied %s "
                "(window=%s, weight_strength=%.2f)",
                effective_mode, window, effective_weight,
            )
        return result

    except Exception as exc:
        logger.warning("stock_vs_sector_filter failed: %s; skipped", exc)
        return pred


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
