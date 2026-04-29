"""
Market regime detection features.

Computes a set of cross-sectional / time-series signals that characterise the
current market regime (trending, mean-reverting, volatile, calm) and exposes
them as a factor loadable via the FactorRegistry.

Registered name: "regime"

Features produced
-----------------
regime_trend_{w}d        — index-level trend z-score (rolling mean / std of index return)
regime_vol_{w}d          — cross-sectional return volatility (std of daily returns across stocks)
regime_breadth_{w}d      — fraction of stocks with positive N-day return (market breadth)
regime_corr_{w}d         — median pairwise return correlation proxy (dispersion ratio)
regime_drawdown          — current index drawdown from rolling {dd_window}-day high
regime_label             — integer label: 0=calm_bull, 1=volatile_bull, 2=calm_bear, 3=volatile_bear

All features are added to every (instrument, datetime) row on a given date
(i.e., they are cross-sectionally constant within a day — pure regime signals).

Usage in config/model.yaml
--------------------------
features:
  factors:
    - name: regime
      windows: [20, 60]      # optional; default [20, 60]
      dd_window: 120         # optional; default 120
"""
from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("regime")
class RegimeFeatureEngine(BaseFactor):
    """
    Market regime detection factor.

    Parameters
    ----------
    windows    : list of look-back windows (days) for trend/vol/breadth signals
    dd_window  : rolling high window for drawdown calculation
    """

    name = "regime"

    def __init__(
        self,
        windows: Optional[List[int]] = None,
        dd_window: int = 120,
    ):
        self.windows = windows or [20, 60]
        self.dd_window = int(dd_window)

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute regime features from price_data.

        Parameters
        ----------
        price_data : DataFrame with (instrument, datetime) MultiIndex.
                     Must contain a 'real_close' or '$close' column.

        Returns
        -------
        DataFrame with (instrument, datetime) MultiIndex and regime columns.
        Each column is constant across instruments within a day.
        """
        close_col = "real_close" if "real_close" in price_data.columns else "$close"
        if close_col not in price_data.columns:
            logger.warning("RegimeFeatureEngine: no close column found; skipped")
            return None

        # Build a (datetime × instrument) close price pivot
        close = (
            price_data[close_col]
            .reset_index()
            .pivot(index="datetime", columns="instrument", values=close_col)
            .sort_index()
        )
        if close.empty:
            return None

        # Daily cross-sectional returns
        ret = close.pct_change().fillna(0.0)

        # Index-level proxy: equal-weight average daily return
        idx_ret = ret.mean(axis=1)

        regime_cols: dict = {}

        for w in self.windows:
            # --- Trend z-score: rolling mean / rolling std of index return ---
            roll_mean = idx_ret.rolling(w, min_periods=max(1, w // 2)).mean()
            roll_std  = idx_ret.rolling(w, min_periods=max(1, w // 2)).std().replace(0, np.nan)
            regime_cols[f"regime_trend_{w}d"] = (roll_mean / roll_std).fillna(0.0)

            # --- Cross-sectional volatility (dispersion of daily returns) ---
            cs_vol = ret.std(axis=1).rolling(w, min_periods=max(1, w // 2)).mean()
            regime_cols[f"regime_vol_{w}d"] = cs_vol.fillna(0.0)

            # --- Breadth: fraction of stocks positive over past w days ---
            cum_ret_w = (1 + ret).rolling(w, min_periods=max(1, w // 2)).apply(
                np.prod, raw=True
            ) - 1
            breadth = (cum_ret_w > 0).mean(axis=1)
            regime_cols[f"regime_breadth_{w}d"] = breadth.fillna(0.5)

            # --- Correlation proxy: 1 - (cs_vol / index_vol)
            # High ratio → stocks moving together (trending); low → dispersion
            idx_vol = idx_ret.rolling(w, min_periods=max(1, w // 2)).std().replace(0, np.nan)
            corr_proxy = 1.0 - (cs_vol / idx_vol).clip(0, 2)
            regime_cols[f"regime_corr_{w}d"] = corr_proxy.fillna(0.0)

        # --- Drawdown from rolling high ---
        dd_w = self.dd_window
        roll_high = close.mean(axis=1).rolling(dd_w, min_periods=1).max()
        idx_level = close.mean(axis=1)
        regime_cols["regime_drawdown"] = (
            (idx_level / roll_high - 1.0).clip(upper=0.0).fillna(0.0)
        )

        # --- Discrete regime label (0-3) ---
        # Uses the shortest window trend and vol signals
        w0 = self.windows[0]
        trend = regime_cols[f"regime_trend_{w0}d"]
        vol   = regime_cols[f"regime_vol_{w0}d"]
        vol_median = vol.rolling(252, min_periods=20).median().fillna(vol.median())
        is_bull     = trend >= 0
        is_volatile = vol > vol_median
        label = (is_volatile.astype(int) * 2 + (~is_bull).astype(int))
        # 0: calm_bull, 1: calm_bear, 2: volatile_bull, 3: volatile_bear
        regime_cols["regime_label"] = label.astype(float)

        # Build a date-level DataFrame, then broadcast to all instruments
        date_df = pd.DataFrame(regime_cols, index=close.index)

        # Rebuild MultiIndex — broadcast each date's regime to all instruments
        instruments = price_data.index.get_level_values("instrument").unique()
        dates = date_df.index

        # Use a cross-join: repeat each date row for every instrument
        date_df_idx = date_df.reset_index().rename(columns={"index": "datetime"})
        inst_df = pd.DataFrame({"instrument": instruments})
        result = date_df_idx.merge(inst_df, how="cross")
        result = result.set_index(["instrument", "datetime"]).sort_index()

        # Keep only dates that appear in price_data
        valid_dates = set(price_data.index.get_level_values("datetime").unique())
        result = result.loc[
            result.index.get_level_values("datetime").isin(valid_dates)
        ]

        logger.info(
            "RegimeFeatureEngine: computed %d columns for %d rows",
            len(result.columns),
            len(result),
        )
        return result
