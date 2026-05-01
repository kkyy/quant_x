"""BalanceSheetFactor — balance sheet ratio and absolute-value factors.

Reads cached data from BalanceSheetFetcher, computes ratio factors and
optionally absolute-value factors and period-over-period changes.

Ratio factors:
    leverage_ratio      total_liabilities / total_equity
    current_ratio       current_assets / current_liabilities
    quick_ratio         (current_assets - inventory) / current_liabilities
    goodwill_to_equity  goodwill / total_equity
    net_debt_ratio      (short_term_debt + long_term_debt - cash) / total_equity

Absolute-value factors (CRITICAL — user specifically requested):
    revenue             营业总收入
    net_profit          净利润
    total_assets        总资产
    total_equity        归属母公司权益

Change factors (optional, when include_change=True):
    leverage_ratio_chg, current_ratio_chg, quick_ratio_chg,
    goodwill_to_equity_chg, net_debt_ratio_chg

Registered name: "balance_sheet"
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)

# All ratio metrics we can compute
_RATIO_METRICS = [
    "leverage_ratio",
    "current_ratio",
    "quick_ratio",
    "goodwill_to_equity",
    "net_debt_ratio",
]

# All absolute-value metrics we include
_ABSOLUTE_METRICS = [
    "revenue",
    "net_profit",
    "total_assets",
    "total_equity",
]

# All metrics available
_ALL_METRICS = _RATIO_METRICS + _ABSOLUTE_METRICS


@FactorRegistry.register("balance_sheet")
class BalanceSheetFactor(BaseFactor):
    """Balance sheet ratio and absolute-value factors.

    Parameters
    ----------
    cache_dir : str
        Directory containing per-stock CSV caches from BalanceSheetFetcher.
    cache_ttl_days : int
        Unused (cache is pre-populated by the fetcher), kept for interface
        consistency.
    metrics : list[str], optional
        Subset of metric names to compute.  Defaults to all ratio metrics
        plus all absolute-value metrics.
        Available ratios: leverage_ratio, current_ratio, quick_ratio,
        goodwill_to_equity, net_debt_ratio.
        Available absolute values: revenue, net_profit, total_assets,
        total_equity.
    include_change : bool
        If True, add period-over-period change factors for ratio metrics
        (e.g. leverage_ratio_chg, current_ratio_chg, ...).
    """

    name = "balance_sheet"

    def __init__(
        self,
        cache_dir: str = "./cache/balance_sheet",
        cache_ttl_days: int = 30,
        metrics: Optional[List[str]] = None,
        include_change: bool = False,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.include_change = include_change
        self.metrics = metrics if metrics is not None else list(_ALL_METRICS)

    # ── backward compat ────────────────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "include_change"):
            self.include_change = False
        if not hasattr(self, "metrics"):
            self.metrics = list(_ALL_METRICS)
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 30
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/balance_sheet")

    # ── BaseFactor interface ───────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute balance sheet factors aligned to price_data index.

        Parameters
        ----------
        price_data : DataFrame with (instrument, datetime) MultiIndex.

        Returns
        -------
        DataFrame with same MultiIndex, factor columns, or None.
        """
        bs_data = self._load_balance_sheet_cache()
        if bs_data is None or bs_data.empty:
            logger.warning("BalanceSheetFactor: no balance sheet cache data available")
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        price_dates = price_data.index.get_level_values(1).unique()

        # Union balance sheet dates with price dates so that ffill can propagate
        # from report dates (quarterly) into the daily price dates.
        bs_dates = bs_data.index.get_level_values(1).unique()
        all_dates = price_dates.union(bs_dates).sort_values()

        # Build target index covering all instruments and the union of dates
        target_idx = pd.MultiIndex.from_product(
            [instruments, all_dates], names=["instrument", "datetime"]
        )

        # Reindex to full target, then forward-fill within each instrument
        bs_data = bs_data.reindex(target_idx)
        bs_data = bs_data.groupby(level=0, group_keys=False).ffill()

        # Now filter back to just the price_data dates
        price_idx = pd.MultiIndex.from_product(
            [instruments, price_dates], names=["instrument", "datetime"]
        )
        bs_data = bs_data.reindex(price_idx)

        # Compute ratio factors
        result_parts = []

        if "leverage_ratio" in self.metrics:
            ratio = self._safe_div(
                bs_data["total_liabilities"], bs_data["total_equity"]
            )
            result_parts.append(ratio.rename("leverage_ratio"))

        if "current_ratio" in self.metrics:
            ratio = self._safe_div(
                bs_data["current_assets"], bs_data["current_liabilities"]
            )
            result_parts.append(ratio.rename("current_ratio"))

        if "quick_ratio" in self.metrics:
            quick_assets = bs_data["current_assets"] - bs_data["inventory"]
            ratio = self._safe_div(quick_assets, bs_data["current_liabilities"])
            result_parts.append(ratio.rename("quick_ratio"))

        if "goodwill_to_equity" in self.metrics:
            ratio = self._safe_div(bs_data["goodwill"], bs_data["total_equity"])
            result_parts.append(ratio.rename("goodwill_to_equity"))

        if "net_debt_ratio" in self.metrics:
            net_debt = (
                bs_data["short_term_debt"] + bs_data["long_term_debt"]
                - bs_data["cash"]
            )
            ratio = self._safe_div(net_debt, bs_data["total_equity"])
            result_parts.append(ratio.rename("net_debt_ratio"))

        # Include absolute-value factors
        for col in _ABSOLUTE_METRICS:
            if col in self.metrics and col in bs_data.columns:
                result_parts.append(bs_data[col].rename(col))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Period-over-period change factors for ratios
        if self.include_change:
            for ratio_name in _RATIO_METRICS:
                if ratio_name in result.columns:
                    chg = result[ratio_name].groupby(level=0).diff()
                    result[f"{ratio_name}_chg"] = chg

        # Reindex to price_data index (ensures exact alignment)
        result = result.reindex(price_data.index)

        return result

    # ── private helpers ────────────────────────────────────────────────────

    @staticmethod
    def _safe_div(
        numerator: pd.Series, denominator: pd.Series
    ) -> pd.Series:
        """Division that returns NaN where denominator is zero or missing."""
        with np.errstate(divide="ignore", invalid="ignore"):
            result = numerator / denominator
        result = result.replace([np.inf, -np.inf], np.nan)
        return result

    def _load_balance_sheet_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached balance sheet CSV files and concatenate."""
        if not self.cache_dir.exists():
            return None

        files = sorted(self.cache_dir.glob("*.csv"))
        if not files:
            return None

        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(
                    "BalanceSheetFactor: failed to read %s: %s", f, exc
                )
        if not frames:
            return None
        return pd.concat(frames).sort_index()
