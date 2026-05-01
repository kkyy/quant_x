"""Institutional visit (机构调研) factor provider.

Reads cached data from InstitutionalVisitFetcher, computes rolling visit
count and change factors.

Factors computed:
- visit_count_{w}d : rolling sum of visitor_count over *lookback_days* calendar days
- visit_count_chg  : visit_count_{w}d(t) / visit_count_{w}d(t - lookback_days) - 1

Missing data is filled with 0 (no visits = 0, not NaN).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("visit")
class InstitutionalVisitFactor(BaseFactor):
    """Institutional visit factors from cached visit data.

    Parameters
    ----------
    cache_dir : str
        Directory for visit CSV caches (written by InstitutionalVisitFetcher).
    cache_ttl_days : int
        Not used directly (fetcher controls TTL), kept for interface consistency.
    lookback_days : int
        Calendar-day lookback window for rolling visit count (default 30).
    """

    name = "visit"

    def __init__(
        self,
        cache_dir: str = "./cache/visit",
        cache_ttl_days: int = 7,
        lookback_days: int = 30,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.lookback_days = lookback_days

    # ── backward compat attribute ───────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/visit")
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 7
        if not hasattr(self, "lookback_days"):
            self.lookback_days = 30

    # ── BaseFactor interface ──────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        visits = self._load_visit_cache()
        if visits is None or visits.empty:
            logger.warning("InstitutionalVisitFactor: no visit cache data available")
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Build target MultiIndex from price_data
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )

        # Aggregate: group by (instrument, datetime), sum visitor_count
        # This handles cases where multiple visit events occur on the same day
        if "visitor_count" not in visits.columns:
            logger.warning("InstitutionalVisitFactor: visitor_count column missing from cache")
            return None

        daily_visits = visits[["visitor_count"]].groupby(
            [visits.index.get_level_values(0), visits.index.get_level_values(1)]
        ).sum()
        daily_visits.index.names = ["instrument", "datetime"]

        # Reindex to full target index, fill missing with 0
        daily_visits = daily_visits.reindex(target_idx, fill_value=0)

        # Forward-fill within each instrument (visit events are sparse;
        # keep the cumulative daily count visible)
        # Actually, we don't ffill — each day either has visits (count > 0)
        # or doesn't (count = 0). The rolling window handles the accumulation.

        result_parts = []

        # ── visit_count_{w}d: rolling sum over lookback_days calendar days ──
        # Use a date-based window: for each (instrument, date), sum all
        # visitor_count in [date - lookback_days, date]
        w = self.lookback_days
        col_name = f"visit_count_{w}d"

        # Efficient date-based rolling: unstack to (datetime × instrument),
        # then use rolling with a window size that covers lookback_days
        # trading days. Since lookback_days is in calendar days and trading
        # days are ~70% of calendar days, we use min_periods=1 and a
        # window of lookback_days rows (which covers at least lookback_days
        # calendar days in practice).
        vc_unstacked = daily_visits["visitor_count"].unstack("instrument")

        # Use a row-based rolling window: lookback_days rows ≈ lookback_days
        # calendar days (slightly conservative, which is fine)
        rolling_sum = vc_unstacked.rolling(window=w, min_periods=1).sum()
        rolling_count = rolling_sum.stack()
        rolling_count.index.names = ["datetime", "instrument"]
        rolling_count = rolling_count.swaplevel().sort_index()
        rolling_count.name = col_name

        result_parts.append(rolling_count)

        # ── visit_count_chg: current / (value from lookback_days ago) - 1 ──
        # Shift by lookback_days rows within each instrument
        vc_series = daily_visits["visitor_count"].groupby(level=0)
        rolling_count_grouped = rolling_count.groupby(level=0)

        # The change factor: current rolling count vs rolling count w days ago
        prev_count = rolling_count_grouped.shift(w)
        chg = (rolling_count / prev_count.replace(0, np.nan)) - 1
        chg.name = "visit_count_chg"

        result_parts.append(chg)

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Fill missing with 0 for visit_count, NaN for change
        # (no visits = 0 visitors, but change is undefined when there were
        # zero prior visits)
        if col_name in result.columns:
            result[col_name] = result[col_name].fillna(0)

        # Reindex to price_data
        result = result.reindex(price_data.index)

        # After reindex, fill visit_count NaN with 0 (new instruments/dates
        # that had no visits at all)
        if col_name in result.columns:
            result[col_name] = result[col_name].fillna(0)

        return result

    # ── Cache loading ────────────────────────────────────────────────────────

    def _load_visit_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached visit files and concatenate."""
        if not self.cache_dir.exists():
            return None
        files = sorted(self.cache_dir.glob("visits_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"InstitutionalVisitFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        return pd.concat(frames).sort_index()
