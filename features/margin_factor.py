"""Margin trading (融资融券) factor provider.

Reads cached data from MarginTradeFetcher, computes raw and change factors.

Raw factors:
    margin_balance           融资余额
    margin_buy_amt           融资买入额
    short_balance            融券余量
    short_sell_vol           融券卖出量

Change factors:
    margin_balance_chg_pct   融资余额变化率 (pct_change)
    short_sell_ratio         融券余额占比 = short_balance / (margin_balance + short_balance)

Windowed factors (per window w):
    margin_balance_chg_{w}d  融�余额w日变动 (diff)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("margin")
class MarginFactor(BaseFactor):
    """Margin trading factors from cached margin data.

    Parameters
    ----------
    cache_dir : str
        Directory for per-day margin CSV caches (written by MarginTradeFetcher).
    cache_ttl_days : int
        Not used directly — freshness is managed by the fetcher.  Kept for
        API consistency with other factor classes.
    include_change : bool
        If True, add change and ratio factors.
    windows : list[int], optional
        Lookback windows for windowed change factors.  Default [5, 10, 20].
    """

    name = "margin"

    def __init__(
        self,
        cache_dir: str = "./cache/margin",
        cache_ttl_days: int = 1,
        include_change: bool = True,
        windows: Optional[List[int]] = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.include_change = include_change
        self.windows = windows or [5, 10, 20]

    # ── backward compat attribute ───────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "include_change"):
            self.include_change = True
        if not hasattr(self, "windows"):
            self.windows = [5, 10, 20]
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 1
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/margin")

    # ── BaseFactor interface ────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute margin factors aligned to price_data MultiIndex."""
        margin = self._load_margin_cache()
        if margin is None or margin.empty:
            logger.warning("MarginFactor: no margin cache data available")
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Build target MultiIndex and reindex; fill missing with NaN
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        margin = margin.reindex(target_idx)

        # Forward-fill within each instrument (margin data is daily but may
        # have gaps for suspended stocks)
        margin = margin.groupby(level=0, group_keys=False).ffill()

        result_parts: List[pd.DataFrame] = []

        # ── Raw factors ─────────────────────────────────────────────────────
        raw_cols = [
            "margin_balance",
            "margin_buy_amt",
            "short_balance",
            "short_sell_vol",
        ]
        existing_raw = [c for c in raw_cols if c in margin.columns]
        if existing_raw:
            result_parts.append(margin[existing_raw])

        # ── Change factors ──────────────────────────────────────────────────
        if self.include_change:
            # margin_balance_chg_pct: percent change of margin balance
            if "margin_balance" in margin.columns:
                chg_pct = margin["margin_balance"].groupby(level=0).pct_change()
                result_parts.append(chg_pct.rename("margin_balance_chg_pct"))

            # short_sell_ratio: short_balance / (margin_balance + short_balance)
            # Handle division by zero by replacing 0 denominator with NaN
            if "short_balance" in margin.columns and "margin_balance" in margin.columns:
                denom = margin["margin_balance"] + margin["short_balance"]
                denom = denom.replace(0, np.nan)
                ratio = margin["short_balance"] / denom
                result_parts.append(ratio.rename("short_sell_ratio"))

            # Windowed change factors: margin_balance_chg_{w}d
            if "margin_balance" in margin.columns:
                for w in self.windows:
                    chg_w = margin["margin_balance"].groupby(level=0).diff(w)
                    result_parts.append(chg_w.rename(f"margin_balance_chg_{w}d"))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data index exactly
        result = result.reindex(price_data.index)
        return result

    # ── Cache loading ───────────────────────────────────────────────────────

    def _load_margin_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached margin CSV files and concatenate."""
        if not self.cache_dir.exists():
            return None
        files = sorted(self.cache_dir.glob("margin_*.csv"))
        if not files:
            return None
        frames: List[pd.DataFrame] = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"MarginFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        return pd.concat(frames).sort_index()
