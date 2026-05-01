"""Insider trade (股东增减持) factor provider.

Reads cached transaction-level data from InsiderTradeFetcher, aggregates to
(instrument, date) level, then computes rolling factors.

Aggregation step:
- insider_net_buy_shares  = sum(shares_changed * direction) per (instrument, date)
- insider_buy_count      = count of 增持 trades per (instrument, date)
- insider_sell_count     = count of 减持 trades per (instrument, date)

Rolling factors (over lookback window):
- insider_net_buy_pct_{w}d  = rolling sum of (pct_of_total * direction) over w days
- insider_buy_count_{w}d    = rolling sum of buy_count over w days
- insider_sell_count_{w}d   = rolling sum of sell_count over w days

Missing data is filled with 0 (zero insider trades is meaningful, not an
absence of information).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("insider")
class InsiderFactor(BaseFactor):
    """Insider trade factors from cached transaction-level data.

    Parameters
    ----------
    cache_dir : str
        Directory containing cached insider CSVs from InsiderTradeFetcher.
    cache_ttl_days : int
        Minimum age (in days) before a cache file is considered stale.
        Set to 0 to always re-read.
    lookback_days : int
        Maximum look-back period when loading cached files relative to
        the latest date in price_data.
    windows : list[int], optional
        Rolling window sizes in trading days.  Defaults to [5, 20, 60].
    """

    name = "insider"

    def __init__(
        self,
        cache_dir: str = "./cache/insider",
        cache_ttl_days: int = 1,
        lookback_days: int = 90,
        windows: Optional[List[int]] = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.lookback_days = lookback_days
        self.windows = windows or [5, 20, 60]

    # ── backward compat for pickled models ──────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/insider")
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 1
        if not hasattr(self, "lookback_days"):
            self.lookback_days = 90
        if not hasattr(self, "windows"):
            self.windows = [5, 20, 60]

    # ── BaseFactor interface ────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute insider trade factors aligned to price_data index.

        Steps:
        1. Load all transaction-level insider CSVs from cache dir.
        2. Aggregate transactions to (instrument, datetime) daily level.
        3. Build a target MultiIndex covering all instruments × dates from
           price_data, fill missing with 0.
        4. Compute rolling factors over each window.
        5. Reindex to price_data.index.
        """
        tx_data = self._load_insider_cache()
        if tx_data is None or tx_data.empty:
            logger.warning("InsiderFactor: no insider cache data available")
            return None

        # Step 2: aggregate transaction-level to (instrument, date) level
        daily = self._aggregate_transactions(tx_data)
        if daily is None or daily.empty:
            logger.warning("InsiderFactor: aggregation produced no data")
            return None

        # Step 3: build target index and fill missing with 0
        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        daily = daily.reindex(target_idx, fill_value=0)

        # Step 4: compute rolling factors
        result_parts = []
        for w in self.windows:
            # Net buy pct: rolling sum of (pct_of_total * direction)
            net_buy_pct = (
                daily["pct_of_total_net"]
                .groupby(level=0)
                .rolling(w, min_periods=1)
                .sum()
            )
            # .rolling on MultiIndex produces an extra level; drop it
            if net_buy_pct.index.nlevels > 2:
                net_buy_pct = net_buy_pct.droplevel(0)
            result_parts.append(net_buy_pct.rename(f"insider_net_buy_pct_{w}d"))

            # Buy count: rolling sum
            buy_count = (
                daily["buy_count"]
                .groupby(level=0)
                .rolling(w, min_periods=1)
                .sum()
            )
            if buy_count.index.nlevels > 2:
                buy_count = buy_count.droplevel(0)
            result_parts.append(buy_count.rename(f"insider_buy_count_{w}d"))

            # Sell count: rolling sum
            sell_count = (
                daily["sell_count"]
                .groupby(level=0)
                .rolling(w, min_periods=1)
                .sum()
            )
            if sell_count.index.nlevels > 2:
                sell_count = sell_count.droplevel(0)
            result_parts.append(sell_count.rename(f"insider_sell_count_{w}d"))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Step 5: reindex to price_data
        result = result.reindex(price_data.index)

        # Fill NaN with 0 (no insider trades is meaningful)
        result = result.fillna(0)

        return result

    # ── Aggregation ─────────────────────────────────────────────────────────

    @staticmethod
    def _aggregate_transactions(
        tx_data: pd.DataFrame,
    ) -> Optional[pd.DataFrame]:
        """Aggregate transaction-level data to daily (instrument, date) level.

        Input columns expected: direction, shares_changed, pct_of_total,
        pct_of_float (and optionally others).

        Output columns:
        - net_buy_shares : sum(shares_changed * direction)
        - pct_of_total_net : sum(pct_of_total * direction)
        - pct_of_float_net : sum(pct_of_float * direction)
        - buy_count : number of 增持 (direction=1) transactions
        - sell_count : number of 减持 (direction=-1) transactions
        - tx_count : total number of transactions
        """
        if tx_data is None or tx_data.empty:
            return None

        df = tx_data.copy()

        # Ensure numeric types
        for col in ["direction", "shares_changed", "pct_of_total", "pct_of_float"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Compute signed values for aggregation
        if "shares_changed" in df.columns and "direction" in df.columns:
            df["_signed_shares"] = df["shares_changed"] * df["direction"]
        else:
            df["_signed_shares"] = 0.0

        if "pct_of_total" in df.columns and "direction" in df.columns:
            df["_signed_pct_total"] = df["pct_of_total"] * df["direction"]
        else:
            df["_signed_pct_total"] = 0.0

        if "pct_of_float" in df.columns and "direction" in df.columns:
            df["_signed_pct_float"] = df["pct_of_float"] * df["direction"]
        else:
            df["_signed_pct_float"] = 0.0

        # Buy / sell indicators
        if "direction" in df.columns:
            df["_is_buy"] = (df["direction"] == 1).astype(int)
            df["_is_sell"] = (df["direction"] == -1).astype(int)
        else:
            df["_is_buy"] = 0
            df["_is_sell"] = 0

        # Group by (instrument, datetime) — index already has this MultiIndex
        agg_dict = {
            "_signed_shares": "sum",
            "_signed_pct_total": "sum",
            "_signed_pct_float": "sum",
            "_is_buy": "sum",
            "_is_sell": "sum",
        }
        # Only aggregate columns that exist
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}

        if not agg_dict:
            return None

        grouped = df.groupby(level=[0, 1]).agg(agg_dict)

        # Rename to final column names
        rename_map = {
            "_signed_shares": "net_buy_shares",
            "_signed_pct_total": "pct_of_total_net",
            "_signed_pct_float": "pct_of_float_net",
            "_is_buy": "buy_count",
            "_is_sell": "sell_count",
        }
        grouped = grouped.rename(columns=rename_map)
        grouped["tx_count"] = grouped.get("buy_count", 0) + grouped.get(
            "sell_count", 0
        )

        return grouped

    # ── Cache loading ───────────────────────────────────────────────────────

    def _load_insider_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached insider files and concatenate."""
        files = sorted(self.cache_dir.glob("insider_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"InsiderFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        combined = pd.concat(frames)

        # Filter to lookback window if we have enough data
        if not combined.empty and self.lookback_days > 0:
            max_date = combined.index.get_level_values(1).max()
            cutoff = max_date - pd.Timedelta(days=self.lookback_days)
            mask = combined.index.get_level_values(1) >= cutoff
            combined = combined[mask]

        return combined.sort_index()
