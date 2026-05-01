"""Institutional holdings (机构持仓) factor provider.

Reads cached data from InstitutionalHoldFetcher, computes raw and change factors.

Raw factors:
    fund_hold_count       持有基金家数 (number of funds holding the stock)
    fund_hold_count_chg   基金家数变化 (change in fund count from prior quarter)
    qfii_hold_flag        QFII是否持有 (1 if QFII holds, 0 otherwise)
    ss_hold_flag          社保是否持有 (1 if social security holds, 0 otherwise)
    qfii_new_entry        QFII新进 (1 if QFII newly entered, 0 otherwise)
    ss_new_entry          社保新进 (1 if social security newly entered, 0 otherwise)

Note: Many stocks have NO institutional holdings. Missing values are filled
with 0 for count/flag columns, and NaN for derived ratio columns.

Registered name: "institutional"
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("institutional")
class InstitutionalFactor(BaseFactor):
    """Institutional holdings factors from cached data.

    Parameters
    ----------
    cache_dir : str
        Directory for institutional CSV caches (written by InstitutionalHoldFetcher).
    cache_ttl_days : int
        Kept for API consistency; freshness is managed by the fetcher.
    include_change : bool
        If True, add change factors (fund_hold_count_chg, qfii_new_entry, etc.).
    """

    name = "institutional"

    def __init__(
        self,
        cache_dir: str = "./cache/institutional",
        cache_ttl_days: int = 30,
        include_change: bool = True,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.include_change = include_change

    # ── backward compat ─────────────────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "include_change"):
            self.include_change = True
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 30
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/institutional")

    # ── BaseFactor interface ────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute institutional factors aligned to price_data MultiIndex."""
        self._ensure_runtime_defaults()

        fund_data = self._load_cache("fund")
        qfii_data = self._load_cache("qfii")
        ss_data = self._load_cache("ss")

        if (fund_data is None or fund_data.empty) and (
            qfii_data is None or qfii_data.empty
        ) and (ss_data is None or ss_data.empty):
            logger.warning(
                "InstitutionalFactor: no institutional cache data available"
            )
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        result_parts: List[pd.DataFrame] = []

        # ── Fund holdings factors ───────────────────────────────────────────
        if fund_data is not None and not fund_data.empty:
            # Detect new-entry before ffill (on the raw quarterly data)
            fund_aligned = self._align_to_daily(fund_data, instruments, dates)

            if "fund_count" in fund_aligned.columns:
                # Fill missing (no fund holding) with 0, rename to output name
                fund_hold_count = fund_aligned["fund_count"].fillna(0).rename(
                    "fund_hold_count"
                )
                result_parts.append(fund_hold_count)

                if self.include_change:
                    # fund_hold_count_chg: change from prior quarter.
                    # After ffill, diff() captures the jump at quarter boundaries.
                    fund_count_chg = (
                        fund_hold_count.groupby(level=0).diff().rename(
                            "fund_hold_count_chg"
                        )
                    )
                    result_parts.append(fund_count_chg)

        # ── QFII holdings factors ──────────────────────────────────────────
        if qfii_data is not None and not qfii_data.empty:
            # Extract new-entry signal on raw quarterly data BEFORE ffill
            qfii_new_entry_raw = self._extract_new_entry(qfii_data)
            qfii_aligned = self._align_to_daily(qfii_data, instruments, dates)

            # qfii_hold_flag: 1 if any QFII holds, 0 otherwise
            if "inst_count" in qfii_aligned.columns:
                qfii_flag = (qfii_aligned["inst_count"].fillna(0) > 0).astype(float)
                qfii_flag = qfii_flag.rename("qfii_hold_flag")
                result_parts.append(qfii_flag)

            if self.include_change and qfii_new_entry_raw is not None:
                # Align new-entry to daily grid: ffill the boolean, then
                # detect the first transition from 0→1 as the signal day.
                qfii_new_aligned = self._align_to_daily(
                    qfii_new_entry_raw, instruments, dates
                )
                qfii_new_flag = qfii_new_aligned["new_entry"].fillna(0)
                # Only flag the first date where new_entry transitions to 1
                qfii_new_diff = qfii_new_flag.groupby(level=0).diff().fillna(0)
                qfii_new = (qfii_new_diff > 0).astype(float).rename("qfii_new_entry")
                result_parts.append(qfii_new)

        # ── Social security holdings factors ────────────────────────────────
        if ss_data is not None and not ss_data.empty:
            # Extract new-entry signal on raw quarterly data BEFORE ffill
            ss_new_entry_raw = self._extract_new_entry(ss_data)
            ss_aligned = self._align_to_daily(ss_data, instruments, dates)

            # ss_hold_flag: 1 if social security holds, 0 otherwise
            if "inst_count" in ss_aligned.columns:
                ss_flag = (ss_aligned["inst_count"].fillna(0) > 0).astype(float)
                ss_flag = ss_flag.rename("ss_hold_flag")
                result_parts.append(ss_flag)

            if self.include_change and ss_new_entry_raw is not None:
                ss_new_aligned = self._align_to_daily(
                    ss_new_entry_raw, instruments, dates
                )
                ss_new_flag = ss_new_aligned["new_entry"].fillna(0)
                ss_new_diff = ss_new_flag.groupby(level=0).diff().fillna(0)
                ss_new = (ss_new_diff > 0).astype(float).rename("ss_new_entry")
                result_parts.append(ss_new)

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data index exactly
        result = result.reindex(price_data.index)
        return result

    # ── New-entry extraction ────────────────────────────────────────────────

    @staticmethod
    def _extract_new_entry(data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Extract a binary new-entry series from the hold_change column.

        Works on the raw quarterly data (before ffill) so that the "新进"
        string is only present at the quarter date, not carried forward.

        Returns DataFrame with (instrument, datetime) MultiIndex and a single
        column ``new_entry`` (1.0 where hold_change contains "新进", else 0.0).
        """
        if "hold_change" not in data.columns:
            return None

        new_entry = (
            data["hold_change"].fillna("").astype(str).str.contains("新进")
        ).astype(float)
        new_entry = new_entry.rename("new_entry")
        return new_entry.to_frame()

    # ── Alignment helpers ───────────────────────────────────────────────────

    def _align_to_daily(
        self,
        data: pd.DataFrame,
        instruments: List[str],
        dates: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """Align quarterly data to daily frequency via forward-fill.

        Parameters
        ----------
        data : DataFrame with (instrument, datetime) MultiIndex
        instruments : list of instrument strings from price_data
        dates : DatetimeIndex of trading dates from price_data

        Returns
        -------
        DataFrame reindexed to (instrument, date) MultiIndex with ffill.
        """
        # Only include instruments that are in both the data and price_data
        data_instruments = data.index.get_level_values(0).unique().tolist()
        target_instruments = [i for i in data_instruments if i in instruments]

        # Combine date grids so ffill can carry quarterly values forward
        data_dates = data.index.get_level_values(1).unique()
        all_dates = dates.union(data_dates).sort_values()

        extended_idx = pd.MultiIndex.from_product(
            [target_instruments, all_dates], names=["instrument", "datetime"]
        )
        data = data.reindex(extended_idx)

        # Forward-fill within each instrument
        data = data.groupby(level=0, group_keys=False).ffill()

        # Filter back to price_data dates only
        price_idx = pd.MultiIndex.from_product(
            [target_instruments, dates], names=["instrument", "datetime"]
        )
        data = data.reindex(price_idx)
        return data

    # ── Cache loading ───────────────────────────────────────────────────────

    def _load_cache(self, hold_type: str) -> Optional[pd.DataFrame]:
        """Load and combine cached data for one institutional type.

        Parameters
        ----------
        hold_type : str
            One of "fund", "qfii", "ss".

        Returns
        -------
        DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        if not self.cache_dir.exists():
            return None

        pattern = f"{hold_type}_hold_*.csv"
        files = sorted(self.cache_dir.glob(pattern))
        if not files:
            return None

        frames: List[pd.DataFrame] = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(
                    "InstitutionalFactor: failed to read %s: %s", f, exc
                )

        if not frames:
            return None

        combined = pd.concat(frames).sort_index()
        # Deduplicate: keep first (earliest file loaded first via sort)
        combined = combined[~combined.index.duplicated(keep="first")]
        return combined
