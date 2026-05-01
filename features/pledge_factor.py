"""Pledge (股权质押) factor provider.

Reads cached data from PledgeFetcher, computes raw and change factors.

Raw factors: pledge_ratio, pledge_shares, pledge_mv,
             unlimited_pledge_shares, limited_pledge_shares

Change factors (if include_change=True): pledge_ratio_chg

Pledge data is sparse (not every stock has pledges every day), so missing
values are filled with 0 and forward-filled within each instrument group.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("pledge")
class PledgeFactor(BaseFactor):
    """Stock pledge factors from cached pledge data.

    Parameters
    ----------
    cache_dir : str
        Directory for pledge CSV caches (same as PledgeFetcher).
    cache_ttl_days : int
        Not used at factor level (fetcher handles freshness), kept for
        interface consistency.
    include_change : bool
        If True, add pledge_ratio_chg (period-over-period diff).
    """

    name = "pledge"

    def __init__(
        self,
        cache_dir: str = "./cache/pledge",
        cache_ttl_days: int = 1,
        include_change: bool = True,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.include_change = include_change

    # ── backward compat attribute ───────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "include_change"):
            self.include_change = True
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 1

    # ── BaseFactor interface ──────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        pledge = self._load_pledge_cache()
        if pledge is None or pledge.empty:
            logger.warning("PledgeFactor: no pledge cache data available")
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Build target index from price_data and reindex
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        pledge = pledge.reindex(target_idx, fill_value=0)

        # Forward-fill within each instrument
        pledge = pledge.groupby(level=0, group_keys=False).ffill()

        result_parts = []

        # Raw factors — all numeric columns present in pledge data
        raw_cols = [
            "pledge_ratio", "pledge_shares", "pledge_mv",
            "unlimited_pledge_shares", "limited_pledge_shares",
        ]
        existing_raw = [c for c in raw_cols if c in pledge.columns]
        if existing_raw:
            result_parts.append(pledge[existing_raw])

        # Change factors
        if self.include_change and "pledge_ratio" in pledge.columns:
            chg = pledge["pledge_ratio"].groupby(level=0).diff()
            result_parts.append(chg.rename("pledge_ratio_chg"))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data
        result = result.reindex(price_data.index)
        return result

    # ── internals ────────────────────────────────────────────────────────────

    def _load_pledge_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached pledge files and concatenate."""
        files = sorted(self.cache_dir.glob("pledge_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"PledgeFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        return pd.concat(frames).sort_index()
