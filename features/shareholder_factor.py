"""Shareholder count (股东户数) factor provider.

Reads cached data from ShareholderCountFetcher, computes raw and change factors.

Raw factors:
    sh_count              股东户数 (forward-filled from quarterly/monthly data)
    sh_count_chg_pct      股东户数变化比例 (directly from API or computed)
    shares_per_holder     户均持股数量
    value_per_holder      户均持股金额/市值

Change factors (when include_change=True):
    sh_count_diff         股东户数较上期变化 (period-over-period diff)
    sh_per_share_holding_change  户均持股数量较上期变化

Note: Shareholder count DECREASING is a bullish signal (concentration).
The factor values preserve the raw sign so the model can learn this
relationship — a negative sh_count_diff means fewer holders = more
concentrated ownership = potentially bullish.

Registered name: "shareholder"
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("shareholder")
class ShareholderFactor(BaseFactor):
    """Shareholder count factors from cached data.

    Parameters
    ----------
    cache_dir : str
        Directory for shareholder CSV caches (written by ShareholderCountFetcher).
    cache_ttl_days : int
        Kept for API consistency; freshness is managed by the fetcher.
    include_change : bool
        If True, add period-over-period change factors.
    """

    name = "shareholder"

    def __init__(
        self,
        cache_dir: str = "./cache/shareholder",
        cache_ttl_days: int = 30,
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
            self.cache_ttl_days = 30
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/shareholder")

    # ── BaseFactor interface ────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute shareholder factors aligned to price_data MultiIndex."""
        self._ensure_runtime_defaults()

        sh_data = self._load_shareholder_cache()
        if sh_data is None or sh_data.empty:
            logger.warning("ShareholderFactor: no shareholder cache data available")
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Combine shareholder dates with price_data dates so that ffill
        # can carry quarterly values into the daily grid.
        sh_instruments = sh_data.index.get_level_values(0).unique().tolist()
        sh_dates = sh_data.index.get_level_values(1).unique()
        all_dates = dates.union(sh_dates).sort_values()

        # Build extended index: only instruments present in price_data
        target_instruments = [i for i in sh_instruments if i in instruments]
        extended_idx = pd.MultiIndex.from_product(
            [target_instruments, all_dates], names=["instrument", "datetime"]
        )
        sh_data = sh_data.reindex(extended_idx)

        # Forward-fill within each instrument (shareholder data is reported
        # quarterly or monthly — ffills to daily frequency)
        sh_data = sh_data.groupby(level=0, group_keys=False).ffill()

        # Now filter back to price_data dates only
        price_idx = pd.MultiIndex.from_product(
            [target_instruments, dates], names=["instrument", "datetime"]
        )
        sh_data = sh_data.reindex(price_idx)

        result_parts: List[pd.DataFrame] = []

        # ── Raw factors ─────────────────────────────────────────────────────
        raw_cols = ["sh_count", "sh_count_chg_pct", "shares_per_holder", "value_per_holder"]
        existing_raw = [c for c in raw_cols if c in sh_data.columns]
        if existing_raw:
            result_parts.append(sh_data[existing_raw])

        # ── Change factors ──────────────────────────────────────────────────
        if self.include_change:
            # sh_count_diff: period-over-period change in shareholder count
            if "sh_count" in sh_data.columns:
                # Use diff within each instrument group; this captures the
                # change from the last reported period (which may be months
                # apart due to quarterly reporting)
                sh_diff = sh_data["sh_count"].groupby(level=0).diff()
                result_parts.append(sh_diff.rename("sh_count_diff"))

            # sh_per_share_holding_change: change in shares_per_holder
            if "shares_per_holder" in sh_data.columns:
                sph_chg = sh_data["shares_per_holder"].groupby(level=0).diff()
                result_parts.append(sph_chg.rename("sh_per_share_holding_change"))

            # Compute sh_count_chg_pct if not already available from bulk API
            if "sh_count_chg_pct" not in sh_data.columns and "sh_count" in sh_data.columns:
                prev_count = sh_data["sh_count"].groupby(level=0).shift(1)
                denom = prev_count.replace(0, np.nan)
                chg_pct = (sh_data["sh_count"] - prev_count) / denom
                result_parts.append(chg_pct.rename("sh_count_chg_pct"))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data index exactly
        result = result.reindex(price_data.index)
        return result

    # ── Cache loading ───────────────────────────────────────────────────────

    def _load_shareholder_cache(self) -> Optional[pd.DataFrame]:
        """Load and combine cached shareholder data.

        Strategy:
        1. Load per-stock detail files ({SYMBOL}.csv) — these have historical
           time series and are preferred.
        2. Load bulk snapshot (gdhs_latest.csv) — only the latest value per
           stock, used as supplement for stocks without detail data.
        3. Combine: per-stock detail takes priority; bulk fills gaps.
        """
        if not self.cache_dir.exists():
            return None

        detail_frames: List[pd.DataFrame] = []
        detail_instruments = set()

        # Per-stock detail files
        for f in sorted(self.cache_dir.glob("*.csv")):
            if f.name == "gdhs_latest.csv":
                continue
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                detail_frames.append(df)
                detail_instruments.update(df.index.get_level_values(0).unique())
            except Exception as exc:
                logger.debug(
                    "ShareholderFactor: failed to read %s: %s", f, exc
                )

        # Bulk snapshot
        bulk_file = self.cache_dir / "gdhs_latest.csv"
        bulk_df = None
        if bulk_file.exists():
            try:
                bulk_df = pd.read_csv(bulk_file, index_col=[0, 1], parse_dates=[1])
                bulk_df.index.names = ["instrument", "datetime"]
            except Exception as exc:
                logger.debug(
                    "ShareholderFactor: failed to read bulk %s: %s",
                    bulk_file,
                    exc,
                )
                bulk_df = None

        # Combine: detail first, then bulk for instruments not in detail
        all_frames = detail_frames[:]
        if bulk_df is not None and not bulk_df.empty:
            if detail_instruments:
                missing = bulk_df.index.get_level_values(0).unique()
                missing = [i for i in missing if i not in detail_instruments]
                if missing:
                    bulk_subset = bulk_df.loc[
                        bulk_df.index.get_level_values(0).isin(missing)
                    ]
                    if not bulk_subset.empty:
                        all_frames.append(bulk_subset)
            else:
                all_frames.append(bulk_df)

        if not all_frames:
            return None

        combined = pd.concat(all_frames).sort_index()

        # Deduplicate: prefer detail over bulk for overlapping (instrument, datetime)
        combined = combined[~combined.index.duplicated(keep="first")]
        return combined
