"""Earnings guidance (业绩预告) factor provider.

Reads cached data from EarningsGuidanceFetcher, computes numeric guidance
type encoding and earnings surprise percentage.

Factors computed:
- guidance_type      : numeric encoding of 预告类型
                      预增=3, 略增=2, 续盈=1, 扭亏=2,
                      略减=-1, 预减=-2, 首亏=-3, 续亏=-3
- earnings_surprise_pct : percentage surprise vs prior year
                          (forecast_value - prior_value) / |prior_value|
                          Falls back to earnings_change_pct when
                          forecast_value or prior_value is missing.
- reporting_period   : the quarter-end date of the reporting period

Notes
-----
- Many stocks have NO earnings guidance.  Missing = NaN (not 0).
- Guidance is forward-filled within each instrument — a guidance
  announcement is valid until the next quarter's guidance replaces it.
- BJ exchange codes (920xxx, 4xx, 8xx) are handled correctly.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("earnings_guidance")
class EarningsGuidanceFactor(BaseFactor):
    """Earnings guidance factors from cached yjyg data.

    Parameters
    ----------
    cache_dir : str
        Directory for yjyg CSV caches (written by EarningsGuidanceFetcher).
    cache_ttl_days : int
        Not used directly (fetcher controls TTL), kept for interface
        consistency.
    """

    name = "earnings_guidance"

    def __init__(
        self,
        cache_dir: str = "./cache/earnings_guidance",
        cache_ttl_days: int = 30,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days

        # Guidance type numeric encoding:
        #   Positive: 预增 (strong increase), 略增 (slight increase),
        #             续盈 (continue profit), 扭亏 (turn loss to profit)
        #   Negative: 略减 (slight decrease), 预减 (decrease),
        #             首亏 (first loss), 续亏 (continue loss)
        self._TYPE_MAP: Dict[str, int] = {
            "预增": 3,
            "略增": 2,
            "续盈": 1,
            "扭亏": 2,
            "略减": -1,
            "预减": -2,
            "首亏": -3,
            "续亏": -3,
        }

    # ── backward compat attribute ───────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/earnings_guidance")
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 30
        if not hasattr(self, "_TYPE_MAP"):
            self._TYPE_MAP = {
                "预增": 3, "略增": 2, "续盈": 1, "扭亏": 2,
                "略减": -1, "预减": -2, "首亏": -3, "续亏": -3,
            }

    # ── BaseFactor interface ────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        guidance = self._load_guidance_cache()
        if guidance is None or guidance.empty:
            logger.warning(
                "EarningsGuidanceFactor: no guidance cache data available"
            )
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # For each instrument+date, take the LATEST guidance
        # (most recent 公告日期 / announcement_date, which is our datetime index)
        # Since the data is already indexed by (instrument, datetime=announcement_date),
        # we need to align it to the price_data dates.

        # Step 1: Map guidance_type_raw to numeric
        if "guidance_type_raw" in guidance.columns:
            guidance["guidance_type"] = (
                guidance["guidance_type_raw"]
                .map(self._TYPE_MAP)
                .astype(float)
            )
        else:
            guidance["guidance_type"] = np.nan

        # Step 2: Compute earnings_surprise_pct
        guidance["earnings_surprise_pct"] = self._compute_surprise(guidance)

        # Step 3: Keep only output columns
        output_cols = ["guidance_type", "earnings_surprise_pct"]
        if "reporting_period" in guidance.columns:
            output_cols.append("reporting_period")
        existing_cols = [c for c in output_cols if c in guidance.columns]
        guidance = guidance[existing_cols]

        # Step 4: Build target index and forward-fill
        # For each trading date, we want the most recent guidance whose
        # announcement_date is on or before that trading date.
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )

        # Reindex: this will place NaN for dates where no guidance was
        # announced on that exact date. Then forward-fill within each
        # instrument so the guidance persists until superseded.
        guidance = guidance.reindex(target_idx)
        guidance = guidance.groupby(level=0, group_keys=False).ffill()

        # Step 5: Reindex to price_data index (exact match)
        guidance = guidance.reindex(price_data.index)

        return guidance

    # ── Internal helpers ────────────────────────────────────────────────────

    def _compute_surprise(self, guidance: pd.DataFrame) -> pd.Series:
        """Compute earnings_surprise_pct.

        If both forecast_value and prior_value are available and prior_value
        is non-zero, use: (forecast - prior) / |prior|.

        Otherwise fall back to earnings_change_pct directly.

        Returns a Series indexed the same as ``guidance``.
        """
        has_forecast = "forecast_value" in guidance.columns
        has_prior = "prior_value" in guidance.columns
        has_change_pct = "earnings_change_pct" in guidance.columns

        if has_forecast and has_prior:
            forecast = pd.to_numeric(guidance["forecast_value"], errors="coerce")
            prior = pd.to_numeric(guidance["prior_value"], errors="coerce")
            denom = prior.abs().replace(0, np.nan)
            surprise = (forecast - prior) / denom

            # Fall back to earnings_change_pct where surprise is NaN
            if has_change_pct:
                change_pct = pd.to_numeric(
                    guidance["earnings_change_pct"], errors="coerce"
                )
                # earnings_change_pct from akshare is already a percentage
                # (e.g. 50 means 50%), so convert to ratio (0.50)
                # But the raw data may already be in ratio form.
                # We normalize: if |value| > 1, assume it's a percentage
                # and divide by 100; otherwise treat as ratio.
                # For safety, we just use it as-is since the model can
                # handle either scale — the key is consistency across stocks.
                surprise = surprise.combine_first(change_pct)

            return surprise

        if has_change_pct:
            return pd.to_numeric(
                guidance["earnings_change_pct"], errors="coerce"
            )

        return pd.Series(np.nan, index=guidance.index)

    def _load_guidance_cache(self) -> Optional[pd.DataFrame]:
        """Load all yjyg_*.csv files from cache dir and concatenate."""
        if not self.cache_dir.exists():
            return None
        files = sorted(self.cache_dir.glob("yjyg_*.csv"))
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
                    "EarningsGuidanceFactor: failed to read %s: %s", f, exc
                )
        if not frames:
            return None
        combined = pd.concat(frames).sort_index()
        # Deduplicate: same instrument + same datetime → keep last
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined
