"""Share repurchase (回购) factor provider.

Reads cached data from RepurchaseFetcher, computes repurchase completion
and active-plan factors.

Key challenge: Repurchase data is plan-level (one row per stock's latest plan).
Factors must be forward-filled across the daily price index.

Factors produced
----------------
- repurchase_completion_pct : done_amount / plan_amount (see plan resolution logic)
    plan_amount resolution:
      1. Use plan_amount_upper if > 0
      2. Else use (plan_amount_upper + plan_amount_lower) / 2 if either > 0
      3. Else NaN
- repurchase_active : 1 if progress indicates an active plan (not "完成"), 0 otherwise

Notes
-----
- Most stocks have NO repurchase plan. Missing values are left as NaN (not 0).
  This is intentional — NaN signals "no plan exists" which is qualitatively
  different from "0% completion".
- RepurchaseFactor loads all repurchase_*.csv files from the cache directory
  and takes the latest plan per instrument (by announcement_date).

Registered name: "repurchase"
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("repurchase")
class RepurchaseFactor(BaseFactor):
    """Repurchase factors from cached repurchase plan data.

    Parameters
    ----------
    cache_dir : str
        Directory for repurchase CSV caches (same as RepurchaseFetcher).
    cache_ttl_days : int
        Not used at factor level (fetcher handles freshness), kept for
        interface consistency.
    """

    name = "repurchase"

    def __init__(
        self,
        cache_dir: str = "./cache/repurchase",
        cache_ttl_days: int = 1,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days

    # ── backward compat ───────────────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/repurchase")
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 1

    # ── BaseFactor interface ──────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute repurchase factors from cached repurchase data.

        Parameters
        ----------
        price_data : DataFrame with (instrument, datetime) MultiIndex.

        Returns
        -------
        DataFrame with (instrument, datetime) MultiIndex and factor columns,
        or None if no repurchase data is available.
        """
        self._ensure_runtime_defaults()

        # Load cached repurchase data
        repurchase = self._load_repurchase_cache()
        if repurchase is None or repurchase.empty:
            logger.warning("RepurchaseFactor: no repurchase cache data available")
            return None

        # For each instrument, keep only the latest repurchase plan
        repurchase = self._latest_plan_per_instrument(repurchase)

        # Get instruments and dates from price_data
        instruments = sorted(price_data.index.get_level_values(0).unique().tolist())
        dates = price_data.index.get_level_values(1).unique()

        # Compute factors per instrument that has repurchase data
        result_frames = []
        for inst in instruments:
            if inst not in repurchase.index:
                # No repurchase plan for this instrument — skip, will be NaN
                continue

            plan = repurchase.loc[inst]
            # plan could be a Series (single row) or DataFrame (multiple rows)
            if isinstance(plan, pd.DataFrame):
                plan = plan.iloc[0]

            # Compute factor values
            completion_pct = self._compute_completion_pct(plan)
            active_flag = self._compute_active_flag(plan)

            # Build a DataFrame aligned to price_data dates
            inst_df = pd.DataFrame(
                {
                    "repurchase_completion_pct": completion_pct,
                    "repurchase_active": active_flag,
                },
                index=dates,
            )
            inst_df.index = pd.MultiIndex.from_product(
                [[inst], dates], names=["instrument", "datetime"]
            )
            result_frames.append(inst_df)

        if not result_frames:
            logger.warning("RepurchaseFactor: no valid factors computed")
            return None

        # Combine all instruments
        combined = pd.concat(result_frames)
        combined = combined.sort_index()

        # Forward-fill within each instrument group
        # (repurchase factors change only when new plans are announced)
        combined = combined.groupby(level=0, group_keys=False).ffill()

        # Reindex to price_data index (instruments without repurchase → NaN)
        combined = combined.reindex(price_data.index)

        return combined

    # ── private helpers ───────────────────────────────────────────────────

    @staticmethod
    def _compute_completion_pct(plan: pd.Series) -> float:
        """Compute repurchase completion percentage.

        plan_amount resolution:
          1. Use plan_amount_upper if > 0
          2. Else use (plan_amount_upper + plan_amount_lower) / 2 if either > 0
          3. Else NaN

        done_amount / plan_amount, capped at [0, inf) — over-completion is possible.
        """
        done = pd.to_numeric(plan.get("done_amount", np.nan), errors="coerce")
        upper = pd.to_numeric(plan.get("plan_amount_upper", np.nan), errors="coerce")
        lower = pd.to_numeric(plan.get("plan_amount_lower", np.nan), errors="coerce")

        # Resolve plan_amount
        plan_amount = np.nan
        if pd.notna(upper) and upper > 0:
            plan_amount = upper
        elif pd.notna(upper) or pd.notna(lower):
            # At least one exists; use their average (treating NaN as 0 for avg)
            u = upper if pd.notna(upper) else 0.0
            l = lower if pd.notna(lower) else 0.0
            avg = (u + l) / 2.0
            if avg > 0:
                plan_amount = avg

        if pd.isna(plan_amount) or plan_amount <= 0:
            return np.nan

        if pd.isna(done) or done < 0:
            return np.nan

        return done / plan_amount

    @staticmethod
    def _compute_active_flag(plan: pd.Series) -> int:
        """Return 1 if the repurchase plan is active (not completed), else 0.

        Active means progress does NOT contain "完成".
        """
        progress = str(plan.get("progress", ""))
        if not progress or progress == "nan":
            return 0
        if "完成" in progress:
            return 0
        return 1

    @staticmethod
    def _latest_plan_per_instrument(df: pd.DataFrame) -> pd.DataFrame:
        """Keep only the latest repurchase plan per instrument.

        Sorts by datetime within each instrument and keeps the last row.
        """
        if df.empty:
            return df
        # Sort by datetime, then keep the last entry per instrument
        df = df.sort_index(level="datetime")
        # Group by instrument and take the last row
        return df.groupby(level=0).last()

    def _load_repurchase_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached repurchase CSV files and concatenate."""
        files = sorted(self.cache_dir.glob("repurchase_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug("RepurchaseFactor: failed to read %s: %s", f, exc)
        if not frames:
            return None
        return pd.concat(frames).sort_index()
