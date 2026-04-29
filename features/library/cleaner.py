"""Cross-sectional factor cleaning — winsorization, z-score, NaN filling.

All operations are applied **per datetime slice** (cross-sectionally) to
avoid look-ahead bias.  The cleaner is stateless: no fitting required.

Typical usage
-------------
    cleaner = FactorCleaner(winsorize_sigma=3.0, zscore=True, fill_method="zero")
    clean_factors = cleaner.transform(raw_factors)

Or via FactorPipeline:

    pipeline = FactorPipeline(factors, cleaner=FactorCleaner())
    features  = pipeline.compute_with_cleaning(price_data)
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class FactorCleaner:
    """Cross-sectional cleaning pipeline for factor DataFrames.

    Parameters
    ----------
    winsorize_sigma : float
        Cap values beyond ±*N* cross-sectional standard deviations.
        Set to ``0`` or ``None`` to disable.
    zscore : bool
        Standardize to zero mean and unit variance cross-sectionally
        *after* winsorization.
    fill_method : str
        How to handle remaining NaN values after winsorization / z-score:
        ``"zero"``  — replace with 0 (safe default for tree models),
        ``"median"``— replace with cross-sectional median,
        ``"ffill"`` — forward-fill within each instrument time-series,
        ``"drop"``  — drop columns with any NaN (aggressive).
    min_coverage : float
        Drop columns whose valid fraction (across all rows) is below this
        threshold before applying other transforms.  Range [0, 1].
    """

    def __init__(
        self,
        winsorize_sigma: float = 3.0,
        zscore: bool = True,
        fill_method: str = "zero",
        min_coverage: float = 0.3,
    ):
        self.winsorize_sigma = winsorize_sigma
        self.zscore = zscore
        self.fill_method = fill_method
        self.min_coverage = min_coverage

    # ── public API ────────────────────────────────────────────────────────────

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply full cleaning pipeline to *df*.

        Parameters
        ----------
        df : DataFrame
            Factor values with (instrument, datetime) MultiIndex.
            All numeric columns are processed; non-numeric columns are passed
            through unchanged (e.g. categorical sector_id).

        Returns
        -------
        DataFrame
            Same shape and index as *df* (after column-drop for low coverage).
        """
        if df is None or df.empty:
            return df

        df = self._drop_low_coverage(df)
        if df.empty:
            return df

        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = [c for c in df.columns if c not in num_cols]

        num_part = df[num_cols].copy()

        # 1. Cross-sectional winsorization
        if self.winsorize_sigma:
            num_part = self._apply_cross_sectional(num_part, self._winsorize)

        # 2. Cross-sectional z-score
        if self.zscore:
            num_part = self._apply_cross_sectional(num_part, self._zscore)

        # 3. NaN fill
        num_part = self._fill_nan(num_part)

        if cat_cols:
            return pd.concat([num_part, df[cat_cols]], axis=1)
        return num_part

    # ── internals ─────────────────────────────────────────────────────────────

    def _drop_low_coverage(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.min_coverage <= 0:
            return df
        valid_frac = df.notna().mean(axis=0)
        keep = valid_frac[valid_frac >= self.min_coverage].index
        dropped = set(df.columns) - set(keep)
        if dropped:
            logger.debug(f"FactorCleaner: dropped {len(dropped)} low-coverage columns: {sorted(dropped)}")
        return df[keep]

    @staticmethod
    def _apply_cross_sectional(df: pd.DataFrame, fn) -> pd.DataFrame:
        """Apply *fn(slice_df) → slice_df* per datetime level."""
        if isinstance(df.index, pd.MultiIndex):
            # (instrument, datetime) MultiIndex — group by the datetime level
            date_level = df.index.get_level_values(1)
            result = df.copy()
            for dt in date_level.unique():
                mask = date_level == dt
                result.iloc[mask] = fn(df.iloc[mask]).values
            return result
        # Flat DatetimeIndex: each row is already one cross-section
        return df.apply(fn, axis=0)

    def _winsorize(self, cross: pd.DataFrame) -> pd.DataFrame:
        mean = cross.mean()
        std = cross.std().replace(0, np.nan)
        lo = mean - self.winsorize_sigma * std
        hi = mean + self.winsorize_sigma * std
        return cross.clip(lower=lo, upper=hi, axis=1)

    @staticmethod
    def _zscore(cross: pd.DataFrame) -> pd.DataFrame:
        mean = cross.mean()
        std = cross.std().replace(0, np.nan)
        return (cross - mean) / std

    def _fill_nan(self, df: pd.DataFrame) -> pd.DataFrame:
        method = self.fill_method
        if method == "zero":
            return df.fillna(0.0)
        if method == "median":
            return df.apply(lambda col: col.fillna(col.median()), axis=0)
        if method == "ffill":
            # ffill within each instrument's time series
            if isinstance(df.index, pd.MultiIndex):
                return (
                    df.groupby(level=0, group_keys=False)
                    .apply(lambda g: g.ffill())
                    .fillna(0.0)   # any leading NaN after ffill → 0
                )
            return df.ffill().fillna(0.0)
        if method == "drop":
            before = df.shape[1]
            df = df.dropna(axis=1)
            logger.debug(f"FactorCleaner: drop NaN removed {before - df.shape[1]} columns")
            return df
        raise ValueError(
            f"Unknown fill_method '{method}'. "
            "Choose: zero | median | ffill | drop"
        )
