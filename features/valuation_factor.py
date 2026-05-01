"""Valuation factor provider.

Reads cached per-stock valuation CSVs from ValuationFetcher and produces
a (instrument, datetime) MultiIndex DataFrame aligned to price_data.

Available metrics (from stock_value_em):
  market_cap        — total market capitalization (absolute value)
  float_market_cap  — float market capitalization (absolute value)
  total_shares      — total shares outstanding (absolute value)
  float_shares      — float shares outstanding (absolute value)
  pe_ttm            — price-to-earnings (TTM)
  pe_static         — price-to-earnings (static)
  pb                — price-to-book
  peg               — PEG ratio
  pcf               — price-to-cash-flow
  ps_ttm            — price-to-sales (TTM)

Additional metrics from fallback (stock_a_lg_indicator):
  dyr               — dividend yield

When include_change=True, the factor also computes period-over-period
pct_change for ratio metrics: pe_ttm_chg, pb_chg, ps_ttm_chg, pcf_chg.

Registered name: "valuation"
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)

# All available metrics from the valuation data
_ALL_VALUATION_METRICS = [
    "market_cap",
    "float_market_cap",
    "total_shares",
    "float_shares",
    "pe_ttm",
    "pe_static",
    "pb",
    "peg",
    "pcf",
    "ps_ttm",
    "dyr",
]

# Ratio metrics for which we compute pct_change when include_change=True
_RATIO_METRICS = ["pe_ttm", "pb", "ps_ttm", "pcf"]


@FactorRegistry.register("valuation")
class ValuationFactor(BaseFactor):
    """Valuation factor provider from cached per-stock data.

    Parameters
    ----------
    cache_dir : str
        Directory containing per-stock valuation CSVs (written by ValuationFetcher).
    cache_ttl_days : int
        Not used directly (cache freshness is managed by the fetcher), but
        kept for API consistency.  Default 1.
    metrics : list[str], optional
        Subset of metric names to include.  Default: all available metrics.
        This factor provides ABSOLUTE-VALUE data (market_cap, float_market_cap,
        total_shares) that the user specifically requested as missing from the
        current system.
    include_change : bool
        If True, compute period-over-period pct_change for ratio metrics
        (pe_ttm_chg, pb_chg, ps_ttm_chg, pcf_chg).
    precomputed : DataFrame, optional
        Provide your own (instrument, datetime) MultiIndex DataFrame to skip
        the cache load entirely — useful for testing or custom data.
    """

    name = "valuation"

    def __init__(
        self,
        cache_dir: str = "./cache/valuation",
        cache_ttl_days: int = 1,
        metrics: Optional[List[str]] = None,
        include_change: bool = False,
        precomputed: Optional[pd.DataFrame] = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.metrics = metrics if metrics is not None else list(_ALL_VALUATION_METRICS)
        self.include_change = include_change
        self.precomputed = precomputed

    # ── backward compat ──────────────────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "include_change"):
            self.include_change = False
        if not hasattr(self, "metrics"):
            self.metrics = list(_ALL_VALUATION_METRICS)
        if not hasattr(self, "precomputed"):
            self.precomputed = None
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 1

    # ── BaseFactor interface ─────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute valuation factors from cached per-stock data.

        Parameters
        ----------
        price_data : DataFrame with (instrument, datetime) MultiIndex.

        Returns
        -------
        DataFrame with same MultiIndex and valuation factor columns,
        or None if no data is available.
        """
        self._ensure_runtime_defaults()

        if self.precomputed is not None:
            combined = self.precomputed
        else:
            # Load all per-stock CSVs from cache dir
            combined = self._load_all_cache()
            if combined is None or combined.empty:
                logger.warning("ValuationFactor: no cached valuation data available")
                return None

        # Align to price_data
        result = self._align(combined, price_data)
        if result is None:
            return None

        # Filter to requested metrics
        keep = [c for c in self.metrics if c in result.columns]
        if not keep:
            return None
        result = result[keep]

        # Add change factors if requested
        if self.include_change:
            result = self._compute_change_factors(result)

        # Reindex to price_data index
        result = result.reindex(price_data.index)
        return result

    # ── internals ────────────────────────────────────────────────────────────

    def _load_all_cache(self) -> Optional[pd.DataFrame]:
        """Load and concatenate all per-stock valuation CSVs from cache dir."""
        if not self.cache_dir.exists():
            return None

        files = sorted(self.cache_dir.glob("*.csv"))
        if not files:
            return None

        frames: List[pd.DataFrame] = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                if not df.empty:
                    frames.append(df)
            except Exception as exc:
                logger.debug("ValuationFactor: failed to read %s: %s", f, exc)

        if not frames:
            return None
        return pd.concat(frames).sort_index()

    def _align(
        self, data: pd.DataFrame, price_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """Forward-fill valuation data to match the price_data MultiIndex."""
        instruments = price_data.index.get_level_values(0).unique()
        dates = price_data.index.get_level_values(1).unique()

        target = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        aligned = data.reindex(target)

        # Forward-fill within each instrument (valuation data is daily but
        # may have gaps for suspended stocks)
        aligned = aligned.groupby(level=0, group_keys=False).apply(
            lambda g: g.ffill()
        )

        return aligned

    def _compute_change_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """Compute period-over-period pct_change for ratio metrics.

        Adds columns: pe_ttm_chg, pb_chg, ps_ttm_chg, pcf_chg.
        """
        result = data.copy()

        for metric in _RATIO_METRICS:
            if metric in result.columns:
                chg_name = f"{metric}_chg"
                result[chg_name] = (
                    result[metric]
                    .groupby(level=0)
                    .pct_change(fill_method=None)
                )

        return result
