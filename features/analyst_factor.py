"""Analyst consensus forecast factor provider.

Reads cached data from AnalystForecastFetcher, computes coverage, rating,
and EPS-based factors.

Factors computed:
- analyst_coverage : number of analyst reports (report_count)
- buy_rating_ratio : (buy + outperform) / total ratings
- consensus_eps_growth : (forward_eps - current_eps) / abs(current_eps)
- current_eps_forecast : current-year consensus EPS (level factor)
- consensus_eps_forecast : next-year consensus EPS (level factor)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("analyst")
class AnalystFactor(BaseFactor):
    """Analyst consensus forecast factors from cached forecast data.

    Parameters
    ----------
    cache_dir : str
        Directory for forecast CSV caches (written by AnalystForecastFetcher).
    cache_ttl_days : int
        Not used directly (fetcher controls TTL), kept for interface consistency.
    windows : list[int], optional
        Look-back windows for rolling factors (reserved for future use).
    """

    name = "analyst"

    def __init__(
        self,
        cache_dir: str = "./cache/analyst",
        cache_ttl_days: int = 3,
        windows: Optional[List[int]] = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.windows = windows or [5, 20]

    # ── backward compat attribute ───────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "cache_dir"):
            self.cache_dir = Path("./cache/analyst")
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 3
        if not hasattr(self, "windows"):
            self.windows = [5, 20]

    # ── BaseFactor interface ──────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        forecast = self._load_forecast_cache()
        if forecast is None or forecast.empty:
            logger.warning("AnalystFactor: no forecast cache data available")
            return None

        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Build target MultiIndex; missing instruments stay NaN (analyst
        # coverage is sparse — only ~2700 stocks have it)
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        forecast = forecast.reindex(target_idx)

        # Forward-fill within each instrument (analyst forecasts change
        # infrequently — new reports only appear every few weeks)
        forecast = forecast.groupby(level=0, group_keys=False).ffill()

        result_parts = []

        # ── analyst_coverage ────────────────────────────────────────────────
        if "report_count" in forecast.columns:
            result_parts.append(forecast["report_count"].rename("analyst_coverage"))

        # ── buy_rating_ratio ───────────────────────────────────────────────
        rating_cols = [
            "buy_rating", "outperform_rating", "neutral_rating",
            "underperform_rating", "sell_rating",
        ]
        has_ratings = all(c in forecast.columns for c in rating_cols)
        if has_ratings:
            total_ratings = (
                forecast["buy_rating"]
                + forecast["outperform_rating"]
                + forecast["neutral_rating"]
                + forecast["underperform_rating"]
                + forecast["sell_rating"]
            )
            buy_bull = forecast["buy_rating"] + forecast["outperform_rating"]
            buy_ratio = buy_bull / total_ratings.replace(0, np.nan)
            result_parts.append(buy_ratio.rename("buy_rating_ratio"))

        # ── consensus_eps_growth ────────────────────────────────────────────
        if (
            "current_eps_forecast" in forecast.columns
            and "consensus_eps_forecast" in forecast.columns
        ):
            current = forecast["current_eps_forecast"]
            forward = forecast["consensus_eps_forecast"]
            # Growth = (forward - current) / |current|
            # Avoid division by zero or near-zero
            denom = current.abs().replace(0, np.nan)
            eps_growth = (forward - current) / denom
            result_parts.append(eps_growth.rename("consensus_eps_growth"))

        # ── Level EPS factors (useful when trailing EPS is unavailable) ─────
        if "current_eps_forecast" in forecast.columns:
            result_parts.append(
                forecast["current_eps_forecast"].rename("current_eps_forecast")
            )
        if "consensus_eps_forecast" in forecast.columns:
            result_parts.append(
                forecast["consensus_eps_forecast"].rename("consensus_eps_forecast")
            )

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data
        result = result.reindex(price_data.index)
        return result

    # ── Cache loading ────────────────────────────────────────────────────────

    def _load_forecast_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached forecast files and concatenate."""
        if not self.cache_dir.exists():
            return None
        files = sorted(self.cache_dir.glob("forecast_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"AnalystFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        return pd.concat(frames).sort_index()
