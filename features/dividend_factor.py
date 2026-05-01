"""Dividend (分红) factor provider.

Reads cached data from DividendFetcher, computes dividend yield and consistency factors.

Key challenge: Dividend data is event-based (irregular dates).
Factors must be forward-filled across the daily price index.

Factors produced
----------------
- div_yield_ttm        : trailing 12-month cash dividend yield (cash_dividend / 10 / close_price)
- div_consistency      : count of consecutive years with non-zero dividends (up to lookback_years)
- div_growth_rate      : (current_year_div - prior_year_div) / abs(prior_year_div), capped at [-1, 5]

Notes
-----
- cash_dividend from akshare follows A-share convention: amount per 10 shares.
  To get per-share dividend, divide by 10.
  Yield = (per_share_dividend) / close_price = cash_dividend / 10 / close_price.
- div_consistency counts consecutive years with non-zero dividends from the most
  recent year backwards, up to lookback_years.
- div_growth_rate compares the most recent complete calendar year to the prior year.

Registered name: "dividend"
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("dividend")
class DividendFactor(BaseFactor):
    """Dividend factors from cached dividend history data.

    Parameters
    ----------
    cache_dir : str
        Directory for dividend CSV caches (same as DividendFetcher).
    cache_ttl_days : int
        Not used at factor level (fetcher handles freshness), kept for
        interface consistency.
    lookback_years : int
        Maximum number of years to look back for consistency and growth
        calculations.
    """

    name = "dividend"

    def __init__(
        self,
        cache_dir: str = "./cache/dividend",
        cache_ttl_days: int = 30,
        lookback_years: int = 5,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.lookback_years = lookback_years

    # ── backward compat ───────────────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "cache_ttl_days"):
            self.cache_ttl_days = 30
        if not hasattr(self, "lookback_years"):
            self.lookback_years = 5

    # ── BaseFactor interface ──────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute dividend factors from cached dividend data.

        Parameters
        ----------
        price_data : DataFrame with (instrument, datetime) MultiIndex,
                     must contain a close price column.

        Returns
        -------
        DataFrame with (instrument, datetime) MultiIndex and factor columns,
        or None if no dividend data is available.
        """
        self._ensure_runtime_defaults()

        # Load cached dividend data
        dividend = self._load_dividend_cache()
        if dividend is None or dividend.empty:
            logger.warning("DividendFactor: no dividend cache data available")
            return None

        # Determine close price column
        close_col = None
        for col in ("real_close", "$close", "close"):
            if col in price_data.columns:
                close_col = col
                break
        if close_col is None:
            logger.warning("DividendFactor: no close price column found, skipping.")
            return None

        # Get instruments and dates from price_data
        instruments = sorted(price_data.index.get_level_values(0).unique().tolist())
        dates = price_data.index.get_level_values(1).unique()

        # Get latest close price per instrument (used for yield denominator)
        close_series = price_data[close_col]
        # Per-instrument latest close
        latest_close = (
            close_series.groupby(level=0).last()
        )

        # Compute factors per instrument
        result_frames = []
        for inst in instruments:
            inst_div = dividend.loc[inst] if inst in dividend.index.get_level_values(0) else None
            if inst_div is None or inst_div.empty:
                continue

            # Ensure cash_dividend is numeric
            if "cash_dividend" not in inst_div.columns:
                continue
            inst_div = inst_div.copy()
            inst_div["cash_dividend"] = pd.to_numeric(inst_div["cash_dividend"], errors="coerce").fillna(0.0)

            # Get close price for this instrument
            inst_close = latest_close.get(inst, np.nan)
            if pd.isna(inst_close) or inst_close <= 0:
                continue

            # Build factors aligned to price_data dates
            inst_factors = self._compute_instrument_factors(
                inst_div, dates, inst_close
            )
            if inst_factors is not None and not inst_factors.empty:
                inst_factors["instrument"] = inst
                result_frames.append(inst_factors)

        if not result_frames:
            logger.warning("DividendFactor: no valid factors computed")
            return None

        # Combine all instruments
        combined = pd.concat(result_frames, ignore_index=True)
        combined = combined.set_index(["instrument", "datetime"])
        combined = combined.sort_index()

        # Reindex to price_data index
        combined = combined.reindex(price_data.index)

        # Forward-fill dividend factors within each instrument group
        # (dividend factors change slowly — only when new dividends are declared)
        combined = combined.groupby(level=0, group_keys=False).ffill()

        # Fill remaining NaN with 0 for yield and consistency
        for col in ["div_yield_ttm", "div_consistency"]:
            if col in combined.columns:
                combined[col] = combined[col].fillna(0)

        # div_growth_rate NaN → 0 (no prior dividend)
        if "div_growth_rate" in combined.columns:
            combined["div_growth_rate"] = combined["div_growth_rate"].fillna(0)

        return combined

    # ── private helpers ───────────────────────────────────────────────────

    def _compute_instrument_factors(
        self,
        inst_div: pd.DataFrame,
        target_dates: pd.DatetimeIndex,
        close_price: float,
    ) -> Optional[pd.DataFrame]:
        """Compute dividend factors for a single instrument.

        Parameters
        ----------
        inst_div : DataFrame of dividend events for one instrument
                   (datetime index, must have cash_dividend column)
        target_dates : dates from price_data to align to
        close_price : latest close price for this instrument
        """
        inst_div = inst_div.sort_index()  # sort by datetime
        div_events = inst_div[inst_div["cash_dividend"] > 0].copy()

        if div_events.empty:
            return None

        # Build factor values for each target date
        rows = []
        for dt in target_dates:
            # 1. div_yield_ttm: sum of cash_dividend in trailing 12 months / close_price
            #    cash_dividend is per 10 shares, so divide by 10 to get per-share
            ttm_start = dt - pd.DateOffset(months=12)
            ttm_divs = div_events[
                (div_events.index >= ttm_start) & (div_events.index <= dt)
            ]
            ttm_cash = ttm_divs["cash_dividend"].sum()
            div_yield_ttm = ttm_cash / 10.0 / close_price

            # 2. div_consistency: consecutive years with non-zero dividends
            #    counting backwards from the year of dt
            div_consistency = self._compute_consistency(div_events, dt)

            # 3. div_growth_rate: (current_year_div - prior_year_div) / abs(prior_year_div)
            div_growth_rate = self._compute_growth_rate(div_events, dt)

            rows.append({
                "datetime": dt,
                "div_yield_ttm": div_yield_ttm,
                "div_consistency": div_consistency,
                "div_growth_rate": div_growth_rate,
            })

        if not rows:
            return None

        return pd.DataFrame(rows)

    def _compute_consistency(self, div_events: pd.DataFrame, as_of_date: pd.Timestamp) -> int:
        """Count consecutive years with non-zero dividends, looking backwards.

        Starting from the year of as_of_date, count how many consecutive
        calendar years (going backwards) had at least one non-zero dividend
        event.  Count up to self.lookback_years.

        Parameters
        ----------
        div_events : DataFrame of dividend events with cash_dividend > 0
        as_of_date : reference date (typically from price_data)

        Returns
        -------
        int : number of consecutive years with dividends, 0 to lookback_years
        """
        if div_events.empty:
            return 0

        # Get years with dividends
        div_years = sorted(
            set(div_events.index.year)
        )

        # Start from the year of as_of_date or the most recent dividend year
        # that is <= as_of_date
        start_year = as_of_date.year
        recent_div_years = [y for y in div_years if y <= start_year]
        if not recent_div_years:
            return 0

        # The most recent year with dividends
        most_recent = max(recent_div_years)

        # If the most recent dividend year is too far back, consistency is 0
        if start_year - most_recent > 0:
            # Allow 1 year gap: if company pays annually but hasn't yet this year
            # we still count prior year as the start
            pass

        consecutive = 0
        current_year = most_recent
        div_years_set = set(div_years)

        for _ in range(self.lookback_years):
            if current_year in div_years_set:
                consecutive += 1
                current_year -= 1
            else:
                break

        return consecutive

    def _compute_growth_rate(self, div_events: pd.DataFrame, as_of_date: pd.Timestamp) -> float:
        """Compute year-over-year dividend growth rate.

        Compares total dividends in the most recent complete calendar year
        (or current partial year) to the prior calendar year.

        Parameters
        ----------
        div_events : DataFrame of dividend events with cash_dividend > 0
        as_of_date : reference date

        Returns
        -------
        float : growth rate, capped to [-1, 5] range. 0 if no prior year data.
        """
        if div_events.empty:
            return 0.0

        # Sum dividends per calendar year
        yearly = div_events.groupby(div_events.index.year)["cash_dividend"].sum()

        # Find the two most recent years with dividends up to as_of_date
        eligible_years = sorted([y for y in yearly.index if y <= as_of_date.year], reverse=True)
        if len(eligible_years) < 2:
            return 0.0

        current_year = eligible_years[0]
        prior_year = eligible_years[1]

        current_div = yearly.get(current_year, 0.0)
        prior_div = yearly.get(prior_year, 0.0)

        if prior_div == 0:
            # New dividend payer: if current > 0, growth is infinite; cap at 5
            return 5.0 if current_div > 0 else 0.0

        growth = (current_div - prior_div) / abs(prior_div)

        # Cap to reasonable range
        return max(-1.0, min(5.0, growth))

    def _load_dividend_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached per-stock dividend files and concatenate."""
        files = sorted(self.cache_dir.glob("*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                # Only include files that have dividend-relevant columns
                if "cash_dividend" in df.columns:
                    frames.append(df)
            except Exception as exc:
                logger.debug("DividendFactor: failed to read %s: %s", f, exc)
        if not frames:
            return None
        return pd.concat(frames).sort_index()
