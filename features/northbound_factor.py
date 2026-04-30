"""Northbound capital (沪深港通) factor provider.

Reads cached data from NorthboundFetcher, computes raw and change factors.

Raw factors: nb_hold_pct, nb_hold_mv, nb_net_buy_ratio
Change factors: nb_hold_pct_chg_{w}d, nb_net_buy_ma_{w}d
Sector factors: nb_sector_hold_pct, nb_vs_sector_{w}d (requires sector_map)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("northbound")
class NorthboundFactor(BaseFactor):
    """Northbound capital factors from cached holdings data."""

    name = "northbound"

    def __init__(
        self,
        windows: List[int] = None,
        include_raw: bool = True,
        include_change: bool = True,
        cache_dir: str = "./cache/northbound",
        cache_ttl_days: int = 1,
        sector_map: Optional[Dict[str, str]] = None,
    ):
        self.windows = windows or [5, 10, 20, 60]
        self.include_raw = include_raw
        self.include_change = include_change
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.sector_map = sector_map

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        holdings = self._load_holdings_cache()
        if holdings is None or holdings.empty:
            logger.warning("NorthboundFactor: no holdings cache data available")
            return None

        sm = self.sector_map
        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Fill missing instruments with 0
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        holdings = holdings.reindex(target_idx, fill_value=0)

        # Forward-fill within each instrument
        holdings = holdings.groupby(level=0, group_keys=False).ffill()

        result_parts = []

        # Raw factors
        if self.include_raw:
            raw_cols = ["nb_hold_pct", "nb_hold_mv", "nb_net_buy_ratio"]
            existing_raw = [c for c in raw_cols if c in holdings.columns]
            if existing_raw:
                result_parts.append(holdings[existing_raw])

        # Change factors
        if self.include_change:
            for w in self.windows:
                if "nb_hold_pct" in holdings.columns:
                    chg = holdings["nb_hold_pct"].groupby(level=0).diff(w)
                    result_parts.append(chg.rename(f"nb_hold_pct_chg_{w}d"))
                if "nb_net_buy_ratio" in holdings.columns:
                    ma = holdings["nb_net_buy_ratio"].groupby(level=0).rolling(w, min_periods=1).mean()
                    ma = ma.droplevel(0) if ma.index.nlevels > 2 else ma
                    result_parts.append(ma.rename(f"nb_net_buy_ma_{w}d"))

        # Sector aggregation
        if sm and "nb_hold_pct" in holdings.columns:
            holdings_with_sector = holdings.copy()
            holdings_with_sector["sector"] = holdings_with_sector.index.get_level_values(0).map(sm)
            sector_avg = holdings_with_sector.groupby(
                [holdings_with_sector.index.get_level_values(1), "sector"]
            )["nb_hold_pct"].mean()
            sector_avg.index.names = ["datetime", "sector"]
            # Map back to instrument level
            mapped = []
            for inst, dt in holdings_with_sector.index:
                sec = sm.get(inst)
                if sec and (dt, sec) in sector_avg.index:
                    mapped.append(sector_avg.loc[(dt, sec)])
                else:
                    mapped.append(np.nan)
            sector_hold_pct = pd.Series(mapped, index=holdings_with_sector.index, name="nb_sector_hold_pct")
            result_parts.append(sector_hold_pct)

            # Stock vs sector: stock change minus sector change
            if self.include_change:
                for w in self.windows:
                    stock_chg = holdings["nb_hold_pct"].groupby(level=0).diff(w)
                    sector_chg = sector_hold_pct.groupby(level=0).diff(w)
                    vs_sector = stock_chg - sector_chg
                    result_parts.append(vs_sector.rename(f"nb_vs_sector_{w}d"))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data
        result = result.reindex(price_data.index)
        return result

    def _load_holdings_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached holdings files and concatenate."""
        files = sorted(self.cache_dir.glob("holdings_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"NorthboundFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        return pd.concat(frames).sort_index()
