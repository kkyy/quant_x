"""
Sector rotation factors for A-share market.

A股市场有明显的板块轮动效应，这些因子从个股所属板块的视角
提供额外的 alpha 信号：

1. sector_mom_{w}d       板块动量（板块内股票的平均收益）
2. sector_rel_{w}d       板块相对强度（板块 vs 全市场）
3. stock_vs_sector_{w}d  个股超额收益（个股 vs 板块）
4. sector_rev_{w}d       板块反转（逆向）
5. sector_vol_{w}d       板块波动率
6. sector_id             板块数字编号（供 LightGBM categorical 使用）

Registered name: "sector"
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("sector")
class SectorFactorEngine(BaseFactor):
    """
    Compute sector-based features from price data.

    Input:  price_df  — DataFrame with (instrument, datetime) MultiIndex
                        must contain 'real_close' (or '$close') column
    Output: factor_df — same MultiIndex, sector-factor columns

    Parameters
    ----------
    sector_map        : dict mapping instrument → sector name
    momentum_windows  : look-back windows for momentum / relative-strength
    reversal_windows  : look-back windows for reversal factors
    """

    def __init__(
        self,
        sector_map: Optional[Dict[str, str]] = None,
        momentum_windows: Optional[List[int]] = None,
        reversal_windows: Optional[List[int]] = None,
        stock_vs_sector_windows: Optional[List[int]] = None,
        concept_map: Optional[Dict[str, str]] = None,
        include_sector_momentum: bool = True,
        include_sector_relative: bool = True,
        include_stock_vs_sector: bool = True,
        include_sector_reversal: bool = True,
        include_sector_volatility: bool = True,
        include_sector_id: bool = True,
        include_concept: bool = True,
        include_concept_id: bool = True,
    ):
        self.sector_map = sector_map or {}
        self.concept_map = concept_map or {}
        self.mom_windows = momentum_windows or [5, 10, 20, 60]
        self.rev_windows = reversal_windows or [5, 20]
        self.stock_vs_sector_windows = stock_vs_sector_windows or [5, 20]
        self.include_sector_momentum = include_sector_momentum
        self.include_sector_relative = include_sector_relative
        self.include_stock_vs_sector = include_stock_vs_sector
        self.include_sector_reversal = include_sector_reversal
        self.include_sector_volatility = include_sector_volatility
        self.include_sector_id = include_sector_id
        self.include_concept = include_concept
        self.include_concept_id = include_concept_id

    # ── BaseFactor interface ───────────────────────────────────────────────────

    def _ensure_compat_attrs(self) -> None:
        """Backward-compat: old pickles may lack flags added after training."""
        for attr, default in (
            ("mom_windows", [5, 10, 20, 60]),
            ("rev_windows", [5, 20]),
            ("stock_vs_sector_windows", [5, 20]),
            ("include_sector_momentum", True),
            ("include_sector_relative", True),
            ("include_stock_vs_sector", True),
            ("include_sector_reversal", True),
            ("include_sector_volatility", True),
            ("include_sector_id", True),
            ("include_concept", True),
            ("include_concept_id", True),
        ):
            if not hasattr(self, attr):
                setattr(self, attr, default)

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Return sector factor DataFrame (instrument, datetime) MultiIndex."""
        self._ensure_compat_attrs()
        if not self.sector_map and not self.concept_map:
            logger.warning("SectorFactorEngine: sector_map and concept_map are both empty, skipping.")
            return None
        return self.compute_all(price_data)

    # ── public ────────────────────────────────────────────────────────────────

    def compute_all(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """Return sector factor DataFrame (instrument, datetime) MultiIndex."""
        close_col = "real_close" if "real_close" in price_df.columns else "$close"

        # Pivot → (datetime × instrument)
        close = price_df[close_col].unstack("instrument")
        rets = close.pct_change(fill_method=None)

        instruments = close.columns.tolist()
        pieces = []

        if self.sector_map:
            sector_s = pd.Series(
                {i: self.sector_map.get(i, "Unknown") for i in instruments}
            )
            if self.include_sector_momentum:
                for w in self.mom_windows:
                    f = self._sector_momentum(rets, sector_s, w)
                    pieces.append(f.stack().rename(f"sector_mom_{w}d"))

            if self.include_sector_relative:
                for w in self.mom_windows:
                    f = self._sector_rel_strength(rets, sector_s, w)
                    pieces.append(f.stack().rename(f"sector_rel_{w}d"))

            if self.include_stock_vs_sector:
                for w in self.stock_vs_sector_windows:
                    f = self._stock_vs_sector(rets, sector_s, w)
                    pieces.append(f.stack().rename(f"stock_vs_sector_{w}d"))

            if self.include_sector_reversal:
                # Reversal = short momentum - long momentum (mean-reversion signal)
                for w_short, w_long in zip(self.rev_windows, self.mom_windows[len(self.rev_windows):] or [self.mom_windows[-1]]):
                    f = self._sector_reversal(rets, sector_s, w_short, w_long)
                    pieces.append(f.stack().rename(f"sector_rev_{w_short}_{w_long}d"))

            if self.include_sector_volatility:
                for w in [10, 20]:
                    f = self._sector_vol(rets, sector_s, w)
                    pieces.append(f.stack().rename(f"sector_vol_{w}d"))

            if self.include_sector_id:
                sector_id = self._sector_id(instruments, sector_s)
                id_df = pd.DataFrame(
                    {inst: sector_id[inst] for inst in instruments},
                    index=close.index,
                )
                pieces.append(id_df.stack().rename("sector_id"))

        # Concept factors (短期动量更有效)
        if self.include_concept and self.concept_map:
            concept_s = pd.Series(
                {i: self.concept_map.get(i, "Unknown") for i in instruments}
            )
            for w in [5, 10, 20]:
                f = self._sector_momentum(rets, concept_s, w)
                pieces.append(f.stack().rename(f"concept_mom_{w}d"))

            for w in [5, 10]:
                f = self._sector_rel_strength(rets, concept_s, w)
                pieces.append(f.stack().rename(f"concept_rel_{w}d"))

            if self.include_concept_id:
                concept_id = self._sector_id(instruments, concept_s)
                cid_df = pd.DataFrame(
                    {inst: concept_id[inst] for inst in instruments},
                    index=close.index,
                )
                pieces.append(cid_df.stack().rename("concept_id"))

        if not pieces:
            logger.warning("SectorFactorEngine: all factor groups disabled, skipping.")
            return pd.DataFrame(index=price_df.index)

        result = pd.concat(pieces, axis=1)
        result.index.names = ["datetime", "instrument"]
        return result.swaplevel().sort_index()

    # ── private helpers ───────────────────────────────────────────────────────

    def _sector_momentum(
        self, rets: pd.DataFrame, sector_s: pd.Series, window: int
    ) -> pd.DataFrame:
        cum = (1 + rets).rolling(window).apply(np.prod, raw=True) - 1
        return self._map_sector_stat(cum, sector_s, "mean")

    def _sector_rel_strength(
        self, rets: pd.DataFrame, sector_s: pd.Series, window: int
    ) -> pd.DataFrame:
        mom = self._sector_momentum(rets, sector_s, window)
        mkt = rets.rolling(window).sum().mean(axis=1)
        return mom.subtract(mkt, axis=0)

    def _stock_vs_sector(
        self, rets: pd.DataFrame, sector_s: pd.Series, window: int
    ) -> pd.DataFrame:
        stock_ret = rets.rolling(window).sum()
        sector_ret = self._sector_momentum(rets, sector_s, window)
        return stock_ret - sector_ret

    def _sector_reversal(
        self, rets: pd.DataFrame, sector_s: pd.Series, window_short: int, window_long: int
    ) -> pd.DataFrame:
        """Reversal = short-window sector momentum minus long-window sector momentum.

        Captures mean-reversion after short-term over-reaction relative to
        the longer-term trend.  A negative value (recent under-performance vs
        longer trend) signals potential mean-reversion upward.
        """
        mom_short = self._sector_momentum(rets, sector_s, window_short)
        mom_long  = self._sector_momentum(rets, sector_s, window_long)
        return mom_short - mom_long

    def _sector_vol(
        self, rets: pd.DataFrame, sector_s: pd.Series, window: int
    ) -> pd.DataFrame:
        return self._map_sector_stat(
            rets.rolling(window).std(), sector_s, "mean"
        )

    def _map_sector_stat(
        self,
        metric: pd.DataFrame,
        sector_s: pd.Series,
        agg: str = "mean",
    ) -> pd.DataFrame:
        """Map per-sector aggregate statistic back to each member stock.

        Vectorised implementation: O(dates × sectors) instead of the previous
        O(sectors × instruments) double-loop.
        """
        # Align sector_s to metric columns (instruments present in price data)
        aligned_sector = sector_s.reindex(metric.columns).fillna("Unknown")

        if agg == "mean":
            sector_agg = metric.T.groupby(aligned_sector).mean().T   # (dates × sectors)
        elif agg == "std":
            sector_agg = metric.T.groupby(aligned_sector).std().T
        else:
            raise ValueError(f"Unsupported agg='{agg}'")

        # Broadcast sector stats back to individual instruments
        # aligned_sector maps instrument → sector_name; sector_agg[sector_name] gives the value
        result = sector_agg.reindex(columns=aligned_sector.values)
        result.columns = metric.columns
        return result

    @staticmethod
    def _sector_id(instruments: list, sector_s: pd.Series) -> pd.Series:
        sectors = sorted(sector_s.unique())
        mapping = {s: i for i, s in enumerate(sectors)}
        return sector_s.map(mapping).fillna(-1).astype(int)
