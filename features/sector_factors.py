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
"""
from __future__ import annotations
import logging
from typing import Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class SectorFactorEngine:
    """
    Compute sector-based features from price data.

    Input:  price_df  —  DataFrame with (instrument, datetime) MultiIndex
                         must contain 'real_close' column
    Output: factor_df —  same MultiIndex, sector-factor columns
    """

    def __init__(
        self,
        sector_map: Dict[str, str],
        momentum_windows: List[int] = None,
        reversal_windows: List[int] = None,
    ):
        self.sector_map = sector_map
        self.mom_windows = momentum_windows or [5, 10, 20, 60]
        self.rev_windows = reversal_windows or [5, 20]

    # ── public ────────────────────────────────────────────────────────────────

    def compute_all(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """Return sector factor DataFrame (instrument, datetime) MultiIndex."""
        close_col = "real_close" if "real_close" in price_df.columns else "$close"

        # Pivot → (datetime × instrument)
        close = price_df[close_col].unstack("instrument")
        rets = close.pct_change()

        instruments = close.columns.tolist()
        sector_s = pd.Series(
            {i: self.sector_map.get(i, "Unknown") for i in instruments}
        )

        pieces = []

        for w in self.mom_windows:
            f = self._sector_momentum(rets, sector_s, w)
            pieces.append(f.stack().rename(f"sector_mom_{w}d"))

        for w in self.mom_windows:
            f = self._sector_rel_strength(rets, sector_s, w)
            pieces.append(f.stack().rename(f"sector_rel_{w}d"))

        for w in [5, 20]:
            f = self._stock_vs_sector(rets, sector_s, w)
            pieces.append(f.stack().rename(f"stock_vs_sector_{w}d"))

        for w in self.rev_windows:
            f = self._sector_reversal(rets, sector_s, w)
            pieces.append(f.stack().rename(f"sector_rev_{w}d"))

        for w in [10, 20]:
            f = self._sector_vol(rets, sector_s, w)
            pieces.append(f.stack().rename(f"sector_vol_{w}d"))

        # Categorical sector id
        sector_id = self._sector_id(instruments, sector_s)
        # broadcast across dates
        id_df = pd.DataFrame(
            {inst: sector_id[inst] for inst in instruments},
            index=close.index,
        )
        pieces.append(id_df.stack().rename("sector_id"))

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
        self, rets: pd.DataFrame, sector_s: pd.Series, window: int
    ) -> pd.DataFrame:
        return -self._sector_momentum(rets, sector_s, window)

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
        """
        For each sector, compute aggregate of `metric` across member stocks,
        then broadcast back to each member stock column.
        """
        sectors = sector_s.unique()
        sector_agg: Dict[str, pd.Series] = {}

        for sec in sectors:
            members = [c for c in sector_s[sector_s == sec].index if c in metric.columns]
            if members:
                if agg == "mean":
                    sector_agg[sec] = metric[members].mean(axis=1)
                elif agg == "std":
                    sector_agg[sec] = metric[members].std(axis=1)

        result = pd.DataFrame(index=metric.index, columns=metric.columns, dtype=float)
        for inst in metric.columns:
            sec = sector_s.get(inst, "Unknown")
            if sec in sector_agg:
                result[inst] = sector_agg[sec]

        return result

    @staticmethod
    def _sector_id(instruments: list, sector_s: pd.Series) -> pd.Series:
        sectors = sorted(sector_s.unique())
        mapping = {s: i for i, s in enumerate(sectors)}
        return sector_s.map(mapping).fillna(-1).astype(int)
