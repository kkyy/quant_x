"""Stock universe filtering."""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import List, Optional
import logging

from .utils import load_stock_names, code_to_qlib_instrument

logger = logging.getLogger(__name__)


class UniverseFilter:
    """
    Filter prediction signals by universe rules.

    Rules (all configurable via strategy.universe_filter in YAML):
    - exclude_kcb:  drop 科创板 SH688xxx
    - exclude_list: drop specific instruments
    - min_price:    drop stocks below price threshold
    - exclude_st:   drop ST stocks (name-based, best-effort)
    - exclude_suspended: drop stocks with zero volume (suspended)
    """

    def __init__(self, strategy_config: dict):
        self.cfg = strategy_config.get("universe_filter", {})

    def requires_price_data(self) -> bool:
        return (
            bool(self.cfg.get("min_price"))
            or bool(self.cfg.get("exclude_suspended", True))
            or bool(self.cfg.get("min_avg_volume"))
            or bool(self.cfg.get("min_avg_amount"))
        )

    def _load_stock_names(self) -> dict:
        """Load {qlib_code: name} using the shared cached loader."""
        return load_stock_names()

    @staticmethod
    def _to_qlib_code(code: str) -> str:
        return code_to_qlib_instrument(code)

    def filter(
        self,
        pred: pd.Series,
        price_data: Optional[pd.DataFrame] = None,
    ) -> pd.Series:
        """Return filtered prediction series."""
        instrs = pred.index.get_level_values("instrument")
        mask = pd.Series(True, index=pred.index)

        # 科创板
        if self.cfg.get("exclude_kcb", True):
            mask &= ~instrs.str.startswith("SH688")

        # 手动排除列表
        exclude = self.cfg.get("exclude_list", []) or []
        if exclude:
            mask &= ~instrs.isin(exclude)

        # 股价下限
        min_price = self.cfg.get("min_price")
        if min_price and price_data is not None and "real_close" in price_data.columns:
            price_series = price_data["real_close"].sort_index()
            aligned_prices = price_series.reindex(pred.index)

            if aligned_prices.isna().any():
                latest = (
                    price_series.reset_index()
                    .sort_values("datetime")
                    .groupby("instrument")["real_close"]
                    .last()
                )
                fallback = pred.index.get_level_values("instrument").map(latest)
                aligned_prices = aligned_prices.fillna(pd.Series(fallback, index=pred.index))

            mask &= aligned_prices.ge(min_price).fillna(False)

        # ST 股票排除（基于名称，best-effort）
        if self.cfg.get("exclude_st", True):
            stock_names = self._load_stock_names()
            st_values = pd.Series(
                ~instrs.map(lambda c: "ST" in stock_names.get(c, "")),
                index=pred.index,
            )
            n_st = (~st_values).sum()
            if n_st:
                logger.info(f"Universe filter: excluded {int(n_st)} ST stocks")
            mask &= st_values

        # 停牌排除（最新交易日成交量为 0）
        if self.cfg.get("exclude_suspended", True) and price_data is not None and "$volume" in price_data.columns:
            vol_series = price_data["$volume"].sort_index()
            latest_vol = (
                vol_series.reset_index()
                .sort_values("datetime")
                .groupby("instrument")["$volume"]
                .last()
            )
            suspended = pd.Series(
                instrs.map(lambda c: latest_vol.get(c, 1) == 0),
                index=pred.index,
            )
            n_sus = suspended.sum()
            if n_sus:
                logger.info(f"Universe filter: excluded {int(n_sus)} suspended stocks (zero volume)")
            mask &= ~suspended

        # 流动性下限：N 日平均成交量
        min_avg_vol = self.cfg.get("min_avg_volume")
        avg_vol_window = int(self.cfg.get("avg_volume_window", 20))
        if min_avg_vol and price_data is not None and "$volume" in price_data.columns:
            vol_series = price_data["$volume"].sort_index()
            avg_vol = (
                vol_series.reset_index()
                .sort_values("datetime")
                .groupby("instrument")["$volume"]
                .apply(lambda s: s.tail(avg_vol_window).mean())
            )
            illiquid = pd.Series(
                instrs.map(lambda c: avg_vol.get(c, 0) < min_avg_vol),
                index=pred.index,
            )
            n_illiq = illiquid.sum()
            if n_illiq:
                logger.info(
                    f"Universe filter: excluded {int(n_illiq)} stocks below "
                    f"min_avg_volume={min_avg_vol} (window={avg_vol_window}d)"
                )
            mask &= ~illiquid

        # 流动性下限：N 日平均成交额（元）
        min_avg_amt = self.cfg.get("min_avg_amount")
        avg_amt_window = int(self.cfg.get("avg_amount_window", 20))
        if min_avg_amt and price_data is not None and "$amount" in price_data.columns:
            amt_series = price_data["$amount"].sort_index()
            avg_amt = (
                amt_series.reset_index()
                .sort_values("datetime")
                .groupby("instrument")["$amount"]
                .apply(lambda s: s.tail(avg_amt_window).mean())
            )
            illiquid_amt = pd.Series(
                instrs.map(lambda c: avg_amt.get(c, 0) < min_avg_amt),
                index=pred.index,
            )
            n_illiq_amt = illiquid_amt.sum()
            if n_illiq_amt:
                logger.info(
                    f"Universe filter: excluded {int(n_illiq_amt)} stocks below "
                    f"min_avg_amount={min_avg_amt} (window={avg_amt_window}d)"
                )
            mask &= ~illiquid_amt

        n_before, n_after = len(pred), int(mask.sum())
        logger.info(f"Universe filter: {n_before} → {n_after} stocks")
        return pred[mask]
