"""Stock universe filtering."""
from __future__ import annotations
import pandas as pd
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class UniverseFilter:
    """
    Filter prediction signals by universe rules.

    Rules (all configurable via strategy.universe_filter in YAML):
    - exclude_kcb:  drop 科创板 SH688xxx
    - exclude_list: drop specific instruments
    - min_price:    drop stocks below price threshold
    """

    def __init__(self, strategy_config: dict):
        self.cfg = strategy_config.get("universe_filter", {})

    def requires_price_data(self) -> bool:
        return bool(self.cfg.get("min_price"))

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

        n_before, n_after = len(pred), int(mask.sum())
        logger.debug(f"Universe filter: {n_before} → {n_after} stocks")
        return pred[mask]
