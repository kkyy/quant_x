"""Stock universe filtering."""
from __future__ import annotations
import json
from pathlib import Path
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
    - exclude_st:   drop ST stocks (name-based, best-effort)
    - exclude_suspended: drop stocks with zero volume (suspended)
    """

    def __init__(self, strategy_config: dict):
        self.cfg = strategy_config.get("universe_filter", {})

    def requires_price_data(self) -> bool:
        return bool(self.cfg.get("min_price")) or bool(self.cfg.get("exclude_suspended", True))

    def _load_stock_names(self) -> dict:
        """Load {qlib_code: name} from sector_stocks.json for ST detection."""
        path = Path(__file__).parent.parent / "crawler" / "data" / "sector_stocks.json"
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            names = {}
            for category in data.values():
                for sector in category.values():
                    for stock in sector.get("stocks", []):
                        code = stock.get("code", "")
                        name = stock.get("name", "")
                        if code and name:
                            names[self._to_qlib_code(code)] = name
            return names
        except Exception:
            return {}

    @staticmethod
    def _to_qlib_code(code: str) -> str:
        code = str(code).strip()
        if len(code) != 6 or not code.isdigit():
            return code
        prefix = int(code[0])
        if prefix in (0, 2, 3):
            return f"SZ{code}"
        if prefix in (6, 9):
            return f"SH{code}"
        if prefix in (4, 8):
            return f"BJ{code}"
        return code

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

        n_before, n_after = len(pred), int(mask.sum())
        logger.info(f"Universe filter: {n_before} → {n_after} stocks")
        return pred[mask]
