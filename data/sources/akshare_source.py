"""Akshare supplementary OHLCV data source.

Uses ``akshare.stock_zh_a_hist`` to fetch unadjusted daily price data.

Notes
-----
- ``adjust=""`` returns raw (unadjusted) prices.
- ``adjclose`` is set to ``close`` for gap rows — corporate-action splits are
  rare in the recent-day windows we fill, so factor=1 is safe in practice.
- vwap formula matches the Dolt export: ``amount / volume * 10``  (volume in
  lots, amount in yuan; the *10 factor is inherited from the Dolt SQL view).
"""
from __future__ import annotations

import logging

import pandas as pd

from .base import BaseDataSource

logger = logging.getLogger(__name__)

# akshare column names → our standard names
_RENAME = {
    "日期": "tradedate",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",   # 手 (lots)
    "成交额": "amount",   # 元 (yuan)
}


class AkshareSource(BaseDataSource):
    """Fetch daily OHLCV from akshare (东方财富 via akshare wrapper)."""

    @property
    def name(self) -> str:
        return "akshare"

    def fetch(self, qlib_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            import akshare as ak
        except ImportError:
            logger.error("akshare is not installed")
            return pd.DataFrame(columns=self.SOURCE_COLUMNS)

        code = self.to_bare_code(qlib_symbol)
        # akshare expects dates without dashes
        start = start_date.replace("-", "")
        end = end_date.replace("-", "")

        try:
            raw = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="",   # unadjusted
            )
        except Exception as exc:
            logger.warning(f"[akshare] fetch failed for {qlib_symbol}: {exc}")
            return pd.DataFrame(columns=self.SOURCE_COLUMNS)

        if raw is None or raw.empty:
            return pd.DataFrame(columns=self.SOURCE_COLUMNS)

        df = raw.rename(columns=_RENAME)
        df["tradedate"] = pd.to_datetime(df["tradedate"])
        df["symbol"] = qlib_symbol
        # No splits assumed → adjclose == close (factor = 1)
        df["adjclose"] = df["close"]
        # Reproduce Dolt's vwap formula
        df["vwap"] = df["amount"] / df["volume"] * 10

        return self._build_output(df)
