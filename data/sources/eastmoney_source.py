"""East Money (东方财富) supplementary OHLCV data source.

Reuses the existing ``crawler/eastmoney`` SDK in this repo — no extra
dependencies required beyond what the crawler already uses.

Notes
-----
- ``AdjustType.NONE`` returns raw (unadjusted) prices, same as the Dolt export.
- ``adjclose`` is set to ``close`` for recent gap rows (splits rare).
- vwap formula: ``amount / volume * 10`` — matches Dolt SQL view convention.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from .base import BaseDataSource

# Make crawler package importable when running from project root
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logger = logging.getLogger(__name__)

# KlineAPI column names → our standard names
_RENAME = {
    "日期": "tradedate",
    "开盘价": "open",
    "收盘价": "close",
    "最高价": "high",
    "最低价": "low",
    "成交量(手)": "volume",   # 手 (lots)
    "成交额(元)": "amount",   # 元 (yuan)
}


class EastMoneySource(BaseDataSource):
    """Fetch daily OHLCV from East Money API (crawler/eastmoney SDK)."""

    @property
    def name(self) -> str:
        return "eastmoney"

    def fetch(self, qlib_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            from crawler.eastmoney.kline import KlineAPI
            from crawler.eastmoney.enums import AdjustType, KlineInterval
        except ImportError as exc:
            logger.error(f"crawler.eastmoney not importable: {exc}")
            return pd.DataFrame(columns=self.SOURCE_COLUMNS)

        code = self.to_bare_code(qlib_symbol)
        # EastMoney expects "YYYYMMDD" format
        start = start_date.replace("-", "")
        end = end_date.replace("-", "")

        try:
            api = KlineAPI()
            raw = api.get_kline(
                code=code,
                interval=KlineInterval.DAY,
                adjust=AdjustType.NONE,
                start_date=start,
                end_date=end,
                limit=5000,
            )
        except Exception as exc:
            logger.warning(f"[eastmoney] fetch failed for {qlib_symbol}: {exc}")
            return pd.DataFrame(columns=self.SOURCE_COLUMNS)

        if raw is None or raw.empty:
            return pd.DataFrame(columns=self.SOURCE_COLUMNS)

        df = raw.rename(columns=_RENAME)
        df["tradedate"] = pd.to_datetime(df["tradedate"])
        df["symbol"] = qlib_symbol
        df["adjclose"] = df["close"]
        df["vwap"] = df["amount"] / df["volume"] * 10

        return self._build_output(df)
