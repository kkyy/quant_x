"""
东方财富 K 线接口

支持板块（BK 代码）和个股（A 股代码）
"""
from __future__ import annotations

import pandas as pd

from .client import EastMoneyClient
from .enums import KlineInterval, AdjustType, to_secid


# K 线数据列顺序（API 返回逗号分隔字符串）
_KLINE_COLUMNS = [
    "日期",
    "开盘价",
    "收盘价",
    "最高价",
    "最低价",
    "成交量(手)",
    "成交额(元)",
    "振幅(%)",
    "涨跌幅(%)",
    "涨跌额",
    "换手率(%)",
]


class KlineAPI:
    """
    东方财富 K 线接口封装

    Parameters
    ----------
    client : EastMoneyClient | None
        传入已有客户端；None 时自动创建默认客户端
    """

    def __init__(self, client: EastMoneyClient | None = None):
        self._client = client or EastMoneyClient()

    def get_kline(
        self,
        code: str,
        interval: KlineInterval = KlineInterval.DAY,
        adjust: AdjustType = AdjustType.FORWARD,
        start_date: str = "",
        end_date: str = "",
        limit: int = 500,
    ) -> pd.DataFrame:
        """
        获取 K 线数据（支持板块和个股）

        Parameters
        ----------
        code : str
            板块代码（如 "BK0428"）或 A 股代码（如 "600519"）
        interval : KlineInterval
            K 线周期（默认日 K）
        adjust : AdjustType
            复权方式（默认前复权）
        start_date : str
            开始日期，格式 "20230101"；空字符串表示不限
        end_date : str
            结束日期，格式 "20231231"；空字符串表示不限
        limit : int
            最大返回条数

        Returns
        -------
        pd.DataFrame
            列：日期、开盘价、收盘价、最高价、最低价、成交量、成交额、振幅、涨跌幅、涨跌额、换手率
        """
        params: dict = {
            "secid": to_secid(code),
            "klt": interval.value,
            "fqt": adjust.value,
            "lmt": limit,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        }
        if start_date:
            params["beg"] = start_date
        if end_date:
            params["end"] = end_date

        data = self._client.kline_get(params)
        klines: list[str] = data.get("data", {}).get("klines", [])

        rows = []
        for line in klines:
            parts = line.split(",")
            if len(parts) < len(_KLINE_COLUMNS):
                continue
            row = {}
            for col, val in zip(_KLINE_COLUMNS, parts):
                try:
                    row[col] = float(val) if col != "日期" else val
                except (ValueError, TypeError):
                    row[col] = None
            rows.append(row)

        df = pd.DataFrame(rows, columns=_KLINE_COLUMNS)
        if not df.empty:
            df["日期"] = pd.to_datetime(df["日期"])
        return df
