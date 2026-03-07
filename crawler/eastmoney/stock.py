"""
东方财富个股行情接口

包含：
1. 批量个股实时行情
2. 个股资金流向（单日）
"""
from __future__ import annotations

import pandas as pd

from .client import EastMoneyClient
from .enums import QuoteField, FundField


# 默认个股行情字段
_STOCK_QUOTE_FIELDS = [
    QuoteField.CODE,
    QuoteField.NAME,
    QuoteField.PRICE,
    QuoteField.PCT_CHANGE,
    QuoteField.CHANGE,
    QuoteField.VOLUME,
    QuoteField.AMOUNT,
    QuoteField.AMPLITUDE,
    QuoteField.TURNOVER,
    QuoteField.PE,
    QuoteField.VOL_RATIO,
    QuoteField.HIGH,
    QuoteField.LOW,
    QuoteField.OPEN,
    QuoteField.PREV_CLOSE,
    QuoteField.MARKET_CAP,
    QuoteField.CIRC_MARKET_CAP,
]

# 个股资金流向字段
_STOCK_FUND_FIELDS = [
    QuoteField.CODE,
    QuoteField.NAME,
    FundField.MAIN_NET_INFLOW,
    FundField.SUPER_LARGE_INFLOW,
    FundField.SUPER_LARGE_OUTFLOW,
    FundField.LARGE_INFLOW,
    FundField.LARGE_OUTFLOW,
    FundField.MID_INFLOW,
    FundField.MID_OUTFLOW,
    FundField.SMALL_INFLOW,
    FundField.SMALL_OUTFLOW,
]

# f 字段 → 中文列名
_FIELD_NAMES: dict[str, str] = {
    "f12": "代码",
    "f14": "名称",
    "f2":  "最新价",
    "f3":  "涨跌幅(%)",
    "f4":  "涨跌额",
    "f5":  "成交量(手)",
    "f6":  "成交额(元)",
    "f7":  "振幅(%)",
    "f8":  "换手率(%)",
    "f9":  "市盈率",
    "f10": "量比",
    "f15": "最高价",
    "f16": "最低价",
    "f17": "开盘价",
    "f18": "昨收价",
    "f20": "总市值",
    "f21": "流通市值",
    "f62": "主力净流入",
    "f66": "超大单流入",
    "f69": "超大单流出",
    "f72": "大单流入",
    "f75": "大单流出",
    "f78": "中单流入",
    "f84": "中单流出",
    "f81": "小单流入",
    "f87": "小单流出",
}

# 市场筛选：沪深 A 股（含主板、中小板、创业板）
_FS_A_SHARE = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"


def _parse_diff(data: dict, fields: list[str]) -> list[dict]:
    diff: dict = data.get("data", {}).get("diff", {})
    rows = []
    for item in diff.values():
        row = {}
        for f in fields:
            val = item.get(f, None)
            if val == "-":
                val = None
            row[_FIELD_NAMES.get(f, f)] = val
        rows.append(row)
    return rows


class StockAPI:
    """
    东方财富个股行情接口封装

    Parameters
    ----------
    client : EastMoneyClient | None
        传入已有客户端；None 时自动创建默认客户端
    """

    def __init__(self, client: EastMoneyClient | None = None):
        self._client = client or EastMoneyClient()

    # ------------------------------------------------------------------
    # 实时行情
    # ------------------------------------------------------------------

    def get_stock_quote(
        self,
        stock_codes: list[str] | None = None,
        bk_code: str | None = None,
        fields: list[str] | None = None,
        page_size: int = 500,
    ) -> pd.DataFrame:
        """
        获取个股实时行情

        可按板块成分（bk_code）或全市场（stock_codes=None 且 bk_code=None）拉取。

        Parameters
        ----------
        stock_codes : list[str] | None
            指定股票代码列表（如 ["600519", "000001"]）；None 表示不过滤
        bk_code : str | None
            按板块成分筛选（如 "BK0428"）；与 stock_codes 互斥
        fields : list[str] | None
            指定返回字段；None 使用默认字段集
        page_size : int
            每页条数

        Returns
        -------
        pd.DataFrame
        """
        fields = fields or [f.value for f in _STOCK_QUOTE_FIELDS]

        if bk_code:
            fs = f"b:{bk_code}"
        elif stock_codes:
            # 构造 fs 参数：每个代码需带市场前缀
            parts = []
            for code in stock_codes:
                mkt = "1" if code.startswith("6") else "0"
                parts.append(f"s:{mkt}{code}")
            fs = ",".join(parts)
        else:
            fs = _FS_A_SHARE

        rows = []
        pn = 1
        while True:
            params = {
                "pn": pn,
                "pz": page_size,
                "fs": fs,
                "fields": ",".join(fields),
                "fid": "f3",
                "po": 1,
            }
            data = self._client.clist_get(params)
            batch = _parse_diff(data, fields)
            rows.extend(batch)
            total = data.get("data", {}).get("total", 0)
            if len(rows) >= total or not batch:
                break
            pn += 1

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # 资金流向
    # ------------------------------------------------------------------

    def get_stock_fund_flow(
        self,
        bk_code: str | None = None,
        page_size: int = 500,
    ) -> pd.DataFrame:
        """
        获取个股资金流向（主力 / 超大单 / 大单 / 中单 / 小单）

        Parameters
        ----------
        bk_code : str | None
            按板块成分筛选；None 拉取全市场
        page_size : int
            每页条数

        Returns
        -------
        pd.DataFrame
        """
        fields = [f.value for f in _STOCK_FUND_FIELDS]
        fs = f"b:{bk_code}" if bk_code else _FS_A_SHARE

        rows = []
        pn = 1
        while True:
            params = {
                "pn": pn,
                "pz": page_size,
                "fs": fs,
                "fields": ",".join(fields),
                "fid": "f62",
                "po": 1,
            }
            data = self._client.clist_get(params)
            batch = _parse_diff(data, fields)
            rows.extend(batch)
            total = data.get("data", {}).get("total", 0)
            if len(rows) >= total or not batch:
                break
            pn += 1

        return pd.DataFrame(rows)
