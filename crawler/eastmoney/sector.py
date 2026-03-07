"""
东方财富板块接口

包含：
1. 板块列表（行业 / 概念 / 地域）
2. 板块成分股列表
3. 板块资金流向
"""
from __future__ import annotations

import pandas as pd

from .client import EastMoneyClient
from .enums import SectorType, QuoteField, FundField


# 默认字段集
_SECTOR_QUOTE_FIELDS = [
    QuoteField.CODE,
    QuoteField.NAME,
    QuoteField.PRICE,
    QuoteField.PCT_CHANGE,
    QuoteField.CHANGE,
    QuoteField.VOLUME,
    QuoteField.AMOUNT,
    QuoteField.AMPLITUDE,
    QuoteField.TURNOVER,
    QuoteField.MARKET_CAP,
    QuoteField.CIRC_MARKET_CAP,
]

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
]

_FUND_FIELDS = [
    QuoteField.CODE,
    QuoteField.NAME,
    FundField.MAIN_NET_INFLOW,
    FundField.SUPER_LARGE_INFLOW,
    FundField.SUPER_LARGE_OUTFLOW,
    FundField.LARGE_INFLOW,
    FundField.LARGE_OUTFLOW,
    FundField.MID_INFLOW,
    FundField.SMALL_INFLOW,
]

# f 字段 → 中文列名映射
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
    "f81": "小单流入",
}


def _parse_diff(data: dict, fields: list[str]) -> list[dict]:
    """从 clist 响应的 data.diff 中提取指定字段"""
    diff: dict = data.get("data", {}).get("diff", {})
    rows = []
    for item in diff.values():
        row = {}
        for f in fields:
            val = item.get(f, None)
            # API 用 "-" 表示无效值
            if val == "-":
                val = None
            row[_FIELD_NAMES.get(f, f)] = val
        rows.append(row)
    return rows


class SectorAPI:
    """
    东方财富板块接口封装

    Parameters
    ----------
    client : EastMoneyClient | None
        传入已有客户端；None 时自动创建默认客户端
    """

    def __init__(self, client: EastMoneyClient | None = None):
        self._client = client or EastMoneyClient()

    # ------------------------------------------------------------------
    # 板块列表
    # ------------------------------------------------------------------

    def get_sector_list(
        self,
        sector_type: SectorType = SectorType.INDUSTRY,
        fields: list[str] | None = None,
        page_size: int = 200,
    ) -> pd.DataFrame:
        """
        获取板块实时行情列表（全量，自动翻页）

        Parameters
        ----------
        sector_type : SectorType
            行业 / 概念 / 地域
        fields : list[str] | None
            指定返回字段（使用 QuoteField 枚举值）；None 使用默认字段集
        page_size : int
            每页条数（最大约 500）

        Returns
        -------
        pd.DataFrame
            列名为中文
        """
        fields = fields or [f.value for f in _SECTOR_QUOTE_FIELDS]
        rows = []
        pn = 1
        while True:
            params = {
                "pn": pn,
                "pz": page_size,
                "fs": sector_type.value,
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
    # 板块成分股
    # ------------------------------------------------------------------

    def get_sector_stocks(
        self,
        bk_code: str,
        fields: list[str] | None = None,
        page_size: int = 500,
    ) -> pd.DataFrame:
        """
        获取板块成分股列表（全量，自动翻页）

        Parameters
        ----------
        bk_code : str
            板块代码，如 "BK0428"（新能源车）
        fields : list[str] | None
            指定返回字段；None 使用默认股票字段集
        page_size : int
            每页条数

        Returns
        -------
        pd.DataFrame
        """
        fields = fields or [f.value for f in _STOCK_QUOTE_FIELDS]
        rows = []
        pn = 1
        while True:
            params = {
                "pn": pn,
                "pz": page_size,
                "fs": f"b:{bk_code}",
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
    # 板块资金流
    # ------------------------------------------------------------------

    def get_sector_fund_flow(
        self,
        sector_type: SectorType = SectorType.INDUSTRY,
        page_size: int = 200,
    ) -> pd.DataFrame:
        """
        获取板块资金流向（全量，自动翻页）

        Parameters
        ----------
        sector_type : SectorType
            行业 / 概念 / 地域
        page_size : int
            每页条数

        Returns
        -------
        pd.DataFrame
            包含主力净流入、超大单、大单、中单、小单流入流出
        """
        fields = [f.value for f in _FUND_FIELDS]
        rows = []
        pn = 1
        while True:
            params = {
                "pn": pn,
                "pz": page_size,
                "fs": sector_type.value,
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
