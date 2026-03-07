"""
东方财富接口枚举定义

板块代码可通过 scripts/fetch_sector_enums.py 刷新至 data/sector_codes.json
"""
from enum import Enum


# ---------------------------------------------------------------------------
# 板块类型（fs 参数）
# ---------------------------------------------------------------------------

class SectorType(str, Enum):
    """板块类型，对应 clist 接口 fs 参数"""
    INDUSTRY = "m:90+t:2"   # 行业板块（~497个）
    CONCEPT  = "m:90+t:3"   # 概念板块（~468个）
    REGION   = "m:90+t:1"   # 地域板块（31个）


# ---------------------------------------------------------------------------
# K 线周期（klt 参数）
# ---------------------------------------------------------------------------

class KlineInterval(int, Enum):
    """K线周期，对应 kline 接口 klt 参数"""
    MIN1   = 1
    MIN5   = 5
    MIN15  = 15
    MIN30  = 30
    MIN60  = 60
    DAY    = 101
    WEEK   = 102
    MONTH  = 103


# ---------------------------------------------------------------------------
# 复权类型（fqt 参数）
# ---------------------------------------------------------------------------

class AdjustType(int, Enum):
    """复权方式，对应 kline 接口 fqt 参数"""
    NONE     = 0   # 不复权
    FORWARD  = 1   # 前复权
    BACKWARD = 2   # 后复权


# ---------------------------------------------------------------------------
# 市场类型（secid 前缀）
# ---------------------------------------------------------------------------

class MarketType(int, Enum):
    """市场类型，对应 secid 前缀"""
    SECTOR   = 90   # 板块（BK 代码）
    SHANGHAI = 1    # 沪市 A 股（6 开头）
    SHENZHEN = 0    # 深市 / 创业板（0 / 3 开头）


def to_secid(code: str) -> str:
    """
    将股票或板块代码转换为 secid 格式

    Parameters
    ----------
    code : str
        板块代码（如 BK0428）或 A 股代码（如 600519 / 000001）

    Returns
    -------
    str
        secid 字符串，如 "90.BK0428"、"1.600519"、"0.000001"
    """
    if code.startswith("BK"):
        return f"90.{code}"
    if code.startswith("6"):
        return f"1.{code}"   # 沪市
    return f"0.{code}"       # 深市 / 创业板 / 科创板（688 不在板块行情里）


# ---------------------------------------------------------------------------
# 行情字段（fields 参数）
# ---------------------------------------------------------------------------

class QuoteField(str, Enum):
    """行情字段，f 系列编号"""
    # 标识
    CODE        = "f12"   # 代码（股票代码 / 板块代码）
    NAME        = "f14"   # 名称
    # 行情
    PRICE       = "f2"    # 最新价
    PCT_CHANGE  = "f3"    # 涨跌幅（%）
    CHANGE      = "f4"    # 涨跌额
    VOLUME      = "f5"    # 成交量（手）
    AMOUNT      = "f6"    # 成交额（元）
    AMPLITUDE   = "f7"    # 振幅（%）
    TURNOVER    = "f8"    # 换手率（%）
    PE          = "f9"    # 市盈率
    VOL_RATIO   = "f10"   # 量比
    HIGH        = "f15"   # 最高价
    LOW         = "f16"   # 最低价
    OPEN        = "f17"   # 开盘价
    PREV_CLOSE  = "f18"   # 昨收价
    # 市值
    MARKET_CAP       = "f20"  # 总市值
    CIRC_MARKET_CAP  = "f21"  # 流通市值


# ---------------------------------------------------------------------------
# 资金流字段
# ---------------------------------------------------------------------------

class FundField(str, Enum):
    """资金流向字段"""
    MAIN_NET_INFLOW       = "f62"   # 主力净流入
    SUPER_LARGE_INFLOW    = "f66"   # 超大单流入
    SUPER_LARGE_OUTFLOW   = "f69"   # 超大单流出
    LARGE_INFLOW          = "f72"   # 大单流入
    LARGE_OUTFLOW         = "f75"   # 大单流出
    MID_INFLOW            = "f78"   # 中单流入
    MID_OUTFLOW           = "f84"   # 中单流出
    SMALL_INFLOW          = "f81"   # 小单流入
    SMALL_OUTFLOW         = "f87"   # 小单流出


# ---------------------------------------------------------------------------
# 板块代码枚举（常用 / 热门）
# 完整列表见 data/sector_codes.json，可通过 SectorCodeRegistry 加载
# ---------------------------------------------------------------------------

class SectorCode(str, Enum):
    """常用板块代码（BK 编号）"""
    # ---- 热门行业 ----
    AVIATION            = "BK0420"
    RAILWAY_HIGHWAY     = "BK0421"
    LOGISTICS           = "BK0422"
    CEMENT              = "BK0424"
    PUBLIC_UTILITY      = "BK0427"
    NEW_ENERGY_VEHICLE  = "BK0428"
    AUTO_WHOLE          = "BK0430"
    AUTO_PARTS          = "BK0431"
    HOME_APPLIANCE      = "BK0432"
    CONSUMER_ELECTRONICS= "BK0433"
    ELECTRONIC_COMPONENT= "BK0434"
    SEMICONDUCTOR       = "BK0435"
    POWER_EQUIPMENT     = "BK0436"
    PHOTOVOLTAIC        = "BK0437"
    WIND_POWER          = "BK0438"
    ENERGY_STORAGE      = "BK0439"
    BATTERY             = "BK0440"
    TELECOM_EQUIPMENT   = "BK0441"
    TELECOM_SERVICE     = "BK0442"
    COMPUTER_HARDWARE   = "BK0443"
    SOFTWARE            = "BK0444"
    IT_SERVICE          = "BK0445"
    INTERNET            = "BK0446"
    MEDIA               = "BK0448"
    GAME                = "BK0449"
    MEDICAL_DEVICE      = "BK0450"
    MEDICAL_SERVICE     = "BK0451"
    PHARMA_CHEM         = "BK0452"
    BIOTECH             = "BK0453"
    TRADITIONAL_MEDICINE= "BK0454"
    FOOD_PROCESS        = "BK0456"
    BEVERAGE            = "BK0457"
    AGRICULTURE         = "BK0458"
    CHEMICAL            = "BK0459"
    STEEL               = "BK0463"
    NON_FERROUS_METAL   = "BK0464"
    RARE_METAL          = "BK0465"
    MINING              = "BK0466"
    COAL                = "BK0467"
    OIL_GAS             = "BK0468"
    ELECTRICITY         = "BK0469"
    TEXTILE_APPAREL     = "BK0471"
    REAL_ESTATE         = "BK0474"
    AI                  = "BK0475"
    LIQUOR              = "BK0477"
    BANK                = "BK0478"
    BROKERAGE           = "BK0479"
    INSURANCE           = "BK0480"
    DEFENSE             = "BK0482"
    AEROSPACE           = "BK0483"
    SHIPBUILDING        = "BK0484"
    COMPUTING_POWER     = "BK0492"
    CLOUD_COMPUTING     = "BK0493"
    BIG_DATA            = "BK0494"
    CYBERSECURITY       = "BK0495"
    # ---- 热门概念 ----
    HYDROGEN_ENERGY     = "BK0537"
    CARBON_NEUTRAL      = "BK0538"
    DIGITAL_ECONOMY     = "BK0539"
    METAVERSE           = "BK0540"
    CHATGPT             = "BK0541"
    LARGE_MODEL         = "BK0542"
    ROBOT               = "BK0543"
    LOW_ALTITUDE        = "BK0544"
    SATELLITE_INTERNET  = "BK0545"
    SOLID_STATE_BATTERY = "BK0546"
    QUANTUM_COMPUTING   = "BK0548"
    HUMANOID_ROBOT      = "BK0550"
    AUTONOMOUS_DRIVING  = "BK0551"
    AIGC                = "BK0552"
    OPTICAL_MODULE      = "BK0559"
    CPO                 = "BK0560"
    LIQUID_COOLING      = "BK0561"
    PCB                 = "BK0562"
    HBM                 = "BK0563"
    CHIP_DOMESTIC       = "BK0565"
    NUCLEAR_POWER       = "BK0569"
    SODIUM_BATTERY      = "BK0571"
    LITHIUM_MINE        = "BK0572"
    RARE_EARTH_MAGNET   = "BK0573"
    DEEPSEEK            = "BK0581"


# ---------------------------------------------------------------------------
# 运行时板块代码注册表（从 JSON 文件加载完整列表）
# ---------------------------------------------------------------------------

import json
import pathlib

_DATA_FILE = pathlib.Path(__file__).parent.parent / "data" / "sector_codes.json"


class SectorCodeRegistry:
    """从 data/sector_codes.json 加载完整板块代码，支持按名称/代码查找"""

    _cache: dict | None = None

    @classmethod
    def _load(cls) -> dict:
        if cls._cache is None:
            with open(_DATA_FILE, encoding="utf-8") as f:
                cls._cache = json.load(f)
        return cls._cache

    @classmethod
    def get_all(cls, sector_type: str) -> list[dict]:
        """返回指定板块类型的完整列表，每项 {'code': ..., 'name': ...}"""
        return cls._load().get(sector_type, [])

    @classmethod
    def find_by_name(cls, name: str, sector_type: str | None = None) -> list[dict]:
        """按名称关键词搜索板块"""
        data = cls._load()
        types = [sector_type] if sector_type else ["industry", "concept", "region"]
        result = []
        for t in types:
            for item in data.get(t, []):
                if name in item["name"]:
                    result.append({**item, "type": t})
        return result

    @classmethod
    def find_by_code(cls, code: str) -> dict | None:
        """按板块代码查找，返回 {'code', 'name', 'type'} 或 None"""
        data = cls._load()
        for t, items in data.items():
            if t.startswith("_"):
                continue
            for item in items:
                if item["code"] == code:
                    return {**item, "type": t}
        return None


# ---------------------------------------------------------------------------
# 板块成分股注册表（从 data/sector_stocks.json 加载）
# ---------------------------------------------------------------------------

_STOCKS_FILE = pathlib.Path(__file__).parent.parent / "data" / "sector_stocks.json"


class SectorStocksRegistry:
    """
    从 data/sector_stocks.json 加载板块成分股，支持多维度查询。

    数据文件由 scripts/fetch_sector_stocks.py 生成，文件不存在时所有方法
    返回空结果（不抛异常），方便在数据未就绪时仍可导入模块。
    """

    _cache: dict | None = None

    @classmethod
    def _load(cls) -> dict:
        if cls._cache is None:
            if not _STOCKS_FILE.exists():
                cls._cache = {}
            else:
                with open(_STOCKS_FILE, encoding="utf-8") as f:
                    cls._cache = json.load(f)
        return cls._cache

    @classmethod
    def reload(cls) -> None:
        """强制重新加载文件（数据更新后调用）"""
        cls._cache = None

    @classmethod
    def get_stocks(cls, bk_code: str, sector_type: str | None = None) -> list[dict]:
        """
        获取指定板块的成分股列表

        Parameters
        ----------
        bk_code : str
            板块代码，如 "BK0428"
        sector_type : str | None
            "industry" / "concept" / "region"；None 时自动搜索全部类型

        Returns
        -------
        list[dict]
            每项 {'code': '000001', 'name': '平安银行'}
        """
        data = cls._load()
        types = [sector_type] if sector_type else ["industry", "concept", "region"]
        for t in types:
            entry = data.get(t, {}).get(bk_code)
            if entry:
                return entry["stocks"]
        return []

    @classmethod
    def find_sectors_by_stock(cls, stock_code: str) -> list[dict]:
        """
        查询某只股票所属的全部板块

        Parameters
        ----------
        stock_code : str
            A 股代码，如 "600519"

        Returns
        -------
        list[dict]
            每项 {'bk_code': ..., 'name': ..., 'type': ...}
        """
        data = cls._load()
        result = []
        for t, sectors in data.items():
            if t.startswith("_"):
                continue
            for bk_code, entry in sectors.items():
                if any(s["code"] == stock_code for s in entry.get("stocks", [])):
                    result.append({"bk_code": bk_code, "name": entry["name"], "type": t})
        return result

    @classmethod
    def stock_sector_map(cls, sector_type: str = "industry") -> dict[str, list[str]]:
        """
        生成股票 → [板块代码] 的映射（用于因子构建等场景）

        Parameters
        ----------
        sector_type : str
            "industry" / "concept" / "region"

        Returns
        -------
        dict[str, list[str]]
            {'000001': ['BK0473', 'BK0478'], ...}
        """
        data = cls._load()
        mapping: dict[str, list[str]] = {}
        for bk_code, entry in data.get(sector_type, {}).items():
            for s in entry.get("stocks", []):
                mapping.setdefault(s["code"], []).append(bk_code)
        return mapping

    @classmethod
    def sector_stock_map(cls, sector_type: str = "industry") -> dict[str, list[str]]:
        """
        生成板块代码 → [股票代码] 的映射

        Returns
        -------
        dict[str, list[str]]
            {'BK0428': ['000625', '002594', ...], ...}
        """
        data = cls._load()
        return {
            bk_code: [s["code"] for s in entry.get("stocks", [])]
            for bk_code, entry in data.get(sector_type, {}).items()
        }
