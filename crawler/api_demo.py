"""
东方财富接口调用示例

运行：
    python crawler/api_demo.py
"""
from eastmoney import (
    SectorAPI, KlineAPI, StockAPI,
    SectorType, KlineInterval, AdjustType,
    SectorCode, SectorCodeRegistry, SectorStocksRegistry,
    to_secid,
)


def demo_sector_list():
    """1. 获取行业板块实时行情"""
    print("=" * 60)
    print("行业板块实时行情（Top 10 by 涨跌幅）")
    api = SectorAPI()
    df = api.get_sector_list(SectorType.INDUSTRY)
    print(df.sort_values("涨跌幅(%)", ascending=False).head(10).to_string(index=False))
    return df


def demo_concept_fund_flow():
    """2. 获取概念板块资金流向"""
    print("\n" + "=" * 60)
    print("概念板块实时行情（Top 10 by 主力净流入）")
    api = SectorAPI()
    fund_df = api.get_sector_fund_flow(SectorType.CONCEPT)
    print(fund_df.sort_values("主力净流入", ascending=False).head(10).to_string(index=False))
    return fund_df


def demo_sector_stocks():
    """3. 获取板块成分股（新能源车 BK0428）"""
    print("\n" + "=" * 60)
    print(f"新能源车（{SectorCode.NEW_ENERGY_VEHICLE}）成分股（Top 10 by 涨跌幅）")
    api = SectorAPI()
    df = api.get_sector_stocks(SectorCode.NEW_ENERGY_VEHICLE.value)
    print(df.sort_values("涨跌幅(%)", ascending=False).head(10).to_string(index=False))
    return df


def demo_sector_kline():
    """4. 获取板块日K线（人工智能 BK0475）"""
    print("\n" + "=" * 60)
    print(f"人工智能（{SectorCode.AI}）日K线（最近 10 条）")
    api = KlineAPI()
    df = api.get_kline(SectorCode.AI.value, interval=KlineInterval.DAY, limit=10)
    print(df.to_string(index=False))
    return df


def demo_stock_kline():
    """5. 获取个股日K线（贵州茅台 600519）"""
    print("\n" + "=" * 60)
    print("贵州茅台（600519）日K线（最近 10 条）")
    api = KlineAPI()
    df = api.get_kline("600519", interval=KlineInterval.DAY, adjust=AdjustType.FORWARD, limit=10)
    print(df.to_string(index=False))
    return df


def demo_stock_quote():
    """6. 获取个股实时行情（指定板块成分）"""
    print("\n" + "=" * 60)
    print(f"DeepSeek（{SectorCode.DEEPSEEK}）成分股行情（Top 10 by 涨跌幅）")
    api = StockAPI()
    df = api.get_stock_quote(bk_code=SectorCode.DEEPSEEK.value)
    print(df.sort_values("涨跌幅(%)", ascending=False).head(10).to_string(index=False))
    return df


def demo_stock_fund_flow():
    """7. 获取个股资金流向（指定板块成分）"""
    print("\n" + "=" * 60)
    print(f"人形机器人（{SectorCode.HUMANOID_ROBOT}）成分股资金流向（Top 10 by 主力净流入）")
    api = StockAPI()
    df = api.get_stock_fund_flow(bk_code=SectorCode.HUMANOID_ROBOT.value)
    print(df.sort_values("主力净流入", ascending=False).head(10).to_string(index=False))
    return df


def demo_registry_search():
    """8. 按名称搜索板块代码"""
    print("\n" + "=" * 60)
    print("搜索包含 '芯片' 的板块：")
    results = SectorCodeRegistry.find_by_name("芯片")
    for r in results:
        print(f"  [{r['type']:8s}] {r['code']}  {r['name']}")

    print("\n查找代码 BK0492 的信息：")
    info = SectorCodeRegistry.find_by_code("BK0492")
    print(f"  {info}")


def demo_sector_stocks_registry():
    """9. 板块成分股注册表（需先运行 scripts/fetch_sector_stocks.py）"""
    print("\n" + "=" * 60)
    print("板块成分股注册表示例（数据文件不存在时返回空）：")

    # 查询某板块的成分股
    stocks = SectorStocksRegistry.get_stocks("BK0428")
    print(f"  新能源车(BK0428) 成分股数量: {len(stocks)}")
    if stocks:
        print(f"  前5只: {[s['code'] + ' ' + s['name'] for s in stocks[:5]]}")

    # 查询某股票属于哪些板块
    sectors = SectorStocksRegistry.find_sectors_by_stock("600519")
    print(f"\n  贵州茅台(600519) 所属板块数量: {len(sectors)}")
    for s in sectors[:5]:
        print(f"    [{s['type']:8s}] {s['bk_code']}  {s['name']}")

    # 生成 stock->sector 映射（用于因子）
    mapping = SectorStocksRegistry.stock_sector_map("industry")
    print(f"\n  行业映射表股票数量: {len(mapping)}")


def demo_to_secid():
    """9. secid 工具函数演示"""
    print("\n" + "=" * 60)
    print("to_secid() 示例：")
    for code in ["BK0475", "600519", "000001", "300750", "688981"]:
        print(f"  {code:8s} -> {to_secid(code)}")


if __name__ == "__main__":
    # 离线演示（无需网络）
    demo_registry_search()
    demo_to_secid()
    demo_sector_stocks_registry()

    # 以下需要网络访问
    # demo_sector_list()
    # demo_concept_fund_flow()
    # demo_sector_stocks()
    # demo_sector_kline()
    # demo_stock_kline()
    # demo_stock_quote()
    # demo_stock_fund_flow()
