#!/usr/bin/env python3
"""
外部数据拉取脚本 — 拉取各类外部数据并缓存到本地。

用法:
    python run_fetch_data.py --type financial                    # 拉取所有 A 股财务数据
    python run_fetch_data.py --type financial --symbols SH600519,SZ000001  # 指定股票
    python run_fetch_data.py --type northbound                   # 拉取北向资金持仓数据
    python run_fetch_data.py --type financial --universe csi300  # 只拉沪深300成分股
    python run_fetch_data.py --type financial --force            # 强制刷新（忽略缓存TTL）
    python run_fetch_data.py --type pledge                       # 拉取股权质押数据
    python run_fetch_data.py --type margin                       # 拉取融资融券数据
    python run_fetch_data.py --type all                          # 拉取全部
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.utils.logger import setup_logger

logger = setup_logger("run_fetch_data")

# ── 通用 fetcher 配置 ─────────────────────────────────────────────────────────
_FETCHER_REGISTRY = {
    "financial":          ("FinancialFetcher",          "./cache/financial",          7),
    "northbound":         ("NorthboundFetcher",         "./cache/northbound",         1),
    "pledge":             ("PledgeFetcher",             "./cache/pledge",             1),
    "margin":             ("MarginTradeFetcher",        "./cache/margin",             1),
    "insider":            ("InsiderTradeFetcher",       "./cache/insider",            1),
    "analyst":            ("AnalystForecastFetcher",    "./cache/analyst",            3),
    "shareholder":        ("ShareholderCountFetcher",   "./cache/shareholder",        30),
    "dividend":           ("DividendFetcher",           "./cache/dividend",           30),
    "valuation":          ("ValuationFetcher",          "./cache/valuation",          1),
    "balance_sheet":      ("BalanceSheetFetcher",       "./cache/balance_sheet",      30),
    "earnings_guidance":  ("EarningsGuidanceFetcher",   "./cache/earnings_guidance",  30),
    "institutional":      ("InstitutionalHoldFetcher",  "./cache/institutional",      30),
    "repurchase":         ("RepurchaseFetcher",         "./cache/repurchase",         1),
    "visit":              ("InstitutionalVisitFetcher", "./cache/visit",              7),
    "sw1_industry":       ("SW1IndustryFetcher",        "./cache",                    30),
}


def _get_all_ashare_symbols() -> list[str]:
    """获取全部 A 股代码（qlib 格式 SH600519 / SZ000001 / BJ920000）。"""
    import akshare as ak

    stocks = ak.stock_info_a_code_name()
    symbols = []
    for code in stocks["code"]:
        if code.startswith("920"):
            symbols.append(f"BJ{code}")
        elif code.startswith(("6", "9")):
            symbols.append(f"SH{code}")
        elif code.startswith(("4", "8")):
            symbols.append(f"BJ{code}")
        else:
            symbols.append(f"SZ{code}")
    return symbols


def _get_universe_symbols(universe: str) -> list[str]:
    """获取指定指数成分股（qlib 格式）。"""
    import akshare as ak

    index_map = {
        "csi300": "000300",
        "csi500": "000905",
        "csi800": "000906",
        "csi1000": "000852",
        "sse50": "000016",
    }
    idx_code = index_map.get(universe.lower())
    if idx_code is None:
        logger.error(f"未知指数: {universe}，可选: {list(index_map.keys())}")
        sys.exit(1)

    df = ak.index_stock_cons_csindex(symbol=idx_code)
    codes = df["成分券代码"].tolist()
    symbols = []
    for code in codes:
        code = str(code).zfill(6)
        if code.startswith("920"):
            symbols.append(f"BJ{code}")
        elif code.startswith(("6", "9")):
            symbols.append(f"SH{code}")
        elif code.startswith(("4", "8")):
            symbols.append(f"BJ{code}")
        else:
            symbols.append(f"SZ{code}")
    return symbols


def _count_cached(cache_dir: Path, suffix: str = ".csv") -> int:
    if not cache_dir.exists():
        return 0
    return len([f for f in cache_dir.iterdir() if f.suffix == suffix and not f.name.endswith("_cf.csv")])


def _get_fetcher_cls(name: str):
    """Dynamically import and return the fetcher class."""
    from quant_ex.data import fetchers as fetcher_mod
    cls_name = _FETCHER_REGISTRY[name][0]
    return getattr(fetcher_mod, cls_name)


def fetch_generic(name: str, symbols: list[str], cache_dir: str, ttl_days: int, batch_size: int = 100):
    """Generic fetch function for any registered fetcher type."""
    fetcher_cls = _get_fetcher_cls(name)
    fetcher = fetcher_cls(cache_dir=cache_dir, cache_ttl_days=ttl_days)
    before = _count_cached(fetcher.cache_dir)
    start = time.time()

    try:
        fetcher.refresh_cache(symbols)
    except TypeError:
        # Some fetchers don't accept symbols (bulk APIs)
        fetcher.refresh_cache([])

    after = _count_cached(fetcher.cache_dir)
    total_size = sum(f.stat().st_size for f in fetcher.cache_dir.iterdir() if f.suffix == ".csv") if fetcher.cache_dir.exists() else 0
    logger.info("完成 [%s]: 新增 %d, 总缓存 %d 文件, %.1f MB, 耗时 %.0fs",
                name, after - before, after, total_size / 1024 / 1024, time.time() - start)


def main():
    all_types = list(_FETCHER_REGISTRY.keys())
    parser = argparse.ArgumentParser(description="外部数据拉取")
    parser.add_argument(
        "--type",
        choices=all_types + ["all"],
        default="financial",
        help=f"拉取类型: {', '.join(all_types)}, all=全部",
    )
    parser.add_argument("--symbols", type=str, default=None, help="指定股票，逗号分隔: SH600519,SZ000001")
    parser.add_argument("--universe", type=str, default=None, help="指数成分股: csi300, csi500, csi800, csi1000, sse50")
    parser.add_argument("--cache-dir", type=str, default=None, help="缓存目录（覆盖默认值）")
    parser.add_argument("--ttl", type=int, default=None, help="缓存TTL天数，0=强制刷新（默认: 每种类型不同）")
    parser.add_argument("--force", action="store_true", help="强制刷新，等同 --ttl 0")
    parser.add_argument("--batch-size", type=int, default=100, help="每批拉取股票数（默认: 100）")
    args = parser.parse_args()

    # 确定股票列表
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
        logger.info("指定股票: %d 只", len(symbols))
    elif args.universe:
        symbols = _get_universe_symbols(args.universe)
        logger.info("指数 %s 成分股: %d 只", args.universe, len(symbols))
    else:
        symbols = _get_all_ashare_symbols()
        logger.info("全部 A 股: %d 只", len(symbols))

    # 确定要拉取的类型
    if args.type == "all":
        types_to_fetch = all_types
    else:
        types_to_fetch = [args.type]

    for name in types_to_fetch:
        _, default_cache, default_ttl = _FETCHER_REGISTRY[name]
        cache_dir = args.cache_dir or default_cache
        ttl = 0 if args.force else (args.ttl if args.ttl is not None else default_ttl)
        logger.info("=== 拉取 %s 数据 (TTL=%d 天, 缓存=%s) ===", name, ttl, cache_dir)
        try:
            fetch_generic(name, symbols, cache_dir, ttl, args.batch_size)
        except Exception as exc:
            logger.error("拉取 %s 失败: %s", name, exc)


if __name__ == "__main__":
    main()
