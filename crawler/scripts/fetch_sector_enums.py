"""
刷新板块代码枚举

从东方财富实时接口拉取全量行业 / 概念 / 地域板块代码，
写入 crawler/data/sector_codes.json。

用法：
    python crawler/scripts/fetch_sector_enums.py
    python crawler/scripts/fetch_sector_enums.py --proxy http://127.0.0.1:7890
    python crawler/scripts/fetch_sector_enums.py --no-proxy   # 禁用系统代理
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

# 兼容直接运行或 package 导入
_ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT))

from crawler.eastmoney.client import EastMoneyClient  # noqa: E402
from crawler.eastmoney.enums import SectorType         # noqa: E402

_OUT_FILE = pathlib.Path(__file__).parent.parent / "data" / "sector_codes.json"
_PAGE_SIZE = 100
_SLEEP_SEC = 0.5   # 翻页间隔，避免触发限流


def fetch_all(client: EastMoneyClient, sector_type: SectorType) -> list[dict]:
    """拉取某类板块的全量代码列表"""
    items: list[dict] = []
    pn = 1
    while True:
        params = {
            "pn": pn,
            "pz": _PAGE_SIZE,
            "fs": sector_type.value,
            "fields": "f12,f14",
            "fid": "f12",
            "po": 1,
        }
        try:
            data = client.clist_get(params)
        except Exception as exc:
            print(f"  [warn] 请求失败 (type={sector_type.name}, pn={pn}): {exc}")
            break

        diff: dict = data.get("data", {}).get("diff", {})
        if not diff:
            break

        batch = [{"code": d["f12"], "name": d["f14"]} for d in diff.values()]
        items.extend(batch)

        total = data.get("data", {}).get("total", 0)
        print(f"  {sector_type.name:10s} p{pn}: +{len(batch):3d} => {len(items):4d}/{total}")

        if len(items) >= total or len(batch) < _PAGE_SIZE:
            break

        pn += 1
        time.sleep(_SLEEP_SEC)

    return sorted(items, key=lambda x: x["code"])


def main() -> None:
    parser = argparse.ArgumentParser(description="刷新东方财富板块代码枚举")
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument("--proxy", metavar="URL", help="指定代理，如 http://127.0.0.1:7890")
    proxy_group.add_argument("--no-proxy", action="store_true", help="禁用所有代理（含系统代理）")
    args = parser.parse_args()

    if args.no_proxy:
        proxies: dict | None = {"http": None, "https": None}
    elif args.proxy:
        proxies = {"http": args.proxy, "https": args.proxy}
    else:
        proxies = None  # 跟随系统代理

    client = EastMoneyClient(proxies=proxies, max_retries=3, retry_backoff=1.0)

    result: dict = {"_meta": {}}
    for sector_type, key in [
        (SectorType.INDUSTRY, "industry"),
        (SectorType.CONCEPT,  "concept"),
        (SectorType.REGION,   "region"),
    ]:
        print(f"\n>>> 拉取 {sector_type.name} ...")
        items = fetch_all(client, sector_type)
        result[key] = items
        result["_meta"][key] = len(items)

    result["_meta"]["source"] = "https://push2.eastmoney.com/api/qt/clist/get"
    result["_meta"]["note"] = "Run this script to refresh"

    _OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n已写入 {_OUT_FILE}")
    for k, v in result["_meta"].items():
        if k not in ("source", "note"):
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
