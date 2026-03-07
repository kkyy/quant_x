"""
拉取所有板块的成分股列表并写入 data/sector_stocks.json

每次请求使用独立的 curl 进程（新 TCP 连接），规避不稳定代理的连接复用问题。

用法：
    python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890
    python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --resume
    python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --type industry
"""
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
import urllib.parse

_ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT))

_CODES_FILE  = pathlib.Path(__file__).parent.parent / "data" / "sector_codes.json"
_STOCKS_FILE = pathlib.Path(__file__).parent.parent / "data" / "sector_stocks.json"
_PAGE_SIZE   = 200
_SLEEP_SEC   = 0.4
_RETRY_MAX   = 3


def curl_get(url: str, params: dict, proxy: str | None, timeout: int = 12) -> dict:
    """
    用 subprocess curl 发送一次 GET 请求，返回解析后的 JSON。
    每次调用创建全新 TCP 连接，proxy 断连不会污染后续请求。
    """
    full_url = url + "?" + urllib.parse.urlencode(params)
    cmd = [
        "curl", "-s",
        "--max-time", str(timeout),
        "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "-H", "Referer: https://quote.eastmoney.com/",
        "-H", "Accept-Language: zh-CN,zh;q=0.9",
    ]
    if proxy:
        cmd += ["--proxy", proxy]
    else:
        cmd.append("--noproxy")
        cmd.append("*")
    cmd.append(full_url)

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"curl exit={result.returncode}  stderr={result.stderr.strip()}")
    return json.loads(result.stdout)


def fetch_stocks(bk_code: str, proxy: str | None) -> list[dict]:
    """拉取单个板块的全量成分股（代码+名称），失败时重试"""
    items: list[dict] = []
    pn = 1
    base_url = "https://push2.eastmoney.com/api/qt/clist/get"

    while True:
        params = {
            "pn": pn,
            "pz": _PAGE_SIZE,
            "fs": f"b:{bk_code}",
            "fields": "f12,f14",
            "fid": "f3",
            "po": 1,
        }
        resp = None
        for attempt in range(1, _RETRY_MAX + 1):
            try:
                resp = curl_get(base_url, params, proxy)
                break
            except Exception as exc:
                if attempt == _RETRY_MAX:
                    raise RuntimeError(f"p{pn} fail after {_RETRY_MAX} retries: {exc}") from exc
                time.sleep(1.5 * attempt)

        diff: dict = resp.get("data", {}).get("diff", {})
        if not diff:
            break
        batch = [{"code": v["f12"], "name": v["f14"]} for v in diff.values()]
        items.extend(batch)

        total = resp.get("data", {}).get("total", 0)
        if len(items) >= total or len(batch) < _PAGE_SIZE:
            break
        pn += 1
        time.sleep(_SLEEP_SEC)

    return sorted(items, key=lambda x: x["code"])


def main() -> None:
    parser = argparse.ArgumentParser(description="拉取板块成分股（curl 版）")
    parser.add_argument("--proxy", metavar="URL", default=None,
                        help="代理地址，如 http://127.0.0.1:7890")
    parser.add_argument("--type", choices=["industry", "concept", "region", "all"],
                        default="all", help="板块类型（默认 all）")
    parser.add_argument("--resume", action="store_true",
                        help="跳过已有数据，断点续传")
    args = parser.parse_args()

    with open(_CODES_FILE, encoding="utf-8") as f:
        codes_db: dict = json.load(f)

    if _STOCKS_FILE.exists():
        with open(_STOCKS_FILE, encoding="utf-8") as f:
            stocks_db: dict = json.load(f)
    else:
        stocks_db = {}

    sector_types = (
        ["industry", "concept", "region"] if args.type == "all" else [args.type]
    )

    total_ok = 0
    total_fail = 0

    for stype in sector_types:
        sectors: list[dict] = codes_db.get(stype, [])
        print(f"\n>>> {stype.upper()}  共 {len(sectors)} 个板块")
        if stype not in stocks_db:
            stocks_db[stype] = {}

        for i, sector in enumerate(sectors, 1):
            bk_code = sector["code"]
            name    = sector["name"]

            if args.resume and bk_code in stocks_db[stype]:
                cnt = len(stocks_db[stype][bk_code]["stocks"])
                print(f"  [{i:4d}/{len(sectors)}] {bk_code} {name:<14s} skip({cnt})")
                total_ok += 1
                continue

            try:
                stocks = fetch_stocks(bk_code, args.proxy)
                stocks_db[stype][bk_code] = {"name": name, "stocks": stocks}
                print(f"  [{i:4d}/{len(sectors)}] {bk_code} {name:<14s} {len(stocks):4d} 只")
                total_ok += 1
            except Exception as exc:
                print(f"  [{i:4d}/{len(sectors)}] {bk_code} {name:<14s} [FAIL] {exc}")
                total_fail += 1
                time.sleep(3.0)
                continue

            time.sleep(_SLEEP_SEC)

            # 每 50 个板块保存一次，减少意外中断的损失
            if total_ok % 50 == 0:
                _save(stocks_db)

        # 每类拉完都保存一次，防止中途崩溃丢数据
        _save(stocks_db)
        print(f"  已保存 {stype} ({len(stocks_db[stype])} 个板块)")

    print(f"\n写入 {_STOCKS_FILE}")
    for stype in sector_types:
        print(f"  {stype}: {len(stocks_db.get(stype, {}))} 个板块")
    print(f"成功={total_ok}  失败={total_fail}")


def _save(data: dict) -> None:
    _STOCKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STOCKS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(_STOCKS_FILE)


if __name__ == "__main__":
    main()
