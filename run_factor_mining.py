#!/usr/bin/env python3
"""
因子自动挖掘脚本。

遍历预定义的表达式模板，用 rank-IC / ICIR 评估每个因子，
保存通过阈值的因子到 cache/mined_factors.json。

用法:
    python run_factor_mining.py
    python run_factor_mining.py --min-ic 0.03 --min-icir 0.4 --top-n 30
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.utils.logger import setup_logger
from quant_ex.utils.config import load_config
from quant_ex.data.loader import DataLoader
from quant_ex.features.factor_mining import FactorMiner

logger = setup_logger("factor_mining")


def main(min_ic: float = 0.02, min_icir: float = 0.3, top_n: int = 20):
    config = load_config()
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info("=== 因子自动挖掘 ===")

    data_loader = DataLoader(config)
    tcfg = config.get("training", {})

    dataset = data_loader.build_dataset(
        segments={
            "train": (tcfg.get("fit_start", "2015-01-01"), tcfg.get("fit_end", "2021-12-31")),
            "valid": (tcfg.get("valid_start", "2022-01-01"), tcfg.get("valid_end", "2023-12-31")),
            "test":  (tcfg.get("test_start", "2024-01-01"), today),
        },
        instruments=config.get("market", {}).get("name", "csi300"),
    )

    df = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    label = df.xs("label", axis=1, level=0).squeeze()
    X_tr = df.xs("feature", axis=1, level=0)

    price_data = X_tr

    miner = FactorMiner(
        price_data=price_data,
        label=label,
        top_n=top_n,
        min_ic=min_ic,
        min_icir=min_icir,
    )

    def progress(count, total, name, ic, icir):
        if count % 10 == 0:
            logger.info(f"进度: {count}/{total}  最近: {name}  IC={ic:.4f} ICIR={icir:.4f}")

    results = miner.mine(progress_cb=progress)
    miner.save_factors(results)

    print(f"\n=== 挖掘完成：共找到 {len(results)} 个有效因子 ===")
    for r in results[:15]:
        print(f"  {r.name:30s}  IC={r.ic:+.4f}  ICIR={r.icir:+.4f}")
        print(f"    {r.expression}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="自动因子挖掘")
    parser.add_argument("--min-ic",   type=float, default=0.02)
    parser.add_argument("--min-icir", type=float, default=0.3)
    parser.add_argument("--top-n",    type=int,   default=20)
    args = parser.parse_args()
    main(args.min_ic, args.min_icir, args.top_n)
