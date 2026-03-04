#!/usr/bin/env python3
"""
模型训练脚本。

用法:
    # 训练标准 qlib LightGBM（Alpha158，使用 MLflow 记录）
    python run_train.py

    # 训练带板块因子的自定义模型（保存为 .pkl）
    python run_train.py --with-sector --no-qlib

    # 指定配置文件
    python run_train.py --config my.yaml
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.utils.logger import setup_logger
from quant_ex.utils.config import load_config
from quant_ex.data.loader import DataLoader
from quant_ex.data.sector import SectorDataProvider
from quant_ex.models.trainer import ModelTrainer

logger = setup_logger("run_train")


def main(
    config_path: str = None,
    qlib_native: bool = True,
    with_sector: bool = False,
):
    config = load_config(config_path)
    logger.info(f"=== 模型训练 (qlib_native={qlib_native}, sector={with_sector}) ===")

    data_loader = DataLoader(config)
    sector_provider = SectorDataProvider(config) if with_sector else None

    price_data = None
    if with_sector and not qlib_native:
        logger.info("加载价格数据以计算板块因子 …")
        from datetime import datetime
        price_data = data_loader.load_price_data(
            instruments=config.get("market", {}).get("name", "csi300"),
            start_time=config.get("training", {}).get("fit_start", "2015-01-01"),
            end_time=datetime.now().strftime("%Y-%m-%d"),
        )

    trainer = ModelTrainer(config, data_loader, sector_provider)
    model, dataset, rid = trainer.train(
        qlib_native=qlib_native,
        price_data=price_data,
        use_sector_factors=with_sector,
    )

    if rid:
        print(f"\n✅ 训练完成！Recorder ID: {rid}")
        print(f"请将以下内容填入 config/base.yaml:")
        print(f"  experiment:")
        print(f"    latest_recorder_id: \"{rid}\"")
    else:
        print("\n✅ 模型已保存到 models/ 目录")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="模型训练")
    parser.add_argument("--config",      type=str,  default=None)
    parser.add_argument("--no-qlib",     action="store_true", help="使用自定义 LGBMAlphaModel")
    parser.add_argument("--with-sector", action="store_true", help="加入板块轮动因子")
    args = parser.parse_args()
    main(
        config_path=args.config,
        qlib_native=not args.no_qlib,
        with_sector=args.with_sector,
    )
