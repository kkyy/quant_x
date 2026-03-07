#!/usr/bin/env python3
"""
模型训练脚本。

用法:
    # 训练默认模型（model.yaml 中 model.type 指定，默认 lgbm）
    python run_train.py

    # 快速切换模型
    python run_train.py --model xgb
    python run_train.py --model ridge
    python run_train.py --model lasso

    # 加入板块因子
    python run_train.py --model lgbm --with-sector

    # 使用 qlib native 训练（MLflow 记录，仅支持 lgbm）
    python run_train.py --qlib-native

    # 列出所有已注册模型和因子
    python run_train.py --list-registry

    # 指定配置文件
    python run_train.py --model xgb --config my.yaml
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


def list_registry():
    from quant_ex.models.base import ModelRegistry
    from quant_ex.features.base import FactorRegistry

    print("\n=== Registered Models ===")
    for name in ModelRegistry.list():
        cls = ModelRegistry.get(name)
        print(f"  {name:<12} → {cls.__name__}")

    print("\n=== Registered Factors ===")
    for name in FactorRegistry.list():
        cls = FactorRegistry.get(name)
        print(f"  {name:<12} → {cls.__name__}")
    print()


def main(
    config_path: str = None,
    model_name: str = None,
    qlib_native: bool = False,
    with_sector: bool = False,
    tag: str = None,
):
    config = load_config(config_path)

    # Resolve model name: CLI > config file > default
    if model_name is None:
        model_name = config.get("model", {}).get("type", "lgbm")

    logger.info(f"=== 模型训练 | model={model_name}, tag={tag}, qlib_native={qlib_native}, sector={with_sector} ===")

    data_loader = DataLoader(config)
    sector_provider = SectorDataProvider(config) if with_sector else None

    price_data = None
    feat_cfg = config.get("model", {}).get("features", {})
    needs_price = (
        with_sector
        or feat_cfg.get("factors")
        or feat_cfg.get("use_sector_factors")
        or feat_cfg.get("use_mined_factors")
    )

    if needs_price and not qlib_native:
        logger.info("加载价格数据以计算额外因子 …")
        from datetime import datetime
        price_data = data_loader.load_price_data(
            instruments=config.get("market", {}).get("name", "csi300"),
            start_time=config.get("training", {}).get("fit_start", "2015-01-01"),
            end_time=datetime.now().strftime("%Y-%m-%d"),
        )

    # Build sector map if needed
    if with_sector and sector_provider is not None:
        logger.info("加载板块数据 …")
        sector_provider.load()

    trainer = ModelTrainer(config, data_loader, sector_provider)

    model, dataset, rid = trainer.train(
        model_name=model_name,
        qlib_native=qlib_native,
        price_data=price_data,
        use_sector_factors=with_sector,
        tag=tag,
    )

    if rid:
        print(f"\n✅ 训练完成！Recorder ID: {rid}")
        print(f"请将以下内容填入 config/base.yaml:")
        print(f"  experiment:")
        print(f'    latest_recorder_id: "{rid}"')
    else:
        print(f"\n✅ 模型 [{model_name}] 已保存到 models/ 目录")

    # Print feature importance summary
    try:
        imp = model.feature_importance(top_n=10)
        if not imp.empty:
            print("\nTop-10 Feature Importance:")
            print(imp.to_string(index=False))
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="模型训练")
    parser.add_argument("--config",       type=str,  default=None,
                        help="配置文件路径（默认自动合并 base.yaml + model.yaml）")
    parser.add_argument("--model",        type=str,  default=None,
                        help="模型名称: lgbm | xgb | ridge | lasso（覆盖 model.yaml 中的 type）")
    parser.add_argument("--qlib-native",  action="store_true",
                        help="使用 qlib 原生 LGBModel + MLflow 记录")
    parser.add_argument("--with-sector",  action="store_true",
                        help="加入板块轮动因子")
    parser.add_argument("--tag",          type=str,  default=None,
                        help="实验标签，用于命名模型文件，例如 baseline | sector_ablation")
    parser.add_argument("--list-registry", action="store_true",
                        help="列出所有注册的模型和因子，然后退出")

    # Legacy compat
    parser.add_argument("--no-qlib",      action="store_true",
                        help="[deprecated] 等同于不加 --qlib-native")

    args = parser.parse_args()

    if args.list_registry:
        list_registry()
        sys.exit(0)

    main(
        config_path=args.config,
        model_name=args.model,
        qlib_native=args.qlib_native,
        with_sector=args.with_sector,
        tag=args.tag,
    )
