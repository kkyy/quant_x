#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 回测：网格搜索（topk / n_drop / hold_thresh 参数扫描）
#
# 默认扫描 csi300 池，结果写入 backtest_results/grid_<date>.csv
#
# 用法：
#   bash command/backtest/backtest_grid.sh                        # 默认参数
#   bash command/backtest/backtest_grid.sh --seeds                # 多 seed 稳健性
#   bash command/backtest/backtest_grid.sh --markets csi300,csi500,csi1000
#   bash command/backtest/backtest_grid.sh \
#       --topk 5,10,15 --n-drop 1,3,5 --hold-thresh 5,8,10
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

./.venv/bin/python run_backtest.py \
    --config config/csi1000_balanced_overlay.yaml \
    --model-path models/lgbm_sector_csi1000_balanced_20260428_235851.pkl \
    --topk 5,10,15 \
    --n-drop 1,3,5 \
    --hold-thresh 5,8,10 \
    "$@"
