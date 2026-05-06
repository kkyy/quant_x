#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Walk-forward 时间交叉验证（跨年度折叠，稳健性评估）
#
# 默认：train_universes=csi1000，eval=csi300，7个折叠（2020-2026）
# 结果：optimization_results/walk_forward_*.csv
#       含 Sharpe、MaxDD、IC、t 检验 p-value 等跨折叠统计
#
# 用法：
#   bash command/backtest/walk_forward.sh                         # 默认
#   bash command/backtest/walk_forward.sh --workers 4             # 并行折叠
#   bash command/backtest/walk_forward.sh \
#       --topk 5,10,15 --n-drop 1,3 --hold-thresh 5,8
#   bash command/backtest/walk_forward.sh \
#       --folds-config config/walk_forward_folds.yaml             # 自定义折叠
#
# 注意：完整扫描约需 30-60 分钟；建议先用 --topk 10 --n-drop 3 快速验证。
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

./.venv/bin/python run_walk_forward_validation.py \
    --train-universes csi1000 \
    --eval-market csi300 \
    --topk 5,10,15 \
    --n-drop 1,3,5 \
    --hold-thresh 5,8,10 \
    --workers 3 \
    "$@"
