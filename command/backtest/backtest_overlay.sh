#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 回测：csi1000_balanced_overlay 策略（单次，默认参数）
#
# 用法：
#   bash command/backtest/backtest_overlay.sh
#   bash command/backtest/backtest_overlay.sh --start 2023-01-01 --end 2025-12-31
#   bash command/backtest/backtest_overlay.sh --market csi500   # 换股票池
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

./.venv/bin/python run_backtest.py \
    --config config/csi1000_balanced_overlay.yaml \
    --model-path models/lgbm_sector_csi1000_balanced_20260428_235851.pkl \
    "$@"
