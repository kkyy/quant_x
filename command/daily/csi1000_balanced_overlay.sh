#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 每日调仓信号（csi1000_balanced_overlay 策略，CSI300 池，账户 15万）
#
# 用法：
#   1. 收盘后用实际持仓更新下方 POSITIONS 变量（格式: INSTRUMENT:手数，逗号分隔）
#   2. 运行脚本，结果推送 Bark 并写入 signals/daily_rebalance_cache/
#
# 无持仓（首次建仓）时将 POSITIONS 置空即可。
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

# ── 修改这里 ─────────────────────────────────────────────────────────────────
POSITIONS="SH600489:900,SH600900:900,SH601021:500,SH603259:100,SH603993:1300"
# POSITIONS=""   # 首次建仓时注释掉上面一行，取消本行注释
# ─────────────────────────────────────────────────────────────────────────────

PYTHON="./.venv/bin/python"
CONFIG="config/csi1000_balanced_overlay.yaml"
MODEL="models/lgbm_sector_csi1000_balanced_20260428_235851.pkl"

if [[ -n "$POSITIONS" ]]; then
    "$PYTHON" run_scheduled_rebalance.py \
        --config "$CONFIG" \
        --model-path "$MODEL" \
        --positions "$POSITIONS" \
        --min-action-value 1000 \
        "$@"
else
    "$PYTHON" run_scheduled_rebalance.py \
        --config "$CONFIG" \
        --model-path "$MODEL" \
        --min-action-value 1000 \
        "$@"
fi
