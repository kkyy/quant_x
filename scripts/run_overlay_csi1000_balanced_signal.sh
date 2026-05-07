#!/usr/bin/env bash
# =============================================================================
# csi1000_balanced_overlay 模型 - csi300 股票池  每日收盘后调仓信号生成脚本
#
# 首次建仓参数（2026-04-29 信号 / 2026-04-30 执行）:
#   策略: csi300 / topk=5 / n_drop=5 / hold=5
#   模型: lgbm_sector_csi1000_balanced_20260428_235851.pkl（CSI1000 训练）
#   配置: config/csi1000_balanced_overlay.yaml
#   账户: 150,000 元
#   SVS过滤: stock_vs_sector window=20 keep_top_pct=0.4
#
# 使用说明:
#   1. 收盘后用实际持仓更新下方 POSITIONS 变量
#      格式: INSTRUMENT:手数:建仓日期（日期可选，用于显示持股天数和逐股 hold 保护）
#   2. 运行脚本，结果推送 Bark 并写入 signals/daily_rebalance_cache/
#
#   无持仓（首次建仓）时将 POSITIONS 置空即可。
#
#   收盘后日常运行:
#       bash scripts/run_overlay_csi1000_balanced_signal.sh
#
#   测试/调试 (跳过 qlib 数据更新 + 不发送通知):
#       bash scripts/run_overlay_csi1000_balanced_signal.sh --dry-run --skip-update
#
#   复现历史信号:
#       bash scripts/run_overlay_csi1000_balanced_signal.sh --today 2026-04-29 --skip-update --dry-run
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PYTHON="$PROJECT_ROOT/.venv/bin/python"
MODEL="models/lgbm_sector_csi1000_balanced_20260428_235851.pkl"
CONFIG="config/csi1000_balanced_overlay.yaml"

# ── 修改这里 ─────────────────────────────────────────────────────────────────
# 当前持仓（含建仓日期，用于计算持股天数和逐股 hold 保护）
POSITIONS="SH600489:900:2026-04-29,SH600900:900:2026-04-29,SH601021:500:2026-04-29,SH603259:100:2026-04-29,SH603993:1300:2026-04-29"
# POSITIONS=""   # 首次建仓时注释掉上面一行，取消本行注释
# ──────────────────────────────────────────────────────────────────────────────

cd "$PROJECT_ROOT"

if [[ -n "$POSITIONS" ]]; then
    exec "$PYTHON" run_scheduled_rebalance.py \
        --config "$CONFIG" \
        --model-path "$MODEL" \
        --positions "$POSITIONS" \
        --min-action-value 1000 \
        "$@"
else
    exec "$PYTHON" run_scheduled_rebalance.py \
        --config "$CONFIG" \
        --model-path "$MODEL" \
        --min-action-value 1000 \
        "$@"
fi
