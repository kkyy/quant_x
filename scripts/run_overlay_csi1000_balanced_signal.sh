#!/usr/bin/env bash
# =============================================================================
# csi1000_balanced_overlay 模型 - csi300 股票池  每日收盘后调仓信号生成脚本
#
# 首次建仓参数（2026-04-29 信号 / 2026-04-30 执行）:
#   策略: csi300 / topk=5 / n_drop=5 / hold=5
#   模型: lgbm_sector_csi1000_balanced_20260428_235851.pkl（CSI1000 训练）
#   配置: config/csi1000_balanced_overlay.yaml
#   账户: 150,000 元
#   回测起点: previous_trade_date（自动取信号日前一个交易日，一天热身）
#   SVS过滤: stock_vs_sector window=20 keep_top_pct=0.4
#   信号来源确认: signals/daily_rebalance_cache/rebalance_2026-04-29.json
#
# 首次建仓持仓（2026-04-30 执行价格为 2026-04-29 未复权收盘价）:
#   SH600489 中金黄金  900股 @ 26.07  ≈23,463元
#   SH600900 长江电力  900股 @ 26.77  ≈24,093元
#   SH601021 春秋航空  500股 @ 47.20  ≈23,600元
#   SH603259 药明康德  100股 @ 111.18 ≈11,118元
#   SH603993 洛阳钼业 1300股 @ 19.19  ≈24,947元
#   合计持仓市值: ≈107,221 元
#
# 使用说明:
#   收盘后日常运行 (发送 Bark 通知, 先更新 qlib 数据):
#       bash scripts/run_overlay_csi1000_balanced_signal.sh
#
#   测试/调试 (跳过 qlib 数据更新 + 不发送通知):
#       bash scripts/run_overlay_csi1000_balanced_signal.sh --dry-run --skip-update
#
#   复现历史信号 (传入 --today 日期, 不发送):
#       bash scripts/run_overlay_csi1000_balanced_signal.sh --today 2026-04-29 --skip-update --dry-run
#
# 注意: 由于 Alpha158 特征归一化参数随 qlib 数据更新而变化，
#       历史日期的复现结果可能与原始信号存在轻微差异。
#       原始信号已永久保存于:
#           signals/daily_rebalance_cache/rebalance_2026-04-29.json
#           signals/daily_rebalance_cache/rebalance_2026-04-29_original.json
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PYTHON="$PROJECT_ROOT/.venv/bin/python"
MODEL="models/lgbm_sector_csi1000_balanced_20260428_235851.pkl"
CONFIG="config/csi1000_balanced_overlay.yaml"

cd "$PROJECT_ROOT"

exec "$PYTHON" run_scheduled_rebalance.py \
    --config "$CONFIG" \
    --model-path "$MODEL" \
    "$@"
