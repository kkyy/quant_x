#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 数据拉取：全量（所有因子数据，约需 30-60 分钟）
#
# 拉取范围：financial / northbound / analyst / valuation / margin / pledge /
#            insider / institutional / repurchase / shareholder / dividend /
#            balance_sheet / earnings_guidance / visit
# 缓存位置：cache/<type>/
#
# 用法：
#   bash command/data/fetch_all.sh                  # 全量，尊重各类型 TTL
#   bash command/data/fetch_all.sh --force          # 强制忽略缓存，全量刷新
#   bash command/data/fetch_all.sh --universe csi300  # 仅 CSI300 成分股
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[data] 开始全量拉取... $(date)"
./.venv/bin/python run_fetch_data.py --type all "$@"
echo "[data] 完成 $(date)"
