#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 更新 qlib 行情数据（完整流水线：Dolt → SQL → CSV → normalize → dump_bin）
#
# 约需 15-40 分钟（含 dolt pull）。
#
# 用法：
#   bash command/data/update_qlib.sh                          # 完整流水线
#   bash command/data/update_qlib.sh --skip-dolt-pull         # 跳过 dolt 同步
#   bash command/data/update_qlib.sh --supplement-source akshare  # akshare 补齐
#   bash command/data/update_qlib.sh --reuse-dolt-server      # 复用已运行的 dolt
#
# Dolt Lock 问题：
#   如果提示 LOCK 文件冲突，先运行: pkill -f 'dolt sql-server'
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[qlib] 开始更新 qlib 行情数据... $(date)"
./.venv/bin/python run_update_qlib_data.py "$@"
echo "[qlib] 完成 $(date)"
