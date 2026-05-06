#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 数据拉取：低频基本面（财务 + 分析师 + 股东 + 机构持仓，约 7-30d TTL）
#
# 建议每周或季报季后运行一次。
#
# 用法：
#   bash command/data/fetch_fundamental.sh
#   bash command/data/fetch_fundamental.sh --force   # 强制刷新全部
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[data] 拉取基本面低频数据... $(date)"
for TYPE in financial analyst shareholder institutional balance_sheet dividend earnings_guidance; do
    echo "  → $TYPE"
    ./.venv/bin/python run_fetch_data.py --type "$TYPE" "$@"
done
echo "[data] 完成 $(date)"
