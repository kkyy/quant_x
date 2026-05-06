#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 数据拉取：每日高频因子（估值 + 融资融券 + 北向资金）
#
# TTL：valuation=1d，margin=1d，northbound 独立更新
# 建议在收盘后（17:00 后）运行，确保当日数据可用。
#
# 用法：
#   bash command/data/fetch_daily.sh
#   bash command/data/fetch_daily.sh --force   # 强制刷新
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[data] 拉取每日高频因子数据... $(date)"
for TYPE in valuation margin northbound; do
    echo "  → $TYPE"
    ./.venv/bin/python run_fetch_data.py --type "$TYPE" "$@"
done
echo "[data] 完成 $(date)"
