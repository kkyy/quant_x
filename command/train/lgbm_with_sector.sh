#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 训练：LightGBM + 行业因子（CSI1000 宇宙，balanced 研究线）
#
# 产物：models/lgbm_sector_csi1000_balanced_<timestamp>.pkl
#       + _meta.json / _feature_importance.json（自动忽略）
#
# 说明：
#   训练宇宙由 config/base.yaml market.name 控制，默认 csi300。
#   若需 csi1000 宇宙，在 base.yaml 中将 market.name 改为 csi1000
#   或传入自定义 config（--config your_override.yaml）后再运行本脚本。
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

TAG="sector_balanced_$(date +%Y%m%d)"

echo "[train] tag=$TAG"
./.venv/bin/python run_train.py \
    --model lgbm \
    --with-sector \
    --tag "$TAG" \
    "$@"
