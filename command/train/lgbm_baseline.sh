#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 训练：LightGBM 纯 Alpha158（无自定义因子，快速 baseline）
#
# 产物：models/lgbm_baseline_<timestamp>.pkl
# 用途：作为消融实验对照组；验证自定义因子的增量贡献。
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

TAG="baseline_$(date +%Y%m%d)"

echo "[train] tag=$TAG  (Alpha158 only, no custom factors)"
./.venv/bin/python run_train.py \
    --model lgbm \
    --no-extra-factors \
    --tag "$TAG" \
    "$@"
