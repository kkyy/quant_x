#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 训练：qlib 原生 LGBModel（MLflow 追踪模式）
#
# 产物：mlruns/ 下 Recorder，训练完成后把 Recorder ID 写入
#       config/base.yaml → experiment.latest_recorder_id
#
# 用法：
#   bash command/train/lgbm_qlib_native.sh
#   # 训练完复制输出的 Recorder ID，填入 config/base.yaml
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[train] qlib-native LGBModel with MLflow tracking"
./.venv/bin/python run_train.py \
    --qlib-native \
    "$@"
