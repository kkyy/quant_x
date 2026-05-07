#!/usr/bin/env bash
# Factor ablation training script
# Runs 4 training variants sequentially and logs results

set -e
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY=$PROJ/.venv/bin/python
LOG_DIR=/tmp/ablation_logs
mkdir -p "$LOG_DIR"

echo "========================================"
echo "Factor Ablation Training"
echo "========================================"

echo "[1/4] Control: Alpha158 + technical only"
$PY $PROJ/run_train.py --model lgbm --tag ablation_control 2>&1 | tee "$LOG_DIR/control.log"
echo "Control done."

echo "[2/4] A: Alpha158 + technical + fundamental"
$PY $PROJ/run_train.py --model lgbm --tag ablation_fundamental --config $PROJ/config/ablation_fundamental.yaml 2>&1 | tee "$LOG_DIR/fundamental.log"
echo "Fundamental done."

echo "[3/4] B: Alpha158 + technical + northbound"
$PY $PROJ/run_train.py --model lgbm --tag ablation_northbound --config $PROJ/config/ablation_northbound.yaml 2>&1 | tee "$LOG_DIR/northbound.log"
echo "Northbound done."

echo "[4/4] C: Alpha158 + technical + fundamental + northbound"
$PY $PROJ/run_train.py --model lgbm --tag ablation_fund_nb --config $PROJ/config/ablation_fundamental_northbound.yaml 2>&1 | tee "$LOG_DIR/fund_nb.log"
echo "Fund+NB done."

echo "========================================"
echo "All training complete. Models saved to $PROJ/models/"
ls -lt $PROJ/models/lgbm_ablation_*.pkl 2>/dev/null | head -10
echo "========================================"
