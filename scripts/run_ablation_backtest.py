#!/usr/bin/env python3
"""
Run backtest for all 4 ablation models and print comparison.
Run from project root: .venv/bin/python scripts/run_ablation_backtest.py
"""
import subprocess
import sys
import os
import json

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = os.path.join(PROJ, ".venv", "bin", "python")
LOG_DIR = "/tmp/ablation_logs"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.join(PROJ, "backtest_results", "ablation"), exist_ok=True)

variants = [
    ("ablation_control",     "models/lgbm_ablation_control_20260501_212042.pkl"),
    ("ablation_fundamental", "models/lgbm_ablation_fundamental_20260501_212256.pkl"),
    ("ablation_northbound",  "models/lgbm_ablation_northbound_20260501_212451.pkl"),
    ("ablation_fund_nb",     "models/lgbm_ablation_fund_nb_20260501_212651.pkl"),
]

TOPK = "15"
N_DROP = "3"
HOLD_THRESH = "5"
START = "2024-01-01"
END = "2026-04-29"
MARKET = "csi1000"

for tag, model_rel in variants:
    print(f"\n{'='*60}")
    print(f"Backtest: {tag}")
    print(f"{'='*60}")
    out_csv = os.path.join(PROJ, "backtest_results", "ablation", f"{tag}.csv")
    model_path = os.path.join(PROJ, model_rel)
    cmd = [
        PY, os.path.join(PROJ, "run_backtest.py"),
        "--model-path", model_path,
        "--topk", TOPK,
        "--n-drop", N_DROP,
        "--hold-thresh", HOLD_THRESH,
        "--start", START,
        "--end", END,
        "--market", MARKET,
        "--output-csv", out_csv,
    ]
    log_file = os.path.join(LOG_DIR, f"bt_{tag}.log")
    with open(log_file, "w") as f:
        ret = subprocess.run(cmd, cwd=PROJ, stdout=f, stderr=subprocess.STDOUT)
    # Show last 30 lines of backtest log
    with open(log_file) as f:
        lines = f.readlines()
    for line in lines[-30:]:
        print(line, end="")
    if ret.returncode != 0:
        print(f"ERROR: backtest {tag} failed with code {ret.returncode}")
        sys.exit(1)
    print(f"\n✅ {tag} backtest done -> {out_csv}")

print("\nAll ablation backtests complete!")
