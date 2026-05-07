#!/usr/bin/env python3
"""
Run backtest for all 4 ablation models and print comparison.
Auto-discovers latest model files by tag prefix.
Run from project root: .venv/bin/python scripts/run_ablation_backtest.py
"""
import glob
import os
import subprocess
import sys

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = os.path.join(PROJ, ".venv", "bin", "python")
LOG_DIR = "/tmp/ablation_logs"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.join(PROJ, "backtest_results", "ablation"), exist_ok=True)


def _find_model(tag):
    pattern = os.path.join(PROJ, "models", f"lgbm_{tag}_*.pkl")
    matches = sorted(glob.glob(pattern))
    if not matches:
        print(f"WARNING: no model found for tag '{tag}' (pattern: {pattern})")
        return None
    return matches[-1]  # latest by timestamp


variants = [
    "ablation_control",
    "ablation_fundamental",
    "ablation_northbound",
    "ablation_fund_nb",
]

TOPK = "15"
N_DROP = "3"
HOLD_THRESH = "5"
START = "2024-01-01"
END = "2026-04-29"
MARKET = "csi1000"

for tag in variants:
    model_path = _find_model(tag)
    if model_path is None:
        print(f"Skipping {tag}: model not found")
        continue
    print(f"\n{'='*60}")
    print(f"Backtest: {tag}")
    print(f"Model: {os.path.basename(model_path)}")
    print(f"{'='*60}")
    out_csv = os.path.join(PROJ, "backtest_results", "ablation", f"{tag}.csv")
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
    with open(log_file) as f:
        lines = f.readlines()
    for line in lines[-30:]:
        print(line, end="")
    if ret.returncode != 0:
        print(f"ERROR: backtest {tag} failed with code {ret.returncode}")
        sys.exit(1)
    print(f"\n✅ {tag} backtest done -> {out_csv}")

print("\nAll ablation backtests complete!")
