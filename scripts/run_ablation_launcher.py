#!/usr/bin/env python3
"""
Quick launcher for factor ablation training.
Run from project root: .venv/bin/python scripts/run_ablation_launcher.py
"""
import subprocess
import sys
import os

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = os.path.join(PROJ, ".venv", "bin", "python")
LOG_DIR = "/tmp/ablation_logs"
os.makedirs(LOG_DIR, exist_ok=True)

variants = [
    ("ablation_control", None),
    ("ablation_fundamental", os.path.join(PROJ, "config", "ablation_fundamental.yaml")),
    ("ablation_northbound", os.path.join(PROJ, "config", "ablation_northbound.yaml")),
    ("ablation_fund_nb", os.path.join(PROJ, "config", "ablation_fundamental_northbound.yaml")),
]

for i, (tag, config_path) in enumerate(variants, 1):
    print(f"\n{'='*60}")
    print(f"[{i}/{len(variants)}] Training: {tag}")
    print(f"{'='*60}")
    cmd = [PY, os.path.join(PROJ, "run_train.py"), "--model", "lgbm", "--tag", tag]
    if config_path:
        cmd += ["--config", config_path]
    log_file = os.path.join(LOG_DIR, f"{tag}.log")
    with open(log_file, "w") as f:
        ret = subprocess.run(cmd, cwd=PROJ, stdout=f, stderr=subprocess.STDOUT)
    # Show last 15 lines of log
    with open(log_file) as f:
        lines = f.readlines()
    for line in lines[-15:]:
        print(line, end="")
    if ret.returncode != 0:
        print(f"ERROR: {tag} failed with code {ret.returncode}")
        sys.exit(1)
    print(f"\n✅ {tag} done")

print("\nAll ablation training complete!")
