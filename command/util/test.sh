#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 运行测试套件
#
# 用法：
#   bash command/util/test.sh                       # 全量测试
#   bash command/util/test.sh test/test_trainer.py  # 单文件
#   bash command/util/test.sh -k "universe"         # 按名称过滤
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

./.venv/bin/python -m pytest test/ -v "$@"
