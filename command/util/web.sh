#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# 启动 Web Dashboard（生产模式：API + 静态前端同一进程，端口 8000）
#
# 访问：http://localhost:8000
#
# 开发模式（两个终端）：
#   Terminal 1: bash command/util/web.sh
#   Terminal 2: cd web/frontend && npm run dev   # Vite :5173，代理 /api → :8000
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[web] 启动 Dashboard → http://localhost:8000"
./.venv/bin/python web/run_web.py "$@"
