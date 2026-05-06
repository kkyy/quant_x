#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Web Dashboard 启动脚本
#
# 生产模式（默认）：
#   bash command/util/web.sh
#   → 自动构建前端 → 启动 API + 静态前端，端口 8000
#
# 开发模式：
#   bash command/util/web.sh --dev
#   → 启动后端 :8000 + 前端 Vite dev server :5173（热更新）
#
# 仅构建前端（不启动）：
#   bash command/util/web.sh --build-only
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

BUILD_FRONTEND=1
DEV_MODE=0
BUILD_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --dev)          DEV_MODE=1; BUILD_FRONTEND=0 ;;
    --no-build)     BUILD_FRONTEND=0 ;;
    --build-only)   BUILD_ONLY=1 ;;
  esac
done

if [ "$BUILD_ONLY" -eq 1 ]; then
  echo "[web] 构建前端..."
  cd web/frontend && npm run build && cd ../..
  echo "[web] 构建完成 → web/frontend/dist/"
  exit 0
fi

if [ "$BUILD_FRONTEND" -eq 1 ]; then
  echo "[web] 构建前端..."
  cd web/frontend && npm run build && cd ../..
  echo "[web] 构建完成"
fi

if [ "$DEV_MODE" -eq 1 ]; then
  echo "[web] 开发模式启动"
  echo "  后端 → http://localhost:8000"
  echo "  前端 → http://localhost:5173  (热更新，代理 /api → :8000)"
  echo ""
  # 启动后端（后台）
  ./.venv/bin/python web/run_web.py &
  BACKEND_PID=$!
  # 启动前端 dev server（前台）
  cd web/frontend && npx vite --host
  # 前端退出后清理后端
  kill "$BACKEND_PID" 2>/dev/null || true
else
  echo "[web] 生产模式 → http://localhost:8000"
  ./.venv/bin/python web/run_web.py "$@"
fi
