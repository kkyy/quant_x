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
# 启动前会自动清理占用 Web 端口的旧进程。
#
# 仅构建前端（不启动）：
#   bash command/util/web.sh --build-only
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail
cd "$(dirname "$0")/../.."

BUILD_FRONTEND=1
DEV_MODE=0
BUILD_ONLY=0
BACKEND_PORT=8000
FRONTEND_PORT=5173

for arg in "$@"; do
  case "$arg" in
    --dev)          DEV_MODE=1; BUILD_FRONTEND=0 ;;
    --no-build)     BUILD_FRONTEND=0 ;;
    --build-only)   BUILD_ONLY=1 ;;
  esac
done

kill_port() {
  local port="$1"
  local pids

  if ! command -v lsof >/dev/null 2>&1; then
    echo "[web] 未找到 lsof，跳过端口 ${port} 清理"
    return 0
  fi

  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -z "$pids" ]; then
    return 0
  fi

  echo "[web] 端口 ${port} 被占用，清理旧进程: ${pids//$'\n'/ }"
  kill $pids 2>/dev/null || true

  for _ in {1..20}; do
    sleep 0.1
    pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    [ -z "$pids" ] && return 0
  done

  echo "[web] 旧进程未退出，强制清理端口 ${port}: ${pids//$'\n'/ }"
  kill -9 $pids 2>/dev/null || true
}

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

kill_port "$BACKEND_PORT"
if [ "$DEV_MODE" -eq 1 ]; then
  kill_port "$FRONTEND_PORT"
fi

if [ "$DEV_MODE" -eq 1 ]; then
  echo "[web] 开发模式启动"
  echo "  后端 → http://localhost:${BACKEND_PORT}"
  echo "  前端 → http://localhost:${FRONTEND_PORT}  (热更新，代理 /api → :${BACKEND_PORT})"
  echo ""
  # 启动后端（后台）
  ./.venv/bin/python web/run_web.py &
  BACKEND_PID=$!
  cleanup_backend() {
    kill "$BACKEND_PID" 2>/dev/null || true
    kill_port "$BACKEND_PORT"
  }
  trap cleanup_backend EXIT INT TERM
  # 启动前端 dev server（前台）
  cd web/frontend && npx vite --host --port "$FRONTEND_PORT" --strictPort
else
  echo "[web] 生产模式 → http://localhost:${BACKEND_PORT}"
  ./.venv/bin/python web/run_web.py "$@"
fi
