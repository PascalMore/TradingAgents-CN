#!/bin/bash
# TradingAgents-CN 一键启动脚本
# 前端: Vue3 + Vite (localhost:3000)
# 后端: FastAPI (localhost:8000)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FRONTEND_PORT=3000
BACKEND_PORT=8000
RESTART_DELAY=3        # 进程挂掉后等待几秒再重启
HEALTH_CHECK_INTERVAL=15  # 监控循环间隔（秒）
BACKEND_STARTUP_TIMEOUT=30  # 后端启动最大等待秒数

# ==================== 停止旧进程 ====================
if [ -f ./stop_all.sh ]; then
    ./stop_all.sh 2>/dev/null
    sleep 2
fi

mkdir -p logs

# ==================== 启动后端 ====================
start_backend() {
    echo "📡 启动后端 API (port $BACKEND_PORT)..."
    source .venv/bin/activate
    nohup .venv/bin/python -m app.main > logs/backend.log 2>&1 &
    BACKEND_PID=$!
    echo "$BACKEND_PID" > logs/backend.pid
    echo "✅ 后端已启动 (PID: $BACKEND_PID)"

    # 等待后端端口就绪
    local waited=0
    while [ $waited -lt $BACKEND_STARTUP_TIMEOUT ]; do
        if ss -tlnp 2>/dev/null | grep -q ":$BACKEND_PORT "; then
            echo "✅ 后端端口 $BACKEND_PORT 已就绪（等待 ${waited}s）"
            return 0
        fi
        if ! ps -p $BACKEND_PID > /dev/null 2>&1; then
            echo "❌ 后端进程启动后立即退出，请检查 logs/backend.log"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "⚠️ 后端启动超时（${BACKEND_STARTUP_TIMEOUT}s），继续执行..."
    return 0
}

# ==================== 启动前端 ====================
start_frontend() {
    echo "🖥️ 启动前端 Vue (port $FRONTEND_PORT)..."
    cd "$SCRIPT_DIR/frontend"

    # 直接用 npx vite 启动，避免 npm run dev 的父进程退出导致 vite 孤儿进程问题
    if ! command -v npx &> /dev/null; then
        echo "❌ 未找到 npx，无法启动前端"
        FRONTEND_PID=0
        cd "$SCRIPT_DIR"
        return 1
    fi

    nohup npx vite --host 0.0.0.0 > "$SCRIPT_DIR/logs/frontend.log" 2>&1 &
    FRONTEND_PID=$!
    echo "✅ 前端已启动 (vite, PID: $FRONTEND_PID)"

    cd "$SCRIPT_DIR"
    echo "$FRONTEND_PID" > logs/frontend.pid

    # 等待前端端口就绪
    local waited=0
    while [ $waited -lt 20 ]; do
        if ss -tlnp 2>/dev/null | grep -q ":$FRONTEND_PORT "; then
            echo "✅ 前端端口 $FRONTEND_PORT 已就绪（等待 ${waited}s）"
            return 0
        fi
        if ! ps -p $FRONTEND_PID > /dev/null 2>&1; then
            echo "❌ 前端进程启动后立即退出，请检查 logs/frontend.log"
            FRONTEND_PID=0
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "⚠️ 前端启动超时（20s），继续执行..."
    return 0
}

# ==================== 首次启动 ====================
echo "🚀 启动 TradingAgents-CN..."

start_backend || exit 1
start_frontend

echo ""
echo "========================================"
echo "🎉 TradingAgents-CN 启动完成!"
echo "========================================"
echo "后端 API: http://localhost:$BACKEND_PORT"
echo "API 文档: http://localhost:$BACKEND_PORT/docs"
echo "前端 Vue: http://localhost:$FRONTEND_PORT"
echo ""
echo "停止服务: ./stop_all.sh"
echo "========================================"

# ==================== 监控循环（自动重启）====================
while true; do
    # 检查后端
    if ! ps -p $BACKEND_PID > /dev/null 2>&1; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') ⚠️ 后端进程 ($BACKEND_PID) 已退出，${RESTART_DELAY}s 后重启..."
        sleep $RESTART_DELAY
        start_backend || {
            echo "❌ 后端重启失败，停止整个服务"
            ./stop_all.sh
            exit 1
        }
    fi

    # 检查前端（只在曾经启动成功的情况下检查）
    if [ "$FRONTEND_PID" -ne 0 ] && ! ps -p $FRONTEND_PID > /dev/null 2>&1; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') ⚠️ 前端进程 ($FRONTEND_PID) 已退出，${RESTART_DELAY}s 后重启..."
        sleep $RESTART_DELAY
        start_frontend
    fi

    sleep $HEALTH_CHECK_INTERVAL
done
