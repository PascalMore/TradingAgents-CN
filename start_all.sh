#!/bin/bash
# TradingAgents-CN 一键启动脚本
# 前端: Vue3 + Vite (localhost:5173)
# 后端: FastAPI (localhost:8000)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 启动 TradingAgents-CN..."

# ==================== 停止旧进程 ====================
if [ -f ./stop_all.sh ]; then
    ./stop_all.sh 2>/dev/null
    sleep 2
fi

# ==================== 后端 ====================
echo "📡 启动后端 API (port 8000)..."
source .venv/bin/activate
nohup .venv/bin/python -m app.main > logs/backend.log 2>&1 &
BACKEND_PID=$!
echo "✅ 后端已启动 (PID: $BACKEND_PID)"

# 等待后端就绪
sleep 5

# ==================== 前端 (Vue3 + Vite) ====================
echo "🖥️ 启动前端 Vue (port 5173)..."
cd "$SCRIPT_DIR/frontend"

# 检查是否有 yarn/npm
if command -v yarn &> /dev/null; then
    nohup yarn dev --host 0.0.0.0 > "$SCRIPT_DIR/logs/frontend.log" 2>&1 &
    FRONTEND_PID=$!
    echo "✅ 前端已启动 (yarn, PID: $FRONTEND_PID)"
elif command -v npm &> /dev/null; then
    nohup npm run dev -- --host 0.0.0.0 > "$SCRIPT_DIR/logs/frontend.log" 2>&1 &
    FRONTEND_PID=$!
    echo "✅ 前端已启动 (npm, PID: $FRONTEND_PID)"
else
    echo "❌ 未找到 yarn 或 npm，无法启动前端"
    FRONTEND_PID=0
fi

cd "$SCRIPT_DIR"

# 保存 PID
echo "$BACKEND_PID" > logs/backend.pid
[ "$FRONTEND_PID" -ne 0 ] && echo "$FRONTEND_PID" > logs/frontend.pid

echo ""
echo "========================================"
echo "🎉 TradingAgents-CN 启动完成!"
echo "========================================"
echo "后端 API: http://localhost:8000"
echo "API 文档: http://localhost:8000/docs"
echo "前端 Vue: http://localhost:5173"
echo ""
echo "停止服务: ./stop_all.sh"
echo "========================================"

# 监控子进程
while true; do
    if ! ps -p $BACKEND_PID > /dev/null 2>&1; then
        echo "⚠️ 后端已退出，停止服务..."
        ./stop_all.sh
        exit 1
    fi
    if [ "$FRONTEND_PID" -ne 0 ] && ! ps -p $FRONTEND_PID > /dev/null 2>&1; then
        echo "⚠️ 前端已退出..."
        FRONTEND_PID=0
    fi
    sleep 30
done