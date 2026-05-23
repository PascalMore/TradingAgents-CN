#!/bin/bash
# TradingAgents-CN 停止脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🛑 停止 TradingAgents-CN..."

# 读取 PID 并停止
if [ -f logs/backend.pid ]; then
    BACKEND_PID=$(cat logs/backend.pid)
    if ps -p $BACKEND_PID > /dev/null 2>&1; then
        kill $BACKEND_PID
        echo "✅ 后端已停止 (PID: $BACKEND_PID)"
    fi
    rm logs/backend.pid
fi

if [ -f logs/frontend.pid ]; then
    FRONTEND_PID=$(cat logs/frontend.pid)
    if ps -p $FRONTEND_PID > /dev/null 2>&1; then
        kill $FRONTEND_PID
        echo "✅ 前端已停止 (PID: $FRONTEND_PID)"
    fi
    rm logs/frontend.pid
fi

# 清理残留进程
pkill -f "python.*app.main" 2>/dev/null && echo "🧹 清理后端残留进程"
pkill -f "vite" 2>/dev/null && echo "🧹 清理 Vite 前端残留进程"

# 按端口清理前端（备用方案）
for port in 3000 3001 3002 3003 3004 5173; do
    pid=$(lsof -ti :$port 2>/dev/null)
    if [ -n "$pid" ]; then
        kill $pid 2>/dev/null && echo "🧹 清理端口 $port 残留进程 (PID: $pid)"
    fi
done

echo "🎉 停止完成"
