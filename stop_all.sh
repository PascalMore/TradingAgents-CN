#!/bin/bash
# TradingAgents-CN 停止脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🛑 停止 TradingAgents-CN..."

# 通过 PID 文件停止（优先）
for pidfile in logs/backend.pid logs/frontend.pid; do
    if [ -f "$pidfile" ]; then
        PID=$(cat "$pidfile")
        if [ -n "$PID" ] && ps -p $PID > /dev/null 2>&1; then
            # 杀掉该 PID 的整个进程组
            kill -- -$PID 2>/dev/null || kill $PID 2>/dev/null
            echo "✅ 已停止 (PID: $PID)"
        fi
        rm -f "$pidfile"
    fi
done

# 按端口清理残留进程（精确匹配，不用 pkill -f）
for port in 8000 3000 3001 5173; do
    pid=$(lsof -ti :$port 2>/dev/null)
    if [ -n "$pid" ]; then
        kill $pid 2>/dev/null && echo "🧹 清理端口 $port 残留进程 (PID: $pid)"
    fi
done

echo "🎉 停止完成"
