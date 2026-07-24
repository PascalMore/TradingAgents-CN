#!/bin/bash
# TradingAgents-CN 一键停止脚本 (YQuant 增强版: 单 PID 杀, 不误杀 process group + 明确退出码)
#
# 退出码:
#   0 = 全部进程已停止 (或本来就没在跑)
#   1 = 残留端口清理失败

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_PREFIX="[stop_all $(date '+%Y-%m-%d %H:%M:%S')]"
log()  { echo "$LOG_PREFIX $*"; }
fail() { echo "$LOG_PREFIX ❌ $*" >&2; exit "$1"; }

log "🛑 停止 TradingAgents-CN..."

# 通过 PID 文件停止（优先，单 PID 杀, 不带 process group）
for pidfile in logs/backend.pid logs/frontend.pid; do
    if [ -f "$pidfile" ]; then
        PID=$(cat "$pidfile" || true)
        if [ -n "${PID:-}" ] && ps -p "$PID" > /dev/null 2>&1; then
            # 关键修复: start_all.sh 下已改为单 PID spawn, stop 也应单 PID 杀, 不用 kill -- -$PID
            kill "$PID" 2>/dev/null || true
            sleep 1
            # 兜底: 还在就 KILL
            if ps -p "$PID" > /dev/null 2>&1; then
                kill -KILL "$PID" 2>/dev/null || true
            fi
            log "✅ 已停止 (PID: $PID)"
        fi
        rm -f "$pidfile"
    fi
done

# 按端口清理残留进程（精确匹配，不用 pkill -f）
PORT_CLEAN_FAIL=0
for port in 8000 3000 3001 5173; do
    pid=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "${pid:-}" ]; then
        if kill "$pid" 2>/dev/null; then
            log "🧹 清理端口 $port 残留进程 (PID: $pid)"
        else
            log "❌ 清理端口 $port 残留 (PID: $pid) 失败"
            PORT_CLEAN_FAIL=1
        fi
    fi
done

[ "$PORT_CLEAN_FAIL" = 1 ] && fail 1 || log "🎉 停止完成"
